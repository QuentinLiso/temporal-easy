# 04 · Raising application errors

> **Level:** daily use · **Read time:** ~15 min
> **Sources:** [Encyclopedia — Application failures](https://docs.temporal.io/encyclopedia/application-failures) · [Encyclopedia — Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies) (Non-Retryable Errors, Per-error next Retry delay) · [Go SDK — Error handling](https://docs.temporal.io/develop/go/best-practices/error-handling) · [`temporal` package](https://pkg.go.dev/go.temporal.io/sdk/temporal)

---

## 1. The problem

Your payment gateway answers a charge in five different ways: approved, declined, insufficient funds,
invalid card, or "slow down, retry in 30 seconds". If your Activity turns all the failures into
`errors.New(...)`, Temporal sees **one** kind of failure: a retryable error with no Type. The result:

- a declined card is retried until the Retry Policy gives up, with the customer waiting;
- the Workflow can't tell "declined" from "gateway down", so it can't react differently;
- the gateway's "retry in 30 seconds" is ignored, and the backoff hammers it every second.

An **ApplicationError** fixes all three. It is the only Temporal failure type your code creates
directly ([chapter 02](02-go-error-types.md)), and it carries exactly what the Workflow and the Retry Policy
need: a **Type**, a **non-retryable** flag, **Details**, a **next retry delay** and a **Category**.

---

## 2. The four constructors

All live in `go.temporal.io/sdk/temporal` and return an `error` whose concrete type is
`*temporal.ApplicationError`.

| Constructor | Retryable? | Use it for |
| --- | --- | --- |
| `NewApplicationError(msg, errType, details...)` | yes | A typed, transient failure |
| `NewApplicationErrorWithCause(msg, errType, cause, details...)` | yes | Same, keeping the original error in the chain |
| `NewNonRetryableApplicationError(msg, errType, cause, details...)` | **no** | A permanent failure: retrying the same input can't help |
| `NewApplicationErrorWithOptions(msg, errType, ApplicationErrorOptions{…})` | your choice | Everything else: `NonRetryable`, `Cause`, `Details`, `NextRetryDelay`, `Category` |

`ApplicationErrorOptions` in SDK v1.50.0:

| Field | Effect |
| --- | --- |
| `NonRetryable bool` | Stops retries **whatever the Retry Policy says** |
| `Cause error` | Kept as the error's cause (`Unwrap()`), visible in the UI |
| `Details []any` | Payloads the caller decodes with `Details(&v)` |
| `NextRetryDelay time.Duration` | Overrides the Retry Policy's interval for the **next** attempt only (Server ≥ 1.24.2) |
| `Category ApplicationErrorCategory` | `ApplicationErrorCategoryBenign` = expected failure, quieter logs and metrics |

And what the receiver reads: `Type()`, `Message()`, `NonRetryable()`, `HasDetails()`, `Details(&v)`,
`NextRetryDelay()`, `Category()`, `Unwrap()`.

> **What a plain Go error becomes.** An `errors.New("boom")` returned from an Activity arrives as an
> ApplicationError with an **empty** Type, and `fmt.Errorf("…: %w", err)` arrives with Type
> `"wrapError"` (both checked on a dev server). The Encyclopedia says the error's "type name" is
> preserved. In Go, that's the reflected type name, which is meaningless for the standard library's
> errors. That's the main reason to always set a Type yourself.

---

## 3. Types are part of your API: make them constants

The Workflow decides what to do by comparing `appErr.Type()` with a string. A typo on either side and
the branch silently never runs. Put the Types in the Activity's package, next to the Details types,
and use the constants on both sides:

```go
// file: payments/errors.go
package payments

// Error types raised by the payments Activities. Workflows switch on these
// strings, so they are exported constants, never retyped literals.
const (
	ErrTypeCardDeclined       = "CardDeclined"
	ErrTypeInsufficientFunds  = "InsufficientFunds"
	ErrTypeInvalidCard        = "InvalidCard"
	ErrTypeGatewayRateLimited = "GatewayRateLimited"
)

// DeclineDetails travels with CardDeclined and InsufficientFunds errors.
type DeclineDetails struct {
	DeclineCode  string
	MissingCents int64
}
```

These strings are a **contract**: a Workflow still running last month's code will receive them, and so
will a Workflow in another language. Rename them like you would rename a public API field.

---

## 4. Example: mapping a gateway's answers

```go
// file: payments/gateway.go
package payments

import (
	"context"
	"time"
)

type ChargeRequest struct {
	OrderID     string
	CardToken   string
	AmountCents int64
}

// ChargeResponse is what our payment service provider returns.
type ChargeResponse struct {
	Status          string // "approved", "declined", "insufficient_funds", "invalid_card", "rate_limited"
	AuthorizationID string
	DeclineCode     string
	MissingCents    int64
	RetryAfter      time.Duration
}

// Gateway is the payment service provider's API (an HTTP client in production).
type Gateway interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResponse, error)
}
```

```go
// file: payments/activities.go
package payments

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/temporal"
)

type PaymentActivities struct {
	Gateway Gateway
}

// ChargeCard maps every gateway outcome to the error the Workflow should see.
func (a *PaymentActivities) ChargeCard(ctx context.Context, req ChargeRequest) (string, error) {
	resp, err := a.Gateway.Charge(ctx, req)
	if err != nil {
		// Network error, 5xx, timeout: transient. A plain error is retryable.
		return "", fmt.Errorf("call payment gateway: %w", err)
	}

	switch resp.Status {
	case "approved":
		return resp.AuthorizationID, nil

	case "declined":
		// Retrying the same card won't help: non-retryable, with details.
		return "", temporal.NewNonRetryableApplicationError(
			"card declined by issuer", ErrTypeCardDeclined, nil,
			DeclineDetails{DeclineCode: resp.DeclineCode})

	case "insufficient_funds":
		return "", temporal.NewNonRetryableApplicationError(
			"insufficient funds", ErrTypeInsufficientFunds, nil,
			DeclineDetails{DeclineCode: resp.DeclineCode, MissingCents: resp.MissingCents})

	case "invalid_card":
		return "", temporal.NewNonRetryableApplicationError(
			"card token is invalid or expired", ErrTypeInvalidCard, nil)

	case "rate_limited":
		// Retryable, but wait as long as the gateway asked, not the policy's backoff.
		return "", temporal.NewApplicationErrorWithOptions(
			"payment gateway rate limit", ErrTypeGatewayRateLimited,
			temporal.ApplicationErrorOptions{NextRetryDelay: resp.RetryAfter})

	default:
		// Unknown status: retryable by default, typed so it shows up clearly.
		return "", temporal.NewApplicationError(
			fmt.Sprintf("unexpected gateway status %q", resp.Status), "UnexpectedGatewayStatus")
	}
}
```

What each branch gives the Workflow:

| Gateway answer | Error | Retried? | Workflow sees |
| --- | --- | --- | --- |
| network error / 5xx | `fmt.Errorf` | yes, per Retry Policy | Type `"wrapError"`, after the last attempt |
| declined | `NewNonRetryableApplicationError` + Details | **no** | Type `CardDeclined`, decline code |
| insufficient funds | `NewNonRetryableApplicationError` + Details | **no** | Type `InsufficientFunds`, missing amount |
| invalid card | `NewNonRetryableApplicationError` | **no** | Type `InvalidCard` |
| rate limited | `NewApplicationErrorWithOptions` + `NextRetryDelay` | yes, after `RetryAfter` | Type `GatewayRateLimited` if it never recovers |
| unknown status | `NewApplicationError` | yes | Type `UnexpectedGatewayStatus` |

> The network-error branch uses `fmt.Errorf`, which is fine **only** because that error is retryable
> anyway. Never wrap a non-retryable ApplicationError with `fmt.Errorf`: it becomes retryable.
> [Chapter 06](06-wrapping-and-retryability.md) explains why.

---

## 5. Details: structured data for the caller

`Details` is how the Activity tells the Workflow *why* in a machine-readable way. Pass any
serializable value (a struct is best) and decode it on the other side with `Details(&v)`:

```go
// file: checkout/charge.go
package checkout

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/acme/shop/payments"
)

// ChargeOrderWorkflow shows how the Workflow reads the Type and Details set by the Activity.
func ChargeOrderWorkflow(ctx workflow.Context, req payments.ChargeRequest) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	var pa *payments.PaymentActivities
	var authID string
	err := workflow.ExecuteActivity(ctx, pa.ChargeCard, req).Get(ctx, &authID)

	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.Type() == payments.ErrTypeInsufficientFunds {
		var d payments.DeclineDetails
		if appErr.HasDetails() && appErr.Details(&d) == nil {
			workflow.GetLogger(ctx).Info("Customer is short", "order", req.OrderID, "missingCents", d.MissingCents)
		}
	}
	return authID, err
}
```

Checked on a dev server: the `DeclineDetails` struct survives the trip **Activity → Workflow →
Client**. When the Workflow returns the Activity error, a Client calling `run.Get` can still
`errors.As` it to `*temporal.ApplicationError`, read Type `InsufficientFunds`, and decode
`{DeclineCode:51 MissingCents:1250}`.

Rules of thumb:

- Always check `HasDetails()` and the error from `Details(&v)`: an older Activity version may not send them.
- Details go through the Data Converter like any payload. If you encrypt payloads, they're encrypted too.
  The **message and stack trace are not** encrypted by default; a custom
  [Failure Converter](https://docs.temporal.io/failure-converter) can encrypt them
  (`temporal.NewDefaultFailureConverter` with `EncodeCommonAttributes: true`). Don't put card numbers
  or emails in messages.

---

## 6. NextRetryDelay: let the failure set the pace

The Retry Policy's backoff is a guess. Sometimes the failure knows better: a `429 Retry-After: 30`,
a bank's maintenance window, a quota that resets at the top of the hour. Set `NextRetryDelay` and the
Service waits **that** long before the next attempt, instead of the computed interval.

Checked on a dev server (SDK v1.50.0, Server 1.32.0): with `InitialInterval: 200ms`, an Activity
returning `NextRetryDelay: 3s` was retried after 3s, then 3s again, and succeeded on attempt 3.

What still applies:

- `MaximumAttempts` and the Activity's `ScheduleToCloseTimeout` still cap the retries.
- The delay applies to the **next** attempt only. Return it on every failure that needs it.
- A zero value means "use the Retry Policy". To retry almost immediately, set a small value such as
  `time.Millisecond` (from the SDK's doc comment).
- It's ignored by Servers older than 1.24.2.

The Encyclopedia's "Per-error next Retry delay" section only links Java, Rust and TypeScript. Go has
had it for a while: `ApplicationErrorOptions.NextRetryDelay`.

---

## 7. Category Benign: expected failures without the noise

Some failures are part of normal operation: "not restocked yet" when polling a warehouse, "no new
messages" when polling an inbox. Using the Retry Policy as the polling loop is a valid pattern, but
every attempt logs an `ERROR` and counts as a failed Activity in your metrics.

```go
// file: inventory/activities.go
package inventory

import (
	"context"

	"go.temporal.io/sdk/temporal"
)

const ErrTypeNotRestockedYet = "NotRestockedYet"

// Warehouse is the stock service (an HTTP client in production).
type Warehouse interface {
	Available(ctx context.Context, sku string) (int, error)
}

type InventoryActivities struct {
	Warehouse Warehouse
}

// WaitForRestock fails until the SKU is back in stock. Those failures are
// expected: the Retry Policy is the polling loop. Benign keeps them out of
// error logs and failure metrics.
func (a *InventoryActivities) WaitForRestock(ctx context.Context, sku string) (int, error) {
	n, err := a.Warehouse.Available(ctx, sku)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, temporal.NewApplicationErrorWithOptions(
			"SKU "+sku+" not restocked yet", ErrTypeNotRestockedYet,
			temporal.ApplicationErrorOptions{Category: temporal.ApplicationErrorCategoryBenign})
	}
	return n, nil
}
```

What `ApplicationErrorCategoryBenign` changes in SDK v1.50.0 (from the source, and seen on a dev server):

- The Worker logs the Activity failure at **Debug** instead of Error.
- The failure isn't counted in the SDK's failed-Activity, failed-Local-Activity and failed-Workflow metrics.
- Nothing else: retries, history and the Workflow's view are unchanged.

The SDK checks the category on the **outermost** error only (`err.(*ApplicationError)`): a benign error
wrapped in `fmt.Errorf` is logged at Error again ([chapter 06](06-wrapping-and-retryability.md)).
More on benign errors and cancellation in the Activities guide:
[Failures, Cancellation & Benign errors](../../activities/docs/05-failures-and-cancellation.md).

---

## 8. Raising errors from Workflows

The same constructors work in Workflow code. In Go, **any error returned from a Workflow function fails
the Workflow Execution** ([chapter 03](03-workflow-task-vs-execution.md)), so the job of an
ApplicationError there is to give the caller a Type and Details:

```go
// file: checkout/validate.go
package checkout

import (
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const ErrTypeInvalidOrder = "InvalidOrder"

type OrderLine struct {
	SKU      string
	Quantity int
}

type PlaceOrderInput struct {
	OrderID   string
	CardToken string
	Lines     []OrderLine
}

// ValidateOrderWorkflow rejects bad input before any side effect happens.
func ValidateOrderWorkflow(ctx workflow.Context, in PlaceOrderInput) error {
	if len(in.Lines) == 0 {
		// Returning an ApplicationError from a Workflow fails the Workflow Execution.
		// NonRetryable only matters if the Workflow was started with a Retry Policy.
		return temporal.NewNonRetryableApplicationError("order has no lines", ErrTypeInvalidOrder, nil, in.OrderID)
	}
	for _, l := range in.Lines {
		if l.Quantity <= 0 {
			return temporal.NewNonRetryableApplicationError("quantity must be positive", ErrTypeInvalidOrder, nil, l.SKU)
		}
	}
	return nil
}
```

Checked on a dev server: started with a Workflow Retry Policy (`MaximumAttempts: 3`), this Workflow
failed once and wasn't retried, because the error is non-retryable. The Client got Type `InvalidOrder`
and the order ID in Details.

---

## 9. Who decides retryability: the implementer or the caller?

There are two places to say "don't retry this":

| Where | Who sets it | Scope |
| --- | --- | --- |
| `NonRetryable: true` on the error (or `NewNonRetryableApplicationError`) | The **implementer** of the Activity | Every caller, always. A Retry Policy can't override it |
| `RetryPolicy.NonRetryableErrorTypes` ([chapter 07](07-retry-policies.md)) | The **caller** (the Workflow) | This call only |

The Encyclopedia frames it as "caller versus implementer". In practice:

- Hard-code **non-retryable** only when *no* caller could ever want a retry with the same input: invalid
  card token, malformed request, a business rule that won't change.
- Leave everything else **retryable with a Type**, and let each caller list the Types it wants to stop on.
  "Insufficient funds" is permanent for checkout, but a subscription renewal might retry it daily.

---

## 10. Key takeaways

- Return `*temporal.ApplicationError` from Activities for every failure the Workflow should understand.
  A plain Go error arrives with an empty or meaningless Type.
- Make Types exported constants shared by the Activity and Workflow packages. They're a contract.
- `NewNonRetryableApplicationError` stops retries regardless of the Retry Policy. Use it for failures no
  retry can fix, and leave the rest to the caller's `NonRetryableErrorTypes`.
- Use **Details** (a struct) for machine-readable context. Keep secrets out of messages.
- `NextRetryDelay` lets the failure set the next retry interval (Server ≥ 1.24.2). `MaximumAttempts` and
  `ScheduleToCloseTimeout` still apply.
- `ApplicationErrorCategoryBenign` downgrades expected failures to Debug logs and keeps them out of
  failure metrics, but only on the outermost error.
