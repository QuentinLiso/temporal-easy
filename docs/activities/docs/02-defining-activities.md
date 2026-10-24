# 02 · Defining Activities in Go

> **Level:** core concept · **Read time:** ~15 min
> **Sources:** [Encyclopedia — Activity Definition](https://docs.temporal.io/activity-definition) · [Go SDK — Activity basics](https://docs.temporal.io/develop/go/activities/basics)

An **Activity Definition** is the code that runs when a Worker picks up an Activity Task. In Go it is
an **exported function** or an **exported method on a struct**. This chapter covers how to write
one, how to register it, and how to design it so retries are safe.

---

## 1. Function or struct method?

Both work. Use a **struct** when the Activity needs dependencies that you build once per process,
for example:

- an application-level DB pool
- a client connection to another service (payment gateway, S3, an LLM API)
- reusable utilities (template engine, metrics)
- any other expensive resource

Every exported method of the struct becomes its **own Activity Type**. The Go SDK docs use struct
methods everywhere, and so does this roadmap.

> ⚠️ Use struct methods for **Activities only**. Registering struct methods as *Workflows* is strongly
> discouraged because it can cause non-determinism errors.

```go
// file: payments/types.go
package payments

import "context"

// ChargeCardInput is a single struct: you can add fields later without breaking the signature.
type ChargeCardInput struct {
	OrderID      string
	CustomerID   string
	AmountCents  int64
	Currency     string
	PaymentToken string // tokenized card, never the raw PAN
}

type ChargeReceipt struct {
	ChargeID   string
	ApprovedAt string
}

type RefundInput struct {
	ChargeID    string
	AmountCents int64
	Reason      string
}

// PaymentGateway is the external dependency (Stripe, Adyen...). It is an interface so
// you can swap a fake in tests.
type PaymentGateway interface {
	Charge(ctx context.Context, req GatewayChargeRequest) (GatewayChargeResponse, error)
	Refund(ctx context.Context, chargeID string, amountCents int64, idempotencyKey string) error
}

type GatewayChargeRequest struct {
	AmountCents    int64
	Currency       string
	Source         string
	IdempotencyKey string
}

type GatewayChargeResponse struct {
	ID         string
	ApprovedAt string
}
```

```go
// file: payments/activities.go
package payments

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// PaymentActivities groups every payment-related Activity. Fields are shared,
// long-lived dependencies created once when the Worker starts.
type PaymentActivities struct {
	Gateway PaymentGateway
}

// ChargeCard is registered as Activity Type "ChargeCard".
func (a *PaymentActivities) ChargeCard(ctx context.Context, in ChargeCardInput) (ChargeReceipt, error) {
	logger := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)
	logger.Info("Charging card", "orderID", in.OrderID, "attempt", info.Attempt)

	resp, err := a.Gateway.Charge(ctx, GatewayChargeRequest{
		AmountCents:    in.AmountCents,
		Currency:       in.Currency,
		Source:         in.PaymentToken,
		IdempotencyKey: IdempotencyKey(ctx), // see section 6
	})
	if err != nil {
		return ChargeReceipt{}, err // returning an error = this attempt failed; Temporal retries
	}
	return ChargeReceipt{ChargeID: resp.ID, ApprovedAt: resp.ApprovedAt}, nil
}

// RefundCharge is registered as Activity Type "RefundCharge". It returns only an error.
func (a *PaymentActivities) RefundCharge(ctx context.Context, in RefundInput) error {
	return a.Gateway.Refund(ctx, in.ChargeID, in.AmountCents, IdempotencyKey(ctx))
}
```

And a plain-function Activity, which needs no dependencies:

```go
// file: payments/fraud.go
package payments

import "context"

type FraudScoreInput struct {
	CustomerID  string
	AmountCents int64
	Country     string
}

// ScoreTransaction is a plain function Activity.
func ScoreTransaction(ctx context.Context, in FraudScoreInput) (float64, error) {
	score := 0.05
	if in.AmountCents > 500_00 {
		score += 0.30
	}
	if in.Country != "FR" {
		score += 0.10
	}
	return score, nil
}
```

---

## 2. Parameters

| Rule | Why |
| --- | --- |
| The first parameter should be `context.Context` | Optional, but you need it for `activity.GetLogger`, `GetInfo`, `RecordHeartbeat` and cancellation |
| Pass **one struct** for your business data | You can add fields later without breaking callers or in-flight Executions |
| Everything must be **serializable** | No channels, functions, variadic arguments or unsafe pointers |
| Keep payloads **small** | One argument may be at most **2 MB**, and a whole gRPC message at most **4 MB** |

When the Activity is called from a Workflow, its arguments **and** its result are stored in the
Workflow's **Event History**, and that history is sent to Workers during replay. Pass IDs and
references (an S3 key, a row ID) instead of large blobs.

## 3. Return values

A Go Activity returns either:

- `error`, or
- `(T, error)` where `T` is serializable. Prefer a struct so you can add fields later.

Return values have the same payload limits: 2 MB by default per payload and a hard 4 MB per gRPC message.

---

## 4. What the `context.Context` gives you

Inside an Activity the `ctx` is a normal `context.Context` that the SDK enriches. The helpers you'll use most:

| Helper | What it returns |
| --- | --- |
| `activity.GetLogger(ctx)` | A logger already tagged with Workflow/Activity IDs |
| `activity.GetInfo(ctx)` | `Attempt`, `ActivityID`, `ActivityType.Name`, `TaskQueue`, `TaskToken`, `WorkflowExecution`, timeouts, `IsWorkflowActivity()`… |
| `activity.GetMetricsHandler(ctx)` | A metrics handler for custom counters and timers |
| `activity.RecordHeartbeat(ctx, details...)` | Liveness and progress ([chapter 04](04-timeouts-retries-heartbeats.md#4-heartbeats)) |
| `activity.HasHeartbeatDetails` / `GetHeartbeatDetails` | The checkpoint saved by the previous attempt |
| `activity.GetWorkerStopChannel(ctx)` | Closed when the Worker is shutting down |
| `ctx.Done()` | Closed on cancellation, timeout, Pause or Reset ([chapter 05](05-failures-and-cancellation.md)) |

---

## 5. Register the Activity on a Worker and choose its Activity Type

An Activity Definition **doesn't run until a Worker registers it** and polls the Task Queue its
tasks are sent to. Registration maps an **Activity Type** (a name) to your function. A caller can
then start the Activity by name without having a reference to the code.

- One Worker can register many Activities.
- **One registration serves every caller.** The same registered Activity can be started as a
  Workflow Activity or as a Standalone Activity with no code change.

```go
// file: cmd/payments-worker/main.go
package main

import (
	"context"
	"log"

	"github.com/acme/shop/payments"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// fakeGateway stands in for a real payment provider SDK.
type fakeGateway struct{}

func (fakeGateway) Charge(ctx context.Context, req payments.GatewayChargeRequest) (payments.GatewayChargeResponse, error) {
	return payments.GatewayChargeResponse{ID: "ch_" + req.IdempotencyKey, ApprovedAt: "2026-10-23T10:00:00Z"}, nil
}

func (fakeGateway) Refund(ctx context.Context, chargeID string, amountCents int64, idempotencyKey string) error {
	return nil
}

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, "payments", worker.Options{})

	// 1) Struct: every exported method becomes an Activity Type
	//    -> "ChargeCard", "RefundCharge"
	w.RegisterActivity(&payments.PaymentActivities{Gateway: fakeGateway{}})

	// 2) Function with a custom Activity Type name
	w.RegisterActivityWithOptions(payments.ScoreTransaction, activity.RegisterOptions{
		Name: "fraud.ScoreTransaction",
	})

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped", err)
	}
}
```

How names are derived:

| Registration | Resulting Activity Type(s) |
| --- | --- |
| `RegisterActivity(ScoreTransaction)` | `ScoreTransaction` |
| `RegisterActivityWithOptions(ScoreTransaction, {Name: "fraud.ScoreTransaction"})` | `fraud.ScoreTransaction` |
| `RegisterActivity(&PaymentActivities{})` | `ChargeCard`, `RefundCharge` |
| `RegisterActivityWithOptions(&PaymentActivities{}, {Name: "payments."})` | `payments.ChargeCard`, `payments.RefundCharge` (for a struct, `Name` is a **prefix**) |

Callers can reference an Activity in two ways:

```go
// file: payments/caller_example.go
package payments

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func ChargeExampleWorkflow(ctx workflow.Context, in ChargeCardInput) (ChargeReceipt, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})

	// a) Function reference: the SDK checks argument types against the definition.
	var a *PaymentActivities
	var receipt ChargeReceipt
	if err := workflow.ExecuteActivity(ctx, a.ChargeCard, in).Get(ctx, &receipt); err != nil {
		return ChargeReceipt{}, err
	}

	// b) String Activity Type: no compile-time link. Useful across services and languages,
	//    and mandatory when you registered a custom Name.
	var score float64
	err := workflow.ExecuteActivity(ctx, "fraud.ScoreTransaction",
		FraudScoreInput{CustomerID: in.CustomerID, AmountCents: in.AmountCents, Country: "FR"}).Get(ctx, &score)
	return receipt, err
}
```

> 💡 If you set a custom `Name`, the SDK recommends `worker.Options{DisableRegistrationAliasing: true}`
> and calling the Activity by that string name **everywhere**, so a function reference and a string
> name can't point to two different Activity Types.

---

## 6. Design for retries: idempotency and granularity

### Why idempotency is non-negotiable

Temporal **retries** failed attempts. So your function **may run more than once**, and it can even
partly finish more than once. One classic case: the attempt succeeds, then the Worker crashes **just
before** it reports the result. The Service never sees the completion and runs the Activity again.
Without idempotency, that is a double charge.

> *An Activity is idempotent if multiple Activity Task Executions do not change the state of the
> system beyond the first one.*

The usual tool is an **idempotency key**: a value that stays the same across retries of this Activity
Execution and is unique across Executions. The **service you call** enforces it, not the Activity.
Payment APIs, for example, ignore a second request that carries the same key. The Encyclopedia
recommends **Workflow Run ID + Activity ID**. For a Standalone Activity there is no Workflow, so use
the Activity ID together with the Activity Run ID.

```go
// file: payments/idempotency.go
package payments

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// IdempotencyKey is stable across retries of this Activity Execution and unique
// across Executions.
func IdempotencyKey(ctx context.Context) string {
	info := activity.GetInfo(ctx)
	if info.IsWorkflowActivity() {
		return info.WorkflowExecution.RunID + "-" + info.ActivityID
	}
	// Standalone Activity: no Workflow Run, so use the Activity's own IDs.
	return info.ActivityID + "-" + info.ActivityRunID
}
```

### Granularity: one Activity = one step that can be retried on its own

An Activity is **atomic** from Temporal's point of view: on retry, the **whole function** runs again.
Take an Activity that (1) looks up a DB row, (2) calls a microservice, (3) writes a file. If step 3
fails, steps 1 and 2 are executed again too. Split it:

```go
// file: invoicing/activities.go
package invoicing

import (
	"context"
	"fmt"
)

type InvoiceActivities struct{}

type LoadInvoiceInput struct{ InvoiceID string }
type InvoiceData struct {
	InvoiceID   string
	CustomerID  string
	AmountCents int64
}

type ERPSyncResult struct{ ERPReference string }

type ArchiveInput struct {
	InvoiceID    string
	ERPReference string
}

// Step 1 — a read.
func (a *InvoiceActivities) LoadInvoice(ctx context.Context, in LoadInvoiceInput) (InvoiceData, error) {
	return InvoiceData{InvoiceID: in.InvoiceID, CustomerID: "CUST-77", AmountCents: 129_00}, nil
}

// Step 2 — the external call, retried on its own.
func (a *InvoiceActivities) PushInvoiceToERP(ctx context.Context, inv InvoiceData) (ERPSyncResult, error) {
	return ERPSyncResult{ERPReference: fmt.Sprintf("ERP-%s", inv.InvoiceID)}, nil
}

// Step 3 — the write that may fail on permissions: only this step is retried.
func (a *InvoiceActivities) ArchiveInvoicePDF(ctx context.Context, in ArchiveInput) error {
	return nil
}
```

There is a trade-off: three Activities means three Executions in the Event History instead of one.

### Don't write retry loops inside Activities

> ⚠️ Don't hand-roll retry logic in an Activity. It makes the timeout you need longer, hides
> failures from metrics, and makes debugging harder in the UI. Return the error and let the
> [Retry Policy](04-timeouts-retries-heartbeats.md#3-retry-policy) do its job.

### What Temporal guarantees

With a Retry Policy that allows retries, Temporal guarantees that the Activity is **observed as
completed exactly once**, but it may be **executed** several times. A non-idempotent Activity doesn't
break the Temporal Platform; it breaks **your** business data.

---

## 7. Best-practices checklist

- [ ] First parameter is `context.Context`, then **one** input struct.
- [ ] Returns `error` or `(Struct, error)`, and all of it is serializable.
- [ ] Payloads are small (IDs and references, not blobs).
- [ ] Dependencies live in the Activity struct and are built once at Worker start.
- [ ] Writes are **idempotent** (idempotency key = Run ID + Activity ID).
- [ ] Each Activity is one step that can be retried on its own.
- [ ] No hand-written retry loops.
- [ ] It finishes within a few minutes **or** it heartbeats ([chapter 04](04-timeouts-retries-heartbeats.md)).
- [ ] At least one timeout is set at the call site, usually Start-To-Close.

⬅️ Previous: [01 · What is an Activity](01-what-is-an-activity.md) · ➡️ Next: [03 · Calling Activities from a Workflow](03-workflow-activities.md)
