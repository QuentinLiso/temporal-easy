# 08 · Interacting with and managing running Workflows

> **Level:** daily use · **Read time:** ~20 min
> **Sources:** [Encyclopedia — Temporal Client](https://docs.temporal.io/encyclopedia/temporal-client) · [`client.Client`](https://pkg.go.dev/go.temporal.io/sdk/client#Client) · [Standalone Activity](https://docs.temporal.io/standalone-activity)

---

## 1. The problem

Starting a Workflow and reading its result covers the two ends of its life. In between, the outside
world must **talk to it**: add an item to a cart, ask what's in it, apply a coupon and know whether
it was accepted, cancel the order. Operators must **find** Workflows and **stop** them. Some jobs
don't need a Workflow at all.

The Go Client page focuses on connecting, starting and results. This chapter covers the rest of the
operations the Encyclopedia lists (list, Query, Signal, Standalone Activities and Nexus Operations),
plus the ones the Go Client adds: Update, cancel, terminate, describe.

| Need | Client call | Waits for the Workflow? | Recorded in history? |
| --- | --- | --- | --- |
| Send data, fire-and-forget | `SignalWorkflow` | No | Yes |
| Send data, start the Workflow if needed | `SignalWithStartWorkflow` | No | Yes |
| Read state | `QueryWorkflow` | Needs a Worker to answer | No |
| Change state **and** get an answer (or a rejection) | `UpdateWorkflow` | Yes, until the chosen stage | Yes, if accepted |
| Ask it to stop gracefully | `CancelWorkflow` | No | Yes |
| Stop it now | `TerminateWorkflow` | No | Yes |
| Inspect | `DescribeWorkflowExecution`, `GetWorkflowHistory` | No | — |
| Find many | `ListWorkflow`, `CountWorkflow` | No | — |

---

## 2. The Workflow we'll talk to

A shopping cart that lives until checkout or 24 hours of inactivity:

```go
// file: cart/workflow.go
package cart

import (
	"errors"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	TaskQueue         = "carts"
	AddItemSignal     = "add-item"
	CheckoutSignal    = "checkout"
	GetCartQuery      = "get-cart"
	ApplyCouponUpdate = "apply-coupon"
	inactivityTimeout = 24 * time.Hour
)

type Item struct {
	SKU        string
	Quantity   int
	PriceCents int64
}

type Cart struct {
	CustomerID    string
	Items         []Item
	Coupon        string
	DiscountCents int64
}

func (c Cart) TotalCents() int64 {
	var t int64
	for _, it := range c.Items {
		t += int64(it.Quantity) * it.PriceCents
	}
	return t - c.DiscountCents
}

// ShoppingCartWorkflow returns the cart at checkout, or an empty cart if abandoned.
func ShoppingCartWorkflow(ctx workflow.Context, customerID string) (Cart, error) {
	cart := Cart{CustomerID: customerID}

	// Query: read-only, answered from the current state.
	if err := workflow.SetQueryHandler(ctx, GetCartQuery, func() (Cart, error) { return cart, nil }); err != nil {
		return Cart{}, err
	}

	// Update: validated, changes state, returns a value to the caller.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, ApplyCouponUpdate,
		func(ctx workflow.Context, code string) (int64, error) {
			cart.Coupon = strings.ToUpper(code)
			cart.DiscountCents = cart.TotalCents() / 10 // 10% off
			return cart.TotalCents(), nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(code string) error {
				if cart.Coupon != "" {
					return errors.New("a coupon is already applied")
				}
				if !strings.HasPrefix(strings.ToUpper(code), "WELCOME") {
					return errors.New("unknown coupon")
				}
				return nil
			},
		}); err != nil {
		return Cart{}, err
	}

	addItems := workflow.GetSignalChannel(ctx, AddItemSignal)
	checkout := workflow.GetSignalChannel(ctx, CheckoutSignal)

	for {
		timerCtx, cancelTimer := workflow.WithCancel(ctx)
		idle := workflow.NewTimer(timerCtx, inactivityTimeout)

		checkedOut, abandoned := false, false
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(addItems, func(ch workflow.ReceiveChannel, _ bool) {
			var it Item
			ch.Receive(ctx, &it)
			cart.Items = append(cart.Items, it)
		})
		sel.AddReceive(checkout, func(ch workflow.ReceiveChannel, _ bool) {
			ch.Receive(ctx, nil)
			checkedOut = true
		})
		sel.AddFuture(idle, func(workflow.Future) { abandoned = true })
		sel.Select(ctx)
		cancelTimer()

		if ctx.Err() != nil { // the Workflow was cancelled
			return Cart{}, ctx.Err()
		}
		if checkedOut {
			return cart, nil
		}
		if abandoned {
			return Cart{CustomerID: customerID}, nil
		}
	}
}
```

---

## 3. Signals: send data, don't wait

```go
// file: cart/client.go
package cart

import (
	"context"

	"go.temporal.io/sdk/client"
)

func workflowID(customerID string) string { return "cart-" + customerID }

// AddItem sends a Signal to the customer's cart, and starts the cart if it doesn't exist yet.
// One call, atomic: no "start then signal" race when two requests arrive together.
func AddItem(ctx context.Context, c client.Client, customerID string, it Item) error {
	_, err := c.SignalWithStartWorkflow(ctx,
		workflowID(customerID), AddItemSignal, it, // the Signal
		client.StartWorkflowOptions{TaskQueue: TaskQueue}, // used only if the cart must be started
		ShoppingCartWorkflow, customerID,
	)
	return err
}

// Checkout signals an existing cart. Empty Run ID = the running Workflow with this ID.
func Checkout(ctx context.Context, c client.Client, customerID string) error {
	return c.SignalWorkflow(ctx, workflowID(customerID), "", CheckoutSignal, nil)
}
```

`SignalWorkflow` returns once the Service has **recorded** the Signal. It doesn't wait for the
Workflow to process it, and it works even if no Worker is running.

---

## 4. Queries: read state

```go
// file: cart/query.go
package cart

import (
	"context"

	"go.temporal.io/sdk/client"
)

// GetCart reads the cart's current state. A Worker must be running to answer.
func GetCart(ctx context.Context, c client.Client, customerID string) (Cart, error) {
	val, err := c.QueryWorkflow(ctx, workflowID(customerID), "", GetCartQuery)
	if err != nil {
		return Cart{}, err
	}
	var cart Cart
	err = val.Get(&cart)
	return cart, err
}
```

A Query is **not** written to the Event History, and its handler must not change state. A Worker
runs the Query handler on the Workflow's current state, so Queries fail if no Worker is available.
They also work on **closed** Workflows (the Worker replays the history to answer).

---

## 5. Updates: change state and get an answer

A Signal can't tell you "unknown coupon". An **Update** can: the validator runs first, and a rejected
Update returns an error to the caller **without** being written to the history.

```go
// file: cart/update.go
package cart

import (
	"context"

	"go.temporal.io/sdk/client"
)

// ApplyCoupon returns the new total, or the validator's error ("unknown coupon").
func ApplyCoupon(ctx context.Context, c client.Client, customerID, code string) (int64, error) {
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID(customerID),
		UpdateName:   ApplyCouponUpdate,
		Args:         []any{code},
		WaitForStage: client.WorkflowUpdateStageCompleted, // wait for the handler's return value
	})
	if err != nil {
		return 0, err // includes validation rejections
	}
	var newTotal int64
	err = handle.Get(ctx, &newTotal)
	return newTotal, err
}
```

An Update needs a running Worker, since the handler runs in Workflow code. `WaitForStage:
client.WorkflowUpdateStageAccepted` returns as soon as the validator has passed. Then call
`handle.Get` later. Message passing is covered in depth in the
[Workflows guide, chapter 06](../../workflows/docs/06-message-passing.md).

---

## 6. Cancel, terminate, describe

```go
// file: cart/admin.go
package cart

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
)

// CancelCart asks the Workflow to stop. Its ctx is cancelled and it can clean up
// (here, ShoppingCartWorkflow returns ctx.Err()).
func CancelCart(ctx context.Context, c client.Client, customerID string) error {
	return c.CancelWorkflow(ctx, workflowID(customerID), "")
}

// PurgeCart stops it immediately. No Workflow code runs after this. Use as a last resort.
func PurgeCart(ctx context.Context, c client.Client, customerID, reason string) error {
	return c.TerminateWorkflow(ctx, workflowID(customerID), "", reason)
}

// DescribeCart returns a one-line summary for an admin screen.
func DescribeCart(ctx context.Context, c client.Client, customerID string) (string, error) {
	d, err := c.DescribeWorkflowExecution(ctx, workflowID(customerID), "")
	if err != nil {
		return "", err
	}
	info := d.GetWorkflowExecutionInfo()
	return fmt.Sprintf("%s: %s since %s, %d events",
		info.GetExecution().GetWorkflowId(), info.GetStatus(),
		info.GetStartTime().AsTime().Format("2006-01-02 15:04"), info.GetHistoryLength()), nil
}
```

---

## 7. List and count Workflow Executions

`ListWorkflow` takes a **visibility query** (SQL-like, on built-in fields and your Search Attributes)
and returns one page at a time.

```go
// file: cart/list.go
package cart

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

const openCartsQuery = "WorkflowType = 'ShoppingCartWorkflow' AND ExecutionStatus = 'Running'"

// OpenCartIDs pages through every running cart.
func OpenCartIDs(ctx context.Context, c client.Client) ([]string, error) {
	var ids []string
	var pageToken []byte
	for {
		resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         openCartsQuery,
			PageSize:      500,
			NextPageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, e := range resp.GetExecutions() {
			ids = append(ids, e.GetExecution().GetWorkflowId())
		}
		pageToken = resp.GetNextPageToken()
		if len(pageToken) == 0 {
			return ids, nil
		}
	}
}

// CountOpenCarts is cheaper than listing when you only need the number.
func CountOpenCarts(ctx context.Context, c client.Client) (int64, error) {
	resp, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: openCartsQuery})
	if err != nil {
		return 0, err
	}
	return resp.GetCount(), nil
}
```

Visibility is updated asynchronously. A Workflow started a moment ago may not be listed yet.

---

## 8. Without a Workflow: Standalone Activities and Nexus Operations

The Encyclopedia adds two newer Client operations:

- A **Standalone Activity** is a top-level Activity Execution started **directly by a Client**,
  without a Workflow. Temporal still retries it, times it out and records it. Think "durable job
  queue". In Go: `c.ExecuteActivity(ctx, client.StartActivityOptions{…}, fn, args...)`, then
  `handle.Get`. Requires Go SDK ≥ v1.49.0 and Server ≥ 1.32.0.
- A **Standalone Nexus Operation** is a Nexus Operation started by a Client, without a caller
  Workflow. In Go SDK v1.50.0 this goes through `c.NewNexusClient(...)` and is **experimental**.

```go
// file: receipts/standalone.go
package receipts

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
)

// RenderReceiptPDF is an ordinary Activity, registered on a Worker polling "receipts".
func RenderReceiptPDF(ctx context.Context, orderID string) (string, error) {
	return "s3://acme-receipts/" + orderID + ".pdf", nil
}

// RenderReceipt runs one durable, retried job: no Workflow needed.
func RenderReceipt(ctx context.Context, c client.Client, orderID string) (string, error) {
	handle, err := c.ExecuteActivity(ctx, client.StartActivityOptions{
		ID:                  "receipt-" + orderID,
		TaskQueue:           "receipts",
		StartToCloseTimeout: 2 * time.Minute,
	}, RenderReceiptPDF, orderID)
	if err != nil {
		return "", err
	}
	var url string
	err = handle.Get(ctx, &url)
	return url, err
}
```

Standalone Activities are covered in the
[Activities guide, chapter 06](../../activities/docs/06-standalone-activities.md).

---

## 9. Key takeaways

- **Signal**: send data, don't wait, recorded. **SignalWithStart**: same, starting the Workflow if needed, atomically.
- **Query**: read state, needs a Worker, not recorded.
- **Update**: validated change that returns a value. Rejections aren't recorded. Needs a Worker.
- **Cancel** lets the Workflow clean up. **Terminate** stops it immediately.
- **ListWorkflow / CountWorkflow** use visibility queries and pagination. Visibility is eventually consistent.
- Clients can also run **Standalone Activities** and (experimental in Go) **Standalone Nexus Operations**, with no Workflow.
