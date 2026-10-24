# 01 · What is an Activity, and why does it exist?

> **Level:** core concept · **Read time:** ~10 min
> **Sources:** [Encyclopedia — Activities](https://docs.temporal.io/activities) · [Activity Execution](https://docs.temporal.io/activity-execution) · [Go SDK — Activities](https://docs.temporal.io/develop/go/activities)

---

## 1. The problem Activities solve

Imagine you write a checkout in plain Go:

```go
func Checkout(order Order) error {
    if err := inventory.Reserve(order); err != nil { return err } // network call
    if err := payments.Charge(order); err != nil { return err }   // network call, may time out
    if err := email.SendConfirmation(order); err != nil { return err }
    return nil
}
```

Every line can fail, and the failures are hard to handle:

| What goes wrong | What you would have to build yourself |
| --- | --- |
| The payment API times out once in a while | Retry loops with exponential backoff, jitter, max attempts |
| The process crashes after `Charge` succeeded | Store progress somewhere so you don't charge twice on restart |
| The email provider is down for 2 hours | A queue, a scheduler, a dead-letter queue |
| A long job (video transcoding, a 2 GB CSV import) dies at 80% | Checkpointing, resume logic, liveness detection |
| You need to know if a job is stuck | Timeouts, "last seen" heartbeats, dashboards |

Temporal splits your code in two:

- **Workflows** hold the *orchestration* (the order of steps, branching, compensation). They must be
  **deterministic**, so they can't do I/O directly.
- **Activities** hold the *side effects*. They are plain Go functions that call databases, HTTP APIs,
  file systems, LLMs, and so on. They can be **non-deterministic** and use any library.

The Temporal Service wraps every Activity call in a durable **Activity Execution**. That Execution
runs your retries, enforces your timeouts, tracks heartbeats and records the result. You no longer
write any of the plumbing from the table above.

> 💡 **One-liner for your notes:** *an Activity is a normal Go function. Temporal adds durability around it:
> it retries, times out and tracks it until you get one final result.*

### Problems Activities can solve, grouped

- **Reliability of side effects.** Automatic retries with backoff. If a Worker crashes, the Activity is retried on another Worker.
- **Failure detection.** Timeouts and heartbeats show that a Worker is dead or stuck.
- **Long-running work.** Heartbeat checkpoints let a retried attempt continue where the previous one stopped.
- **Memoization.** A completed Activity is never re-executed on Workflow replay, so an expensive LLM call or a large download runs once.
- **Background jobs / job queue.** [Standalone Activities](06-standalone-activities.md) run one durable job without a Workflow.
- **Human / external completion.** [Async completion](08-async-completion.md) lets another system finish the Activity.
- **Operational control.** Operators can Pause, Reset or retune a misbehaving Activity at runtime ([Activity Operations](10-activity-operations.md)).

---

## 2. What an Activity is (and isn't)

From the Encyclopedia: *an Activity is a normal function or method that executes a single,
well-defined action (short or long running), such as calling another service, transcoding a media
file, or sending an email message.*

| | **Workflow** | **Activity** |
| --- | --- | --- |
| Purpose | Orchestration and state | One side-effecting action |
| Determinism | **Must** be deterministic | Can be non-deterministic |
| I/O (HTTP, DB, files, time.Now, rand) | ❌ Forbidden | ✅ Expected |
| Failure handling | Usually doesn't fail on its own; it reacts to Activity failures | Retried automatically per its Retry Policy |
| Re-execution on replay | Replayed from Event History | **Never** re-executed after it completed: the recorded result is reused |
| Context type | `workflow.Context` | `context.Context` |

### Good Activity granularity

The Encyclopedia lists typical Activities:

- a **single write**: update a user profile, submit a card payment
- a **batch of similar writes**: create several orders, send several messages
- **reads followed by a write**: check stock and address, then update the order status
- a **read worth memoizing**: an LLM call, a large download, a slow-polling read

Split larger pieces of work into several Activities. Small Activities are easier to recover, can use
short timeouts, and are easier to make idempotent. See [02 · Defining Activities](02-defining-activities.md#6-design-for-retries-idempotency-and-granularity).

---

## 3. One definition, three ways to run it

![Activities mental model](../img/activities-mental-model.svg)

You **write the Activity once** and **register it once** on a Worker. You can then invoke it in three ways:

| Mode | Started by | Go API | Use it for | Chapter |
| --- | --- | --- | --- | --- |
| **Workflow Activity** | Workflow code | `workflow.ExecuteActivity` | Multi-step orchestration (sequence, branch, compensate, wait for Signals) | [03](03-workflow-activities.md) |
| **Standalone Activity** | A Temporal Client | `client.ExecuteActivity` | One durable background job (send an email, process a webhook, export data) | [06](06-standalone-activities.md) |
| **Local Activity** | Workflow code | `workflow.ExecuteLocalActivity` | Very short, high-throughput steps where latency matters more than full durability | [07](07-local-activities.md) |

Workflow Activities and Standalone Activities both create a **durable Activity Execution** in the
Temporal Service. A Local Activity skips the Service and the Task Queue: it runs inside the Workflow
Worker. It is a performance optimization, **not** a replacement for the other two.

---

## 4. Vocabulary you'll meet everywhere

| Term | Meaning | Go handle |
| --- | --- | --- |
| **Activity Definition** | Your Go function or struct method | `func (a *PaymentActivities) ChargeCard(ctx, in) (out, error)` |
| **Activity Type** | The *name* under which the definition is registered (defaults to the function name) | `activity.RegisterOptions{Name: "..."}` |
| **Activity Execution** | One durable run of an Activity, across all its attempts | `workflow.Future` / `client.ActivityHandle` |
| **Activity Task** | The unit of work the Service puts on a Task Queue for one attempt | — |
| **Activity Task Execution** | One *attempt*: a Worker picked up the task and ran your function | `activity.GetInfo(ctx).Attempt` |
| **Activity Worker** | Your process that polls a Task Queue and runs registered Activities | `worker.New(...)` |
| **Task Queue** | The named queue Activity Tasks are dispatched on | `ActivityOptions.TaskQueue` |
| **Activity ID** | Identifier of an Activity Execution (unique among *open* ones of a Workflow Run; separate ID space for Standalone) | `ActivityOptions.ActivityID` |
| **Task Token** | Opaque identifier of one Activity Task Execution, used for async completion and external heartbeats | `activity.GetInfo(ctx).TaskToken` |
| **Retry Policy** | Rules for retrying failed attempts (backoff, max attempts, non-retryable types) | `temporal.RetryPolicy` |
| **Heartbeat** | A "still alive + progress" ping from a running Activity | `activity.RecordHeartbeat` |

---

## 5. Your first end-to-end Activity in Go

A complete, minimal program: an `OrderPlacedWorkflow` asks an Activity to send a confirmation email.
Create these four files in a module named `github.com/acme/shop` (`go get go.temporal.io/sdk`).

**The Activity:** plain Go with a `context.Context`, one input struct and one output struct.

```go
// file: notifications/activities.go
package notifications

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

type SendOrderConfirmationInput struct {
	OrderID       string
	CustomerEmail string
	TotalCents    int64
}

type SendOrderConfirmationResult struct {
	MessageID string
}

// EmailActivities holds dependencies (an SMTP client, a template engine...).
// Here we just fake the provider call.
type EmailActivities struct {
	SenderAddress string
}

func (a *EmailActivities) SendOrderConfirmation(ctx context.Context, in SendOrderConfirmationInput) (SendOrderConfirmationResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Sending order confirmation", "orderID", in.OrderID, "to", in.CustomerEmail)

	// Real code would call SendGrid / SES / SMTP here. Any error returned is retried by Temporal.
	messageID := fmt.Sprintf("msg-%s", in.OrderID)
	return SendOrderConfirmationResult{MessageID: messageID}, nil
}
```

**The Workflow** schedules the Activity and waits for the result:

```go
// file: notifications/workflow.go
package notifications

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

type OrderPlacedInput struct {
	OrderID       string
	CustomerEmail string
	TotalCents    int64
}

func OrderPlacedWorkflow(ctx workflow.Context, in OrderPlacedInput) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second, // at least one timeout is mandatory
	})

	var a *EmailActivities // nil pointer: only used to reference the method
	var res SendOrderConfirmationResult
	err := workflow.ExecuteActivity(ctx, a.SendOrderConfirmation, SendOrderConfirmationInput{
		OrderID:       in.OrderID,
		CustomerEmail: in.CustomerEmail,
		TotalCents:    in.TotalCents,
	}).Get(ctx, &res)
	if err != nil {
		return "", err
	}
	return res.MessageID, nil
}
```

**The Worker** registers both and polls the `orders` Task Queue:

```go
// file: cmd/worker/main.go
package main

import (
	"log"

	"github.com/acme/shop/notifications"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{}) // localhost:7233, "default" namespace
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	w := worker.New(c, "orders", worker.Options{})
	w.RegisterWorkflow(notifications.OrderPlacedWorkflow)
	w.RegisterActivity(&notifications.EmailActivities{SenderAddress: "orders@acme.example"})

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped", err)
	}
}
```

**The starter** kicks off a Workflow:

```go
// file: cmd/starter/main.go
package main

import (
	"context"
	"log"

	"github.com/acme/shop/notifications"
	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "order-placed-ORD-1042", TaskQueue: "orders"},
		notifications.OrderPlacedWorkflow,
		notifications.OrderPlacedInput{OrderID: "ORD-1042", CustomerEmail: "jane@example.com", TotalCents: 4999},
	)
	if err != nil {
		log.Fatalln("unable to start workflow", err)
	}

	var messageID string
	if err := run.Get(context.Background(), &messageID); err != nil {
		log.Fatalln("workflow failed", err)
	}
	log.Println("confirmation sent:", messageID)
}
```

Run it:

```bash
temporal server start-dev          # terminal 1 — UI on http://localhost:8233
go run ./cmd/worker                # terminal 2
go run ./cmd/starter               # terminal 3
```

Open the Web UI and look at the Workflow's Event History. You'll see `ActivityTaskScheduled`,
`ActivityTaskStarted` and `ActivityTaskCompleted`, the three events every Workflow Activity
produces. Make `SendOrderConfirmation` return an error and watch Temporal retry it with backoff,
without any retry code from you.

---

## 6. Key takeaways

- Activities are **where side effects live**. Workflows orchestrate, Activities act.
- Activity code is **plain Go**: any library, any I/O, non-determinism allowed.
- Temporal wraps every call in a durable **Activity Execution** (retries, timeouts, heartbeats, recorded result).
- **Write once, register once**, then run it as a Workflow Activity, a Standalone Activity or a Local Activity.
- Because of retries, an Activity **may run more than once**. Design it to be **idempotent** (next chapter).

➡️ Next: [02 · Defining Activities in Go](02-defining-activities.md)
