# 01 · What is a Workflow, and why does it exist?

> **Level:** core concept · **Read time:** ~15 min
> **Sources:** [Encyclopedia — Temporal Workflow](https://docs.temporal.io/workflows) · [Workflow Definition](https://docs.temporal.io/workflow-definition) · [Workflow Execution](https://docs.temporal.io/workflow-execution) · [Go SDK — Workflow basics](https://docs.temporal.io/develop/go/workflows/basics)

---

## 1. The problem

Think about a subscription signup:

1. create the customer account,
2. charge the first invoice,
3. send a welcome email,
4. wait **14 days** for the trial to end,
5. convert the trial into a paid plan.

Written as a plain Go function, this breaks in many ways:

- The process **restarts** during the 14-day wait (a deploy, an OOM kill, a node rotation). Where were we?
- The payment API **times out** after the account was created. Do you retry? Do you create the account twice?
- You add a cron job, a `status` column, a retry table and a queue to cover all of this. Now the business logic is spread over five places.

A **Temporal Workflow** lets you write that process as **one ordinary Go function**, and Temporal makes
it **durable**. The function's progress is recorded step by step. If the process dies, another Worker
rebuilds the exact state from the record and carries on from where it stopped, even 14 days later.

> 💡 Workflows are resilient. They can run, and keep running, for years, even if the underlying
> infrastructure fails. If the application crashes, Temporal recreates its pre-failure state so it can
> continue right where it left off.

---

## 2. Three words for "Workflow"

In day-to-day talk, *Workflow* can mean three different things. The docs keep them apart, and so should you.

| Term | What it is | In Go |
| --- | --- | --- |
| **Workflow Definition** | The **code** that defines the Workflow | an exported function, e.g. `SubscriptionSignupWorkflow` |
| **Workflow Type** | The **name** that maps to a Definition | by default the function name, `"SubscriptionSignupWorkflow"` |
| **Workflow Execution** | One **running instance** of a Definition, with its own input and state | started with `client.ExecuteWorkflow(...)`, identified by a Workflow ID + Run ID |

Cardinality:

- One Workflow Type can be instantiated as **many** Workflow Executions (signup of customer `c-42`, signup of customer `c-43`, …).
- A Workflow Type is **scoped by a Task Queue**: the same Type name may map to different Definitions if they run on completely different Workers.

![Workflow mental model](../img/workflow-mental-model.svg)

---

## 3. What a Workflow Execution gives you

The Encyclopedia describes a Workflow Execution as a **durable, reliable and scalable function execution**:

| Property | Meaning | What it means for you |
| --- | --- | --- |
| **Durable** | No imposed time limit. Your code executes effectively once and to completion, for seconds or years. | `workflow.Sleep(ctx, 14*24*time.Hour)` is a normal line of code. |
| **Reliable** | Fully recoverable after a failure. State persists through Worker or Service outages. | You don't write recovery code for crashes. |
| **Scalable** | One Execution has limits, but an application can run millions to billions of them concurrently. | One Execution per order, per customer, per invoice is the normal design. |

Each Execution has **exclusive access to its local state**. It talks to other Executions through
**Signals** ([chapter 06](06-message-passing.md)) and to the outside world through **Activities**.

> The only reason a Workflow Execution fails is **your code returning an error**, not an
> infrastructure outage. Worker crashes and restarts are invisible to the Workflow function.

---

## 4. Workflow vs Activity: who does what

Temporal brings a Workflow back to its state by **re-running its code** against a recorded history
(see [chapter 03](03-determinism-and-replay.md)). So the Workflow code must make the same decisions
every time. That gives a clean split:

| Workflow code (orchestration) | Activity code (side effects) |
| --- | --- |
| Decides **what happens next**: sequence, branches, loops, waits | Does the **actual work**: API calls, DB queries, emails, LLM calls, file I/O |
| Must be **deterministic** | Can be anything; it's retried on failure |
| Uses `workflow.*` APIs (`workflow.Now`, `workflow.Sleep`, …) | Uses normal Go (`net/http`, `database/sql`, …) |
| Re-executed during replay | Executed once per attempt; during replay its **recorded result** is reused |

---

## 5. Vocabulary (keep this glossary handy)

| Term | Short definition |
| --- | --- |
| **Worker** | Your process. It polls a Task Queue and runs your Workflow and Activity code. |
| **Task Queue** | A named queue in the Temporal Service. Workers poll it, the Service puts Tasks on it. |
| **Workflow Task** | A unit of work that tells a Worker "advance this Workflow Execution as far as you can". |
| **Command** | A request your Workflow code emits when a Workflow Task completes, e.g. *ScheduleActivityTask*, *StartTimer*, *CompleteWorkflowExecution*. |
| **Event** | What the Service records in response to a Command or an external action, e.g. `ActivityTaskCompleted`, `TimerFired`, `WorkflowExecutionSignaled`. |
| **Event History** | The append-only, durable log of all Events of one Execution. The source of truth, and an audit log. |
| **Replay** | Re-running the Workflow code and matching its Commands against the Event History to rebuild state. |

---

## 6. Your first Workflow, end to end

The module is `github.com/acme/shop`. Start a dev server with `temporal server start-dev`
(UI at <http://localhost:8233>).

### The Activities: the side effects

```go
// file: subscriptions/activities.go
package subscriptions

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

// BillingGateway and Mailer are the external systems. Swap the fakes for real clients.
type BillingGateway interface {
	Charge(ctx context.Context, customerID string, amountCents int64, idempotencyKey string) (string, error)
}

type Mailer interface {
	Send(ctx context.Context, to, template string) error
}

type SignupActivities struct {
	Billing BillingGateway
	Mail    Mailer
}

func (a *SignupActivities) CreateAccount(ctx context.Context, in SignupInput) (string, error) {
	activity.GetLogger(ctx).Info("Creating account", "customerID", in.CustomerID)
	return "acct_" + in.CustomerID, nil
}

func (a *SignupActivities) ChargeFirstInvoice(ctx context.Context, accountID string, amountCents int64) (string, error) {
	// The Activity ID is stable across retries: a good idempotency key for the gateway.
	key := fmt.Sprintf("%s-%s", activity.GetInfo(ctx).WorkflowExecution.ID, activity.GetInfo(ctx).ActivityID)
	return a.Billing.Charge(ctx, accountID, amountCents, key)
}

func (a *SignupActivities) SendWelcomeEmail(ctx context.Context, email string) error {
	return a.Mail.Send(ctx, email, "welcome")
}

func (a *SignupActivities) ConvertTrial(ctx context.Context, accountID, plan string) error {
	activity.GetLogger(ctx).Info("Converting trial", "accountID", accountID, "plan", plan)
	return nil
}
```

### The Workflow: the business process

```go
// file: subscriptions/workflow.go
package subscriptions

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "subscriptions"

type SignupInput struct {
	CustomerID        string
	Email             string
	Plan              string
	FirstInvoiceCents int64
	TrialDays         int
}

type SignupResult struct {
	AccountID string
	ChargeID  string
	Converted bool
}

func SubscriptionSignupWorkflow(ctx workflow.Context, in SignupInput) (SignupResult, error) {
	logger := workflow.GetLogger(ctx)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumInterval: time.Minute},
	})

	var a *SignupActivities // nil receiver: only used to reference the method names
	var res SignupResult

	if err := workflow.ExecuteActivity(ctx, a.CreateAccount, in).Get(ctx, &res.AccountID); err != nil {
		return SignupResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, a.ChargeFirstInvoice, res.AccountID, in.FirstInvoiceCents).Get(ctx, &res.ChargeID); err != nil {
		return SignupResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, a.SendWelcomeEmail, in.Email).Get(ctx, nil); err != nil {
		// A failed welcome email should not cancel the subscription.
		logger.Warn("Welcome email failed", "error", err)
	}

	// A durable Timer: no goroutine, no cron job, survives restarts.
	trial := time.Duration(in.TrialDays) * 24 * time.Hour
	logger.Info("Trial started", "duration", trial)
	if err := workflow.Sleep(ctx, trial); err != nil {
		return SignupResult{}, err
	}

	if err := workflow.ExecuteActivity(ctx, a.ConvertTrial, res.AccountID, in.Plan).Get(ctx, nil); err != nil {
		return SignupResult{}, err
	}
	res.Converted = true
	return res, nil
}
```

### The Worker: hosts the code

```go
// file: cmd/subscriptions-worker/main.go
package main

import (
	"context"
	"log"

	"github.com/acme/shop/subscriptions"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type fakeBilling struct{}

func (fakeBilling) Charge(ctx context.Context, customerID string, amountCents int64, key string) (string, error) {
	return "ch_" + key, nil
}

type fakeMailer struct{}

func (fakeMailer) Send(ctx context.Context, to, template string) error { return nil }

func main() {
	c, err := client.Dial(client.Options{}) // localhost:7233, namespace "default"
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, subscriptions.TaskQueue, worker.Options{})
	w.RegisterWorkflow(subscriptions.SubscriptionSignupWorkflow)
	w.RegisterActivity(&subscriptions.SignupActivities{Billing: fakeBilling{}, Mail: fakeMailer{}})

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Unable to start Worker", err)
	}
}
```

### The starter: creates an Execution

```go
// file: cmd/start-signup/main.go
package main

import (
	"context"
	"log"

	"github.com/acme/shop/subscriptions"
	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	in := subscriptions.SignupInput{
		CustomerID: "c-42", Email: "jane@example.com", Plan: "pro-monthly",
		FirstInvoiceCents: 2900, TrialDays: 14,
	}
	// The Workflow ID carries business meaning: one signup per customer.
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "signup-" + in.CustomerID,
		TaskQueue: subscriptions.TaskQueue,
	}, subscriptions.SubscriptionSignupWorkflow, in)
	if err != nil {
		log.Fatalln("Unable to start Workflow", err)
	}
	log.Println("Started", "WorkflowID", run.GetID(), "RunID", run.GetRunID())

	// Blocks until the Execution closes (here: after the 14-day trial).
	var res subscriptions.SignupResult
	if err := run.Get(context.Background(), &res); err != nil {
		log.Fatalln("Workflow failed", err)
	}
	log.Printf("Signup done: %+v", res)
}
```

> 🔒 Don't put PII or secrets in Workflow IDs. They are stored in plain text and shown in the UI, CLI
> and logs. Use `signup-c-42`, not `signup-jane@example.com`.

### What happens if the Worker crashes during the 14-day sleep?

1. `workflow.Sleep` emitted a **StartTimer** Command. The Service recorded `TimerStarted` and holds the Timer. The Worker holds nothing important.
2. The Worker process dies. Nothing is lost: the account, the charge ID and the email result are already in the Event History.
3. Fourteen days later the Timer fires (`TimerFired`), and the Service schedules a Workflow Task.
4. A Worker (a new process, maybe on another machine) picks it up. It **replays** the function from the
   top. `CreateAccount`, `ChargeFirstInvoice` and `SendWelcomeEmail` are **not called again**: their
   recorded results are returned. The code reaches `workflow.Sleep`, sees the Timer already fired, and moves on.
5. `ConvertTrial` runs. The Workflow completes.

If no Worker is running when the Timer fires, the Workflow just waits. It continues as soon as a Worker comes back.

---

## Key takeaways

- A **Workflow** is a durable, ordinary-looking Go function that orchestrates a business process.
- Keep the three terms apart: **Definition** (code), **Type** (name), **Execution** (one running instance with an ID).
- Workflow code decides; **Activities** do the side effects.
- Progress lives in the **Event History**. Crashes are handled by **replay**, not by your code.
- Waiting days is cheap: `workflow.Sleep` creates a durable Timer in the Service.

⬅️ Previous: [Roadmap](README.md) · ➡️ Next: [02 · Defining Workflows in Go](02-defining-workflows.md)
