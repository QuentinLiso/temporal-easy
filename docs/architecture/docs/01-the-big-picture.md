# 01 · The big picture: who runs what

> **Level:** core concept · **Read time:** ~10 min
> **Sources:** [Encyclopedia — Temporal architecture](https://docs.temporal.io/encyclopedia/architecture/temporal-architecture) · [How Temporal works](https://docs.temporal.io/encyclopedia/architecture/how-temporal-works) · [About Temporal SDKs](https://docs.temporal.io/encyclopedia/architecture/temporal-sdks)

---

## 1. The problem

You already know how to write a Go service that processes an order: reserve stock, charge the card,
send an email. The hard part is everything around it:

| What goes wrong | What you would build yourself |
| --- | --- |
| The process crashes between "charge" and "email" | A `status` column, a recovery job, a "stuck orders" dashboard |
| The payment API is down for 10 minutes | Retry loops, backoff, a queue to park the work |
| You need 50 machines to keep up with Black Friday | Work distribution, locking, leader election |
| Someone asks "where is order ORD-1042 right now?" | Logging every step, then digging through logs |

Temporal gives you all of this as a platform. To use it well, you need a clear picture of **which
part runs where**. Most beginner confusion ("why does my Workflow not start?", "where does my code
actually run?", "is the Server calling my API?") comes from a fuzzy picture of this split.

---

## 2. Two sides: your environment and the Temporal Service

![Architecture mental model](../img/architecture-mental-model.svg)

There are two sides, and they only talk over the network (gRPC):

| Side | Who runs it | What it contains |
| --- | --- | --- |
| **Your environment** | You | Your **Temporal Application**: Workflow Definitions, Activity Definitions, the code that creates a **Temporal Client**, and the code that configures and starts **Workers**. All of it uses a **Temporal SDK** (here, the Go SDK). |
| **The Temporal Service** | You (self-hosted) or Temporal (Temporal Cloud) | The **Temporal Server** (Frontend, History, Matching and Worker services), its **database**, and optional components such as Elasticsearch (advanced visibility) and Grafana (dashboards). |

Two sentences from the docs to remember:

- **The Client does not execute Workflow or Activity code.** It only sends control requests.
- **The Server does not run your application code.** Your code runs only in your Workers.

The Temporal Service is the source of truth: it records what happened, decides what should happen
next and puts that work on queues. Your Workers do the work. Your application gets its
**durability, scalability and reliability** from the Service, not from your Worker processes.

---

## 3. The three kinds of clients

Three things talk to the Temporal Server, and all of them go through the same front door, the
**Frontend Service**:

| Client | Typical use |
| --- | --- |
| The **Temporal CLI** (`temporal …`) | Start the dev server, start or inspect Workflows, operate in production |
| The **Web UI** (<http://localhost:8233> with the dev server) | See Workflow Executions, their Event History, pending Activities |
| A **Temporal Client** from an SDK, inside your application | Start Workflows from an HTTP handler, send Signals, read results |

Workers also connect to the Frontend, with the same Client object, to poll for work. More on this in
[chapter 05](05-workers.md).

---

## 4. One application, file by file

Here is a complete, minimal Temporal Application. The order is processed by a Workflow that calls
three Activities. Look at **who runs each file**.

| File | Runs in | Role |
| --- | --- | --- |
| `orders/workflow.go` | Your Worker process | Workflow Definition: the steps, in order |
| `orders/activities.go` | Your Worker process | Activity Definitions: the side effects |
| `cmd/worker/main.go` | Your Worker process | Connects to the Service, registers the code, polls a Task Queue |
| `cmd/starter/main.go` | Any process (API, CLI, cron) | Uses a Temporal Client to start a Workflow and wait for its result |
| — | The Temporal Service | Records Events, schedules Tasks, retries, timers. **No code of yours.** |

```go
// file: orders/workflow.go
package orders

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the queue that Workers for the orders domain poll.
const TaskQueue = "orders"

type ProcessOrderInput struct {
	OrderID       string
	CustomerEmail string
	SKU           string
	Quantity      int
	AmountCents   int64
}

type ProcessOrderResult struct {
	OrderID   string
	PaymentID string
}

// ProcessOrderWorkflow is the whole order process, written as ordinary Go.
// It runs in a Worker, never in the Temporal Server.
func ProcessOrderWorkflow(ctx workflow.Context, in ProcessOrderInput) (ProcessOrderResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	var a *Activities // only used to name the Activity methods

	if err := workflow.ExecuteActivity(ctx, a.ReserveInventory, in.SKU, in.Quantity).Get(ctx, nil); err != nil {
		return ProcessOrderResult{}, err
	}

	var paymentID string
	if err := workflow.ExecuteActivity(ctx, a.ChargePayment, in.OrderID, in.AmountCents).Get(ctx, &paymentID); err != nil {
		return ProcessOrderResult{}, err
	}

	if err := workflow.ExecuteActivity(ctx, a.SendConfirmation, in.CustomerEmail, in.OrderID).Get(ctx, nil); err != nil {
		return ProcessOrderResult{}, err
	}

	return ProcessOrderResult{OrderID: in.OrderID, PaymentID: paymentID}, nil
}
```

```go
// file: orders/activities.go
package orders

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// External systems are interfaces: swap in your real clients.
type Inventory interface {
	Reserve(ctx context.Context, sku string, quantity int) error
}

type PaymentGateway interface {
	Charge(ctx context.Context, orderID string, amountCents int64) (paymentID string, err error)
}

type Mailer interface {
	Send(ctx context.Context, to, subject, body string) error
}

// Activities holds the side effects of the order process.
// The Worker calls these methods when it receives an Activity Task.
type Activities struct {
	Inventory Inventory
	Payments  PaymentGateway
	Mailer    Mailer
}

func (a *Activities) ReserveInventory(ctx context.Context, sku string, quantity int) error {
	activity.GetLogger(ctx).Info("Reserving stock", "sku", sku, "quantity", quantity)
	return a.Inventory.Reserve(ctx, sku, quantity)
}

func (a *Activities) ChargePayment(ctx context.Context, orderID string, amountCents int64) (string, error) {
	return a.Payments.Charge(ctx, orderID, amountCents)
}

func (a *Activities) SendConfirmation(ctx context.Context, email, orderID string) error {
	return a.Mailer.Send(ctx, email, "Order "+orderID+" confirmed", "Thanks for your order!")
}
```

```go
// file: orders/fakes.go
package orders

import (
	"context"
	"fmt"
	"log"
)

// Fakes so the example runs without real infrastructure.
type FakeInventory struct{}

func (FakeInventory) Reserve(context.Context, string, int) error { return nil }

type FakePayments struct{}

func (FakePayments) Charge(_ context.Context, orderID string, _ int64) (string, error) {
	return fmt.Sprintf("pay_%s", orderID), nil
}

type LogMailer struct{}

func (LogMailer) Send(_ context.Context, to, subject, _ string) error {
	log.Printf("email to %s: %s", to, subject)
	return nil
}
```

```go
// file: cmd/worker/main.go
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/orders"
)

// The Worker process: YOUR code, YOUR infrastructure.
func main() {
	// Connects to the Frontend Service (default 127.0.0.1:7233, "default" Namespace).
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	w := worker.New(c, orders.TaskQueue, worker.Options{})
	w.RegisterWorkflow(orders.ProcessOrderWorkflow)
	w.RegisterActivity(&orders.Activities{
		Inventory: orders.FakeInventory{},
		Payments:  orders.FakePayments{},
		Mailer:    orders.LogMailer{},
	})

	// Blocks and polls the "orders" Task Queue until SIGINT/SIGTERM.
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped", err)
	}
}
```

```go
// file: cmd/starter/main.go
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"github.com/acme/shop/orders"
)

// The starter: any process that holds a Temporal Client. It runs no Workflow code.
func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "order-ORD-1042", TaskQueue: orders.TaskQueue},
		orders.ProcessOrderWorkflow,
		orders.ProcessOrderInput{
			OrderID: "ORD-1042", CustomerEmail: "jane@example.com",
			SKU: "MUG-BLUE", Quantity: 2, AmountCents: 2598,
		},
	)
	if err != nil {
		log.Fatalln("unable to start workflow", err)
	}

	var result orders.ProcessOrderResult
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalln("workflow failed", err)
	}
	log.Printf("order %s paid with %s", result.OrderID, result.PaymentID)
}
```

### Run it

```sh
temporal server start-dev          # terminal 1: the Temporal Service (dev mode, UI on :8233)
go run ./cmd/worker                # terminal 2: your Worker
go run ./cmd/starter               # terminal 3: your Client
```

Now try the experiment that makes the architecture click:

1. Stop the Worker (Ctrl+C) and run the starter again. The starter **does not fail**: the Workflow
   is created, and its first Task waits on the `orders` Task Queue. The Web UI shows it as *Running*.
2. Start the Worker again. It picks up the waiting Task and the order completes.

The Service kept the state. The Worker was only borrowed to run the code.

---

## 5. Where to go next

| Question | Chapter |
| --- | --- |
| What exactly does the Go SDK give me? | [02 · Temporal SDKs](02-temporal-sdks.md) |
| What can a Client do, and how does it connect? | [03 · The Temporal Client](03-temporal-client.md) |
| What are Frontend, History, Matching? | [04 · Inside the Temporal Server](04-inside-the-server.md) |
| How do Workers get their work? | [05 · Workers](05-workers.md) |
| What happens on the wire, step by step? | [06 · Life of a Workflow Execution](06-workflow-lifecycle.md) |

---

## 6. Key takeaways

- A Temporal Application is **your code**: Workflows, Activities, Client setup, Worker setup. It is built with an SDK.
- The **Temporal Service** (Server + database + optional components) stores state and schedules work. It never runs your code.
- **Clients** send control requests. **Workers** run Workflow and Activity code. Both talk to the **Frontend** over gRPC.
- If no Worker is running, work **waits** on a Task Queue. Nothing is lost.
