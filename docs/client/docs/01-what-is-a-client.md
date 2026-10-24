# 01 · What is a Temporal Client, and why does it exist?

> **Level:** core concept · **Read time:** ~10 min
> **Sources:** [Encyclopedia — Temporal Client](https://docs.temporal.io/encyclopedia/temporal-client) · [Go SDK — Temporal Client](https://docs.temporal.io/develop/go/client/temporal-client)

---

## 1. The problem

You wrote a Workflow and a Worker. Now the rest of your system has to use them:

- the checkout API must **start** an order Workflow when a customer pays,
- the support back-office must **show** where an order is and **change** its delivery address,
- a nightly job must **list** the orders stuck for more than a day,
- the mobile app must **wait for** or **fetch** the result.

None of these programs should run Workflow code, poll Task Queues or know what an Event History looks
like. They need a small, typed API to **talk to the Temporal Service**. That's the **Temporal Client**.

---

## 2. What a Temporal Client is

![Temporal Client mental model](../img/client-mental-model.svg)

A Temporal Client is an object from the SDK that lets your code **communicate with the Temporal
Service**. In Go it is the `client.Client` interface from `go.temporal.io/sdk/client`, created with
`client.Dial`. It opens a gRPC connection to the Service's Frontend and sends requests on your behalf.

The Encyclopedia lists the most common operations:

| Operation | Go method | Chapter |
| --- | --- | --- |
| Start a Workflow Execution | `c.ExecuteWorkflow` | [04](04-starting-workflows.md) |
| Get the result of a Workflow Execution | `run.Get`, `c.GetWorkflow(...).Get` | [07](07-getting-results.md) |
| List Workflow Executions | `c.ListWorkflow`, `c.CountWorkflow` | [08](08-interacting-and-managing.md) |
| Query a Workflow Execution | `c.QueryWorkflow` | [08](08-interacting-and-managing.md) |
| Signal a Workflow Execution | `c.SignalWorkflow`, `c.SignalWithStartWorkflow` | [08](08-interacting-and-managing.md) |
| Start and manage **Standalone Activities** (no Workflow) | `c.ExecuteActivity`, `c.GetActivityHandle` | [08](08-interacting-and-managing.md) |
| Start a **Standalone Nexus Operation** (no caller Workflow) | `c.NewNexusClient(...)` (experimental) | [08](08-interacting-and-managing.md) |

The Go Client can do more than the Encyclopedia lists: Updates, cancel, terminate, describe, read the
Event History, reset, complete an Activity asynchronously, manage Schedules. All of these are
requests to the same Service.

---

## 3. What a Client is not

| A Client… | Because… |
| --- | --- |
| **does not run** Workflow or Activity code | That's the Worker's job. A Client only sends requests. |
| **does not need a Worker to be running** to start a Workflow | The Service records the start and queues the first Task. A Worker picks it up later. |
| **cannot be used inside a Workflow** | Workflow code must be deterministic. A network call from Workflow code breaks replay. Use Child Workflows, `workflow.SignalExternalWorkflow` or an Activity. |
| **can be used inside an Activity** (and often is) | Activities may do any I/O. Inject the Client into your Activity struct. |

A Worker also holds a Client: `worker.New(c, …)` uses it to poll the Service. The same process can
share one Client between its Worker and its own requests.

---

## 4. A first Client, end to end

The examples in this guide revolve around a shop. Here is the Workflow the Client will drive:

```go
// file: orders/workflow.go
package orders

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "orders"

type PlaceOrderInput struct {
	OrderID     string
	CustomerID  string
	AmountCents int64
}

type OrderConfirmation struct {
	OrderID   string
	PaymentID string
}

func PlaceOrderWorkflow(ctx workflow.Context, in PlaceOrderInput) (OrderConfirmation, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var a *Activities
	var paymentID string
	if err := workflow.ExecuteActivity(ctx, a.ChargeCustomer, in.CustomerID, in.AmountCents).Get(ctx, &paymentID); err != nil {
		return OrderConfirmation{}, err
	}
	return OrderConfirmation{OrderID: in.OrderID, PaymentID: paymentID}, nil
}
```

```go
// file: orders/activities.go
package orders

import (
	"context"
	"fmt"
)

type Activities struct{}

func (a *Activities) ChargeCustomer(_ context.Context, customerID string, amountCents int64) (string, error) {
	return fmt.Sprintf("pay_%s_%d", customerID, amountCents), nil // call your payment gateway here
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

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	w := worker.New(c, orders.TaskQueue, worker.Options{})
	w.RegisterWorkflow(orders.PlaceOrderWorkflow)
	w.RegisterActivity(&orders.Activities{})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped", err)
	}
}
```

And the Client side, a checkout service: dial once, start the Workflow, wait for the result.

```go
// file: cmd/checkout/main.go
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"github.com/acme/shop/orders"
)

func main() {
	// 1. Create the Client: one per process. Defaults: 127.0.0.1:7233, Namespace "default".
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	// 2. Start a Workflow Execution.
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "order-ORD-1042",
		TaskQueue: orders.TaskQueue,
	}, orders.PlaceOrderWorkflow, orders.PlaceOrderInput{OrderID: "ORD-1042", CustomerID: "CUS-7", AmountCents: 2598})
	if err != nil {
		log.Fatalln("unable to start workflow", err)
	}
	log.Println("started", "WorkflowID", run.GetID(), "RunID", run.GetRunID())

	// 3. Wait for the result.
	var confirmation orders.OrderConfirmation
	if err := run.Get(context.Background(), &confirmation); err != nil {
		log.Fatalln("workflow failed", err)
	}
	log.Printf("order %s paid: %s", confirmation.OrderID, confirmation.PaymentID)
}
```

```sh
temporal server start-dev     # terminal 1
go run ./cmd/worker           # terminal 2
go run ./cmd/checkout         # terminal 3
```

---

## 5. The Client in a real service

In a real service, the Client is created **once at startup** and shared by every request handler. It
is safe for concurrent use.

```go
// file: internal/checkoutapi/server.go
package checkoutapi

import (
	"encoding/json"
	"net/http"

	"go.temporal.io/sdk/client"

	"github.com/acme/shop/orders"
)

// Server holds the process-wide Temporal Client.
type Server struct {
	Temporal client.Client
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.placeOrder)
	return mux
}

func (s *Server) placeOrder(w http.ResponseWriter, r *http.Request) {
	var in orders.PlaceOrderInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	run, err := s.Temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        "order-" + in.OrderID,
		TaskQueue: orders.TaskQueue,
	}, orders.PlaceOrderWorkflow, in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// Don't wait for the result in the HTTP request: return 202 and let the caller poll.
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"workflowId": run.GetID()})
}
```

And the Client inside an Activity, injected by the Worker's `main`:

```go
// file: loyalty/activities.go
package loyalty

import (
	"context"

	"go.temporal.io/sdk/client"
)

const PointsEarnedSignal = "points-earned"

type Activities struct {
	Temporal client.Client // the Worker's Client, injected at registration
}

// CreditLoyaltyPoints tells the customer's long-running loyalty Workflow about new points.
// Allowed in an Activity. Never call the Client from Workflow code.
func (a *Activities) CreditLoyaltyPoints(ctx context.Context, customerID string, points int) error {
	return a.Temporal.SignalWorkflow(ctx, "loyalty-"+customerID, "", PointsEarnedSignal, points)
}
```

---

## 6. Key takeaways

- A Temporal Client is the SDK object your code uses to **talk to the Temporal Service** over gRPC.
- Core operations: start, get result, list, Query, Signal, plus Standalone Activities and Nexus Operations. The Go Client also does Updates, cancel, terminate and more.
- It never runs Workflow or Activity code. Starting a Workflow doesn't need a running Worker.
- Create **one Client per process**, share it, close it on shutdown. Never use it in Workflow code. It's fine in Activities.
