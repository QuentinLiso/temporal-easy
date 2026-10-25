# 01 · Testing strategy: what to test and with which tool

> **Level:** foundations · **Read time:** ~10 min
> **Sources:** [Go SDK — Testing](https://docs.temporal.io/develop/go/best-practices/testing-suite) · [Go SDK — Debugging](https://docs.temporal.io/develop/go/best-practices/debugging) · [`testsuite` package](https://pkg.go.dev/go.temporal.io/sdk/testsuite)

---

## 1. The problem

`PlaceOrderWorkflow` reserves stock, charges the customer and books a shipment. You want to know,
before it reaches production, that:

- the happy path returns a tracking number;
- a declined card stops the order and doesn't ship anything;
- a flaky payment gateway is retried, but not forever;
- a 3-day "remind the customer" timer fires, without the test waiting 3 days;
- tomorrow's version of the code can still resume the orders started today.

A Workflow is not a plain function. It talks to a Temporal Service, its Activities call external
systems, its timers can last months, and its code is **replayed** from history after a crash. A
plain `go test` that calls `PlaceOrderWorkflow(ctx, in)` doesn't work: there is no `workflow.Context`
to pass it. The Go SDK gives you dedicated tools for each of these questions.

---

## 2. Three kinds of tests

The Go SDK docs sort Temporal tests into three kinds:

| Kind | What runs | Example |
| --- | --- | --- |
| **End-to-end** | A Temporal Server, a Worker with all its Workflows and Activities, a Client that starts and interacts with Workflows | Start `PlaceOrderWorkflow` on a dev server and wait for its result |
| **Integration** | Anything between end-to-end and unit | A Workflow run in the test environment with mocked Activities; an Activity run with a mocked context |
| **Unit** | One piece of Workflow or Activity code (a function or method), with everything it calls mocked | A pure helper that computes a shipping fee |

The docs **recommend writing most of your tests as integration tests**. In Go, that mostly means the
Workflow test environment: it runs your real Workflow code, in the test process, with the Activities
you choose to mock.

---

## 3. The four Go tools

![Testing mental model](../img/testing-mental-model.svg)

| Tool | Package | What it runs | What it fakes | Speed |
| --- | --- | --- | --- | --- |
| **Workflow test environment** | `testsuite.TestWorkflowEnvironment` | Your Workflow, as a coroutine in the test process | The Service, time (a mock clock), Activities and Child Workflows you mock | Milliseconds, even for long timers |
| **Activity test environment** | `testsuite.TestActivityEnvironment` | One Activity, called directly | The Activity context, the Worker, the Workflow | As fast as the Activity |
| **Dev server** | `testsuite.StartDevServer` | A real Temporal Service (the CLI's dev server), real Workers, real Client | Only your external systems | Seconds |
| **Workflow replayer** | `worker.WorkflowReplayer` | Today's Workflow code against recorded Event Histories | Everything: no Activity runs | Milliseconds per history |

Two things that the docs say in general terms and that are worth spelling out for Go:

- The Workflow test environment is **not a server**. There is no Temporal Service, no Task Queue
  polling and no network: the environment plays the Service's role in memory. It's what makes these
  tests fast, and it's also why a few behaviors differ from production (covered in
  [chapter 03](03-mocking-activities.md) and later chapters).
- The docs say to "use the test server" for end-to-end and integration tests with Workers "because
  the test server supports skipping time". The Go SDK has **no time-skipping test server**: in Go,
  time skipping lives in the Workflow test environment, and `testsuite.StartDevServer` runs a
  regular dev server where timers take real time.

---

## 4. Which question, which tool

| You want to check… | Tool | Chapter |
| --- | --- | --- |
| The Workflow's logic: order of steps, branches, results, errors | Workflow test environment | [02](02-workflow-test-environment.md) |
| What happens when an Activity fails, is retried, or a Child Workflow returns something | Workflow test environment + mocks | [03](03-mocking-activities.md) |
| An Activity's own logic, its Heartbeats, its reaction to cancellation | Activity test environment | [04](04-testing-activities.md) |
| Timers, Signals, Queries and Updates, without waiting | Workflow test environment (mock clock, delayed callbacks) | [05](05-time-signals-queries.md) |
| A Workflow that calls a Nexus Operation | Workflow test environment + Nexus mocks | [06](06-mocking-nexus.md) |
| That a code change doesn't break running Workflows | Workflow replayer | [07](07-replay-testing.md) |
| That everything is wired together: Worker registration, Task Queue names, data conversion | Dev server | [08](08-end-to-end-dev-server.md) |
| Why a Workflow behaves oddly, in a debugger or in production | `TEMPORAL_DEBUG`, replay, Web UI, CLI | [09](09-debugging.md) |

---

## 5. A first test

The example module has a `PlaceOrderWorkflow` and its Activities. External systems are interfaces,
so tests never call a real payment gateway.

```go
// file: orders/workflow.go
package orders

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

type PlaceOrderInput struct {
	OrderID     string
	CustomerID  string
	SKU         string
	Quantity    int
	AmountCents int64
}

type PlaceOrderResult struct {
	OrderID        string
	PaymentID      string
	TrackingNumber string
}

// PlaceOrderWorkflow reserves stock, charges the customer, then ships.
func PlaceOrderWorkflow(ctx workflow.Context, in PlaceOrderInput) (PlaceOrderResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	var a *Activities // nil: only the method references are used to name the Activities

	if err := workflow.ExecuteActivity(ctx, a.ReserveInventory, in.SKU, in.Quantity).Get(ctx, nil); err != nil {
		return PlaceOrderResult{}, err
	}
	var paymentID string
	if err := workflow.ExecuteActivity(ctx, a.ChargePayment, in.CustomerID, in.AmountCents).Get(ctx, &paymentID); err != nil {
		return PlaceOrderResult{}, err
	}
	var tracking string
	if err := workflow.ExecuteActivity(ctx, a.CreateShipment, in.OrderID).Get(ctx, &tracking); err != nil {
		return PlaceOrderResult{}, err
	}
	return PlaceOrderResult{OrderID: in.OrderID, PaymentID: paymentID, TrackingNumber: tracking}, nil
}
```

```go
// file: orders/activities.go
package orders

import "context"

// External systems, behind interfaces.
type Inventory interface {
	Reserve(ctx context.Context, sku string, qty int) error
}

type PaymentGateway interface {
	Charge(ctx context.Context, customerID string, amountCents int64) (paymentID string, err error)
}

type Carrier interface {
	CreateShipment(ctx context.Context, orderID string) (trackingNumber string, err error)
}

type Activities struct {
	Inventory Inventory
	Payments  PaymentGateway
	Carrier   Carrier
}

func (a *Activities) ReserveInventory(ctx context.Context, sku string, qty int) error {
	return a.Inventory.Reserve(ctx, sku, qty)
}

func (a *Activities) ChargePayment(ctx context.Context, customerID string, amountCents int64) (string, error) {
	return a.Payments.Charge(ctx, customerID, amountCents)
}

func (a *Activities) CreateShipment(ctx context.Context, orderID string) (string, error) {
	return a.Carrier.CreateShipment(ctx, orderID)
}
```

The first test runs the real Workflow in the Workflow test environment and mocks the three
Activities with fixed answers:

```go
// file: orders/workflow_test.go
package orders

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestPlaceOrderWorkflow_HappyPath(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	var a *Activities
	env.OnActivity(a.ReserveInventory, mock.Anything, "SKU-RED-MUG", 2).Return(nil)
	env.OnActivity(a.ChargePayment, mock.Anything, "cus_42", int64(2400)).Return("pay_9001", nil)
	env.OnActivity(a.CreateShipment, mock.Anything, "ord_1001").Return("1Z999AA10123456784", nil)

	env.ExecuteWorkflow(PlaceOrderWorkflow, PlaceOrderInput{
		OrderID: "ord_1001", CustomerID: "cus_42", SKU: "SKU-RED-MUG", Quantity: 2, AmountCents: 2400,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var res PlaceOrderResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, "pay_9001", res.PaymentID)
	require.Equal(t, "1Z999AA10123456784", res.TrackingNumber)
	env.AssertExpectations(t)
}
```

```text
$ go test -v ./orders
=== RUN   TestPlaceOrderWorkflow_HappyPath
--- PASS: TestPlaceOrderWorkflow_HappyPath (0.04s)
PASS
ok  	github.com/acme/shop/orders	0.412s
```

`go test -v` also prints `DEBUG` log lines from the test environment (one per Activity result, for
instance). That's the environment's default logger; they don't mean anything went wrong.

Three things to notice:

- The mocks name Activities through a **nil `*Activities`**: `a.ChargePayment` is only used to find
  the Activity's name and signature, never called. The real struct isn't even registered.
- The first mock argument is the Activity's `context.Context`, matched with `mock.Anything`. The
  others are the Activity's arguments. The `int64(2400)` matters: a plain `2400` is an `int` and
  wouldn't match.
- `env.AssertExpectations(t)` fails the test if a mock you set up was **never called**. It catches
  a Workflow that silently skipped a step.

---

## 6. A strategy that scales

For a Workflow like `PlaceOrderWorkflow`:

1. **Many Workflow-environment tests** ([02](02-workflow-test-environment.md),
   [03](03-mocking-activities.md)): one per branch, error and retry case, with mocked Activities. They
   run in milliseconds, so there can be dozens.
2. **A few Activity-environment tests** per Activity (04), with fakes for the external systems.
3. **A replay test in CI** (07) on histories of real executions, so a code change that would break
   running orders fails the build instead of production.
4. **One or two end-to-end tests** (08) on a dev server, to prove the Worker is wired correctly:
   names, Task Queue, registration, data conversion.

---

## 7. Key takeaways

- A Workflow can't be called like a plain function: test it through the Go SDK's `testsuite`.
- The docs recommend mostly **integration tests**. In Go, that's the Workflow test environment with
  real or mocked Activities.
- Four tools: Workflow test environment, Activity test environment, dev server, Workflow replayer.
  Pick by the question you're asking, not by habit.
- The Workflow test environment is in-process, has a mock clock, and runs no server. The Go SDK has
  no time-skipping test server: the dev server runs in real time.
- `env.AssertExpectations(t)` turns an unused mock into a test failure.
