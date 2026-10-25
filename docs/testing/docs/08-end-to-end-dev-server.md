# 08 · End-to-end tests with a dev server

> **Level:** production · **Read time:** ~14 min
> **Sources:** [Go SDK — Testing](https://docs.temporal.io/develop/go/best-practices/testing-suite) · [`testsuite.StartDevServer`](https://pkg.go.dev/go.temporal.io/sdk/testsuite#StartDevServer) · [`testsuite.DevServerOptions`](https://pkg.go.dev/go.temporal.io/sdk/testsuite#DevServerOptions)

---

## 1. The problem

The test environment of [chapter 02](02-workflow-test-environment.md) runs your Workflow **in memory**:
no Service, no Task Queue, no serialization over gRPC, no Visibility. It can't tell you that:

- the Worker registers the Workflow under a name the starter doesn't use;
- a Signal sent by the real Client has the wrong payload shape;
- a Search Attribute the Workflow sets doesn't exist in the Namespace;
- the Worker can't decode an input because two services use different structs.

The Go docs list **end-to-end** tests: a real Service and a real Worker, driven by a Client. This chapter
shows how to run them from `go test`, with a real dev server and **fake** external systems.

---

## 2. What the SDK gives you: `testsuite.StartDevServer`

`testsuite.StartDevServer` starts the Temporal CLI's dev server (`temporal server start-dev`) as a child
process, waits until it's ready and returns a `*testsuite.DevServer` with a connected Client.

| `DevServerOptions` field | What it does |
| --- | --- |
| `ExistingPath` | Path to a `temporal` binary already installed. Skips the download |
| `CachedDownload` | Otherwise, download the CLI (`Version`: "default", "latest" or `v1.x.y`; `DestDir`: defaults to the OS temp dir) |
| `ClientOptions` | Options of the returned Client. Its `Namespace` (or `default`) is **registered on startup**. With no `HostPort`, a free port is picked |
| `SearchAttributes` | Search Attributes to register (only the names and types are used) |
| `LogLevel`, `LogFormat` | Server logs. Default level is `warn` |
| `EnableUI`, `UIPort` | Start the Web UI too (off by default: `--headless`) |
| `DBFilename` | SQLite file to persist to. Empty: in memory |
| `ExtraArgs` | More `start-dev` flags, such as `--dynamic-config-value` |
| `Stdout`, `Stderr` | Where the process output goes. Default: the test's own output |

On a laptop with the CLI installed, the server was ready in about **0.4 s** on a free port, so one server
per package (started in `TestMain`) is cheap. `srv.Stop()` interrupts the process and waits for it.

### There is no time skipping here

The Go testing page says: "Because the test server supports skipping time, use the test server for both
end-to-end and integration tests with Workers." That sentence is shared across SDKs. In Go:

- The **in-memory** `TestWorkflowEnvironment` skips time ([chapter 05](05-time-signals-queries.md)).
- `testsuite` has only `StartDevServer`. The SDK v1.50.0 source has no time-skipping test server and no
  option to make the dev server skip time.

On the dev server, **Timers run in real time**: a 1 s `workflow.Sleep` took 1.04 s, and a 500 ms Timer
fired after about 1 s (the Service doesn't promise sub-second Timer precision). Make durations inputs
of your Workflow, so end-to-end tests can pass short ones, and keep long-duration logic in
`TestWorkflowEnvironment` tests.

---

## 3. The Workflow under test

`FulfillOrderWorkflow` reserves stock, waits for the warehouse to Signal that the parcel is packed, and
ships it. If nobody packs it in time, it releases the stock and fails with a `PackTimeout` error. A
`status` Query reports the current step.

```go
// file: fulfillment/workflow.go
package fulfillment

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	FulfillOrderWorkflowType = "FulfillOrder"
	ParcelPackedSignal       = "parcel-packed"
	StatusQuery              = "status"
)

type WorkflowRegistry interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
}

func RegisterWorkflows(r WorkflowRegistry) {
	r.RegisterWorkflowWithOptions(FulfillOrderWorkflow, workflow.RegisterOptions{Name: FulfillOrderWorkflowType})
}

type FulfillOrderInput struct {
	OrderID         string
	ShippingAddress string
	// PackTimeout is how long the warehouse has to pack the parcel.
	// An input, not a constant, so tests can make it short.
	PackTimeout time.Duration
}

type ParcelPacked struct {
	WeightGrams int
}

type FulfillOrderResult struct {
	TrackingNumber string
}

func FulfillOrderWorkflow(ctx workflow.Context, in FulfillOrderInput) (FulfillOrderResult, error) {
	status := "reserving-stock"
	if err := workflow.SetQueryHandler(ctx, StatusQuery, func() (string, error) { return status, nil }); err != nil {
		return FulfillOrderResult{}, err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	var a *Activities

	if err := workflow.ExecuteActivity(ctx, a.ReserveStock, in.OrderID).Get(ctx, nil); err != nil {
		return FulfillOrderResult{}, err
	}

	status = "awaiting-pack"
	var parcel ParcelPacked
	packed := false
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	workflow.NewSelector(ctx).
		AddReceive(workflow.GetSignalChannel(ctx, ParcelPackedSignal), func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &parcel)
			packed = true
			cancelTimer()
		}).
		AddFuture(workflow.NewTimer(timerCtx, in.PackTimeout), func(workflow.Future) {}).
		Select(ctx)

	if !packed {
		status = "pack-timed-out"
		if err := workflow.ExecuteActivity(ctx, a.ReleaseStock, in.OrderID).Get(ctx, nil); err != nil {
			return FulfillOrderResult{}, err
		}
		return FulfillOrderResult{}, temporal.NewNonRetryableApplicationError(
			"warehouse did not pack the parcel in time", "PackTimeout", nil, in.OrderID)
	}

	status = "shipping"
	var res FulfillOrderResult
	if err := workflow.ExecuteActivity(ctx, a.CreateShipment, in.OrderID, in.ShippingAddress, parcel.WeightGrams).Get(ctx, &res.TrackingNumber); err != nil {
		return res, err
	}
	status = "shipped"
	return res, nil
}
```

The Activities wrap two external systems behind interfaces, so tests can inject fakes into a **real**
Worker:

```go
// file: fulfillment/activities.go
package fulfillment

import "context"

type Warehouse interface {
	Reserve(ctx context.Context, orderID string) error
	Release(ctx context.Context, orderID string) error
}

type Carrier interface {
	CreateShipment(ctx context.Context, orderID, address string, weightGrams int) (trackingNumber string, err error)
}

type Activities struct {
	Warehouse Warehouse
	Carrier   Carrier
}

func (a *Activities) ReserveStock(ctx context.Context, orderID string) error {
	return a.Warehouse.Reserve(ctx, orderID)
}

func (a *Activities) ReleaseStock(ctx context.Context, orderID string) error {
	return a.Warehouse.Release(ctx, orderID)
}

func (a *Activities) CreateShipment(ctx context.Context, orderID, address string, weightGrams int) (string, error) {
	return a.Carrier.CreateShipment(ctx, orderID, address, weightGrams)
}
```

---

## 4. One server for the whole package: `TestMain`

```go
// file: e2e/main_test.go
package e2e

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

const namespace = "shop-e2e"

var orderIDKey = temporal.NewSearchAttributeKeyKeyword("OrderID")

// srv is shared by every test in the package: starting it once costs well under a second.
var srv *testsuite.DevServer

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run()) // tests call requireServer and skip themselves
	}

	opts := testsuite.DevServerOptions{
		ClientOptions: &client.Options{
			Namespace: namespace, // registered on startup
			// The default logger prints every Workflow and Activity at DEBUG level.
			Logger: log.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr,
				&slog.HandlerOptions{Level: slog.LevelWarn}))),
		},
		SearchAttributes: temporal.NewSearchAttributes(orderIDKey.ValueSet("")),
		LogLevel:         "error",
	}
	// Prefer the CLI already installed (CI image, Homebrew…); otherwise download it once.
	if path, err := exec.LookPath("temporal"); err == nil {
		opts.ExistingPath = path
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	var err error
	srv, err = testsuite.StartDevServer(ctx, opts)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start dev server:", err)
		os.Exit(1)
	}

	code := m.Run()
	srv.Client().Close()
	if err := srv.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop dev server:", err)
	}
	os.Exit(code)
}

func requireServer(t *testing.T) client.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("end-to-end test: needs a dev server (run without -short)")
	}
	return srv.Client()
}
```

Things worth copying:

- **`ExistingPath` first.** CI images and laptops usually have the CLI. Without it, `StartDevServer`
  downloads the CLI into the temp directory on the first run and reuses it after.
- **A dedicated Namespace** (`shop-e2e`) registered by the server itself, and the Search Attributes the
  Workflows use.
- **A quieter logger.** The Client's default logger prints every Workflow and Activity at DEBUG level,
  which buries test failures.
- **`-short` skips everything.** `go test -short ./...` stays fast and needs no binary.

---

## 5. Tests: a real Worker, fake Activities, a real Client

Fakes record what the Activities asked for:

```go
// file: e2e/fakes_test.go
package e2e

import (
	"context"
	"fmt"
	"sync"
)

// fakeWarehouse records calls instead of calling the warehouse API.
type fakeWarehouse struct {
	mu       sync.Mutex
	reserved map[string]bool
	released []string
}

func newFakeWarehouse() *fakeWarehouse { return &fakeWarehouse{reserved: map[string]bool{}} }

func (w *fakeWarehouse) Reserve(_ context.Context, orderID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reserved[orderID] = true
	return nil
}

func (w *fakeWarehouse) Release(_ context.Context, orderID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.reserved, orderID)
	w.released = append(w.released, orderID)
	return nil
}

func (w *fakeWarehouse) Released() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.released...)
}

// fakeCarrier returns a predictable tracking number.
type fakeCarrier struct {
	mu        sync.Mutex
	addresses []string
}

func (c *fakeCarrier) CreateShipment(_ context.Context, orderID, address string, weightGrams int) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addresses = append(c.addresses, address)
	return fmt.Sprintf("TRK-%s-%dg", orderID, weightGrams), nil
}
```

Each test starts its own Worker on its **own Task Queue** and uses its **own Workflow ID**. Tests in a
package share the server; without unique names, one test's Worker could pick up another test's Tasks,
and a Workflow ID still running from another test would make the start fail with `WorkflowExecutionAlreadyStarted`.

```go
// file: e2e/fulfillment_test.go
package e2e

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/fulfillment"
)

// startWorker runs a real Worker with fake Activities on a Task Queue unique to
// this test, so tests can't steal each other's Tasks.
func startWorker(t *testing.T, c client.Client, acts *fulfillment.Activities) string {
	t.Helper()
	taskQueue := "fulfillment-" + strings.ReplaceAll(t.Name(), "/", "-")
	w := worker.New(c, taskQueue, worker.Options{})
	fulfillment.RegisterWorkflows(w)
	w.RegisterActivity(acts)
	if err := w.Start(); err != nil {
		t.Fatal("start worker:", err)
	}
	t.Cleanup(w.Stop)
	return taskQueue
}

func TestFulfillOrder_ShipsPackedParcel(t *testing.T) {
	c := requireServer(t)
	carrier := &fakeCarrier{}
	taskQueue := startWorker(t, c, &fulfillment.Activities{Warehouse: newFakeWarehouse(), Carrier: carrier})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    "fulfill-" + t.Name(),
		TaskQueue:             taskQueue,
		TypedSearchAttributes: temporal.NewSearchAttributes(orderIDKey.ValueSet("ord-1001")),
	}, fulfillment.FulfillOrderWorkflowType, fulfillment.FulfillOrderInput{
		OrderID:         "ord-1001",
		ShippingAddress: "12 rue de la Paix, Paris",
		PackTimeout:     time.Minute,
	})
	if err != nil {
		t.Fatal("start:", err)
	}

	// A Query is answered by the Worker: poll until the Workflow reaches the step.
	waitForStatus(t, ctx, c, run, "awaiting-pack")

	if err := c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), fulfillment.ParcelPackedSignal,
		fulfillment.ParcelPacked{WeightGrams: 850}); err != nil {
		t.Fatal("signal:", err)
	}

	var res fulfillment.FulfillOrderResult
	if err := run.Get(ctx, &res); err != nil {
		t.Fatal("result:", err)
	}
	if res.TrackingNumber != "TRK-ord-1001-850g" {
		t.Errorf("tracking number = %q", res.TrackingNumber)
	}
	if len(carrier.addresses) != 1 || carrier.addresses[0] != "12 rue de la Paix, Paris" {
		t.Errorf("carrier calls = %v", carrier.addresses)
	}

	// Visibility works too: the Search Attribute was registered by DevServerOptions.
	resp, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: namespace,
		Query:     `OrderID = "ord-1001"`,
	})
	if err != nil {
		t.Fatal("count:", err)
	}
	if resp.GetCount() != 1 {
		t.Errorf("Executions for ord-1001 = %d", resp.GetCount())
	}
}

func TestFulfillOrder_ReleasesStockWhenNotPacked(t *testing.T) {
	c := requireServer(t)
	warehouse := newFakeWarehouse()
	taskQueue := startWorker(t, c, &fulfillment.Activities{Warehouse: warehouse, Carrier: &fakeCarrier{}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "fulfill-" + t.Name(),
		TaskQueue: taskQueue,
	}, fulfillment.FulfillOrderWorkflowType, fulfillment.FulfillOrderInput{
		OrderID:     "ord-1002",
		PackTimeout: 500 * time.Millisecond, // real time on a dev server: keep it short
	})
	if err != nil {
		t.Fatal("start:", err)
	}

	err = run.Get(ctx, nil)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != "PackTimeout" {
		t.Fatalf("want PackTimeout, got %v", err)
	}
	if got := warehouse.Released(); len(got) != 1 || got[0] != "ord-1002" {
		t.Errorf("released = %v", got)
	}
	t.Logf("timed out after %s of wall-clock time", time.Since(start).Round(10*time.Millisecond))
}

func waitForStatus(t *testing.T, ctx context.Context, c client.Client, run client.WorkflowRun, want string) {
	t.Helper()
	var got string
	for {
		v, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), fulfillment.StatusQuery)
		if err == nil && v.Get(&got) == nil && got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("status never reached %q (last %q, err %v)", want, got, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
```

```text
$ go test -v ./e2e
=== RUN   TestFulfillOrder_ShipsPackedParcel
--- PASS: TestFulfillOrder_ShipsPackedParcel (0.10s)
=== RUN   TestFulfillOrder_ReleasesStockWhenNotPacked
Temporal CLI 1.9.1 (Server 1.32.0, UI 2.54.1)

Temporal Server:      localhost:58307
Temporal Persistence: in-memory
Temporal Metrics:     http://localhost:58309/metrics
    fulfillment_test.go:113: timed out after 1.02s of wall-clock time
--- PASS: TestFulfillOrder_ReleasesStockWhenNotPacked (1.03s)
PASS
ok  	github.com/acme/shop/e2e	2.636s

$ go test -short -v ./e2e
=== RUN   TestFulfillOrder_ShipsPackedParcel
    fulfillment_test.go:35: end-to-end test: needs a dev server (run without -short)
--- SKIP: TestFulfillOrder_ShipsPackedParcel (0.00s)
…
```

The server's banner lands in the middle of the output because `Stdout` defaults to the test process;
set `DevServerOptions.Stdout` to `io.Discard` to hide it.

Three details in these tests:

- **Queries need a running Worker** and see the state at the last completed Workflow Task, so
  `waitForStatus` polls instead of querying once.
- **The Signal goes through the Service**, encoded by the real data converter: a payload the Workflow
  can't decode would fail here, not in the in-memory tests.
- **Visibility is real**: `CountWorkflow` with `OrderID = "ord-1001"` finds the Execution because the
  Search Attribute was registered by `DevServerOptions`.

---

## 6. Which kind of test for what

| Question | Test |
| --- | --- |
| Does the Workflow take the right branch? Long Timers, retries, failures? | `TestWorkflowEnvironment` ([chapters 02](02-workflow-test-environment.md)–[05](05-time-signals-queries.md)) |
| Does the Activity call the API correctly, Heartbeat, handle cancellation? | `TestActivityEnvironment` ([chapter 04](04-testing-activities.md)) |
| Does a code change break running Executions? | Replay ([chapter 07](07-replay-testing.md)) |
| Do the Worker, the starter, Signals, Queries and Search Attributes fit together? | **Dev server** (this chapter) |

Keep end-to-end tests few and focused on wiring. They are slower (real Timers, real network) and
everything else is better tested in memory.

---

## 7. Key takeaways

- `testsuite.StartDevServer` runs the CLI dev server from `go test`. Use `ExistingPath` when the CLI is installed; otherwise it's downloaded once.
- Start **one server per package** in `TestMain`, with a dedicated Namespace and the Search Attributes you use. Stop it after `m.Run()`.
- Use a **real Worker** with **fake** Activity dependencies, a **unique Task Queue and Workflow ID** per test.
- Go has **no time-skipping server**: Timers run in real time on the dev server. Make durations Workflow inputs.
- Gate the tests behind `testing.Short()`, so `go test -short` stays fast.

⬅️ Previous: [07 · Replay testing](07-replay-testing.md) · ➡️ Next: [09 · Debugging Workflows](09-debugging.md)
