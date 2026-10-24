# 09 · The Client in production: lifecycle, observability, tests

> **Level:** production · **Read time:** ~15 min
> **Sources:** [Go SDK — Temporal Client](https://docs.temporal.io/develop/go/client/temporal-client) · [`client.Options`](https://pkg.go.dev/go.temporal.io/sdk/client#Options) · [`go.temporal.io/sdk/mocks`](https://pkg.go.dev/go.temporal.io/sdk/mocks)

---

## 1. The problem

The examples so far dial in `main` and call `log.Fatalln` on errors. A production service needs
more: a startup that fails fast but not forever, logs that show which service made which call,
several Namespaces, a clean shutdown, and **unit tests** for code that calls the Client without
running a Temporal Service.

---

## 2. One Client per process, created at startup

The SDK docs say it in a code comment: *"A Temporal Client should be created once per process."*

| Do | Don't |
| --- | --- |
| Dial once in `main`, pass the `client.Client` to handlers, Activities and the Worker | Dial per request (a new gRPC connection each time) |
| Use the **same** Client for the Worker and the app in one process | Keep two Clients to the same Namespace |
| `defer c.Close()` after everything that uses it has stopped | Close it while a Worker or handler still uses it |
| Use `client.NewClientFromExisting` for a second Namespace (shares the connection) | Dial again for each Namespace |

---

## 3. Identity, logging, metrics

`client.Options` carries more than connection settings:

| Option | What it gives you |
| --- | --- |
| `Identity` | A name recorded on the requests this Client makes (e.g. who started, Signaled or terminated a Workflow) and shown as poller identity for Workers. Defaults to `pid@hostname@`. |
| `Logger` | Where SDK logs go. Wrap your `*slog.Logger` with `log.NewStructuredLogger`. |
| `MetricsHandler` | SDK metrics (request latency, failures, Worker slots). Use a contrib handler (Prometheus via Tally, OpenTelemetry). |
| `DataConverter` | How arguments and results are serialized (e.g. encryption). Must match on Clients and Workers. |
| `Interceptors` | Cross-cutting hooks (tracing, auth headers) around every Client call. |

---

## 4. A production `main`

A checkout service that serves HTTP, runs a Worker in the same process, also talks to the `fraud`
Namespace, and shuts down cleanly on `SIGTERM`:

```go
// file: internal/temporalconn/production.go
package temporalconn

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	tlog "go.temporal.io/sdk/log"
)

// DialService builds options from the environment, adds identity and logging, and
// fails fast if the Service isn't reachable within the timeout.
func DialService(ctx context.Context, service string, logger *slog.Logger, timeout time.Duration) (client.Client, error) {
	opts, err := envconfig.LoadClientOptions(envconfig.LoadClientOptionsRequest{})
	if err != nil {
		return nil, fmt.Errorf("load Temporal client options: %w", err)
	}
	host, _ := os.Hostname()
	opts.Identity = fmt.Sprintf("%s@%s", service, host)
	opts.Logger = tlog.NewStructuredLogger(logger.With("component", "temporal"))

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c, err := client.DialContext(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("dial Temporal at %s: %w", opts.HostPort, err)
	}
	return c, nil
}

// ForNamespace returns a Client for another Namespace that reuses c's connection.
func ForNamespace(c client.Client, namespace string, logger *slog.Logger) (client.Client, error) {
	return client.NewClientFromExisting(c, client.Options{
		Namespace: namespace,
		Logger:    tlog.NewStructuredLogger(logger.With("component", "temporal", "namespace", namespace)),
	})
}
```

```go
// file: orders/orders.go
package orders

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "orders"

type Activities struct {
	Fraud client.Client // Client for the "fraud" Namespace
}

// RequestFraudReview asks the fraud team's Workflow (other Namespace) to review the order.
func (a *Activities) RequestFraudReview(ctx context.Context, orderID string) error {
	_, err := a.Fraud.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "review-" + orderID, TaskQueue: "fraud-reviews",
	}, "ReviewOrderWorkflow", orderID)
	return err
}

func PlaceOrderWorkflow(ctx workflow.Context, orderID string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var a *Activities
	return workflow.ExecuteActivity(ctx, a.RequestFraudReview, orderID).Get(ctx, nil)
}
```

```go
// file: cmd/checkout/main.go
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/internal/temporalconn"
	"github.com/acme/shop/orders"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. One Client for the process.
	c, err := temporalconn.DialService(ctx, "checkout", logger, 10*time.Second)
	if err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}
	defer c.Close() // runs last

	fraud, err := temporalconn.ForNamespace(c, "fraud", logger)
	if err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}

	// 2. A Worker that shares the Client.
	w := worker.New(c, orders.TaskQueue, worker.Options{})
	w.RegisterWorkflow(orders.PlaceOrderWorkflow)
	w.RegisterActivity(&orders.Activities{Fraud: fraud})
	if err := w.Start(); err != nil {
		logger.Error("worker failed to start", "error", err)
		os.Exit(1)
	}

	// 3. The HTTP API, using the same Client.
	srv := &http.Server{Addr: ":8080", Handler: routes(c)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	// 4. Shut down in reverse order: stop taking requests, stop the Worker, then close the Client.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	w.Stop()
}

func routes(c client.Client) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders/{id}", func(rw http.ResponseWriter, r *http.Request) {
		_, err := c.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
			ID: "order-" + r.PathValue("id"), TaskQueue: orders.TaskQueue,
		}, orders.PlaceOrderWorkflow, r.PathValue("id"))
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadGateway)
			return
		}
		rw.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, r *http.Request) {
		if _, err := c.CheckHealth(r.Context(), &client.CheckHealthRequest{}); err != nil {
			http.Error(rw, err.Error(), http.StatusServiceUnavailable)
			return
		}
		rw.WriteHeader(http.StatusOK)
	})
	return mux
}
```

---

## 5. Unit-testing code that uses the Client

Your handlers depend on the `client.Client` **interface**, so tests can pass a mock. The SDK ships
testify-based mocks in `go.temporal.io/sdk/mocks`.

```go
// file: payments/service.go
package payments

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

var ErrDuplicate = errors.New("payment already in progress")

type Service struct {
	Temporal client.Client
}

// StartPayment is the code under test.
func (s *Service) StartPayment(ctx context.Context, orderID string, amountCents int64) (string, error) {
	run, err := s.Temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       "payment-" + orderID,
		TaskQueue:                                "payments",
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, "CollectPaymentWorkflow", orderID, amountCents)
	var already *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &already) {
		return "", ErrDuplicate
	}
	if err != nil {
		return "", err
	}
	return run.GetRunID(), nil
}
```

```go
// file: payments/service_test.go
package payments

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
)

func TestStartPayment_ReturnsRunID(t *testing.T) {
	c := mocks.NewClient(t)
	run := mocks.NewWorkflowRun(t)
	run.On("GetRunID").Return("run-123")
	c.On("ExecuteWorkflow", mock.Anything,
		mock.MatchedBy(func(o client.StartWorkflowOptions) bool { return o.ID == "payment-ORD-9" }),
		"CollectPaymentWorkflow", "ORD-9", int64(4200),
	).Return(run, nil)

	got, err := (&Service{Temporal: c}).StartPayment(context.Background(), "ORD-9", 4200)
	if err != nil || got != "run-123" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestStartPayment_Duplicate(t *testing.T) {
	c := mocks.NewClient(t)
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", ""))

	_, err := (&Service{Temporal: c}).StartPayment(context.Background(), "ORD-9", 4200)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
}
```

For tests that should run real Workflows, use the SDK's test environment (`testsuite`) or start a
throwaway dev server (`temporal server start-dev`) in CI.

---

## 6. Production checklist

| Check | Why |
| --- | --- |
| Connection settings come from `envconfig` (profile or env vars), secrets from a secret store | No hard-coded addresses or keys ([02](02-connecting.md), [03](03-cloud-and-security.md)) |
| `DialContext` with a startup timeout | Fail fast with a clear error |
| One Client per process, shared, closed last | One connection, no use-after-close |
| `Identity` set to service@host | Know who started or terminated what |
| `Logger` and `MetricsHandler` wired | SDK problems show up in your logs and dashboards |
| Workflow IDs from business keys, duplicate policy chosen | Idempotent starts ([05](05-workflow-id-policies.md)) |
| HTTP handlers don't block on `run.Get` | Return 202 and let callers poll, Query, or use an Update |
| Handlers depend on `client.Client` (interface) | Unit tests with `mocks.Client` |

---

## 7. Key takeaways

- Create the Client **once**, share it (Worker included), and close it **last** on shutdown.
- Set `Identity`, `Logger` (`log.NewStructuredLogger`) and `MetricsHandler`. Use `NewClientFromExisting` for other Namespaces.
- `DialContext` with a timeout makes startup failures explicit.
- Code that takes a `client.Client` is unit-testable with `go.temporal.io/sdk/mocks`.
