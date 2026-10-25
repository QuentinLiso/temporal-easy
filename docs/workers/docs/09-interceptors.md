# 09 · Interceptors: cross-cutting code around every call

> **Level:** advanced · **Read time:** ~15 min
> **Sources:** [Go SDK — Interceptors](https://docs.temporal.io/develop/go/workers/interceptors) · [`go.temporal.io/sdk/interceptor`](https://pkg.go.dev/go.temporal.io/sdk/interceptor) · [Logging Interceptor sample](https://github.com/temporalio/samples-go/tree/main/logger-interceptor)

---

## 1. The problem

Your team wants three things on **every** Workflow and Activity, in every service:

- a log line with the duration and outcome of each Activity,
- an audit log of who started or Signaled which Workflow,
- a tracing span or a tenant header propagated from the HTTP request to the Activity.

Copying that code into 40 Activities is how it gets forgotten in the 41st. **Interceptors** let you
write it once and plug it into the Client and the Worker. The SDK then calls your code around each
operation, the same way an HTTP middleware wraps each handler.

---

## 2. Inbound vs outbound

An interceptor sits on one side of your code:

| Direction | Called when… | Examples of methods | Base type to embed |
| --- | --- | --- | --- |
| **Client outbound** | your code calls the Client | `ExecuteWorkflow`, `SignalWorkflow`, `QueryWorkflow`, `UpdateWorkflow`, `CancelWorkflow`… | `interceptor.ClientOutboundInterceptorBase` |
| **Workflow inbound** | the Worker calls **into** your Workflow | `ExecuteWorkflow`, `HandleSignal`, `HandleQuery`, `ValidateUpdate`, `ExecuteUpdate` | `interceptor.WorkflowInboundInterceptorBase` |
| **Workflow outbound** | your Workflow calls the SDK | `ExecuteActivity`, `NewTimer`, `ExecuteChildWorkflow`, `GetLogger`… | `interceptor.WorkflowOutboundInterceptorBase` |
| **Activity inbound** | the Worker calls **into** your Activity | `ExecuteActivity` | `interceptor.ActivityInboundInterceptorBase` |
| **Activity outbound** | your Activity calls the SDK | `RecordHeartbeat`, `GetLogger`, `GetInfo`… | `interceptor.ActivityOutboundInterceptorBase` |
| **Nexus inbound / outbound** | Nexus Operation handlers | `StartOperation`, `CancelOperation`… | `interceptor.NexusOperationInboundInterceptorBase` |

Each interceptor holds a `Next` field: the next one in the chain. You override the methods you care
about, do your work, and **call `Next`**. Every method you don't override is forwarded to `Next` by the
embedded `…Base` type. If you forget to call `Next`, the operation never happens.

Two entry points build the chains:

- `interceptor.ClientInterceptor` (`InterceptClient`) wraps the Client's outbound calls.
- `interceptor.WorkerInterceptor` (`InterceptActivity`, `InterceptWorkflow`, `InterceptNexusOperation`) is
  called **for each** Workflow, Activity or Nexus execution, and returns its inbound interceptor. The
  inbound interceptor's `Init` can wrap the matching outbound interceptor.

---

## 3. A Client interceptor: audit who starts what

Based on the pattern from the Go SDK page. It logs every Workflow start and Signal sent by the
Client, then forwards the call:

```go
// file: observability/client_audit.go
package observability

import (
	"context"
	"log/slog"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
)

// ClientAudit logs every Workflow start and Signal made through the Client.
type ClientAudit struct {
	interceptor.ClientInterceptorBase
	Logger *slog.Logger
}

func (a *ClientAudit) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &clientAuditOutbound{
		ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next},
		logger:                        a.Logger,
	}
}

type clientAuditOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	logger *slog.Logger
}

func (o *clientAuditOutbound) ExecuteWorkflow(ctx context.Context, in *interceptor.ClientExecuteWorkflowInput) (client.WorkflowRun, error) {
	run, err := o.Next.ExecuteWorkflow(ctx, in)
	o.logger.InfoContext(ctx, "workflow start requested",
		"workflow_type", in.WorkflowType, "workflow_id", in.Options.ID,
		"task_queue", in.Options.TaskQueue, "error", err)
	return run, err
}

func (o *clientAuditOutbound) SignalWorkflow(ctx context.Context, in *interceptor.ClientSignalWorkflowInput) error {
	err := o.Next.SignalWorkflow(ctx, in)
	o.logger.InfoContext(ctx, "signal sent",
		"workflow_id", in.WorkflowID, "signal", in.SignalName, "error", err)
	return err
}
```

---

## 4. A Worker interceptor: measure every Activity, log every Workflow

One `WorkerInterceptor` returns an Activity inbound interceptor and a Workflow inbound interceptor.
The Activity one times each attempt and reports it to a small `ActivityRecorder` interface (in
production, a metrics backend. In tests, a fake). The Workflow one logs with the **replay-safe**
Workflow logger.

```go
// file: observability/worker_interceptor.go
package observability

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

// ActivityRecorder receives one call per Activity attempt.
type ActivityRecorder interface {
	ObserveActivity(activityType string, attempt int32, took time.Duration, err error)
}

// WorkerObservability is registered on worker.Options.Interceptors.
type WorkerObservability struct {
	interceptor.WorkerInterceptorBase
	Recorder ActivityRecorder
}

func (w *WorkerObservability) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &activityTiming{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
		recorder:                       w.Recorder,
	}
}

func (w *WorkerObservability) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &workflowLogging{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
	}
}

type activityTiming struct {
	interceptor.ActivityInboundInterceptorBase
	recorder ActivityRecorder
}

func (a *activityTiming) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	info := activity.GetInfo(ctx)
	start := time.Now() // Activities aren't replayed: wall-clock time is fine here.
	result, err := a.Next.ExecuteActivity(ctx, in)
	a.recorder.ObserveActivity(info.ActivityType.Name, info.Attempt, time.Since(start), err)
	return result, err
}

type workflowLogging struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (w *workflowLogging) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	// workflow.GetLogger skips log lines while the Worker replays history,
	// so each line is written once, not once per replay.
	logger := workflow.GetLogger(ctx)
	info := workflow.GetInfo(ctx)
	logger.Info("workflow started", "workflow_type", info.WorkflowType.Name)
	result, err := w.Next.ExecuteWorkflow(ctx, in)
	if err != nil {
		logger.Error("workflow failed", "workflow_type", info.WorkflowType.Name, "error", err)
	} else {
		logger.Info("workflow completed", "workflow_type", info.WorkflowType.Name)
	}
	return result, err
}

func (w *workflowLogging) HandleSignal(ctx workflow.Context, in *interceptor.HandleSignalInput) error {
	workflow.GetLogger(ctx).Info("signal received", "signal", in.SignalName)
	return w.Next.HandleSignal(ctx, in)
}
```

A simple production recorder that writes one structured log line per attempt (swap it for a
Prometheus or OpenTelemetry histogram when you have one):

```go
// file: observability/slog_recorder.go
package observability

import (
	"context"
	"log/slog"
	"time"
)

// SlogRecorder writes one structured log line per Activity attempt.
type SlogRecorder struct {
	Logger *slog.Logger
}

func (r SlogRecorder) ObserveActivity(activityType string, attempt int32, took time.Duration, err error) {
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelWarn
	}
	r.Logger.Log(context.Background(), level, "activity attempt finished",
		"activity_type", activityType, "attempt", attempt,
		"took_ms", took.Milliseconds(), "error", err)
}
```

> **Workflow interceptors run during replay.** `ExecuteWorkflow` above is called again each time the
> Workflow is rebuilt from its history (after a cache eviction, on another Worker…). Inside Workflow
> interceptors, use only replay-safe APIs: `workflow.GetLogger`, `workflow.Now`, `workflow.SideEffect`,
> `workflow.GetMetricsHandler`. Never `time.Now()`, `rand`, `slog` directly or a network call. Activity
> interceptors don't have this constraint: Activities are never replayed.

---

## 5. Registering them

```go
// file: cmd/worker/main.go
package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/acme/shop/observability"
	"github.com/acme/shop/shipping"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	c, err := client.Dial(client.Options{
		// Client interceptors: wrap the calls this process makes through the Client.
		Interceptors: []interceptor.ClientInterceptor{&observability.ClientAudit{Logger: logger}},
	})
	if err != nil {
		log.Fatalln("unable to create Temporal client:", err)
	}
	defer c.Close()

	w := worker.New(c, shipping.TaskQueue, worker.Options{
		// Worker interceptors: wrap the Workflows and Activities this Worker runs.
		Interceptors: []interceptor.WorkerInterceptor{
			&observability.WorkerObservability{Recorder: observability.SlogRecorder{Logger: logger}},
		},
	})
	w.RegisterWorkflow(shipping.ShipOrderWorkflow)
	w.RegisterActivity(&shipping.Activities{})

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
```

```go
// file: shipping/shipping.go
package shipping

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "shipping"

type ShipOrderInput struct {
	OrderID string
	Address string
}

type Activities struct{}

func (a *Activities) BookCarrier(ctx context.Context, in ShipOrderInput) (string, error) {
	return fmt.Sprintf("TRACK-%s", in.OrderID), nil
}

func ShipOrderWorkflow(ctx workflow.Context, in ShipOrderInput) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var a *Activities
	var tracking string
	err := workflow.ExecuteActivity(ctx, a.BookCarrier, in).Get(ctx, &tracking)
	return tracking, err
}
```

### Order and placement rules

From the `client.Options` and `worker.Options` doc comments (SDK v1.50.0):

| Rule | Consequence |
| --- | --- |
| **Earlier interceptors wrap later ones** | `[]{A, B}` runs `A` → `B` → your code → `B` → `A`. Put tracing first so its span covers the others. |
| A Client interceptor that **also** implements `WorkerInterceptor` (an `interceptor.Interceptor`, embed `interceptor.InterceptorBase`) is applied to **every Worker** created from that Client | One registration covers both sides: what tracing contrib packages do. |
| When there are interceptors in both places, **Client-level ones wrap Worker-level ones** | A Client-level tracer sees the time spent in Worker-level interceptors. |
| **Never register the same interceptor in both** `client.Options` and `worker.Options` | It would run twice per call. |

Before writing your own for tracing, check the contrib packages (`go.temporal.io/sdk/contrib/opentelemetry`,
`…/opentracing`, `…/datadog`): they are interceptors already, and they propagate the trace through
Temporal headers from Client to Workflow to Activity.

---

## 6. Testing an interceptor

The test environment accepts Worker options, interceptors included. This test runs the Workflow with
the interceptor and checks that the Activity attempt was recorded:

```go
// file: observability/worker_interceptor_test.go
package observability_test

import (
	"sync"
	"testing"
	"time"

	"github.com/acme/shop/observability"
	"github.com/acme/shop/shipping"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

type fakeRecorder struct {
	mu    sync.Mutex
	types []string
}

func (f *fakeRecorder) ObserveActivity(activityType string, attempt int32, took time.Duration, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.types = append(f.types, activityType)
}

func TestActivityAttemptsAreRecorded(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	rec := &fakeRecorder{}
	env.SetWorkerOptions(worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{&observability.WorkerObservability{Recorder: rec}},
	})
	env.RegisterActivity(&shipping.Activities{})

	env.ExecuteWorkflow(shipping.ShipOrderWorkflow, shipping.ShipOrderInput{OrderID: "ord-1042", Address: "12 rue de Rivoli"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	if len(rec.types) != 1 || rec.types[0] != "BookCarrier" {
		t.Fatalf("expected one BookCarrier attempt, got %v", rec.types)
	}
}
```

---

## 7. Key takeaways

- Interceptors are **middleware** for Temporal: write cross-cutting logic once, register it once.
- **Inbound** = the SDK calls your Workflow/Activity. **Outbound** = your code calls the SDK or the Client.
- Embed the matching `…Base` type, override what you need, and **always call `Next`**.
- Register Client interceptors in `client.Options.Interceptors` and Worker interceptors in
  `worker.Options.Interceptors`. Earlier wraps later, Client-level wraps Worker-level, never both.
- **Workflow interceptors replay.** Use `workflow.GetLogger` and other replay-safe APIs inside them.
- Prefer the contrib tracing interceptors to a hand-written one. Next: [graceful shutdown](10-graceful-shutdown.md).
