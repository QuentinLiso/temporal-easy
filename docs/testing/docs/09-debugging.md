# 09 · Debugging Workflows

> **Level:** production · **Read time:** ~14 min
> **Sources:** [Go SDK — Debugging](https://docs.temporal.io/develop/go/best-practices/debugging) · [Go SDK — Replay](https://docs.temporal.io/develop/go/best-practices/testing-suite#replay) · [`worker.WorkflowReplayerOptions`](https://pkg.go.dev/go.temporal.io/sdk/worker#WorkflowReplayerOptions) · [CLI — `temporal workflow`](https://docs.temporal.io/cli/workflow)

---

## 1. The problem

Finance reports that invoice `inv-2024-0042` has been "stuck" for an hour. You want to know where the
Workflow is, why, and what the code did with the data it received. Two habits from ordinary Go break
here:

- **Setting a breakpoint in a running Worker** pauses the Workflow goroutine. After one second the
  SDK's deadlock detector fails the Workflow Task.
- **Reading logs** is incomplete: Workflow code runs again on every replay, so the SDK hides log lines
  during replay, and the Worker that ran the interesting part may be long gone.

The Go docs list the tools: a debugger with `TEMPORAL_DEBUG`, the Web UI, the CLI, replay, tracing and
logging. This chapter shows how they fit together on one invoice.

---

## 2. The Workflow we'll debug

`GenerateInvoiceWorkflow` totals the lines, waits for a human approval above 1,000.00, then renders a
PDF. It logs with `workflow.GetLogger`.

```go
// file: invoicing/workflow.go
package invoicing

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	GenerateInvoiceWorkflowType = "GenerateInvoice"
	ApprovalSignal              = "invoice-approved"
	// Invoices above this amount wait for a human approval.
	approvalThresholdCents = 100_000
)

type WorkflowRegistry interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
}

func RegisterWorkflows(r WorkflowRegistry) {
	r.RegisterWorkflowWithOptions(GenerateInvoiceWorkflow, workflow.RegisterOptions{Name: GenerateInvoiceWorkflowType})
}

type InvoiceLine struct {
	SKU            string
	Quantity       int
	UnitPriceCents int64
}

type GenerateInvoiceInput struct {
	InvoiceID  string
	CustomerID string
	Lines      []InvoiceLine
}

type GenerateInvoiceResult struct {
	TotalCents int64
	PDFURL     string
}

func GenerateInvoiceWorkflow(ctx workflow.Context, in GenerateInvoiceInput) (GenerateInvoiceResult, error) {
	// Replay-aware: these lines are not printed again when the Worker replays the history.
	logger := workflow.GetLogger(ctx)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var a *Activities

	total := totalCents(in.Lines)
	logger.Info("Invoice computed", "InvoiceID", in.InvoiceID, "TotalCents", total)

	if total > approvalThresholdCents {
		logger.Info("Waiting for approval", "InvoiceID", in.InvoiceID)
		var approver string
		workflow.GetSignalChannel(ctx, ApprovalSignal).Receive(ctx, &approver)
		logger.Info("Invoice approved", "InvoiceID", in.InvoiceID, "Approver", approver)
	}

	var res GenerateInvoiceResult
	res.TotalCents = total
	if err := workflow.ExecuteActivity(ctx, a.RenderInvoicePDF, in.InvoiceID, total).Get(ctx, &res.PDFURL); err != nil {
		return res, err
	}
	return res, nil
}

func totalCents(lines []InvoiceLine) int64 {
	var total int64
	for _, l := range lines {
		total += int64(l.Quantity) * l.UnitPriceCents
	}
	return total
}
```

```go
// file: invoicing/activities.go
package invoicing

import (
	"context"
	"fmt"
)

type PDFRenderer interface {
	Render(ctx context.Context, invoiceID string, totalCents int64) (url string, err error)
}

type Activities struct {
	Renderer PDFRenderer
}

func (a *Activities) RenderInvoicePDF(ctx context.Context, invoiceID string, totalCents int64) (string, error) {
	url, err := a.Renderer.Render(ctx, invoiceID, totalCents)
	if err != nil {
		return "", fmt.Errorf("render invoice %s: %w", invoiceID, err)
	}
	return url, nil
}
```

---

## 3. Where is it? The CLI and the Web UI

Start with the Service: it knows the Event History and can ask a Worker for the current stack.

```text
$ temporal workflow describe -w inv-2024-0042
Execution Info:
  WorkflowId            inv-2024-0042
  RunId                 01a1751c-ca07-7280-b1ff-a2645badae03
  Type                  GenerateInvoice
  Namespace             default
  TaskQueue             invoicing
  …
  HistoryLength         4
  …
Pending Activities: 0
Pending Child Workflows: 0
```

No pending Activity, four Events: the Workflow is blocked in its own code. `temporal workflow stack`
sends the built-in `__stack_trace` Query, which a Worker answers with the Workflow's goroutine stacks
(the result is a JSON string; `jq -r` makes it readable):

```text
$ temporal workflow stack -w inv-2024-0042 -o json | jq -r '.queryResult[0]' | head -3
coroutine root [blocked on invoice-approved.Receive]:
github.com/acme/shop/invoicing.GenerateInvoiceWorkflow({0x103e88990, 0x6707ab0f7a70}, {{0x6707ab116330, 0xd}, …})
	…/invoicing/workflow.go:54 +0x27c
```

Line 54 is the `Receive` on the `invoice-approved` Signal channel: it's waiting for the approval
nobody sent. A running Worker is required: with no Worker polling `invoicing`, the command waits until it times out
(`Error: program interrupted` after `--command-timeout 15s`).

Other commands you'll use:

| Command | Shows |
| --- | --- |
| `temporal workflow show -w <id>` | The Event History, Event by Event |
| `temporal workflow show -w <id> -o json > h.json` | The same, as a file the replayer reads |
| `temporal workflow trace -w <id>` | Live progress, including Child Workflows |
| `temporal workflow query -w <id> --type <name>` | Your own Query handlers |
| `temporal workflow describe -w <id>` | Status, pending Activities (with last failure and attempt), pending Children |

The Web UI (`http://localhost:8233` with `temporal server start-dev`) shows the same: the Event History,
a timeline, pending Activities with their last error, and a "Stack Trace" tab that runs the same Query.

---

## 4. Why the debugger trips the deadlock detector

A Workflow function runs as a coroutine that must **yield** to the SDK (at a `Future.Get`, a
`Receive`, a `workflow.Sleep`…) within `DeadlockDetectionTimeout`, **1 second** by default. Blocking
Go calls in Workflow code trip it, and so does a debugger paused on a breakpoint.

The test environment reports it as a `*temporal.PanicError`, returned as the Workflow error:

```go
// file: invoicing/deadlock_test.go
package invoicing

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// lookupTaxRateWorkflow does what you must never do in Workflow code: a blocking
// call (here time.Sleep, in real life an HTTP call) that never yields to the SDK.
func lookupTaxRateWorkflow(ctx workflow.Context, country string) (float64, error) {
	time.Sleep(1500 * time.Millisecond)
	return 0.20, nil
}

func TestDeadlockDetectorCatchesBlockingCall(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(lookupTaxRateWorkflow, "FR")

	err := env.GetWorkflowError()
	var panicErr *temporal.PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("want a PanicError, got %v", err)
	}
	if !strings.Contains(panicErr.Error(), "TMPRL1101") {
		t.Fatalf("unexpected panic: %v", panicErr)
	}
	t.Log(panicErr.Error())
}
```

```text
$ go test -run TestDeadlockDetectorCatchesBlockingCall -v ./invoicing
=== RUN   TestDeadlockDetectorCatchesBlockingCall
    deadlock_test.go:36: [TMPRL1101] Potential deadlock detected: workflow goroutine "root" didn't yield for over 1s
--- PASS: TestDeadlockDetectorCatchesBlockingCall (1.00s)
```

On a real Worker, the same error fails the Workflow Task, which is retried. That is good for the bug
above (a blocking call that must become an Activity) and bad when **you** are the one blocking it with
a breakpoint.

### Turning it off while debugging

| Switch | Scope | Effect |
| --- | --- | --- |
| `TEMPORAL_DEBUG` env var | The whole process | No deadlock detection for Workers that don't set `DeadlockDetectionTimeout`; test environment idle timeout raised from 3 s to 24 h |
| `worker.Options.DeadlockDetectionTimeout` | One Worker | A longer limit. An explicit value wins over `TEMPORAL_DEBUG` |
| `WorkflowReplayerOptions.DisableDeadlockDetection` | One replayer | No detection while replaying (section 5) |

The docs say to set `TEMPORAL_DEBUG` to `true`. The SDK checks only that the variable is **not empty**
(`os.Getenv("TEMPORAL_DEBUG") != ""` in `internal/internal_worker.go`), so `TEMPORAL_DEBUG=false` or
`TEMPORAL_DEBUG=0` **also** turns debug mode on. With `TEMPORAL_DEBUG=false`, the test above fails
because the 1.5 s `time.Sleep` no longer trips anything:

```text
$ TEMPORAL_DEBUG=false go test -run TestDeadlockDetectorCatchesBlockingCall -v ./invoicing
    deadlock_test.go:31: want a PanicError, got <nil>
--- FAIL: TestDeadlockDetectorCatchesBlockingCall (1.50s)
```

Unset it (`unset TEMPORAL_DEBUG`), don't set it to `false`. And keep it out of CI and production: with
it, a Workflow stuck in a blocking call hangs its Workflow Task instead of failing fast.

For a local Worker, put the setting in the IDE's launch configuration rather than your shell. For
VS Code with the Go extension:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "invoicing worker (debug)",
      "type": "go",
      "request": "launch",
      "mode": "debug",
      "program": "${workspaceFolder}/cmd/worker",
      "env": { "TEMPORAL_DEBUG": "1", "TEMPORAL_ADDRESS": "localhost:7233" }
    }
  ]
}
```

Or make the timeout a Worker setting that only your local environment sets:

```go
// file: cmd/worker/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/invoicing"
)

type fakeRenderer struct{}

func (fakeRenderer) Render(_ context.Context, invoiceID string, _ int64) (string, error) {
	return fmt.Sprintf("https://files.acme.example/invoices/%s.pdf", invoiceID), nil
}

func main() {
	c, err := client.Dial(client.Options{HostPort: os.Getenv("TEMPORAL_ADDRESS")})
	if err != nil {
		log.Fatalln("dial:", err)
	}
	defer c.Close()

	opts := worker.Options{}
	// Local debugging only: give breakpoints more than the default 1s before the
	// deadlock detector fails the Workflow Task. Never set this in production.
	if d, err := time.ParseDuration(os.Getenv("SHOP_DEADLOCK_TIMEOUT")); err == nil {
		opts.DeadlockDetectionTimeout = d
	}

	w := worker.New(c, "invoicing", opts)
	invoicing.RegisterWorkflows(w)
	w.RegisterActivity(&invoicing.Activities{Renderer: fakeRenderer{}})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker:", err)
	}
}
```

Even without the deadlock detector, a breakpoint in a **live** Worker holds a Workflow Task. If you stay
paused longer than the Workflow Task timeout (10 s by default), the Service times the Task out and
schedules it again. For anything more than a quick look, debug by replay.

---

## 5. Debug a production Execution by replaying it

The history of `inv-2024-0042` contains everything the Workflow saw: its input, the Signal, the
Activity result. Download it once:

```text
$ temporal workflow show -w inv-2024-0042 -o json > invoicing/testdata/inv-2024-0042.json
```

Then replay it under the debugger. The replayer runs your Workflow code locally, with no Service and no
Activities, so you can stop on any line for as long as you like:

```go
// file: invoicing/debug_replay_test.go
package invoicing

import (
	"log/slog"
	"os"
	"testing"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

// TestDebugInvoiceHistory replays one downloaded history so you can step through
// the Workflow in a debugger. Run it from the IDE with a breakpoint in
// GenerateInvoiceWorkflow, or:
//
//	INVOICE_HISTORY=testdata/inv-2024-0042.json go test -run TestDebugInvoiceHistory -v ./invoicing
func TestDebugInvoiceHistory(t *testing.T) {
	file := os.Getenv("INVOICE_HISTORY")
	if file == "" {
		t.Skip("set INVOICE_HISTORY to a file from `temporal workflow show -o json`")
	}

	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		DisableDeadlockDetection: true, // breakpoints can pause as long as you like
		EnableLoggingInReplay:    true, // print workflow.GetLogger lines during replay
	})
	if err != nil {
		t.Fatal(err)
	}
	RegisterWorkflows(replayer)

	logger := log.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(logger, file); err != nil {
		t.Fatalf("replay %s: %v", file, err)
	}
}
```

Set a breakpoint in `GenerateInvoiceWorkflow` and run the test from the IDE ("debug test"), or run it
from the terminal to see the Workflow's own logs:

```text
$ INVOICE_HISTORY=testdata/inv-2024-0042.json go test -run TestDebugInvoiceHistory -v ./invoicing
=== RUN   TestDebugInvoiceHistory
time=… level=INFO msg="Invoice computed" WorkflowType=GenerateInvoice WorkflowID=ReplayId RunID=01a1751c-… Attempt=1 InvoiceID=inv-2024-0042 TotalCents=135000
time=… level=INFO msg="Waiting for approval" WorkflowType=GenerateInvoice WorkflowID=ReplayId … InvoiceID=inv-2024-0042
time=… level=INFO msg="Invoice approved" WorkflowType=GenerateInvoice WorkflowID=ReplayId … Approver=finance-lead@acme.example
--- PASS: TestDebugInvoiceHistory (0.05s)
PASS
```

What the two options do:

- `DisableDeadlockDetection`: breakpoints don't trip `TMPRL1101`.
- `EnableLoggingInReplay`: `workflow.GetLogger` lines are printed even though everything here is a
  replay. With the default (`false`), the same run prints **nothing** but `--- PASS`.

The replayer reports the Workflow ID as `ReplayId`. If your code or logs depend on the real ID, pass it
with `ReplayWorkflowHistoryWithOptions(…, worker.ReplayWorkflowHistoryOptions{OriginalExecution: …})`.

The same test, with an older history and new code, is how you reproduce a `TMPRL1100` non-determinism
error from production: see [chapter 07](07-replay-testing.md).

---

## 6. Logging that survives replay

`workflow.GetLogger(ctx)` returns a logger that:

- adds `WorkflowType`, `WorkflowID`, `RunID`, `Attempt`, `Namespace` and `TaskQueue` to every line;
- **skips** lines while the Worker replays history, so each line appears once even if the Workflow is
  replayed many times.

The Worker log for the real run of `inv-2024-0042` shows each line once, 16 seconds apart, around the
Signal:

```text
2026/10/25 23:09:43 INFO  Invoice computed Namespace default TaskQueue invoicing … WorkflowID inv-2024-0042 … InvoiceID inv-2024-0042 TotalCents 135000
2026/10/25 23:09:43 INFO  Waiting for approval … WorkflowID inv-2024-0042 … InvoiceID inv-2024-0042
2026/10/25 23:09:59 INFO  Invoice approved … WorkflowID inv-2024-0042 … Approver finance-lead@acme.example
```

Don't use the standard `log` or `fmt` in Workflow code: they print again on every replay. In Activities,
use `activity.GetLogger(ctx)`.

---

## 7. Beyond one Execution: metrics and tracing

The debugging page also points to production tools that this guide doesn't cover in detail (the
[Observability guide](../../observability/docs/README.md) does):

- **Tracing** (OpenTelemetry, Datadog, OpenTracing interceptors) follows one request from the Client
  through the Workflow and its Activities: [Go SDK — Tracing](https://docs.temporal.io/develop/go/platform/observability#tracing).
- **SDK metrics** (Workflow Task latency, Activity failures, Task Queue backlog) and the
  [Worker performance guide](https://docs.temporal.io/develop/worker-performance) explain slowness across many Executions.
- **Server metrics**: [Cloud metrics](https://docs.temporal.io/cloud/metrics/openmetrics) or the
  [self-hosted production checklist](https://docs.temporal.io/self-hosted-guide/production-checklist#scaling-and-metrics).

---

## 8. Key takeaways

- Start with `temporal workflow describe`, `show` and `stack` (or the Web UI): they tell you where an Execution is blocked and why.
- A paused breakpoint in Workflow code trips the **1 s deadlock detector** (`TMPRL1101`). Blocking calls in Workflow code trip it too: move them to Activities.
- `TEMPORAL_DEBUG` disables the detector for the whole process when set to **any** non-empty value, `false` included. Unset it when you're done.
- To debug a real Execution, download its history and **replay it locally** with `DisableDeadlockDetection` and `EnableLoggingInReplay`, under the debugger.
- Log with `workflow.GetLogger`: lines carry the Workflow's IDs and aren't repeated on replay.

⬅️ Previous: [08 · End-to-end tests with a dev server](08-end-to-end-dev-server.md)
