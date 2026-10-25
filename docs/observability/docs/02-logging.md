# 02 · Logging

> **Level:** foundations · **Read time:** ~13 min
> **Sources:** [Go SDK — Log from a Workflow](https://docs.temporal.io/develop/go/platform/observability#logging) · [`workflow.GetLogger`](https://pkg.go.dev/go.temporal.io/sdk/workflow#GetLogger) · [`activity.GetLogger`](https://pkg.go.dev/go.temporal.io/sdk/activity#GetLogger) · [`log` package](https://pkg.go.dev/go.temporal.io/sdk/log)

---

## 1. The problem

A merchant writes to support: "order A-1001 was charged but the customer never got the confirmation
email". You open the logs. With `fmt.Println` in the Workflow you find "order received" three times
for the same order, no Workflow ID on any line, and the email Activity's error somewhere in another
Worker's output, with nothing linking it to A-1001.

Workflow code needs a logger that:

- knows which Workflow Execution, Activity and attempt each line is about;
- doesn't repeat lines when the Workflow **replays**;
- writes in the format and at the level your log pipeline expects.

The SDK gives you one in Workflows, one in Activities, and a hook to plug in your own backend.

---

## 2. Which logger where

| Code | Logger | Why |
| --- | --- | --- |
| Workflow | `workflow.GetLogger(ctx)` | Replay-aware: silent while the Workflow replays. Adds the Workflow's IDs |
| Activity | `activity.GetLogger(ctx)` | Adds the Activity's and its Workflow's IDs, and the attempt number |
| Client, Worker internals | `client.Options.Logger` | The backend behind both loggers above, and the SDK's own messages |

Don't use `log`, `fmt.Println`, `slog` or a global zap logger **in Workflow code**: they print again on
every replay (section 6), and they know nothing about the Execution. In Activities they don't repeat,
but you lose the fields the SDK adds.

---

## 3. A Workflow and an Activity that log

`log.With` adds fields to every line of a logger: here the order ID, once, instead of on every call.

```go
// file: orders/workflow.go
package orders

import (
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type PlaceOrderInput struct {
	OrderID string
	Email   string
}

func PlaceOrderWorkflow(ctx workflow.Context, in PlaceOrderInput) error {
	logger := log.With(workflow.GetLogger(ctx), "OrderID", in.OrderID)
	logger.Info("Order received")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second},
	})
	var a *Activities
	if err := workflow.ExecuteActivity(ctx, a.ReserveStock, in.OrderID).Get(ctx, nil); err != nil {
		logger.Error("Stock reservation failed", "Error", err)
		return err
	}
	if err := workflow.ExecuteActivity(ctx, a.SendConfirmation, in.OrderID, in.Email).Get(ctx, nil); err != nil {
		logger.Error("Confirmation email failed", "Error", err)
		return err
	}
	logger.Info("Order confirmed")
	return nil
}
```

```go
// file: orders/activities.go
package orders

import (
	"context"
	"errors"
	"sync/atomic"

	"go.temporal.io/sdk/activity"
)

// Mailer sends transactional emails.
type Mailer interface {
	Send(ctx context.Context, to, subject string) error
}

type Activities struct {
	Mailer Mailer
}

func (a *Activities) ReserveStock(ctx context.Context, orderID string) error {
	activity.GetLogger(ctx).Info("Stock reserved")
	return nil
}

func (a *Activities) SendConfirmation(ctx context.Context, orderID, email string) error {
	logger := activity.GetLogger(ctx)
	if attempt := activity.GetInfo(ctx).Attempt; attempt > 1 {
		logger.Warn("Retrying confirmation email", "PreviousAttempts", attempt-1)
	}
	if err := a.Mailer.Send(ctx, email, "Your order "+orderID); err != nil {
		return err // the SDK logs failed attempts itself; don't log it twice
	}
	logger.Info("Confirmation email sent", "To", email)
	return nil
}

// FlakyMailer fails its first call, like an SMTP relay that is restarting.
type FlakyMailer struct{ calls atomic.Int32 }

func (m *FlakyMailer) Send(ctx context.Context, to, subject string) error {
	if m.calls.Add(1) == 1 {
		return errors.New("smtp: 421 service not available")
	}
	return nil
}
```

---

## 4. Plugging in your logger: `log.NewStructuredLogger`

`client.Options.Logger` takes a `log.Logger`. `log.NewStructuredLogger` turns any `*slog.Logger` into
one, so `slog` is the bridge to whatever your platform uses:

```go
// file: internal/logging/logging.go
package logging

import (
	"log/slog"
	"os"

	"go.temporal.io/sdk/log"
)

// New returns the SDK logger for a service: JSON (for the log pipeline) or text
// (for a terminal), filtered at the given level, with a "service" field on every line.
func New(service, format string, level slog.Level) log.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler = slog.NewJSONHandler(os.Stdout, opts)
	if format == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return log.NewStructuredLogger(slog.New(handler).With("service", service))
}
```

Already on zap? Bridge it through `zapslog`, as the docs do:

```go
// file: internal/logging/zap.go
package logging

import (
	"log/slog"

	"go.temporal.io/sdk/log"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
)

// NewZap returns the SDK logger backed by zap, and a function to flush it on exit.
func NewZap(service string) (log.Logger, func() error, error) {
	z, err := zap.NewProduction()
	if err != nil {
		return nil, nil, err
	}
	logger := slog.New(zapslog.NewHandler(z.Core())).With("service", service)
	return log.NewStructuredLogger(logger), z.Sync, nil
}
```

The Worker picks the logger from flags. `-log-format sdk-default` leaves `Logger` unset, to show what
the SDK does on its own. `-no-cache` and `-log-replay` exist only to show replay in section 6:

```go
// file: cmd/worker/main.go
package main

import (
	"flag"
	"log"
	"log/slog"
	"os"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/internal/logging"
	"github.com/acme/shop/orders"
)

func main() {
	format := flag.String("log-format", "json", "json, text, zap or sdk-default")
	level := flag.String("log-level", "info", "debug, info, warn or error")
	noCache := flag.Bool("no-cache", false, "demo only: replay the Workflow on every Workflow Task")
	logReplay := flag.Bool("log-replay", false, "demo only: also log while replaying")
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*level)); err != nil {
		log.Fatalln("log level:", err)
	}
	var logger sdklog.Logger
	switch *format {
	case "sdk-default":
		// Logger stays nil: the SDK uses its default logger.
	case "zap":
		zl, flush, err := logging.NewZap("orders-worker")
		if err != nil {
			log.Fatalln("zap:", err)
		}
		defer func() { _ = flush() }()
		logger = zl
	default:
		logger = logging.New("orders-worker", *format, lvl)
	}

	c, err := client.Dial(client.Options{
		HostPort: os.Getenv("TEMPORAL_ADDRESS"), // empty: localhost:7233
		Logger:   logger,
	})
	if err != nil {
		log.Fatalln("dial:", err)
	}
	defer c.Close()

	if *noCache {
		worker.SetStickyWorkflowCacheSize(0)
	}
	w := worker.New(c, "orders", worker.Options{EnableLoggingInReplay: *logReplay})
	w.RegisterWorkflow(orders.PlaceOrderWorkflow)
	w.RegisterActivity(&orders.Activities{Mailer: &orders.FlakyMailer{}})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker:", err)
	}
}
```

```go
// file: cmd/starter/main.go
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/acme/shop/orders"
)

func main() {
	orderID := flag.String("order", "A-1001", "order ID")
	flag.Parse()

	c, err := client.Dial(client.Options{HostPort: os.Getenv("TEMPORAL_ADDRESS")})
	if err != nil {
		log.Fatalln("dial:", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "order-" + *orderID,
		TaskQueue: "orders",
	}, orders.PlaceOrderWorkflow, orders.PlaceOrderInput{OrderID: *orderID, Email: "jane@example.com"})
	if err != nil {
		log.Fatalln("start:", err)
	}
	if err := run.Get(context.Background(), nil); err != nil {
		log.Fatalln("order failed:", err)
	}
	log.Println("order", *orderID, "confirmed")
}
```

---

## 5. What the SDK adds to each line

Run the Worker with `-log-format json` and start one order. Every line carries the Execution's
identity without you passing it. Three of the lines, pretty-printed:

```text
{
  "time": "2026-10-25T23:31:59.174642+01:00",
  "level": "INFO",
  "msg": "Order received",
  "service": "orders-worker",
  "Namespace": "default",
  "TaskQueue": "orders",
  "WorkerID": "98156@J9L636DG4L@",
  "WorkflowType": "PlaceOrderWorkflow",
  "WorkflowID": "order-A-1001",
  "RunID": "01a17531-2bc0-7985-8b60-003b11b978a7",
  "Attempt": 1,
  "OrderID": "A-1001"
}
{
  "time": "2026-10-25T23:31:59.181218+01:00",
  "level": "ERROR",
  "msg": "Activity error.",
  "service": "orders-worker",
  "Namespace": "default",
  "TaskQueue": "orders",
  "WorkerID": "98156@J9L636DG4L@",
  "WorkflowID": "order-A-1001",
  "RunID": "01a17531-2bc0-7985-8b60-003b11b978a7",
  "ActivityType": "SendConfirmation",
  "Attempt": 1,
  "Error": "smtp: 421 service not available"
}
{
  "time": "2026-10-25T23:32:00.188924+01:00",
  "level": "WARN",
  "msg": "Retrying confirmation email",
  "service": "orders-worker",
  "Namespace": "default",
  "TaskQueue": "orders",
  "WorkerID": "98156@J9L636DG4L@",
  "ActivityID": "11",
  "ActivityType": "SendConfirmation",
  "Attempt": 2,
  "WorkflowType": "PlaceOrderWorkflow",
  "WorkflowID": "order-A-1001",
  "RunID": "01a17531-2bc0-7985-8b60-003b11b978a7",
  "PreviousAttempts": 1
}
```

| Logger | Fields added by the SDK |
| --- | --- |
| Worker messages ("Started Worker") | `Namespace`, `TaskQueue`, `WorkerID` |
| The SDK's own "Activity error." line, one per failed attempt | `Namespace`, `TaskQueue`, `WorkerID`, `WorkflowID`, `RunID`, `ActivityType`, `Attempt`, `Error` |
| `workflow.GetLogger` | the above, plus `WorkflowType`, `WorkflowID`, `RunID`, `Attempt` |
| `activity.GetLogger` | `Namespace`, `TaskQueue`, `WorkerID`, `ActivityID`, `ActivityType`, `Attempt`, `WorkflowType`, `WorkflowID`, `RunID` |

`Attempt` in an Activity line is the Activity's attempt, so the retried email shows `Attempt: 2`. In a
Workflow line it is the Workflow's own attempt (a Workflow with a Retry Policy), not the Workflow
Task's.

Search your log backend for `WorkflowID = "order-A-1001"` and you get the Workflow's lines **and**
its Activities' lines, from every Worker that ran them.

---

## 6. Replay: why `workflow.GetLogger` stays quiet

A Worker replays a Workflow from its Event History whenever it no longer has it in its cache: after a
restart, a deploy, or when the cache is full. Replaying re-runs your Workflow code from the start, so
every log statement before the current point runs **again**.

`workflow.GetLogger` returns a logger that drops lines while the Workflow is replaying. To see what it
saves you from, we ran the Worker with `-no-cache`, which forces a full replay on every Workflow Task
(this order has three: start, after `ReserveStock`, after `SendConfirmation`), and `-log-replay`, which
keeps logging during replay. Output in `text` format, with `Namespace`, `TaskQueue`, `WorkerID` and
`RunID` removed:

```text
level=INFO msg="Order received" WorkflowType=PlaceOrderWorkflow WorkflowID=order-A-1004 Attempt=1 OrderID=A-1004
level=INFO msg="Stock reserved" ActivityID=5 ActivityType=ReserveStock Attempt=1 WorkflowType=PlaceOrderWorkflow WorkflowID=order-A-1004
level=INFO msg="Order received" WorkflowType=PlaceOrderWorkflow WorkflowID=order-A-1004 Attempt=1 OrderID=A-1004
level=ERROR msg="Activity error." WorkflowID=order-A-1004 ActivityType=SendConfirmation Attempt=1 Error="smtp: 421 service not available"
level=WARN msg="Retrying confirmation email" ActivityID=11 ActivityType=SendConfirmation Attempt=2 WorkflowType=PlaceOrderWorkflow WorkflowID=order-A-1004 PreviousAttempts=1
level=INFO msg="Confirmation email sent" ActivityID=11 ActivityType=SendConfirmation Attempt=2 WorkflowType=PlaceOrderWorkflow WorkflowID=order-A-1004 To=jane@example.com
level=INFO msg="Order received" WorkflowType=PlaceOrderWorkflow WorkflowID=order-A-1004 Attempt=1 OrderID=A-1004
level=INFO msg="Order confirmed" WorkflowType=PlaceOrderWorkflow WorkflowID=order-A-1004 Attempt=1 OrderID=A-1004
```

| Worker flags | "Order received" lines for one order |
| --- | --- |
| default (sticky cache on) | 1 |
| `-no-cache` | 1: replays are silent |
| `-no-cache -log-replay` (`EnableLoggingInReplay: true`) | 3: once live, then once per replay |

"Order received" comes back at each replay, interleaved with the live lines. `EnableLoggingInReplay`
is for debugging the replay itself: leave it off in production. A plain `slog.Info` or `fmt.Println` in
the Workflow has no replay filter at all: it prints on every replay, whatever the flag.

`workflow.IsReplaying(ctx)` tells you whether the Workflow is replaying, if you build your own
replay-aware helper (for example a metric that must not be counted twice). For logs,
`workflow.GetLogger` already does it.

---

## 7. The default logger prints DEBUG

If you leave `client.Options.Logger` unset, the SDK uses its own logger: Go's standard `log` writing to
**stdout**, with **no level filter**. With `-log-format sdk-default`:

```text
2026/10/25 23:32:01 INFO  Started Worker Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@
2026/10/25 23:32:02 INFO  Order received Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@ WorkflowType PlaceOrderWorkflow WorkflowID order-A-1002 RunID 01a17531-37fb-7ca2-8841-9a20c9343d64 Attempt 1 OrderID A-1002
2026/10/25 23:32:02 DEBUG ExecuteActivity Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@ WorkflowType PlaceOrderWorkflow WorkflowID order-A-1002 RunID 01a17531-37fb-7ca2-8841-9a20c9343d64 Attempt 1 ActivityID 5 ActivityType ReserveStock
2026/10/25 23:32:02 INFO  Stock reserved Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@ ActivityID 5 ActivityType ReserveStock Attempt 1 WorkflowType PlaceOrderWorkflow WorkflowID order-A-1002 RunID 01a17531-37fb-7ca2-8841-9a20c9343d64
2026/10/25 23:32:02 DEBUG ExecuteActivity Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@ WorkflowType PlaceOrderWorkflow WorkflowID order-A-1002 RunID 01a17531-37fb-7ca2-8841-9a20c9343d64 Attempt 1 ActivityID 11 ActivityType SendConfirmation
2026/10/25 23:32:02 ERROR Activity error. Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@ WorkflowID order-A-1002 RunID 01a17531-37fb-7ca2-8841-9a20c9343d64 ActivityType SendConfirmation Attempt 1 Error smtp: 421 service not available
2026/10/25 23:32:03 WARN  Retrying confirmation email Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@ ActivityID 11 ActivityType SendConfirmation Attempt 2 WorkflowType PlaceOrderWorkflow WorkflowID order-A-1002 RunID 01a17531-37fb-7ca2-8841-9a20c9343d64 PreviousAttempts 1
2026/10/25 23:32:03 INFO  Confirmation email sent Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@ ActivityID 11 ActivityType SendConfirmation Attempt 2 WorkflowType PlaceOrderWorkflow WorkflowID order-A-1002 RunID 01a17531-37fb-7ca2-8841-9a20c9343d64 To jane@example.com
2026/10/25 23:32:03 INFO  Order confirmed Namespace default TaskQueue orders WorkerID 98171@J9L636DG4L@ WorkflowType PlaceOrderWorkflow WorkflowID order-A-1002 RunID 01a17531-37fb-7ca2-8841-9a20c9343d64 Attempt 1 OrderID A-1002
```

The `DEBUG` lines (`ExecuteActivity`) come from the SDK itself. The Go Observability page
says "The Temporal SDK core normally uses `WARN` as its default logging level". The Go SDK isn't built on
that core, and its default logger is `log.New(os.Stdout, "", log.LstdFlags)` with no filter
(`internal/log/default_logger.go`). Set a `Logger` with a level in every process,
including starters and admin tools.

---

## 8. Levels and what to log

| Level | Use it for | Example |
| --- | --- | --- |
| `Debug` | Detail you turn on to investigate | the SDK's own `ExecuteActivity`, `NewTimer` lines |
| `Info` | Business milestones of an Execution | "Order received", "Order confirmed" |
| `Warn` | Something odd the code recovered from | "Retrying confirmation email" on attempt 2 |
| `Error` | A step the Workflow gives up on | "Confirmation email failed" before returning the error |

Two habits keep the logs useful:

- **Don't log an Activity's error and return it.** The Worker already logs every failed Activity attempt
  ("Activity error." in section 5), and the Workflow logs the final failure. A third copy only adds
  noise. Note that the SDK logs that line at **`ERROR`** even when the attempt will be retried: alert on
  Workflow failures or on metrics ([chapter 05](05-metrics.md)), not on every `ERROR` line.
- **Put identifiers in fields, not in the message.** `logger.Info("Order received", "OrderID", id)`, not
  `logger.Info("Order " + id + " received")`: the first can be searched and grouped.

Which merchant an order belongs to is a field you want on every line too, but it isn't in the Workflow
input. [Chapter 03](03-context-propagation.md) carries it in the context and adds it with `log.With`.

---

## 9. Key takeaways

- In Workflows use `workflow.GetLogger(ctx)`, in Activities `activity.GetLogger(ctx)`. Never `fmt`,
  `log` or `slog` directly in Workflow code.
- The SDK adds `WorkflowID`, `RunID`, `WorkflowType`, `Attempt` (and `ActivityID`, `ActivityType` for
  Activities) to every line, so you can follow one Execution across Workers.
- `workflow.GetLogger` is silent during replay. `EnableLoggingInReplay` turns that off, for debugging only.
- Set `client.Options.Logger` with `log.NewStructuredLogger(slog.New(handler))`. `slog` bridges to zap and
  others.
- The Go SDK's default logger prints `DEBUG` to stdout. It isn't `WARN`, whatever the docs say.
- `log.With(logger, "OrderID", id)` adds fields once for a whole function.
