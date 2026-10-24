# 02 · Defining Workflows in Go

> **Level:** core concept · **Read time:** ~15 min
> **Sources:** [Go SDK — Workflow basics](https://docs.temporal.io/develop/go/workflows/basics) · [Encyclopedia — Workflow Definition](https://docs.temporal.io/workflow-definition)

---

## 1. The problem

Chapter 01 showed a Workflow that "just works". To write your own, you need to know the contract:

- What can the function signature look like?
- What can I pass in and return?
- How does Temporal find my function by name?
- Which Go features am I **not** allowed to use, and what do I use instead?

Get these right and your Workflows stay easy to evolve. Get them wrong and you get serialization
errors, broken starts after a refactor, or non-determinism errors in production.

---

## 2. The signature

A Go Workflow Definition is an **exported function**:

```text
func <Name>(ctx workflow.Context, input <Struct>) (<Result>, error)
func <Name>(ctx workflow.Context, input <Struct>) error
```

| Rule | Why |
| --- | --- |
| First parameter is `workflow.Context` | Almost every Workflow API needs it. It works like `context.Context`, but `Done()` returns a `workflow.Channel` instead of a Go `chan`. |
| Prefer **one struct** parameter | You can add fields without changing the signature, so running Executions keep working. Type-safe client calls work best with one argument. The **Web UI sends one argument**, so a multi-parameter Workflow can't be started from the UI. |
| Parameters and results must be **serializable** | They go through the Data Converter and are stored in the Event History. No channels, functions, variadic params or unsafe pointers. |
| Return `(T, error)` or `error` | The caller receives **either** the result **or** the error, never both. A non-nil error fails the Execution and the value is ignored. |

### Worker crashes are not your problem here

You don't handle Worker-process failures in a Workflow Definition. Temporal recovers the state and
resumes progress after Worker or Service outages. Spend your error handling on **business** failures
(card declined, out of stock), not infrastructure.

---

## 3. Workflow Type: how Temporal finds your function

By default the **Workflow Type** name is the **function name**. You can override it at registration:

```go
// Option A: Workflow Type "MonthlyInvoiceWorkflow"
w.RegisterWorkflow(invoicing.MonthlyInvoiceWorkflow)

// Option B: Workflow Type "monthly-invoice"
w.RegisterWorkflowWithOptions(invoicing.MonthlyInvoiceWorkflow, workflow.RegisterOptions{Name: "monthly-invoice"})
```

A stable custom name is useful when:

- other teams or other languages start your Workflow **by string**,
- you want to rename the Go function without breaking callers,
- you want a short name in dashboards and List Filters.

> ⚠️ Renaming a Workflow Type is not a refactor: running Executions and every caller still use the old
> name. Treat the Type name like a public API.

---

## 4. Write Go, but the Temporal way

Workflow code is re-executed during replay (details in [chapter 03](03-determinism-and-replay.md)),
so some Go constructs are **forbidden** inside a Workflow. The SDK gives you a replacement for each:

| Don't use in a Workflow | Use instead | Why |
| --- | --- | --- |
| `time.Now()` | `workflow.Now(ctx)` | Returns the time of the current Workflow Task, the same on every replay |
| `time.Sleep(d)` | `workflow.Sleep(ctx, d)` / `workflow.NewTimer(ctx, d)` | Durable Timer, survives restarts ([chapter 05](05-timers-selectors-concurrency.md)) |
| `log`, `fmt.Println` | `workflow.GetLogger(ctx)` | Doesn't print duplicate logs during replay |
| `go f()` | `workflow.Go(ctx, f)` | Deterministic scheduling of Workflow coroutines |
| `chan T` | `workflow.NewChannel(ctx)` / `workflow.NewBufferedChannel(ctx, n)` | Channels the SDK scheduler controls |
| `select { … }` | `workflow.NewSelector(ctx)` | Go's `select` picks randomly among ready cases |
| `sync.WaitGroup`, `sync.Mutex` | `workflow.NewWaitGroup(ctx)`, `workflow.NewMutex(ctx)` | Blocking that the SDK understands |
| `context.Context` | `workflow.Context` | Carries Workflow state, options and cancellation |
| `for k := range someMap` | `for _, k := range workflow.DeterministicKeys(someMap)` | Map iteration order is random in Go |
| HTTP, DB, files, other services | an **Activity** | Side effects must be outside replay |
| `rand`, `uuid.New()` | `workflow.SideEffect` or an Activity | The value must be recorded ([chapter 03](03-determinism-and-replay.md)) |

`workflow.DeterministicKeys[K cmp.Ordered, V any](m map[K]V) []K` returns the map keys **sorted**.
For keys that aren't `cmp.Ordered`, use `workflow.DeterministicKeysFunc(m, cmpFn)`.

### What do I know about "myself"?

`workflow.GetInfo(ctx)` returns read-only facts about the current Execution. Reading it doesn't produce
a Command, so you can add or remove these calls freely. Useful fields:

| Field | Example use |
| --- | --- |
| `WorkflowExecution.ID`, `WorkflowExecution.RunID` | Logging, building idempotency keys |
| `WorkflowType.Name` | Shared code that behaves per Type |
| `TaskQueueName`, `Namespace` | Routing child work |
| `Attempt` | Workflow retry attempt (Workflow retries are rare) |
| `WorkflowStartTime` | "How long have we been running?" |
| `FirstRunID`, `OriginalRunID`, `ContinuedExecutionRunID` | Tracking a chain of runs ([chapter 04](04-workflow-execution-and-ids.md), [chapter 09](09-continue-as-new.md)) |
| `CronSchedule`, `ParentWorkflowExecution` | Knowing how you were started |

---

## 5. A realistic Workflow: monthly invoicing

The Workflow below:

- takes one input struct and returns one result struct,
- aggregates usage per product **from a map, deterministically**,
- runs two independent steps **concurrently** with `workflow.Go` and a `workflow.WaitGroup`,
- logs with the replay-safe logger.

### Activities

```go
// file: invoicing/activities.go
package invoicing

import (
	"context"
	"fmt"
)

type InvoiceLine struct {
	ProductCode string
	Units       int64
	AmountCents int64
}

type InvoiceActivities struct{}

func (a *InvoiceActivities) PriceProductUsage(ctx context.Context, productCode string, units int64) (InvoiceLine, error) {
	const unitPriceCents = 12 // in real life: read from the pricing service
	return InvoiceLine{ProductCode: productCode, Units: units, AmountCents: units * unitPriceCents}, nil
}

func (a *InvoiceActivities) RenderInvoicePDF(ctx context.Context, invoiceID string, lines []InvoiceLine) (string, error) {
	return fmt.Sprintf("s3://invoices/%s.pdf", invoiceID), nil
}

func (a *InvoiceActivities) PostToLedger(ctx context.Context, invoiceID string, totalCents int64) (string, error) {
	return "ledger-" + invoiceID, nil
}
```

### Workflow

```go
// file: invoicing/workflow.go
package invoicing

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "invoicing"

// WorkflowTypeName is the stable, public name of this Workflow Type.
const WorkflowTypeName = "monthly-invoice"

type MonthlyInvoiceInput struct {
	CustomerID string
	Period     string           // e.g. "2026-09"
	UsageUnits map[string]int64 // product code -> units consumed
	// New optional fields can be added here later without breaking running Executions.
}

type InvoiceSummary struct {
	InvoiceID  string
	Lines      []InvoiceLine
	TotalCents int64
	PDFURL     string
	LedgerRef  string
}

func MonthlyInvoiceWorkflow(ctx workflow.Context, in MonthlyInvoiceInput) (InvoiceSummary, error) {
	logger := workflow.GetLogger(ctx)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var a *InvoiceActivities

	summary := InvoiceSummary{InvoiceID: fmt.Sprintf("inv-%s-%s", in.CustomerID, in.Period)}
	logger.Info("Building invoice", "invoiceID", summary.InvoiceID, "products", len(in.UsageUnits))

	// ✅ Sorted keys: the Activities are scheduled in the same order on every replay.
	for _, product := range workflow.DeterministicKeys(in.UsageUnits) {
		var line InvoiceLine
		if err := workflow.ExecuteActivity(ctx, a.PriceProductUsage, product, in.UsageUnits[product]).Get(ctx, &line); err != nil {
			return InvoiceSummary{}, err
		}
		summary.Lines = append(summary.Lines, line)
		summary.TotalCents += line.AmountCents
	}

	// Two independent steps, run concurrently with Workflow coroutines.
	var pdfErr, ledgerErr error
	wg := workflow.NewWaitGroup(ctx)
	wg.Add(2)
	workflow.Go(ctx, func(gctx workflow.Context) {
		defer wg.Done()
		pdfErr = workflow.ExecuteActivity(gctx, a.RenderInvoicePDF, summary.InvoiceID, summary.Lines).Get(gctx, &summary.PDFURL)
	})
	workflow.Go(ctx, func(gctx workflow.Context) {
		defer wg.Done()
		ledgerErr = workflow.ExecuteActivity(gctx, a.PostToLedger, summary.InvoiceID, summary.TotalCents).Get(gctx, &summary.LedgerRef)
	})
	wg.Wait(ctx)

	if pdfErr != nil {
		return InvoiceSummary{}, pdfErr
	}
	if ledgerErr != nil {
		return InvoiceSummary{}, ledgerErr
	}

	info := workflow.GetInfo(ctx)
	logger.Info("Invoice ready", "workflowID", info.WorkflowExecution.ID, "elapsed", workflow.Now(ctx).Sub(info.WorkflowStartTime))
	return summary, nil
}
```

> 💡 Workflow coroutines are not OS threads. Only one runs at a time, and the SDK switches between
> them only at blocking calls (`Get`, `Wait`, `Sleep`, …). That's why the two closures can write to
> `summary` without a lock. It's also why you must never use `go` or `sync` inside a Workflow.

### Worker: register under a custom Type name

```go
// file: cmd/invoicing-worker/main.go
package main

import (
	"log"

	"github.com/acme/shop/invoicing"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, invoicing.TaskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(invoicing.MonthlyInvoiceWorkflow, workflow.RegisterOptions{
		Name: invoicing.WorkflowTypeName, // "monthly-invoice"
	})
	w.RegisterActivity(&invoicing.InvoiceActivities{})

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Unable to start Worker", err)
	}
}
```

### Starter: start by Type name

When you pass a **string** instead of a function, the client starts the Workflow by Type name. That
is how another service, which doesn't import your Go package, would start it. You lose compile-time
argument checks, so keep the input struct in a shared package.

```go
// file: cmd/start-monthly-invoice/main.go
package main

import (
	"context"
	"log"

	"github.com/acme/shop/invoicing"
	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	in := invoicing.MonthlyInvoiceInput{
		CustomerID: "c-42",
		Period:     "2026-09",
		UsageUnits: map[string]int64{"api-calls": 120000, "storage-gb": 340, "seats": 12},
	}
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "monthly-invoice-c-42-2026-09",
		TaskQueue: invoicing.TaskQueue,
	}, "monthly-invoice", in) // by Workflow Type name
	if err != nil {
		log.Fatalln("Unable to start Workflow", err)
	}

	var summary invoicing.InvoiceSummary
	if err := run.Get(context.Background(), &summary); err != nil {
		log.Fatalln("Invoice Workflow failed", err)
	}
	log.Printf("Invoice %s: %d cents, pdf=%s", summary.InvoiceID, summary.TotalCents, summary.PDFURL)
}
```

---

## Key takeaways

- Signature: exported func, `workflow.Context` first, **one struct** in, `(T, error)` or `error` out, everything serializable.
- The Workflow Type defaults to the function name. Override it with `RegisterWorkflowWithOptions` and treat it as a public API.
- Use the `workflow.*` replacements for time, sleep, logging, goroutines, channels, select and sync.
- Never `range` over a map directly. Use `workflow.DeterministicKeys`.
- Don't write crash-recovery code in Workflows. Handle business errors only.

⬅️ Previous: [01 · What is a Workflow?](01-what-is-a-workflow.md) · ➡️ Next: [03 · Determinism and replay](03-determinism-and-replay.md)
