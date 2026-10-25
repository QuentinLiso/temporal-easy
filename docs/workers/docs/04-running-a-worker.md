# 04 · Running a Worker in Go

> **Level:** daily use · **Read time:** ~15 min
> **Sources:** [Go SDK — Run a Worker](https://docs.temporal.io/develop/go/workers/run-worker-process) · [Encyclopedia — Worker Identity](https://docs.temporal.io/workers#worker-identity) · [`worker` package](https://pkg.go.dev/go.temporal.io/sdk/worker)

---

## 1. The problem

The Worker from [chapter 01](01-what-is-a-worker.md) is ten lines and works on a laptop. A Worker
you deploy needs more: register Activities with their dependencies, sometimes several Task Queues
in one binary, credentials for Temporal Cloud, an identity you can trace back to a pod, and a clear
failure when it can't work. This chapter is the Go API for all of that.

---

## 2. The three steps: create, register, run

```text
w := worker.New(c, "orders", worker.Options{}) // 1. a Worker Entity for one Task Queue
w.RegisterWorkflow(orders.PlaceOrderWorkflow)   // 2. what it can execute
w.RegisterActivity(&orders.Activities{…})
err := w.Run(worker.InterruptCh())              // 3. poll until SIGINT/SIGTERM
```

| Step | API | Notes |
| --- | --- | --- |
| Create | `worker.New(client, taskQueue, worker.Options{})` | Reuses the Client's connection. An empty `Options` uses defaults that fit most cases ([chapter 05](05-worker-options.md)) |
| Register | `RegisterWorkflow`, `RegisterActivity`, `RegisterNexusService`, and the `…WithOptions` / `Dynamic…` variants | All registrations **before** `Start`/`Run` |
| Run | `Run(interruptCh)` blocks; `Start()` + `Stop()` for manual control | `worker.InterruptCh()` fires on **SIGINT or SIGTERM** |

---

## 3. Registering Workflows and Activities

### Functions and structs

```go
// file: billing/billing.go
package billing

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "billing"

type Invoice struct {
	InvoiceID   string
	CustomerID  string
	AmountCents int64
}

// Ledger and Mailer are the external systems. Production code wraps a database and an email API.
type Ledger interface {
	Post(ctx context.Context, inv Invoice) error
}

type Mailer interface {
	Send(ctx context.Context, to, subject, body string) error
}

// Activities carries the dependencies. Registering a pointer to it registers every
// exported method as an Activity: PostToLedger and EmailInvoice.
type Activities struct {
	Ledger Ledger
	Mailer Mailer
}

func (a *Activities) PostToLedger(ctx context.Context, inv Invoice) error {
	return a.Ledger.Post(ctx, inv)
}

func (a *Activities) EmailInvoice(ctx context.Context, inv Invoice) error {
	body := fmt.Sprintf("Invoice %s: %.2f EUR", inv.InvoiceID, float64(inv.AmountCents)/100)
	return a.Mailer.Send(ctx, inv.CustomerID, "Your invoice", body)
}

func ProcessInvoiceWorkflow(ctx workflow.Context, inv Invoice) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var a *Activities
	if err := workflow.ExecuteActivity(ctx, a.PostToLedger, inv).Get(ctx, nil); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, a.EmailInvoice, inv).Get(ctx, nil)
}
```

```go
// file: billing/fakes.go
package billing

import (
	"context"
	"log"
)

type LogLedger struct{}

func (LogLedger) Post(_ context.Context, inv Invoice) error {
	log.Printf("ledger: %s %d", inv.InvoiceID, inv.AmountCents)
	return nil
}

type LogMailer struct{}

func (LogMailer) Send(_ context.Context, to, subject, _ string) error {
	log.Printf("mail to %s: %s", to, subject)
	return nil
}
```

- `w.RegisterActivity(&billing.Activities{…})` registers **all exported methods** of the struct.
  This is the recommended way to inject dependencies (DB pools, API clients) into Activities.
- Type names default to the Go function or method name: `ProcessInvoiceWorkflow`, `PostToLedger`.
  Callers in other services or languages use these names.

### Custom names

Use `RegisterWorkflowWithOptions` / `RegisterActivityWithOptions` to choose the Type name, for
example to keep a stable name across a Go refactor:

| Option | Effect |
| --- | --- |
| `workflow.RegisterOptions{Name: "ProcessInvoice"}` | The Workflow Type is `ProcessInvoice` |
| `activity.RegisterOptions{Name: "Billing_"}` **on a struct** | `Name` is a **prefix**: `Billing_PostToLedger`, `Billing_EmailInvoice` |
| `activity.RegisterOptions{Name: "PostToLedger"}` on a function | The Activity Type is that name |
| `worker.Options{DisableRegistrationAliasing: true}` | A function registered under a custom name can **only** be called by that name, not by function reference. Recommended whenever you use custom names |

By default the SDK keeps an **alias**: if a Workflow calls `workflow.ExecuteActivity(ctx, acts.EmailInvoice, …)`
and *this Worker* registered that function as `SendInvoiceEmail`, the call is translated to
`SendInvoiceEmail`. That only works when the Worker running the Workflow has the same registration,
and a Go function name can collide with another type's custom name. With
`DisableRegistrationAliasing: true`, a function reference always means its Go name, so you call
custom-named types by their string name (`ExecuteActivity(ctx, "SendInvoiceEmail", inv)`):
explicit and predictable.

---

## 4. Several Task Queues in one process

A Worker Entity polls **one** Task Queue. To serve several, create several Workers on the **same
Client**. Typical when a small service owns a few queues, or when one queue needs different options
(here: a slow, rate-limited email queue).

```go
// file: internal/workerid/workerid.go
package workerid

import (
	"fmt"
	"os"
)

// Identity returns a Worker identity you can trace back to a pod or task.
// The SDK default is "<pid>@<hostname>@": in a container the pid is usually 1,
// and on ECS or Fargate the hostname is random.
func Identity(service string) string {
	switch {
	case os.Getenv("POD_NAME") != "": // Kubernetes (Downward API)
		return fmt.Sprintf("%s@%s/%s", service, os.Getenv("POD_NAMESPACE"), os.Getenv("POD_NAME"))
	case os.Getenv("ECS_TASK_ID") != "": // set by your task definition / entrypoint
		return fmt.Sprintf("%s@ecs/%s", service, os.Getenv("ECS_TASK_ID"))
	default:
		host, _ := os.Hostname()
		return fmt.Sprintf("%s@%s/%d", service, host, os.Getpid())
	}
}
```

```go
// file: cmd/billing-worker/main.go
package main

import (
	"log"
	"log/slog"
	"os"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/billing"
	"github.com/acme/shop/internal/workerid"
)

const emailTaskQueue = "billing-email"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Address, Namespace and credentials come from TEMPORAL_* env vars or a TOML profile,
	// so the same binary runs against a dev server or Temporal Cloud.
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatalln("load Temporal client options:", err)
	}
	opts.Identity = workerid.Identity("billing-worker") // inherited by every Worker below
	opts.Logger = tlog.NewStructuredLogger(logger)

	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalln("unable to create Temporal client:", err)
	}
	defer c.Close()

	acts := &billing.Activities{Ledger: billing.LogLedger{}, Mailer: billing.LogMailer{}}
	fatal := make(chan error, 2)
	onFatal := func(err error) { fatal <- err }

	// Worker 1: Workflows and ledger Activities on "billing".
	invoices := worker.New(c, billing.TaskQueue, worker.Options{
		DisableRegistrationAliasing: true,
		OnFatalError:                onFatal,
	})
	invoices.RegisterWorkflow(billing.ProcessInvoiceWorkflow)
	invoices.RegisterActivity(acts)

	// Worker 2: emails only, on their own queue, at most 5 per second per Worker.
	emails := worker.New(c, emailTaskQueue, worker.Options{
		DisableWorkflowWorker:     true,
		WorkerActivitiesPerSecond: 5,
		OnFatalError:              onFatal,
	})
	emails.RegisterActivityWithOptions(acts.EmailInvoice, activity.RegisterOptions{Name: "SendInvoiceEmail"})

	for _, w := range []worker.Worker{invoices, emails} {
		if err := w.Start(); err != nil {
			log.Fatalln("unable to start worker:", err)
		}
	}

	select {
	case sig := <-worker.InterruptCh():
		logger.Info("shutting down", "signal", sig)
	case err := <-fatal:
		logger.Error("worker hit a fatal error, shutting down", "error", err)
	}
	emails.Stop()
	invoices.Stop()
}
```

Points to notice:

- **One Client, many Workers.** They share the gRPC connection and the Client's identity, logger
  and Data Converter.
- **`Run` vs `Start`/`Stop`.** `Run` = `Start`, wait for the interrupt channel *or* a fatal error,
  then `Stop`, and return the fatal error if there was one. With several Workers, `Start` each one
  and wait yourself, as above.
- **Startup errors vs fatal errors.** A Namespace that doesn't exist at startup makes `Start`
  return an error (`failed reaching server: Namespace does-not-exist is not found.`, checked on a
  dev server). Once running, a poll that gets a non-retryable error (`NamespaceNotFound`,
  `InvalidArgument`, `ClientVersionNotSupported` in the SDK source) is **fatal**: the Worker calls
  **`OnFatalError`** once and stops itself. Other errors, like a refused credential or the Service
  being down, are retried. Without `OnFatalError`, a `Start`-ed Worker that died is easy to miss:
  the process keeps running and polls nothing. Exit and let your orchestrator restart the pod.
- **`DisableWorkflowWorker: true`** makes an Activity-only Worker. Registering a Workflow on it
  panics (`workflow worker disabled, cannot register workflow`).

---

## 5. Temporal Cloud

Nothing Worker-specific: a Worker uses the Client's connection. Load the Client options from the
environment (`envconfig`) and set `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` and `TEMPORAL_API_KEY`
(or mTLS files) in the deployment. All the details are in the Client guide:
[Temporal Cloud and secured Services](../../client/docs/03-cloud-and-security.md).

---

## 6. Worker Identity

Every poll carries an **identity**. It appears in the Event History (`identity` on
`WorkflowTaskStarted`, `ActivityTaskStarted`, …), in the UI's Workers tab, and in
`temporal task-queue describe`. It's how you answer "which pod ran this Activity?".

| Source | Value |
| --- | --- |
| Encyclopedia ("SDKs set…") | `${pid}@${hostname}` |
| **Go SDK v1.50.0 default** | `<pid>@<hostname>@`, with a **trailing `@`** (the Client's identity; e.g. `70052@ip-10-0-3-17@`) |
| `client.Options.Identity` | Used by the Client **and** every Worker created from it |
| `worker.Options.Identity` | Overrides it for one Worker |

The default is weak in containers: the pid is often `1`, and on ECS the hostname is random. The
Encyclopedia's advice, which `workerid.Identity` above follows: use something **unique**, **traceable**
to a machine, pod or log stream, with useful **context** (service, environment, region), and **short**.

---

## 7. What panics, what doesn't

`worker.New` and the `Register…` calls validate options by **panicking**, so a misconfiguration
crashes at startup (good: before any Task is taken). Checked with SDK v1.50.0:

| Code | Result |
| --- | --- |
| `worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 1}` | panic: `cannot set MaxConcurrentWorkflowTaskExecutionSize to 1` |
| `RegisterWorkflow` on a Worker with `DisableWorkflowWorker: true` | panic: `workflow worker disabled, cannot register workflow` |
| Registering the same Workflow or Activity twice | panic (unless `DisableAlreadyRegisteredCheck`) |
| Versioning options inconsistent ([chapter 11](11-worker-versioning.md)) | panic at `worker.New` or at registration |
| A Worker with **nothing registered** | Starts and polls without error. Every Task it receives fails with "not found" ([chapter 03](03-task-queues.md)) |

The last one is a trap: a Worker that registers nothing is valid as far as the SDK is concerned.

---

## 8. Running locally

```bash
temporal server start-dev                       # terminal 1
go run ./cmd/billing-worker                     # terminal 2
temporal workflow start --task-queue billing --type ProcessInvoiceWorkflow \
  --input '{"InvoiceID":"INV-2026-0042","CustomerID":"cus_829","AmountCents":12900}'
temporal task-queue describe --task-queue billing   # shows your Worker's identity as a poller
```

Run with `POD_NAME=billing-worker-6d4f9 POD_NAMESPACE=shop`, the `Pollers` table of
`temporal task-queue describe` shows `billing-worker@shop/billing-worker-6d4f9` for both the
`workflow` and `activity` pollers (checked on a dev server).

For hot reload during development, the Go SDK docs suggest [`gow`](https://github.com/mitranim/gow):
`gow run ./cmd/billing-worker`.

---

## 9. Key takeaways

- `worker.New(c, taskQueue, worker.Options{})` → `Register…` → `Run(worker.InterruptCh())`. `InterruptCh` handles SIGINT and SIGTERM.
- Register a **struct pointer** to register all its exported methods as Activities with their dependencies. With custom names, set `DisableRegistrationAliasing: true` and call them by name.
- One Worker per Task Queue, all on **one Client**. Use `Start`/`Stop` and `OnFatalError` when you run several.
- Set a meaningful **identity** (`client.Options.Identity`): the Go default `pid@host@` says little in containers.
- Misconfigurations panic at startup; a Worker with no registrations doesn't. Check it polls with `temporal task-queue describe`.
