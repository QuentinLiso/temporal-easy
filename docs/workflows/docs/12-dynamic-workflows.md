# 12 · Dynamic Workflows

> **Level:** sub-feature (advanced) · **Read time:** ~10 min
> **Sources:** [Encyclopedia — Dynamic Handler](https://docs.temporal.io/dynamic-handler) · [Go SDK — Dynamic Workflow](https://docs.temporal.io/develop/go/workflows/dynamic-workflow) · [Encyclopedia — Workflow Type](https://docs.temporal.io/workflow-definition#workflow-type)

---

## 1. The problem

Usually you know every Workflow Type at compile time. You write `AbandonedCartWorkflow`, register it,
and clients start `AbandonedCartWorkflow`.

Some products don't work that way. Think of a marketing platform where **each tenant designs its own
automations** in a UI: "when a cart is abandoned, wait 2 hours, send email A, wait 1 day, tag the
customer". The automations live in a database. Their names (`automation.abandoned-cart`,
`automation.win-back`, …) are **data**, not Go functions. You can't write and deploy one Go function per
tenant automation.

A **Dynamic Workflow** solves this. It is a single, unnamed Workflow function that the Worker runs
**when no Workflow with the requested Workflow Type name is registered**. It receives the Workflow Type
name at runtime and decides what to do with it.

> ⚠️ The Encyclopedia is clear: Dynamic Handlers are a **fallback**, not the main way to structure
> your code. Overusing them makes code harder to maintain and debug. Define Workflows statically
> whenever you can. Reserve dynamic handling for names you really can't know at compile time.

---

## 2. Dynamic Handlers in general

A **Dynamic Handler** is an unnamed handler invoked when no statically defined handler with the
given name exists. Temporal has the concept for Workflows, Activities, Signals, Queries and Updates,
but support differs per SDK.

What the Go SDK v1.50.0 gives you:

| Dynamic … | Go API | Covered in |
| --- | --- | --- |
| Workflow | `worker.RegisterDynamicWorkflow(fn, workflow.DynamicRegisterOptions{})` | this chapter |
| Activity | `worker.RegisterDynamicActivity(fn, activity.DynamicRegisterOptions{})` | Activities roadmap, chapter 09 |
| Signal / Query / Update | **no dynamic handler API** in the `workflow` package | — |

> 📝 I checked the `go.temporal.io/sdk/workflow` package in v1.50.0: there is no
> `SetDynamic…Handler` function. Signal, Query and Update handlers are always registered by name
> (`GetSignalChannel`, `SetQueryHandler`, `SetUpdateHandler`; see [06 · Message passing](06-message-passing.md)).
> `workflow.GetUnhandledSignalNames(ctx)` tells you which Signal names arrived with no channel open,
> which helps with debugging but is not a handler.

---

## 3. The rules (Go SDK)

- Register it with `w.RegisterDynamicWorkflow(fn, workflow.DynamicRegisterOptions{})`. It must be
  registered on the Worker before it can be invoked.
- **Only one** Dynamic Workflow per Worker.
- Its signature takes **exactly one** argument after `ctx`: `converter.EncodedValues`. You decode the
  real arguments yourself with `args.Get(&a, &b, …)`.
- It returns the usual `(T, error)` or `error`.
- **Registered Workflow Types always win.** The dynamic one only runs for names that nothing else
  claims on that Worker.
- Inside it, `workflow.GetInfo(ctx).WorkflowType.Name` tells you which name the client asked for.
- Everything else is a normal Workflow: same determinism rules ([03](03-determinism-and-replay.md)),
  same Activities, Timers, Signals and versioning.

### Optional: `LoadDynamicRuntimeOptions`

`workflow.DynamicRegisterOptions` has a single field in v1.50.0:

```go
LoadDynamicRuntimeOptions func(details workflow.LoadDynamicRuntimeOptionsDetails) (workflow.DynamicRuntimeOptions, error)
```

The SDK calls it with the Workflow Type (`details.WorkflowType`) and lets you return per-type runtime
options. Today `DynamicRuntimeOptions` carries only a `VersioningBehavior`. Per the SDK doc comment,
you need it when you use Worker Versioning and the Worker has no default versioning behavior (see
[11 · Versioning](11-versioning.md)). If you don't use Worker Versioning, leave the options empty.

---

## 4. Example: tenant-configured marketing automations

### The definitions and the Activities

An automation is a list of steps stored per tenant. The Workflow loads the definition through an
Activity, because reading a database is I/O and must not happen in Workflow code.

```go
// file: automations/definitions.go
package automations

import (
	"context"
	"errors"
	"time"
)

// StepKind is one of the building blocks a tenant can use in the automation designer.
type StepKind string

const (
	StepWait      StepKind = "wait"
	StepSendEmail StepKind = "send-email"
	StepAddTag    StepKind = "add-tag"
)

type AutomationStep struct {
	Kind       StepKind
	Wait       time.Duration // for StepWait
	TemplateID string        // for StepSendEmail
	Tag        string        // for StepAddTag
}

type AutomationDefinition struct {
	Name  string // e.g. "automation.abandoned-cart"
	Steps []AutomationStep
}

// AutomationTrigger is the single argument every automation receives.
type AutomationTrigger struct {
	TenantID   string
	CustomerID string
	Email      string
	Context    map[string]string // e.g. {"cartId": "c-123"}
}

// DefinitionStore is the tenant's automation table (Postgres, DynamoDB...).
type DefinitionStore interface {
	Find(ctx context.Context, tenantID, name string) (*AutomationDefinition, error)
}

// Mailer and CRM are the external systems the steps talk to.
type Mailer interface {
	Send(ctx context.Context, tenantID, to, templateID string, vars map[string]string) error
}

type CRM interface {
	AddTag(ctx context.Context, tenantID, customerID, tag string) error
}

type AutomationActivities struct {
	Store  DefinitionStore
	Mailer Mailer
	CRM    CRM
}

// ErrUnknownAutomation is the cause attached when a tenant has no such automation.
var ErrUnknownAutomation = errors.New("unknown automation")

// LoadAutomation returns nil, nil when the automation doesn't exist, so the Workflow decides what to do.
func (a *AutomationActivities) LoadAutomation(ctx context.Context, tenantID, name string) (*AutomationDefinition, error) {
	return a.Store.Find(ctx, tenantID, name)
}

func (a *AutomationActivities) SendTemplatedEmail(ctx context.Context, t AutomationTrigger, templateID string) error {
	return a.Mailer.Send(ctx, t.TenantID, t.Email, templateID, t.Context)
}

func (a *AutomationActivities) TagCustomer(ctx context.Context, t AutomationTrigger, tag string) error {
	return a.CRM.AddTag(ctx, t.TenantID, t.CustomerID, tag)
}
```

### The Dynamic Workflow

```go
// file: automations/run_automation.go
package automations

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const AutomationPrefix = "automation."

type AutomationResult struct {
	Automation    string
	StepsExecuted int
}

// RunAutomation handles EVERY Workflow Type that isn't registered by name on this Worker.
func RunAutomation(ctx workflow.Context, args converter.EncodedValues) (AutomationResult, error) {
	info := workflow.GetInfo(ctx)
	name := info.WorkflowType.Name // e.g. "automation.abandoned-cart"
	logger := workflow.GetLogger(ctx)

	// Unknown naming scheme: fail now and for good. Without this, a typo in a client would
	// leave the Workflow retrying or hanging with nobody noticing.
	if !strings.HasPrefix(name, AutomationPrefix) {
		return AutomationResult{}, temporal.NewApplicationErrorWithOptions(
			fmt.Sprintf("no Workflow registered for type %q", name),
			"UnknownWorkflowType",
			temporal.ApplicationErrorOptions{NonRetryable: true},
		)
	}

	// Decode the single argument we expect.
	var trigger AutomationTrigger
	if err := args.Get(&trigger); err != nil {
		return AutomationResult{}, temporal.NewNonRetryableApplicationError(
			"cannot decode AutomationTrigger for "+name, "BadAutomationInput", err)
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var a *AutomationActivities

	// Load the tenant's definition through an Activity (database I/O).
	var def *AutomationDefinition
	if err := workflow.ExecuteActivity(ctx, a.LoadAutomation, trigger.TenantID, name).Get(ctx, &def); err != nil {
		return AutomationResult{}, err
	}
	if def == nil {
		return AutomationResult{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("tenant %s has no automation %q", trigger.TenantID, name), "UnknownAutomation", ErrUnknownAutomation)
	}

	logger.Info("Running automation", "automation", name, "tenant", trigger.TenantID, "steps", len(def.Steps))
	for i, step := range def.Steps {
		var err error
		switch step.Kind {
		case StepWait:
			err = workflow.Sleep(ctx, step.Wait) // durable Timer: can be hours or days
		case StepSendEmail:
			err = workflow.ExecuteActivity(ctx, a.SendTemplatedEmail, trigger, step.TemplateID).Get(ctx, nil)
		case StepAddTag:
			err = workflow.ExecuteActivity(ctx, a.TagCustomer, trigger, step.Tag).Get(ctx, nil)
		default:
			err = temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("step %d: unsupported kind %q", i, step.Kind), "UnsupportedStep", nil)
		}
		if err != nil {
			return AutomationResult{Automation: name, StepsExecuted: i}, err
		}
	}
	return AutomationResult{Automation: name, StepsExecuted: len(def.Steps)}, nil
}
```

Two details worth noticing:

- The **definition is loaded once, by an Activity, and its result is recorded** in the Event History.
  If the tenant edits the automation while a run is waiting in a `workflow.Sleep`, the running
  execution keeps replaying with the version it loaded. That is exactly what determinism requires.
- An unknown name fails with a **non-retryable** application error. Workflows don't retry by default,
  but this makes the intent explicit and stays correct if someone adds a Workflow Retry Policy later.

### The Worker

The dynamic Workflow sits next to normal, statically registered Workflows.

```go
// file: automations/order_receipt.go
package automations

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// SendOrderReceiptWorkflow is a normal, statically registered Workflow. It always wins over the
// dynamic one for its own name.
func SendOrderReceiptWorkflow(ctx workflow.Context, t AutomationTrigger) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var a *AutomationActivities
	return workflow.ExecuteActivity(ctx, a.SendTemplatedEmail, t, "order-receipt").Get(ctx, nil)
}
```

```go
// file: cmd/automations-worker/main.go
package main

import (
	"context"
	"log"
	"time"

	"github.com/acme/shop/automations"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// In-memory fakes so the sample runs; replace with your DB, ESP and CRM clients.
type memoryStore map[string]*automations.AutomationDefinition

func (m memoryStore) Find(_ context.Context, tenantID, name string) (*automations.AutomationDefinition, error) {
	return m[tenantID+"/"+name], nil
}

type logMailer struct{}

func (logMailer) Send(_ context.Context, tenantID, to, templateID string, _ map[string]string) error {
	log.Printf("[%s] email %s -> %s", tenantID, templateID, to)
	return nil
}

type logCRM struct{}

func (logCRM) AddTag(_ context.Context, tenantID, customerID, tag string) error {
	log.Printf("[%s] tag %s on %s", tenantID, tag, customerID)
	return nil
}

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln(err)
	}
	defer c.Close()

	store := memoryStore{
		"tenant-42/automation.abandoned-cart": {
			Name: "automation.abandoned-cart",
			Steps: []automations.AutomationStep{
				{Kind: automations.StepWait, Wait: 2 * time.Hour},
				{Kind: automations.StepSendEmail, TemplateID: "cart-reminder"},
				{Kind: automations.StepAddTag, Tag: "cart-abandoner"},
			},
		},
	}

	w := worker.New(c, "marketing-automations", worker.Options{})
	w.RegisterWorkflow(automations.SendOrderReceiptWorkflow)                                // static
	w.RegisterDynamicWorkflow(automations.RunAutomation, workflow.DynamicRegisterOptions{}) // fallback
	w.RegisterActivity(&automations.AutomationActivities{Store: store, Mailer: logMailer{}, CRM: logCRM{}})

	log.Fatalln(w.Run(worker.InterruptCh()))
}
```

### The starter: start a Workflow by type name

The client passes the Workflow Type as a **string**. No Go function reference exists for it.

```go
// file: cmd/trigger-automation/main.go
package main

import (
	"context"
	"log"

	"github.com/acme/shop/automations"
	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln(err)
	}
	defer c.Close()

	automationName := "automation.abandoned-cart" // comes from the tenant's configuration
	trigger := automations.AutomationTrigger{
		TenantID:   "tenant-42",
		CustomerID: "cus_8812",
		Email:      "jane@example.com",
		Context:    map[string]string{"cartId": "c-123"},
	}

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "tenant-42-abandoned-cart-c-123", // business ID: one automation run per cart
		TaskQueue: "marketing-automations",
	}, automationName, trigger)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Started", run.GetID(), run.GetRunID())
}
```

What happens on the Worker:

| Workflow Type requested | Runs |
| --- | --- |
| `SendOrderReceiptWorkflow` | the static `SendOrderReceiptWorkflow` (registered names always win) |
| `automation.abandoned-cart` | `RunAutomation`, which loads tenant-42's definition |
| `automation.typo` | `RunAutomation`, which fails non-retryably with `UnknownAutomation` |
| `InvoiceWorkflow` (not on this Worker) | `RunAutomation`, which fails with `UnknownWorkflowType` |

> 💡 The last row is the subtle one: with a Dynamic Workflow registered, a misrouted start is no
> longer "nobody handles it", it is "the fallback handles it". Always reject names you don't expect.

---

## 5. When *not* to use it

- The set of Workflow Types is known at build time: register them explicitly. You get type-checked
  arguments with function references and clearer code.
- You only want **variations** of one process: pass the variation in the input struct
  (`OnboardingInput{Plan: "pro"}`) and keep one static Workflow Type.
- You want to send work to **different Workers**: use Task Queues.

## Key takeaways

- A Dynamic Workflow is the **fallback** for Workflow Types that nothing is registered for on that Worker. **One per Worker.**
- Signature: `func(ctx workflow.Context, args converter.EncodedValues) (T, error)`. Decode with `args.Get(&...)`.
- Route on `workflow.GetInfo(ctx).WorkflowType.Name` and **fail non-retryably** on names you don't know.
- Load data-driven definitions through an **Activity** so replay stays deterministic.
- Go v1.50.0 has dynamic Workflows and Activities, but no dynamic Signal/Query/Update handlers.

⬅️ Previous: [11 · Versioning](11-versioning.md) · ➡️ Next: [13 · Workflow Streams](13-workflow-streams.md)
