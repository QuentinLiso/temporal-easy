# 09 · Dynamic Activities

> **Level:** sub-feature · **Read time:** ~8 min
> **Sources:** [Go SDK — Dynamic Activity](https://docs.temporal.io/develop/go/activities/dynamic-activity) · [Encyclopedia — Activity Type](https://docs.temporal.io/activity-definition#activity-type)

---

## 1. The problem

Normally every Activity Type is **known at compile time**. You register `ChargeCard`, and callers
invoke `ChargeCard`. Some systems don't know the full list of types in advance:

- a **connector or integration platform** where tenants configure actions such as `notify.slack`,
  `notify.teams` or `crm.hubspot.upsertContact`, stored in a database
- a **plugin** system where Activity names come from configuration
- a **catch-all** that logs, rejects or forwards calls to unknown Activity Types, so they don't just time out

A **Dynamic Activity** is invoked at runtime **when no other Activity with the requested name is
registered** on the Worker. It gets the Activity Type name from `activity.GetInfo(ctx)` and the raw
arguments as `converter.EncodedValues`. You decide what to do with them.

## 2. Rules (Go SDK)

- Register with `worker.RegisterDynamicActivity(fn, activity.DynamicRegisterOptions{})`. It must be registered before it can be invoked.
- **Only one** Dynamic Activity per Worker.
- Its signature takes a **single** argument after `ctx`: `converter.EncodedValues`.
- Registered Activities **always win**. The dynamic one is only the fallback for unknown names.
- You **can't** register a struct as a Dynamic Activity, only a function.

---

## 3. Example: tenant-configurable notification actions

```go
// file: connectors/dynamic.go
package connectors

import (
	"context"
	"fmt"
	"strings"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// ActionPayload is the common envelope every connector action receives.
type ActionPayload struct {
	TenantID string
	Message  string
	Target   string // channel, email, contact ID...
}

// Connector is implemented by each integration plugin.
type Connector interface {
	Execute(ctx context.Context, action string, p ActionPayload) (string, error)
}

// Registry maps a prefix ("notify", "crm") to a plugin. It is filled at Worker start-up.
var Registry = map[string]Connector{}

// RunConnectorAction handles EVERY Activity Type that is not explicitly registered.
func RunConnectorAction(ctx context.Context, args converter.EncodedValues) (string, error) {
	info := activity.GetInfo(ctx)
	activityType := info.ActivityType.Name // e.g. "notify.slack"

	var payload ActionPayload
	if err := args.Get(&payload); err != nil {
		return "", temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("cannot decode arguments for %s: %v", activityType, err), "BadConnectorPayload", err)
	}

	prefix, action, ok := strings.Cut(activityType, ".")
	connector, found := Registry[prefix]
	if !ok || !found {
		// Unknown name: fail fast instead of letting the caller wait for a timeout.
		return "", temporal.NewNonRetryableApplicationError(
			"no connector for Activity Type "+activityType, "UnknownConnector", nil)
	}

	activity.GetLogger(ctx).Info("Running connector action",
		"type", activityType, "tenant", payload.TenantID, "attempt", info.Attempt)
	return connector.Execute(ctx, action, payload)
}

// SlackConnector is one plugin.
type SlackConnector struct{ WebhookBaseURL string }

func (s SlackConnector) Execute(ctx context.Context, action string, p ActionPayload) (string, error) {
	return fmt.Sprintf("slack:%s:%s posted to %s", action, p.TenantID, p.Target), nil
}
```

The Workflow calls Activities **by string name**, often read from tenant configuration:

```go
// file: connectors/workflow.go
package connectors

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

type OrderShippedEvent struct {
	TenantID string
	OrderID  string
	Actions  []string // configured by the tenant, e.g. ["notify.slack", "crm.hubspot.logActivity"]
	Channel  string
}

func OrderShippedAutomationWorkflow(ctx workflow.Context, ev OrderShippedEvent) ([]string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 20 * time.Second})

	var results []string
	for _, actionType := range ev.Actions {
		var out string
		err := workflow.ExecuteActivity(ctx, actionType, ActionPayload{
			TenantID: ev.TenantID,
			Message:  "Order " + ev.OrderID + " has shipped",
			Target:   ev.Channel,
		}).Get(ctx, &out)
		if err != nil {
			workflow.GetLogger(ctx).Warn("connector action failed", "action", actionType, "error", err)
			continue
		}
		results = append(results, out)
	}
	return results, nil
}
```

Register it on the Worker, next to any normal Activities:

```go
// file: cmd/connectors-worker/main.go
package main

import (
	"context"
	"log"

	"github.com/acme/shop/connectors"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// HealthCheck is a normal, explicitly registered Activity: it always wins over the dynamic one.
func HealthCheck(ctx context.Context) (string, error) { return "ok", nil }

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln(err)
	}
	defer c.Close()

	connectors.Registry["notify"] = connectors.SlackConnector{WebhookBaseURL: "https://hooks.slack.com"}

	w := worker.New(c, "connectors", worker.Options{})
	w.RegisterWorkflow(connectors.OrderShippedAutomationWorkflow)
	w.RegisterActivity(HealthCheck)
	w.RegisterDynamicActivity(connectors.RunConnectorAction, activity.DynamicRegisterOptions{})

	log.Fatalln(w.Run(worker.InterruptCh()))
}
```

> ⚠️ The Go SDK sample reads `info.WorkflowType.Name` inside the Dynamic Activity. `WorkflowType` is
> a pointer that describes the Workflow that started the Activity, so don't rely on it when the same
> Activity can run as a **Standalone Activity** (no Workflow). Route on `info.ActivityType.Name`, and
> check `info.IsWorkflowActivity()` before you touch any Workflow fields.

---

## 4. When *not* to use it

- When the set of Activity Types is known at build time, register them explicitly. You get
  argument type checks with function references and clearer code.
- To route work to **different Workers**, use **Task Queues**, not a dynamic dispatcher.

## Key takeaways

- A Dynamic Activity is the **fallback** for Activity Types that no Worker registered by name. There is **one per Worker**.
- Signature: `func(ctx context.Context, args converter.EncodedValues) (T, error)`. Decode with `args.Get(&...)`.
- Route on `activity.GetInfo(ctx).ActivityType.Name`. Fail **non-retryably** for unknown names.

⬅️ Previous: [08 · Async completion](08-async-completion.md) · ➡️ Next: [10 · Activity Operations](10-activity-operations.md)
