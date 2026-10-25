# 02 · Using a Namespace from Go

> **Level:** foundations · **Read time:** ~12 min
> **Sources:** [Go SDK — Namespaces](https://docs.temporal.io/develop/go/client/namespaces) · [Go SDK — Temporal Client](https://docs.temporal.io/develop/go/client/temporal-client) · [`client.Options`](https://pkg.go.dev/go.temporal.io/sdk/client#Options)

---

## 1. The problem

Your checkout service must work in `shop-staging` on Monday and in `shop-prod` on Tuesday, with the
same binary. It also starts invoices in the billing team's `billing-prod` Namespace. And when someone
deploys it with a typo in the Namespace name, you want it to **refuse to start**, not to sit there
accepting orders that will never be processed.

---

## 2. The Namespace is a Client option

In Go, a Namespace is chosen **once, when you create the Client**: `client.Options.Namespace`. Every
call the Client makes then targets that Namespace, and so does every Worker built on it.

| Where | How the Namespace is chosen |
| --- | --- |
| Code | `client.Dial(client.Options{Namespace: "shop-prod"})` |
| Environment | `TEMPORAL_NAMESPACE=shop-prod` with `envconfig.LoadDefaultClientOptions()` |
| TOML profile | `namespace = "shop-prod"` in a profile, loaded by `envconfig` |
| Nothing set | `client.DefaultNamespace` (`"default"`) |

The environment and profile options come from `go.temporal.io/sdk/contrib/envconfig`. The
[Client guide](../../client/docs/02-connecting.md) explains how these sources combine. Reading the
Namespace from the environment is what lets one image run in every environment.

A Worker has **no Namespace option** of its own: it polls the Task Queue you give it, **in its
Client's Namespace** (see the [Workers guide](../../workers/docs/04-running-a-worker.md)).

---

## 3. A wrong Namespace isn't caught at connection time

What happens when the Namespace doesn't exist? Observed on a dev server (Go SDK v1.50.0, Temporal
Server 1.32.0) with `Namespace: "does-not-exist"`:

| Call | Result |
| --- | --- |
| `client.Dial` | **succeeds** |
| `client.DialContext` | **succeeds** |
| `c.CheckHealth(ctx, …)` | **succeeds** (it checks the Service, not the Namespace) |
| `c.ExecuteWorkflow(…)` | fails: `*serviceerror.NamespaceNotFound`, "Namespace does-not-exist is not found." |
| `worker.Start()` / `Run()` | fails: "Namespace does-not-exist is not found." |

So an API that only starts Workflows boots fine and fails on the **first request**. Here's the
program used to check it:

```go
// file: cmd/checkns/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// Shows what happens with a Namespace that doesn't exist, without the guard.
func main() {
	ctx := context.Background()
	c, err := client.Dial(client.Options{Namespace: os.Args[1]})
	fmt.Println("Dial:", err)
	if err != nil {
		return
	}
	defer c.Close()

	_, err = c.CheckHealth(ctx, &client.CheckHealthRequest{})
	fmt.Println("CheckHealth:", err)

	_, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: "orders"}, "PlaceOrderWorkflow")
	var notFound *serviceerror.NamespaceNotFound
	fmt.Println("ExecuteWorkflow:", err, "| NamespaceNotFound:", errors.As(err, &notFound))
}
```

```text
$ go run ./cmd/checkns does-not-exist
Dial: <nil>
CheckHealth: <nil>
ExecuteWorkflow: Namespace does-not-exist is not found. | NamespaceNotFound: true
```

---

## 4. A startup guard

Check the Namespace yourself, right after dialing. `DescribeNamespace` is a cheap call that every
Client can make through `c.WorkflowService()`. It also tells you the Namespace's **state**: a
Deprecated Namespace refuses new Workflows ([chapter 05](05-deprecating-and-deleting.md)), so the
guard rejects it too.

```go
// file: internal/temporalconn/guard.go
package temporalconn

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// ErrNamespaceUnusable is returned when the Namespace is missing or can't start new Workflows.
var ErrNamespaceUnusable = errors.New("temporal namespace is not usable")

// RequireNamespace fails fast if c's Namespace doesn't exist or isn't Registered.
// client.Dial doesn't check the Namespace: without this, the first error shows up
// on the first ExecuteWorkflow call or when the Worker starts.
func RequireNamespace(ctx context.Context, c client.Client, namespace string) error {
	resp, err := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	var notFound *serviceerror.NamespaceNotFound
	switch {
	case errors.As(err, &notFound):
		return fmt.Errorf("%w: %q does not exist on this Temporal Service", ErrNamespaceUnusable, namespace)
	case err != nil:
		return fmt.Errorf("describe namespace %q: %w", namespace, err)
	}
	if state := resp.GetNamespaceInfo().GetState(); state != enumspb.NAMESPACE_STATE_REGISTERED {
		return fmt.Errorf("%w: %q is in state %s", ErrNamespaceUnusable, namespace, state)
	}
	return nil
}
```

Wrap it into the one function your services use to connect:

```go
// file: internal/temporalconn/dial.go
package temporalconn

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

// Dial loads the connection settings (TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, …) from the
// environment, connects, and checks the Namespace before returning the Client.
func Dial(ctx context.Context) (client.Client, error) {
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return nil, fmt.Errorf("load Temporal client options: %w", err)
	}
	if opts.Namespace == "" {
		opts.Namespace = client.DefaultNamespace // what the SDK would use anyway
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	c, err := client.DialContext(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("dial Temporal at %s: %w", opts.HostPort, err)
	}
	if err := RequireNamespace(ctx, c, opts.Namespace); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
```

With the guard, a typo or a deprecated Namespace stops the process at startup:

```text
$ TEMPORAL_NAMESPACE=does-not-exist go run ./cmd/worker
temporal namespace is not usable: "does-not-exist" does not exist on this Temporal Service
$ TEMPORAL_NAMESPACE=legacy-orders go run ./cmd/worker
temporal namespace is not usable: "legacy-orders" is in state Deprecated
```

On Temporal Cloud, an API key or certificate scoped to one Namespace may not be allowed to describe
another. Run the guard only for the Namespaces the process actually uses.

---

## 5. Several Namespaces in one process

One Client = one Namespace. To work in a second Namespace, create a second Client. Use
`client.NewClientFromExisting` so both share the same gRPC connection instead of dialing twice (more
in the [Client guide, chapter 09](../../client/docs/09-client-in-production.md)).

The checkout Worker below runs in `shop-prod` and starts invoices in `billing-prod`, from an
Activity:

```go
// file: orders/activities.go
package orders

import (
	"context"

	"go.temporal.io/sdk/client"
)

const TaskQueue = "orders"

type InvoiceRequest struct {
	OrderID     string
	AmountCents int64
}

// Activities run in the Worker's Namespace (shop-prod) but start invoices in billing-prod.
type Activities struct {
	Billing client.Client // Client for the billing Namespace
}

func (a *Activities) RequestInvoice(ctx context.Context, req InvoiceRequest) (string, error) {
	run, err := a.Billing.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "invoice-" + req.OrderID,
		TaskQueue: "billing",
	}, "IssueInvoiceWorkflow", req)
	if err != nil {
		return "", err
	}
	return run.GetRunID(), nil
}
```

```go
// file: orders/workflow.go
package orders

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func PlaceOrderWorkflow(ctx workflow.Context, orderID string, amountCents int64) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})

	var a *Activities
	var invoiceRunID string
	err := workflow.ExecuteActivity(ctx, a.RequestInvoice, InvoiceRequest{
		OrderID: orderID, AmountCents: amountCents,
	}).Get(ctx, &invoiceRunID)
	return invoiceRunID, err
}
```

```go
// file: cmd/worker/main.go
package main

import (
	"context"
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/internal/temporalconn"
	"github.com/acme/shop/orders"
)

func main() {
	ctx := context.Background()

	// TEMPORAL_NAMESPACE=shop-prod
	shop, err := temporalconn.Dial(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	defer shop.Close()

	// Same gRPC connection, other Namespace.
	billingNamespace := os.Getenv("BILLING_NAMESPACE") // e.g. billing-prod
	billing, err := client.NewClientFromExisting(shop, client.Options{Namespace: billingNamespace})
	if err != nil {
		log.Fatalln(err)
	}
	defer billing.Close()
	if err := temporalconn.RequireNamespace(ctx, billing, billingNamespace); err != nil {
		log.Fatalln(err)
	}

	// The Worker polls the "orders" Task Queue of shop-prod: the Namespace of its Client.
	w := worker.New(shop, orders.TaskQueue, worker.Options{})
	w.RegisterWorkflow(orders.PlaceOrderWorkflow)
	w.RegisterActivity(&orders.Activities{Billing: billing})

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln(err)
	}
}
```

Checked on a dev server: `PlaceOrderWorkflow` ran in one Namespace, and the invoice Workflow it
started completed in the other, picked up by the billing Worker from [chapter 01](01-what-is-a-namespace.md).

A Workflow can't start a Workflow in another Namespace **directly**: cross-Namespace Child Workflows
are refused by recent Servers. Going through an Activity, as here, or through **Nexus**, is the way
([chapter 06](06-namespace-strategy.md)).

---

## 6. Namespaces on your laptop

The dev server creates `default`. Pass `--namespace` (repeatable) to create more at startup, so your
local setup matches production names:

```text
$ temporal server start-dev --namespace shop-dev --namespace billing-dev
```

Each extra Namespace is created with a 24-hour retention (checked on CLI 1.9.1). The dev server keeps
everything in memory unless you pass `--db-filename`, so these Namespaces are created again on each
start, which is exactly what you want locally.

---

## 7. Key takeaways

- The Namespace is a **Client option**: `client.Options.Namespace`, or `TEMPORAL_NAMESPACE` with
  `envconfig`. Workers use their Client's Namespace.
- `Dial`, `DialContext` and `CheckHealth` **don't check the Namespace**. The first real call fails with
  `*serviceerror.NamespaceNotFound`.
- Add a **startup guard** with `DescribeNamespace`: fail on a missing or non-Registered Namespace.
- One Client per Namespace. Use `client.NewClientFromExisting` to share the connection.
- `temporal server start-dev --namespace …` creates your Namespaces locally.
