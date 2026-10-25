# 03 · Registering a Namespace

> **Level:** daily use · **Read time:** ~12 min
> **Sources:** [Go SDK — Register a Namespace](https://docs.temporal.io/develop/go/client/namespaces#register-namespace) · [CLI — `temporal operator namespace create`](https://docs.temporal.io/cli/command-reference/operator#create) · [`client.NamespaceClient`](https://pkg.go.dev/go.temporal.io/sdk/client#NamespaceClient)

---

## 1. The problem

A new team, `shop-staging`, needs its own Namespace. You could ask an operator to type a CLI command,
but then the staging environment can't be rebuilt from scratch, and nobody remembers which retention
was chosen. You want Namespace creation to be **code**: repeatable, reviewed, and safe to run twice.

---

## 2. Two ways to register

You must register a Namespace on the Temporal Service before any Client can use it.

| Tool | Command or API | When |
| --- | --- | --- |
| Temporal CLI | `temporal operator namespace create -n shop-staging --retention 72h` | By hand, scripts |
| Go SDK | `client.NewNamespaceClient(opts)` then `nc.Register(ctx, &workflowservice.RegisterNamespaceRequest{…})` | Bootstrap tools, tests, platform automation |

Both talk to a **self-hosted** Service. On **Temporal Cloud**, neither works: create Namespaces in the
Cloud UI or with the `temporal cloud namespace` commands of the Cloud CLI extension.

Use a custom **Authorizer** on the Frontend Service to decide who may create, update or deprecate
Namespaces on a self-hosted Service.

---

## 3. What a registration contains

`RegisterNamespaceRequest` has many fields (replication, archival, global Namespaces). The ones you'll
set every time:

| Field | Meaning | Required? |
| --- | --- | --- |
| `Namespace` | The name Clients will use | yes |
| `WorkflowExecutionRetentionPeriod` | How long closed Workflow Executions are kept before deletion | **yes** through the API |
| `Description` | Free text shown by `describe` | no |
| `OwnerEmail` | Who to call | no |

What the Service did on a dev server (Temporal Server 1.32.0, CLI 1.9.1):

| Request | Result |
| --- | --- |
| API, no retention | `*serviceerror.InvalidArgument`: "A valid retention period is not set on request." |
| CLI, no `--retention` | **Registered with 72h**, the CLI's default |
| API or CLI, retention `1h` | **Registered**, retention 1h |
| API, Namespace already exists | `*serviceerror.NamespaceAlreadyExists`: `Namespace "default" already exists` |

The Go SDK page says the retention "is mandatory" and "the minimum value you can set for this period
is 1 day". The first part holds for the API only (the CLI fills in 72 hours). The second didn't hold on
the dev server, which accepted 1 hour: the minimum is a Service setting, not a rule of the API. If
your organization has a retention policy, **enforce it in your own code**.

---

## 4. An idempotent bootstrap

The `Ensure` function registers a Namespace and treats "already exists" as success, so the bootstrap
can run on every deploy:

```go
// file: internal/nsadmin/ensure.go
package nsadmin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Spec is the Namespace we want to exist.
type Spec struct {
	Name        string
	Retention   time.Duration
	Description string
	OwnerEmail  string
}

// MinRetention is our own policy. Don't count on the Service to enforce a minimum:
// a dev server accepts 1h even though the docs say 1 day.
const MinRetention = 24 * time.Hour

// Ensure registers the Namespace, or does nothing if it already exists.
// It returns true when the Namespace was created by this call.
func Ensure(ctx context.Context, nc client.NamespaceClient, spec Spec) (created bool, err error) {
	if spec.Retention < MinRetention {
		return false, fmt.Errorf("retention %s for %q is below our minimum of %s", spec.Retention, spec.Name, MinRetention)
	}
	err = nc.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        spec.Name,
		WorkflowExecutionRetentionPeriod: durationpb.New(spec.Retention), // mandatory
		Description:                      spec.Description,
		OwnerEmail:                       spec.OwnerEmail,
	})
	var exists *serviceerror.NamespaceAlreadyExists
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &exists):
		return false, nil // already there: bootstrap stays idempotent
	default:
		return false, fmt.Errorf("register namespace %q: %w", spec.Name, err)
	}
}
```

`Ensure` doesn't change an existing Namespace. If its retention or description should follow the
`Spec`, compare with `Describe` and call `Update` ([chapter 04](04-managing-namespaces.md)).

---

## 5. "Takes up to 10 seconds"

The Go SDK page warns that registration "takes up to 10 seconds to complete" and asks you to wait
before starting Workflows.

On a single-node dev server, a brand-new name was usable **immediately**: a Worker had started and a
Workflow had been started in it about 60 ms after the `Register` call. But if something had looked
the name up **before** it existed (a `Describe` that returned `NamespaceNotFound`, as an "ensure"
tool does), Workflow starts and `Update` kept returning `NamespaceNotFound` for 0.3–1 s after
`Register`, while `Describe` already found it. On multi-node clusters each Frontend also learns about
Namespaces from a cache that refreshes periodically. In all these cases, calls return
`NamespaceNotFound` for a short while.

Don't sleep for 10 seconds. Retry a cheap Namespace-scoped call until it succeeds:

```go
// file: internal/nsadmin/wait.go
package nsadmin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// WaitUntilUsable retries a Namespace-scoped call until the Service accepts it.
// On a multi-node cluster, frontends learn about a new Namespace from a cache that
// refreshes periodically, so the first calls can return NamespaceNotFound.
func WaitUntilUsable(ctx context.Context, c client.Client, namespace string) error {
	delay := 200 * time.Millisecond
	for {
		_, err := c.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: namespace,
		})
		var notFound *serviceerror.NamespaceNotFound
		if err == nil {
			return nil
		}
		if !errors.As(err, &notFound) {
			return fmt.Errorf("check namespace %q: %w", namespace, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("namespace %q still not usable: %w", namespace, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(delay*2, 2*time.Second)
	}
}
```

---

## 6. The bootstrap tool

`cmd/bootstrap` reads the Service address and credentials from the environment, ensures the
Namespace exists, and waits until it's usable. Run it before deploying the Workers of a new
environment:

```go
// file: cmd/bootstrap/main.go
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"

	"github.com/acme/shop/internal/nsadmin"
)

func main() {
	name := flag.String("namespace", "shop-staging", "Namespace to create")
	retention := flag.Duration("retention", 72*time.Hour, "Workflow Execution retention period")
	flag.Parse()

	// Address and credentials from the environment (TEMPORAL_ADDRESS, …).
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatalln("load client options:", err)
	}

	nc, err := client.NewNamespaceClient(opts)
	if err != nil {
		log.Fatalln("namespace client:", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	created, err := nsadmin.Ensure(ctx, nc, nsadmin.Spec{
		Name:        *name,
		Retention:   *retention,
		Description: "Shop checkout and fulfillment",
		OwnerEmail:  "platform-team@acme.example",
	})
	if err != nil {
		log.Fatalln(err)
	}

	opts.Namespace = *name
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalln("dial:", err)
	}
	defer c.Close()
	if err := nsadmin.WaitUntilUsable(ctx, c, *name); err != nil {
		log.Fatalln(err)
	}
	log.Printf("namespace %q ready (created=%v)", *name, created)
}
```

Run twice, then with a retention below the policy:

```text
$ go run ./cmd/bootstrap -namespace shop-staging
namespace "shop-staging" ready (created=true)
$ go run ./cmd/bootstrap -namespace shop-staging
namespace "shop-staging" ready (created=false)
$ go run ./cmd/bootstrap -namespace shop-tmp -retention 1h
retention 1h0m0s for "shop-tmp" is below our minimum of 24h0m0s

$ temporal operator namespace describe -n shop-staging
  NamespaceInfo.Description             Shop checkout and fulfillment
  NamespaceInfo.OwnerEmail              platform-team@acme.example
  Config.WorkflowExecutionRetentionTtl  72h0m0s
  …
```

`client.NewNamespaceClient` takes the same `client.Options` as `client.Dial`. Its `Namespace` field is
ignored: each call names the Namespace it acts on.

---

## 7. Key takeaways

- Register a Namespace before any Client uses it: `temporal operator namespace create` or
  `NamespaceClient.Register`. Neither works on Temporal Cloud.
- Through the API, `WorkflowExecutionRetentionPeriod` is **required**. The CLI defaults to **72h**.
- The documented 1-day minimum isn't enforced everywhere (a dev server took 1h). Enforce your own policy.
- `*serviceerror.NamespaceAlreadyExists` is your friend: treat it as success to make bootstrap idempotent.
- Don't sleep after registering. Retry a Namespace-scoped call while it returns `NamespaceNotFound`.
