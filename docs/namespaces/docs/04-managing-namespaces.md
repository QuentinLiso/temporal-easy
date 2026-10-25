# 04 · Describing, updating and listing Namespaces

> **Level:** daily use · **Read time:** ~12 min
> **Sources:** [Go SDK — Manage Namespaces](https://docs.temporal.io/develop/go/client/namespaces#manage-namespaces) · [`client.NamespaceClient`](https://pkg.go.dev/go.temporal.io/sdk/client#NamespaceClient) · [CLI — `temporal operator namespace`](https://docs.temporal.io/cli/command-reference/operator#namespace)

---

## 1. The problem

Once a Namespace exists, someone has to look after it. Billing wants 90 days of history for audits.
The new staging Namespace keeps closed Workflows far too long. An auditor asks "which Namespaces exist,
and who owns each one?". You can do all of this with the CLI, but a team that runs several Namespaces
soon wants it in code: in a deploy job, an internal admin tool or a nightly report.

This chapter builds that code, and shows where the Go SDK stops and the raw API takes over.

---

## 2. What the Go SDK gives you

The Go SDK page says you can "create, update, deprecate, or delete" Namespaces "using either the
Temporal CLI or SDK APIs". The SDK's `client.NamespaceClient` has only **four** methods:

| Method | What it does |
| --- | --- |
| `Register(ctx, *workflowservice.RegisterNamespaceRequest)` | Create a Namespace ([chapter 03](03-registering-namespaces.md)) |
| `Describe(ctx, name)` | Read its info and config |
| `Update(ctx, *workflowservice.UpdateNamespaceRequest)` | Change its info, config or state |
| `Close()` | Close the connection |

Everything else goes through the **raw gRPC services** that every `client.Client` exposes:

| Operation | Go | CLI |
| --- | --- | --- |
| Create | `nc.Register(…)` | `temporal operator namespace create` |
| Describe | `nc.Describe(ctx, name)` | `temporal operator namespace describe -n …` |
| Update | `nc.Update(…)` | `temporal operator namespace update -n …` |
| **List** | `c.WorkflowService().ListNamespaces(…)` | `temporal operator namespace list` |
| **Deprecate** | `nc.Update(…)` with a new state ([chapter 05](05-deprecating-and-deleting.md)) | — |
| **Delete** | `c.OperatorService().DeleteNamespace(…)` ([chapter 05](05-deprecating-and-deleting.md)) | `temporal operator namespace delete -n …` |

> **Temporal Cloud:** none of this works there. On Cloud, manage Namespaces in the Cloud UI or with
> `temporal cloud namespace` commands. The code in this chapter is for self-hosted Services and the
> dev server.

All examples in this chapter live in one small package, `internal/nsadmin`, and one tool, `nsctl`.

---

## 3. Describing a Namespace

`Describe` returns a `DescribeNamespaceResponse` with three parts: `NamespaceInfo` (who and what),
`Config` (how it behaves) and `ReplicationConfig` (multi-cluster setups). Most tools only need a few
fields, so map them into your own type:

```go
// file: internal/nsadmin/describe.go
package nsadmin

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/sdk/client"
)

// Summary is what operators usually want to know about a Namespace.
type Summary struct {
	Name               string
	ID                 string
	State              enumspb.NamespaceState
	Description        string
	OwnerEmail         string
	Retention          time.Duration
	HistoryArchival    enumspb.ArchivalState
	VisibilityArchival enumspb.ArchivalState
	Data               map[string]string
	// What this Service supports for the Namespace (Update, eager start, …).
	Capabilities *namespacepb.NamespaceInfo_Capabilities
}

// Describe reads a Namespace through the SDK's NamespaceClient.
func Describe(ctx context.Context, nc client.NamespaceClient, name string) (Summary, error) {
	resp, err := nc.Describe(ctx, name)
	if err != nil {
		return Summary{}, fmt.Errorf("describe namespace %q: %w", name, err)
	}
	info, cfg := resp.GetNamespaceInfo(), resp.GetConfig()
	return Summary{
		Name:               info.GetName(),
		ID:                 info.GetId(),
		State:              info.GetState(),
		Description:        info.GetDescription(),
		OwnerEmail:         info.GetOwnerEmail(),
		Retention:          cfg.GetWorkflowExecutionRetentionTtl().AsDuration(),
		HistoryArchival:    cfg.GetHistoryArchivalState(),
		VisibilityArchival: cfg.GetVisibilityArchivalState(),
		Data:               info.GetData(),
		Capabilities:       info.GetCapabilities(),
	}, nil
}
```

| Field | Where | Notes |
| --- | --- | --- |
| Name, ID | `NamespaceInfo.Name`, `.Id` | The ID is a UUID; it never changes |
| State | `NamespaceInfo.State` | `Registered`, `Deprecated` or `Deleted` ([chapter 05](05-deprecating-and-deleting.md)) |
| Description, owner | `NamespaceInfo.Description`, `.OwnerEmail` | Free text, for humans |
| Data | `NamespaceInfo.Data` | Your own key/value labels (team, cost center…) |
| Capabilities | `NamespaceInfo.Capabilities` | What the Service supports for this Namespace (Update, eager start, Standalone Activities…) |
| Retention | `Config.WorkflowExecutionRetentionTtl` | How long **closed** Workflows are kept |
| Archival | `Config.HistoryArchivalState`, `.VisibilityArchivalState` | `Disabled` on a dev server |

`Describe` returns `*serviceerror.NamespaceNotFound` when the name doesn't exist. Check it with
`errors.As`, as for the other service errors in the [Client guide](../../client/docs/README.md).

---

## 4. Updating a Namespace

`Update` takes the Namespace name plus the parts you want to change. What it **doesn't** change
matters as much as what it does. On a dev server (Server 1.32.0):

| You send | Result |
| --- | --- |
| `Config{WorkflowExecutionRetentionTtl: 720h}` only | Retention changes. Description, owner and Data stay |
| `UpdateInfo{Description: "…"}` with an empty `OwnerEmail` | Description changes. The owner is **kept** |
| `UpdateInfo{Data: {"cost-center": "cc-4711"}}` when Data was `{"team": "payments-shared"}` | Data becomes **both** keys: Data is merged, not replaced |
| An empty `Config{}` | Nothing changes, no error |

So **empty means "leave it"**: you can't clear a description or remove a Data key with `Update`.
Two small helpers cover the common changes:

```go
// file: internal/nsadmin/update.go
package nsadmin

import (
	"context"
	"fmt"
	"time"

	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
)

// SetRetention changes how long closed Workflow Executions are kept.
// Only the fields you set change: description, owner and archival stay as they are.
func SetRetention(ctx context.Context, nc client.NamespaceClient, name string, retention time.Duration) error {
	err := nc.Update(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: name,
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
		},
	})
	if err != nil {
		return fmt.Errorf("set retention of %q to %s: %w", name, retention, err)
	}
	return nil
}

// SetOwnership updates the human-facing metadata. Data keys are merged into
// the existing ones, not replaced.
func SetOwnership(ctx context.Context, nc client.NamespaceClient, name, description, ownerEmail string, data map[string]string) error {
	err := nc.Update(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: name,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
	})
	if err != nil {
		return fmt.Errorf("update ownership of %q: %w", name, err)
	}
	return nil
}
```

### The Go page's Update example

The Update snippet on the Go page is mostly commented out, and it refers to
`replication.NamespaceReplicationConfig` without saying where `replication` comes from. The packages
are:

| Type | Import |
| --- | --- |
| `UpdateNamespaceRequest` | `go.temporal.io/api/workflowservice/v1` |
| `UpdateNamespaceInfo`, `NamespaceConfig` | `go.temporal.io/api/namespace/v1` (usually aliased `namespacepb`) |
| `NamespaceReplicationConfig` | `go.temporal.io/api/replication/v1` (usually aliased `replicationpb`) |

Of the fields it lists, the ones you'll use are `UpdateInfo` (description, owner, Data, state) and
`Config` (retention, archival). `ReplicationConfig` and `PromoteNamespace` are for multi-cluster
replication; `BadBinaries` and `DeleteBadBinary` belong to the old binary-checksum mechanism that
[Worker Versioning](../../workers/docs/11-worker-versioning.md) replaces.

---

## 5. Listing Namespaces

There's no `List` on `NamespaceClient`. Call `ListNamespaces` on the Workflow Service and follow the
page token:

```go
// file: internal/nsadmin/list.go
package nsadmin

import (
	"context"
	"fmt"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// systemNamespace is the Service's own Namespace. It shows up in ListNamespaces.
const systemNamespace = "temporal-system"

// List returns every user Namespace, following the pagination token.
// NamespaceClient has no List method: this goes through the raw WorkflowService.
func List(ctx context.Context, c client.Client) ([]Summary, error) {
	var out []Summary
	var token []byte
	for {
		resp, err := c.WorkflowService().ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{
			PageSize:      100,
			NextPageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list namespaces: %w", err)
		}
		for _, ns := range resp.GetNamespaces() {
			info, cfg := ns.GetNamespaceInfo(), ns.GetConfig()
			if info.GetName() == systemNamespace {
				continue
			}
			out = append(out, Summary{
				Name:        info.GetName(),
				ID:          info.GetId(),
				State:       info.GetState(),
				Description: info.GetDescription(),
				OwnerEmail:  info.GetOwnerEmail(),
				Retention:   cfg.GetWorkflowExecutionRetentionTtl().AsDuration(),
			})
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return out, nil
		}
	}
}
```

Two things show up, or don't, in the list:

- **`temporal-system`** is listed. It's the Service's own Namespace for internal Workflows: filter it out.
- **Deleted Namespaces aren't.** Neither one deleted with `DeleteNamespace` nor one whose state was set to
  `Deleted` appears ([chapter 05](05-deprecating-and-deleting.md)). Deprecated ones do.

---

## 6. Settings that live in a Namespace

Two more things are configured **per Namespace**, so they belong in the same admin tooling:

**Custom Search Attributes.** A Search Attribute created in one Namespace doesn't exist in another.
On a dev server, `CustomerTier` created in `shop-staging` was missing from `default`:

```bash
temporal operator search-attribute create --namespace shop-staging --name CustomerTier --type Keyword
temporal operator search-attribute list --namespace shop-staging   # CustomerTier  Keyword
temporal operator search-attribute list --namespace default        # (not there)
```

Create them for **every** Namespace your code runs in. A start that sets an unknown Search Attribute
fails: `Namespace default has no mapping defined for search attribute CustomerTier`.

**Archival.** Copying closed histories to long-term storage is enabled per Namespace
(`HistoryArchivalState`, `VisibilityArchivalState` and their URIs), if the Service has archival set
up. On the dev server both are `Disabled`.

---

## 7. Putting it together: `nsctl`

A small command-line tool for the platform team, built on the helpers above. It uses the same
connection settings as every other service in the shop (`envconfig`, see the
[Client guide, chapter 02](../../client/docs/02-connecting.md)):

```go
// file: cmd/nsctl/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"

	"github.com/acme/shop/internal/nsadmin"
)

const usage = `usage:
  nsctl list
  nsctl describe <namespace>
  nsctl set-retention <namespace> <duration, e.g. 720h>
  nsctl set-owner <namespace> <description> <owner-email> [team]`

func main() {
	if len(os.Args) < 2 {
		log.Fatal(usage)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Same connection settings as the rest of the shop: env vars or a TOML profile.
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatalf("load client options: %v", err)
	}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer c.Close()
	nc, err := client.NewNamespaceClient(opts)
	if err != nil {
		log.Fatalf("namespace client: %v", err)
	}
	defer nc.Close()

	switch cmd, args := os.Args[1], os.Args[2:]; {
	case cmd == "list":
		all, err := nsadmin.List(ctx, c)
		if err != nil {
			log.Fatal(err)
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tSTATE\tRETENTION\tOWNER")
		for _, ns := range all {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", ns.Name, ns.State, ns.Retention, ns.OwnerEmail)
		}
		_ = tw.Flush()

	case cmd == "describe" && len(args) == 1:
		s, err := nsadmin.Describe(ctx, nc, args[0])
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("name:        %s (%s)\nstate:       %s\ndescription: %s\nowner:       %s\nretention:   %s\narchival:    history=%s visibility=%s\ndata:        %v\n",
			s.Name, s.ID, s.State, s.Description, s.OwnerEmail, s.Retention, s.HistoryArchival, s.VisibilityArchival, s.Data)

	case cmd == "set-retention" && len(args) == 2:
		d, err := time.ParseDuration(args[1])
		if err != nil {
			log.Fatalf("bad duration %q: %v", args[1], err)
		}
		if err := nsadmin.SetRetention(ctx, nc, args[0], d); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: retention set to %s\n", args[0], d)

	case cmd == "set-owner" && (len(args) == 3 || len(args) == 4):
		var data map[string]string
		if len(args) == 4 {
			data = map[string]string{"team": args[3]}
		}
		if err := nsadmin.SetOwnership(ctx, nc, args[0], args[1], args[2], data); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: ownership updated\n", args[0])

	default:
		log.Fatal(usage)
	}
}
```

An example session (from a dev-server run, Namespace names adapted):

```text
$ go run ./cmd/nsctl set-retention shop-staging 720h
shop-staging: retention set to 720h0m0s
$ go run ./cmd/nsctl set-owner shop-staging "Checkout, staging" "" checkout
shop-staging: ownership updated
$ go run ./cmd/nsctl describe shop-staging
name:        shop-staging (93f43856-5d98-4b94-8a28-99ee47c540dc)
state:       Registered
description: Checkout, staging
owner:       shop-oncall@acme.example
retention:   720h0m0s
archival:    history=Disabled visibility=Disabled
data:        map[team:checkout]
$ go run ./cmd/nsctl list
NAME           STATE       RETENTION  OWNER
default        Registered  24h0m0s
shop-staging   Registered  720h0m0s   shop-oncall@acme.example
legacy-orders  Deprecated  24h0m0s
```

The empty owner in `set-owner` kept `shop-oncall@acme.example`, as section 4 predicts.

---

## 8. Key takeaways

- `NamespaceClient` only does **Register, Describe and Update**. List through `WorkflowService()`,
  delete through `OperatorService()`. None of it works on Temporal Cloud.
- `Update` changes only what you set: empty strings are ignored and `Data` is **merged**. You can't
  clear a value with it.
- `ListNamespaces` includes `temporal-system` and hides deleted Namespaces. Follow `NextPageToken`.
- Search Attributes and archival are **per Namespace**: create them wherever your Workflows run.
