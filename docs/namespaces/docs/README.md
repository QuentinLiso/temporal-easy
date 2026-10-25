# Temporal Namespaces with Go: a roadmap

> A progressive onboarding guide to **Temporal Namespaces** for Go developers. It is based on the
> Temporal Encyclopedia's Namespace page (the concept) and the Go SDK's Namespaces page (how to do it
> in Go), read side by side and checked against the SDK source and a dev server. Every chapter has
> **compilable Go examples** with production-like names.
>
> Companion guides: [Temporal Client with Go](../../client/docs/README.md) (connecting to a Namespace),
> [Temporal Workers with Go](../../workers/docs/README.md) (Workers run inside one Namespace),
> [Temporal Architecture with Go](../../architecture/docs/README.md) (where Namespaces sit in the Service).

![Namespaces mental model](../img/namespaces-mental-model.svg)

---

## Why this matters, in 30 seconds

Every Workflow, Task Queue and Worker lives in a **Namespace**. It decides which Workflow IDs can clash,
how long finished Workflows are kept, who can access them and, on Temporal Cloud, which credentials and
rate limits apply. Getting Namespaces wrong shows up late:

- A Worker connected to a **missing** Namespace connects fine and fails only on its first call.
- The **retention** you pick at registration decides how long finished Workflows can still be debugged.
- A cross-Namespace Child Workflow is **rejected** by the Service, and the parent stays stuck.
- **Deleting** a Namespace is instant, irreversible and unguarded, even for `default`.

This guide covers these, from the concept to operating Namespaces as code.

---

## The roadmap

### Stage 1: Foundations *(core concepts, must-know)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 01 | [What is a Namespace?](01-what-is-a-namespace.md) | Explain what a Namespace isolates, what `default` is, and why a shared Namespace still needs naming rules. |
| 02 | [Using a Namespace from Go](02-using-a-namespace.md) | Point Clients and Workers at a Namespace, catch a missing one at startup, and use several Namespaces over one connection. |

### Stage 2: Daily use *(creating and managing Namespaces)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 03 | [Registering a Namespace](03-registering-namespaces.md) | Create a Namespace with the CLI or Go, choose a retention, make it idempotent, and wait until it's usable. |
| 04 | [Describing, updating and listing Namespaces](04-managing-namespaces.md) | Read and change a Namespace's settings, list Namespaces, and know what each API can and can't change. |

### Stage 3: Advanced *(retiring Namespaces)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 05 | [Deprecating and deleting Namespaces](05-deprecating-and-deleting.md) | Retire a Namespace safely: deprecate, drain, delete, and protect the ones that must never go. |

### Stage 4: Production

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 06 | [Designing your Namespaces](06-namespace-strategy.md) | Decide how to split Namespaces, call across them the supported way, control access, and manage Namespaces as code. |

---

## Core concepts vs. implementation details

| **Core concepts**: understand these first | **Implementation details, options & sub-features**: learn when needed |
| --- | --- |
| A Namespace is a unit of isolation ([01](01-what-is-a-namespace.md)) | OSS vs Cloud extras ([01](01-what-is-a-namespace.md)) |
| Workflow IDs are unique per Namespace ([01](01-what-is-a-namespace.md)) | ID and Task Queue prefixes in a shared Namespace ([06](06-namespace-strategy.md)) |
| A Client and its Workers use one Namespace ([02](02-using-a-namespace.md)) | `TEMPORAL_NAMESPACE`, `NewClientFromExisting`, startup guard ([02](02-using-a-namespace.md)) |
| A Namespace must be registered, with a retention ([03](03-registering-namespaces.md)) | Idempotent registration, propagation delay ([03](03-registering-namespaces.md)) |
| Settings are per Namespace ([04](04-managing-namespaces.md)) | `Update` semantics, listing, per-Namespace Search Attributes and archival ([04](04-managing-namespaces.md)) |
| Registered → Deprecated → Deleted ([05](05-deprecating-and-deleting.md)) | Deletion rename, guarded retirement ([05](05-deprecating-and-deleting.md)) |
| Namespaces don't call each other directly ([06](06-namespace-strategy.md)) | Activity + second Client, Nexus, Namespaces as code ([06](06-namespace-strategy.md)) |

---

## Which Namespace setup do I need?

```text
Starting out, one service, local dev?           → "default" (or start-dev --namespace …)     → chapters 01, 02
Several environments (dev / staging / prod)?    → one Namespace per environment              → chapter 06
Several teams or bounded contexts?
├── Different retention, access or blast radius → one Namespace each                         → chapter 06
└── Same everything                             → share one, prefix IDs and Task Queues       → chapter 06
Must a Workflow call into another Namespace?
├── Simple request/response                     → an Activity with a second Client            → chapter 06
└── A contract between teams                    → Nexus                                       → chapter 06
Creating Namespaces from code or CI?            → idempotent Ensure / Reconcile, retry NotFound → chapters 03, 06
Retiring a Namespace?                           → deprecate, drain, then delete (guarded)     → chapter 05
On Temporal Cloud?                              → Cloud UI or `temporal cloud namespace`, not NamespaceClient → chapter 03
```

---

## Go API cheat sheet

| I want to… | Go API | Chapter |
| --- | --- | --- |
| Connect to a Namespace | `client.Dial(client.Options{Namespace: "shop-prod"})`, or `TEMPORAL_NAMESPACE` with `envconfig` | 02 |
| Fail fast if it doesn't exist | `nc.Describe(ctx, name)` at startup (`Dial` doesn't check) | 02 |
| Second Namespace, same connection | `client.NewClientFromExisting(c, client.Options{Namespace: …})` | 02, 06 |
| Know the current Namespace in a Workflow | `workflow.GetInfo(ctx).Namespace` | 01 |
| Get a Namespace client | `client.NewNamespaceClient(client.Options{…})` | 03 |
| Register | `nc.Register(ctx, &workflowservice.RegisterNamespaceRequest{Namespace, WorkflowExecutionRetentionPeriod: durationpb.New(…)})` | 03 |
| Detect "already exists" | `errors.As(err, new(*serviceerror.NamespaceAlreadyExists))` | 03 |
| Describe | `nc.Describe(ctx, name)` | 04 |
| Change retention / description | `nc.Update(ctx, &workflowservice.UpdateNamespaceRequest{UpdateInfo: …, Config: …})` | 04 |
| List | `c.WorkflowService().ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{PageSize, NextPageToken})` | 04 |
| Deprecate | `nc.Update(…, UpdateInfo: &namespacepb.UpdateNamespaceInfo{State: enumspb.NAMESPACE_STATE_DEPRECATED})` | 05 |
| Delete | `c.OperatorService().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{Namespace: …})` | 05 |
| Call another Namespace | an Activity holding a Client for that Namespace, or Nexus | 06 |

---

## Diagrams

| Diagram | Used in |
| --- | --- |
| [Namespaces mental model](../img/namespaces-mental-model.svg) | README, 01 |
| [Namespace lifecycle](../img/namespaces-lifecycle.svg) | 05 |
| [Namespace topology](../img/namespaces-topology.svg) | 06 |

---

## About the code samples

- Each chapter is a **self-contained module** named `github.com/acme/shop`. Every Go block starts with
  `// file: <path>` so you can paste it into that path.
- All samples were compiled, `gofmt`-checked and `go vet`-checked against **Go SDK v1.50.0**
  (`go.temporal.io/sdk`) and `go.temporal.io/sdk/contrib/envconfig` v1.0.2.
- Every behavior was run on a dev server (Temporal CLI 1.9.1, Server 1.32.0): the same Workflow ID in
  two Namespaces, connecting to a missing Namespace, registration errors and timing, `Update`
  semantics, per-Namespace Search Attributes, deprecation, deletion, a Worker whose Namespace is
  deleted, the rejected cross-Namespace Child Workflow, and the cross-Namespace Activity.
- The Namespace admin APIs (`NamespaceClient`, `OperatorService`) work on a **self-hosted** Service
  only. On Temporal Cloud, use the Cloud UI or `temporal cloud namespace`.
- Run everything locally with `temporal server start-dev` (UI at <http://localhost:8233>).

### Notes from checking the docs against the SDK

- **The SDK can't do everything the page lists.** The Go page says you can "create, update, deprecate,
  or delete" Namespaces with SDK APIs. `NamespaceClient` has only `Register`, `Describe` and `Update`.
  Listing and deleting go through the raw `WorkflowService()` and `OperatorService()`, and deprecating
  is an `Update` of the state ([chapter 04](04-managing-namespaces.md)).
- **`DeprecateNamespace` is itself deprecated.** The RPC still works but is marked "Deprecated." in the
  API; set `State: NAMESPACE_STATE_DEPRECATED` with `Update` instead ([chapter 05](05-deprecating-and-deleting.md)).
- **Minimum retention.** The page says the minimum is 1 day. The dev server accepted **1 hour** through
  the API and the CLI: the minimum is a server setting. The API requires a retention
  (`InvalidArgument` without one), but the CLI fills in **72h** when `--retention` is omitted
  ([chapter 03](03-registering-namespaces.md)).
- **"Up to 10 seconds".** A brand-new Namespace was usable within about 60 ms on a dev server. A name
  that had been looked up before it existed returned `NamespaceNotFound` for 0.3–1 s after `Register`,
  and multi-node clusters add cache delay. Retry `NamespaceNotFound` briefly instead of sleeping
  ([chapter 03](03-registering-namespaces.md)).
- **`Dial` doesn't check the Namespace.** `Dial`, `DialContext` and `CheckHealth` succeed for a missing
  Namespace; the first real call returns `NamespaceNotFound` ([chapter 02](02-using-a-namespace.md)).
- **`NewNamespaceClient` ignores `Options.Namespace`** (`internal/client.go`). Every call names its
  Namespace explicitly ([chapter 03](03-registering-namespaces.md)).
- **`Update` only adds.** Empty strings are ignored, `Data` is merged, and an empty `Config` changes
  nothing: you can't clear a value ([chapter 04](04-managing-namespaces.md)).
- **Deprecation is one-way and not instant.** Deprecated → Registered is rejected (`Invalid namespace
  state update`). New starts were refused about 2 s after deprecating; one made right after still
  went through. Running Workflows kept working and received Signals ([chapter 05](05-deprecating-and-deleting.md)).
- **Setting the state to Deleted isn't a delete.** It hides the Namespace and refuses starts, but
  `Describe` still finds it. Use `DeleteNamespace` ([chapter 05](05-deprecating-and-deleting.md)).
- **Deletion is unguarded.** The dev server deleted `default` with no confirmation. A deleted
  Namespace is renamed `<name>-deleted-<suffix>` and the name can be reused seconds later
  ([chapter 05](05-deprecating-and-deleting.md)).
- **Workers don't notice a deleted Namespace.** A Worker kept polling with no error and no
  `OnFatalError` for 30 s+ ([chapter 05](05-deprecating-and-deleting.md)).
- **Cross-Namespace Child Workflows are rejected.** `ChildWorkflowOptions.Namespace` is deprecated
  (disabled by default since Server 1.30.1). The parent's Workflow Task failed with "cross namespace
  commands are not allowed" and the Workflow stayed Running ([chapter 06](06-namespace-strategy.md)).
- **The Go page's Update sample** uses a `replication` alias without its import
  (`go.temporal.io/api/replication/v1`) and mostly commented-out fields
  ([chapter 04](04-managing-namespaces.md)).

---

## Sources

**Encyclopedia:** [Temporal Namespace](https://docs.temporal.io/namespaces)

**Go SDK:** [Namespaces](https://docs.temporal.io/develop/go/client/namespaces)
