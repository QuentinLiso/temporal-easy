# Temporal Workers with Go: a roadmap

## Why this matters, in 30 seconds

The Temporal Service never runs your code. **Workers do.** They poll Task Queues, run your
Workflows and Activities, and report back. When a Workflow is "stuck", the cause is almost always
on the Worker side: nobody polls that Task Queue, the type isn't registered, the Worker is out of
slots, or a deploy killed it mid-Activity.

Getting Workers right decides:

- **Whether work is picked up at all**: same Task Queue name, same registrations.
- **How fast and how much**: slots, pollers, rate limits, the Workflow cache.
- **Where it runs**: dedicated queues, per-host routing, Sessions.
- **What survives a deploy**: graceful shutdown and Worker Versioning.

---

## The roadmap

### Stage 1: Foundations *(core concepts, must-know)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 01 | [What is a Worker?](01-what-is-a-worker.md) | Tell a Worker Program, Process and Entity apart, explain why the Service never runs your code, and run a first Worker. |
| 02 | [Tasks](02-tasks.md) | Name the three kinds of Tasks, say when a Workflow Task is scheduled, and tell a Workflow Task failure from a Workflow Execution failure. |
| 03 | [Task Queues](03-task-queues.md) | Explain how Workers find work, apply the identical-registration rule, set Task Queues in the right places, and catch a name mismatch. |

### Stage 2: Daily use *(running and sizing Workers)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 04 | [Running a Worker in Go](04-running-a-worker.md) | Create, register and run Workers, host several Task Queues in one process, connect to Cloud, and set a useful Worker Identity. |
| 05 | [Worker options](05-worker-options.md) | Size slots and pollers, rate-limit per Worker or per Task Queue, and run Activity-only Workers. |
| 06 | [Sticky Execution](06-sticky-execution.md) | Explain the sticky queue and the Workflow cache, and tune `StickyScheduleToStartTimeout` and the cache size. |

### Stage 3: Advanced *(routing and cross-cutting code)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 07 | [Task Routing](07-task-routing.md) | Send Activities to special environments, pin a pipeline to one host, and handle priorities. |
| 08 | [Worker Sessions](08-sessions.md) | Run a series of Activities on one Worker with the Session API, and handle Worker loss correctly. |
| 09 | [Interceptors](09-interceptors.md) | Add auditing, metrics and logging around every Client call, Workflow and Activity, and test it. |

### Stage 4: Production

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 10 | [Graceful shutdown](10-graceful-shutdown.md) | Set `WorkerStopTimeout`, write Activities that checkpoint on shutdown, and wire shutdown into your platform. |
| 11 | [Worker Versioning](11-worker-versioning.md) | Ship new Workflow code with Pinned and Auto-Upgrade Workflows, ramp a new Version and drain the old one. |
| 12 | [Serverless Workers](12-serverless-workers.md) | Know when Temporal can start Workers for you, and run a Worker on AWS Lambda or GCP Cloud Run. |

---

## Core concepts vs. implementation details

| **Core concepts**: understand these first | **Implementation details, options & sub-features**: learn when needed |
| --- | --- |
| Workers poll; the Service only hands out Tasks ([01](01-what-is-a-worker.md)) | Worker Program / Process / Entity vocabulary, Data Converter ([01](01-what-is-a-worker.md)) |
| Workflow, Activity and Nexus Tasks ([02](02-tasks.md)) | Workflow Task failure vs Workflow Execution failure ([02](02-tasks.md)) |
| Task Queues, created on demand ([03](03-task-queues.md)) | Partitions, ordering, sync match ([03](03-task-queues.md)) |
| Same Task Queue ⇒ same registrations ([03](03-task-queues.md)) | Custom names, `DisableRegistrationAliasing` ([04](04-running-a-worker.md)) |
| `worker.New` → `Register…` → `Run` ([04](04-running-a-worker.md)) | `Start`/`Stop`, several Workers per process, Identity, `OnFatalError` ([04](04-running-a-worker.md)) |
| Slots and pollers ([05](05-worker-options.md)) | Rate limits, poller autoscaling, eager Activities, `Tuner` ([05](05-worker-options.md)) |
| Sticky Execution ([06](06-sticky-execution.md)) | Cache size, `StickyScheduleToStartTimeout` ([06](06-sticky-execution.md)) |
| Task Routing with dedicated queues ([07](07-task-routing.md)) | Per-host queues, priorities and fairness ([07](07-task-routing.md)), Sessions ([08](08-sessions.md)) |
| — | Interceptors ([09](09-interceptors.md)) |
| Graceful shutdown ([10](10-graceful-shutdown.md)) | Worker stop channel, heartbeat checkpoints, Local Activities on shutdown ([10](10-graceful-shutdown.md)) |
| Worker Deployments and Versions ([11](11-worker-versioning.md)) | Ramping, inheritance rules, `WorkerDeploymentClient` ([11](11-worker-versioning.md)) |
| — | Serverless Workers, `lambdaworker`, Cloud Run ([12](12-serverless-workers.md)) |

---

## Which Worker setup do I need?

```text
Is a Workflow stuck in Running with no progress?
├── No pollers on its Task Queue?        → Task Queue name mismatch / no Worker  → chapter 03
├── "unable to find workflow type"?      → register it on every Worker of that queue → chapters 03, 04
└── Backlog grows, Workers busy?         → slots, pollers, more Workers           → chapter 05

Must some Activities run in a special place?
├── A kind of machine (GPU, PCI zone)    → a dedicated Task Queue + Worker pool   → chapter 07
└── The same host as a previous Activity
    ├── Go, simple                       → Worker Sessions                        → chapter 08
    └── Full control                     → per-host Task Queue                    → chapter 07

Must a downstream API not be flooded?
├── Per Worker                           → WorkerActivitiesPerSecond              → chapter 05
└── Across all Workers                   → TaskQueueActivitiesPerSecond           → chapter 05

Will you deploy Workers while Workflows are running?
├── Activities in flight                 → WorkerStopTimeout + heartbeats         → chapter 10
└── Workflow code changes                → Worker Versioning (or patching)        → chapter 11

Don't want to run Worker processes at all? → Serverless Workers (Public Preview)  → chapter 12
```

---

## Go API cheat sheet

| I want to… | Go API | Chapter |
| --- | --- | --- |
| Create a Worker | `worker.New(c, taskQueue, worker.Options{})` | 04 |
| Register code | `w.RegisterWorkflow(fn)`, `w.RegisterActivity(&Activities{…})`, `…WithOptions(fn, workflow.RegisterOptions{Name: …})` | 04 |
| Run until SIGINT/SIGTERM | `w.Run(worker.InterruptCh())` | 04, 10 |
| Run inside a service | `w.Start()` … `w.Stop()` | 04, 10 |
| Name the Worker | `worker.Options{Identity: …}` (default `pid@hostname@`) | 04 |
| Size concurrency | `MaxConcurrentActivityExecutionSize`, `MaxConcurrentWorkflowTaskExecutionSize`, `MaxConcurrent…TaskPollers` | 05 |
| Rate-limit Activities | `WorkerActivitiesPerSecond` (per Worker), `TaskQueueActivitiesPerSecond` (whole queue) | 05 |
| Activity-only Worker | `worker.Options{DisableWorkflowWorker: true}` | 05 |
| Tune the Workflow cache | `worker.SetStickyWorkflowCacheSize(n)`, `StickyScheduleToStartTimeout` | 06 |
| Route an Activity | `workflow.ActivityOptions{TaskQueue: …}` | 03, 07 |
| Prioritize a Task | `Priority: temporal.Priority{PriorityKey: …, FairnessKey: …}` | 07 |
| Keep Activities on one Worker | `EnableSessionWorker: true`, `workflow.CreateSession`, `workflow.CompleteSession` | 08 |
| Detect a lost Session | `workflow.GetSessionInfo(ctx).SessionState == workflow.SessionStateFailed` | 08 |
| Add an interceptor | `client.Options{Interceptors: …}`, `worker.Options{Interceptors: …}`, `interceptor.WorkerInterceptorBase` | 09 |
| Shut down gracefully | `worker.Options{WorkerStopTimeout: …}`, `activity.GetWorkerStopChannel(ctx)` | 10 |
| Run a versioned Worker | `DeploymentOptions{UseVersioning: true, Version: worker.WorkerDeploymentVersion{…}}`, `RegisterOptions{VersioningBehavior: …}` | 11 |
| Promote a Version | `temporal worker deployment set-current-version`, `c.WorkerDeploymentClient().GetHandle(…).SetCurrentVersion(…)` | 11 |
| Run on AWS Lambda | `lambdaworker.RunWorker(version, func(o *lambdaworker.Options) error {…})` | 12 |

---

## About the code samples

- Each chapter is a **self-contained module** named `github.com/acme/shop`. Every Go block starts with
  `// file: <path>` so you can paste it into that path.
- All samples were compiled, `gofmt`-checked and `go vet`-checked against **Go SDK v1.50.0**
  (`go.temporal.io/sdk`), `go.temporal.io/sdk/contrib/envconfig` v1.0.2 and
  `go.temporal.io/sdk/contrib/aws/lambdaworker` v0.1.1. The interceptor unit test in chapter 09 passes.
- Behaviors were checked on a dev server (Temporal CLI 1.9.1, Server 1.32.0): unknown types and
  Task Queue typos (03), registration panics (04, 11), the sticky → normal queue fallback (06),
  per-host routing and its fallback (07), Sessions including Worker loss and Continue-As-New (08),
  the three `WorkerStopTimeout` cases and heartbeat resume (10), and a Pinned vs Auto-Upgrade
  rollout from v1 to v2 (11).
- External systems are **interfaces or fakes**. Run everything locally with `temporal server start-dev`
  (UI at <http://localhost:8233>).

### Notes from checking the docs against the SDK

- **`WorkerStopTimeout` defaults to 0.** The Go "Run a Worker" page says the Worker waits for in-flight
  Tasks "up to the `WorkerStopTimeout`", but not that the default is **no wait**. With the default,
  `Stop()` returns at once, the Activity context is canceled and the attempt is retried
  ([chapter 10](10-graceful-shutdown.md)).
- **"Replays from the beginning".** The Tasks page says every Workflow Task replays the whole history.
  That only happens on a cache miss; with Sticky Execution the Worker applies just the new events
  ([chapters 02](02-tasks.md) and [06](06-sticky-execution.md)).
- **Default Worker Identity.** The Encyclopedia says `pid@hostname`. In Go it is `pid@hostname@`
  (trailing `@`), inherited from the Client ([chapter 04](04-running-a-worker.md)).
- **Registration without a versioning behavior.** The Go page says it fails at registration. The SDK
  panics only when `UseVersioning` **and** a `Version` are set. `DefaultVersioningBehavior` without
  `UseVersioning` panics earlier, in `worker.New` ([chapter 11](11-worker-versioning.md)).
- **Session failure handling.** When the Session's Worker disappears **during** an Activity, that
  Activity returns a canceled error, not `ErrSessionFailed`; only later Activities get
  `ErrSessionFailed`. Retrying only on `ErrSessionFailed` fails the Workflow (seen on a dev server).
  Check `GetSessionInfo(…).SessionState` instead. A graceful stop fails the Session immediately
  ([chapter 08](08-sessions.md)).
- **Session limit.** `MaxConcurrentSessionExecutionSize` defaults to **1000**, not "a very large value",
  and applies per Worker, not per process ([chapter 08](08-sessions.md)).
- **Sessions and Worker Versioning.** The SDK says Sessions are not compatible with versioning, but
  only rejects the deprecated `UseBuildIDForVersioning`. With `DeploymentOptions.UseVersioning` there
  is no check. Test before combining them ([chapter 08](08-sessions.md)).
- **Server-side rate limit disables eager Activities.** Setting `TaskQueueActivitiesPerSecond` turns off
  eager Activity execution (SDK doc comment). The Task Routing page doesn't say so
  ([chapter 05](05-worker-options.md)).
- **No option reference.** The Go "Run a Worker" page links `worker.Options` to the `internal` package
  and says only that an empty struct works. Chapter 05 has a defaults table read from the source.
- **Fatal errors.** After startup only `NamespaceNotFound`, `InvalidArgument` and
  `ClientVersionNotSupported` stop a Worker (and call `OnFatalError`). A Worker with nothing
  registered starts without error ([chapter 04](04-running-a-worker.md)).
- **`lambdaworker`.** `ShutdownDeadlineBuffer` is computed from the default `WorkerStopTimeout` (5s)
  **before** your configure callback runs, so raising `WorkerStopTimeout` there leaves the buffer at
  7s. Set both. The package also reads `TEMPORAL_TASK_QUEUE` and sets the identity to
  `requestID@functionARN`; neither is on the Go page ([chapter 12](12-serverless-workers.md)).
- **CLI rollout.** `set-current-version` and `set-ramping-version` ask for confirmation; use `--yes`
  in CI ([chapter 11](11-worker-versioning.md)).

---

## Sources

**Encyclopedia:** [Workers](https://docs.temporal.io/workers) · [Tasks](https://docs.temporal.io/tasks) · [Task Queues](https://docs.temporal.io/task-queue) · [Task Queue naming](https://docs.temporal.io/task-queue/naming) · [Task Routing](https://docs.temporal.io/task-routing) · [Sticky Execution](https://docs.temporal.io/sticky-execution) · [Worker shutdown](https://docs.temporal.io/encyclopedia/workers/worker-shutdown) · [Worker Versioning](https://docs.temporal.io/worker-versioning) · [Serverless Workers](https://docs.temporal.io/serverless-workers)

**Go SDK:** [Run a Worker](https://docs.temporal.io/develop/go/workers/run-worker-process) · [Worker Sessions](https://docs.temporal.io/develop/go/workers/sessions) · [Interceptors](https://docs.temporal.io/develop/go/workers/interceptors) · [Serverless Workers](https://docs.temporal.io/develop/go/workers/serverless-workers) · linked from it: [AWS Lambda](https://docs.temporal.io/develop/go/workers/serverless-workers/aws-lambda) · [GCP Cloud Run](https://docs.temporal.io/develop/go/workers/serverless-workers/cloud-run)
