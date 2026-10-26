# Temporal Error handling with Go: a roadmap

## Why this matters, in 30 seconds

Temporal recovers from crashes, network failures and outages on its own. It **cannot** decide what
a declined card, an out-of-stock item or a bug means for your business. That part is your code, and
in Go it has sharp edges:

- An error crosses a process boundary, so **`errors.Is` and your own error types don't survive**. Only a
  message, a Type string, a non-retryable flag and details do.
- **Wrapping** a non-retryable error with `fmt.Errorf` silently makes it retryable.
- A Workflow that **returns** an error fails for good, while one that **panics** stays stuck and
  keeps retrying.
- Timeouts, Retry Policies and compensations decide whether a failure ends in a retry, a clean
  business error or an inconsistent order.

This guide covers those rules, chapter by chapter.

---

## The roadmap

### Stage 1: Foundations *(core concepts, must-know)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 01 | [Platform failures vs application failures](01-platform-vs-application.md) | Tell which failures Temporal recovers from on its own and which need your code, and explain forward vs backward recovery. |
| 02 | [Temporal's error types in Go](02-go-error-types.md) | Name the error types, know what survives the trip from an Activity to a Workflow, and classify any error with `errors.As`. |
| 03 | [Workflow Task failure vs Workflow Execution failure](03-workflow-task-vs-execution.md) | Predict whether a Workflow fails or gets stuck, use `recover` and `defer` safely, and find stuck Workflows. |

### Stage 2: Daily use *(raising and handling errors)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 04 | [Raising application errors](04-application-errors.md) | Return Application errors with a Type, details, a non-retryable flag, a custom retry delay or the Benign category. |
| 05 | [Handling errors in Workflows](05-handling-errors-in-workflows.md) | Branch on timeouts, Types, cancellation, panics and Child Workflow errors, fall back, and map errors to API responses. |
| 06 | [Wrapping errors without losing retryability](06-wrapping-and-retryability.md) | Add context to an error without making a non-retryable error retryable, and test it. |

### Stage 3: Advanced *(retries and failure detection)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 07 | [Retry Policies](07-retry-policies.md) | Read and tune the defaults, compute retry intervals, list non-retryable Types, and decide when a Workflow should retry. |
| 08 | [Detecting failures with timeouts and Heartbeats](08-timeouts-and-heartbeats.md) | Pick Activity timeouts as failure detectors, react to each timeout type, and replace Workflow timeouts with Timers. |

### Stage 4: Production

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 09 | [Backward recovery and an error strategy](09-error-strategy.md) | Classify failures, write a Saga with compensations, inspect failing Activities, and keep error text out of plain sight. |

---

## Core concepts vs. implementation details

| **Core concepts**: understand these first | **Implementation details, options & sub-features**: learn when needed |
| --- | --- |
| Platform vs application failures, forward vs backward recovery ([01](01-platform-vs-application.md)) | — |
| Errors are converted at the boundary; only message, Type, flag and details survive ([02](02-go-error-types.md)) | Type names from `getErrType`, `Is*` helpers, `ServerError`, `NexusOperationError` ([02](02-go-error-types.md)) |
| Return → Execution failure; panic → Task failure ([03](03-workflow-task-vs-execution.md)) | `WorkflowPanicPolicy`, `recover`/`defer` rules, `TemporalReportedProblems` ([03](03-workflow-task-vs-execution.md)) |
| `ApplicationError` with a Type ([04](04-application-errors.md)) | Details, `NextRetryDelay`, Benign category ([04](04-application-errors.md)) |
| Check the wrapper, then the cause ([05](05-handling-errors-in-workflows.md)) | `RetryState`, Child Workflow errors, Client-side mapping, tests ([05](05-handling-errors-in-workflows.md)) |
| Only the outermost error counts ([06](06-wrapping-and-retryability.md)) | A wrap helper that keeps the flag ([06](06-wrapping-and-retryability.md)) |
| Activities retry by default, Workflows don't ([07](07-retry-policies.md)) | Interval formula, `NonRetryableErrorTypes`, Workflow Retry Policy ([07](07-retry-policies.md)) |
| Start-To-Close is the crash detector ([08](08-timeouts-and-heartbeats.md)) | Schedule-To-Start, Heartbeat timeout cap, Workflow timeouts ([08](08-timeouts-and-heartbeats.md)) |
| Compensation for backward recovery ([09](09-error-strategy.md)) | Disconnected contexts, Pending Activities, Failure Converter encryption ([09](09-error-strategy.md)) |

---

## Which error handling do I need?

```text
Where does the failure come from?
├── Worker crash, network, Service outage    → nothing to write, Temporal retries       → chapter 01
└── Your code (bad input, business rule, external API)
    ├── Will retrying help?
    │   ├── Yes, soon                         → return a plain/retryable error           → chapters 04, 07
    │   ├── Yes, but not before X             → NextRetryDelay                           → chapter 04
    │   └── No                                → NewNonRetryableApplicationError          → chapter 04
    │                                           (or NonRetryableErrorTypes on the caller) → chapter 07
    ├── Adding context to an error?           → keep the flag: wrap with ApplicationErrorOptions → chapter 06
    └── Expected and frequent (e.g. "not ready yet")?  → Category: Benign               → chapter 04

In the Workflow, after an Activity fails:
├── Timed out (TimeoutError or RetryState Timeout)? → check this first                  → chapters 05, 08
├── Known Type?                               → switch on ApplicationError.Type()       → chapter 05
├── Earlier steps must be undone?             → compensate (Saga)                       → chapter 09
└── Nothing to do?                            → return a non-retryable ApplicationError → chapters 03, 05

Workflow "Running" but not progressing?       → Workflow Task failure (panic, non-determinism,
                                                bad Activity options)                    → chapters 03, 07, 08
```

---

## Go API cheat sheet

| I want to… | Go API | Chapter |
| --- | --- | --- |
| Return a business error | `temporal.NewApplicationError(msg, errType, details...)` | 04 |
| Stop retries from the Activity | `temporal.NewNonRetryableApplicationError(msg, errType, cause, details...)` | 04 |
| Set delay, cause, category | `temporal.NewApplicationErrorWithOptions(msg, errType, temporal.ApplicationErrorOptions{…})` | 04, 06 |
| Mark an expected error | `ApplicationErrorOptions{Category: temporal.ApplicationErrorCategoryBenign}` | 04 |
| Read Type and details | `errors.As(err, &appErr)`, `appErr.Type()`, `appErr.NonRetryable()`, `appErr.Details(&v)` | 02, 05 |
| Know why retries stopped | `errors.As(err, &actErr)`, `actErr.RetryState()` | 02, 05 |
| Detect a timeout | `errors.As(err, &timeoutErr)`, `timeoutErr.TimeoutType()`, `temporal.IsTimeoutError(err)` | 05, 08 |
| Get the last Heartbeat details | `timeoutErr.HasLastHeartbeatDetails()`, `timeoutErr.LastHeartbeatDetails(&v)` | 08 |
| Detect cancellation / panic | `temporal.IsCanceledError(err)`, `temporal.IsPanicError(err)` | 02, 05 |
| Handle a Child Workflow failure | `errors.As(err, &childErr)` (`*temporal.ChildWorkflowExecutionError`) | 05 |
| Configure retries | `workflow.ActivityOptions{RetryPolicy: &temporal.RetryPolicy{InitialInterval, BackoffCoefficient, MaximumInterval, MaximumAttempts, NonRetryableErrorTypes}}` | 07 |
| Retry a whole Workflow | `client.StartWorkflowOptions{RetryPolicy: …}` | 07 |
| Fail Workflows on panic | `worker.Options{WorkflowPanicPolicy: worker.FailWorkflow}` | 03 |
| Find stuck Workflows | `TemporalReportedProblems IN ("category=WorkflowTaskFailed")` | 03 |
| Run compensations after cancel | `workflow.NewDisconnectedContext(ctx)` | 09 |
| Inspect failing Activities | `c.DescribeWorkflowExecution(ctx, id, "")` → `PendingActivities` | 09 |
| Encrypt error messages | `temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{EncodeCommonAttributes: true, …})` + a codec | 09 |

---

## About the code samples

- Each chapter is a **self-contained module** named `github.com/acme/shop`. Every Go block starts with
  `// file: <path>` so you can paste it into that path.
- All samples were compiled, `gofmt`-checked and `go vet`-checked against **Go SDK v1.50.0**
  (`go.temporal.io/sdk`). The tests in chapters 05 and 06 pass.
- Every behavior the chapters state was run on a dev server (Temporal CLI 1.9.1, Server 1.32.0):
  how each kind of Go error arrives in the Workflow, returned errors vs panics, `recover` in Workflow
  code, wrapping, `NextRetryDelay`, Benign logging, `NonRetryableErrorTypes`, Workflow retries, every
  Activity timeout, invalid Activity options, `TemporalReportedProblems`, the checkout Saga and the
  encrypted Failure Converter.
- External systems are **interfaces or fakes**. Run everything locally with `temporal server start-dev`
  (UI at <http://localhost:8233>).

### Notes from checking the docs against the SDK

- **A returned error fails the Workflow in Go.** The Encyclopedia says a non-Temporal error fails the
  Workflow *Task* and is retried. In Go, a Workflow that **returns** any error, even `errors.New`,
  fails the **Execution** (status Failed). Only a panic or non-determinism fails the Task
  ([chapter 03](03-workflow-task-vs-execution.md)).
- **`recover()` works in Workflow code.** The Go page says you cannot `recover()` from a panic in a
  `defer`. It works. What fails is a deferred function that calls a blocking SDK API (like
  `workflow.Sleep`) during the panic: the Task fails with "yield during panic unwinding"
  ([chapter 03](03-workflow-task-vs-execution.md)).
- **Type names of plain Go errors.** The Encyclopedia says the error's type name is preserved.
  `errors.New` arrives with an **empty** Type, `fmt.Errorf("%w")` with `"wrapError"`, and only your own
  struct types keep a useful name. `errors.Is` and `errors.As` on your own types are false in the
  Workflow ([chapter 02](02-go-error-types.md)).
- **The outermost-error rule isn't on the Go page.** The Encyclopedia warns that `fmt.Errorf` wrapping
  makes a non-retryable error retryable; the Go page doesn't. Verified: the wrapped error was retried
  3 times with Type `wrapError`. The Benign category follows the same rule
  ([chapter 06](06-wrapping-and-retryability.md)).
- **Go supports per-error retry delays.** The Retry Policies page lists Java, Rust and TypeScript for
  "next Retry delay". Go has `ApplicationErrorOptions.NextRetryDelay` (a 3s delay overrode a 200ms
  InitialInterval) ([chapter 04](04-application-errors.md)).
- **A timeout isn't always a `TimeoutError`.** When a Workflow Execution Timeout cuts Activity retries
  short, the Workflow sees `ActivityError` → `ApplicationError` with `RetryState` Timeout. Check timeouts
  first ([chapters 02](02-go-error-types.md), [05](05-handling-errors-in-workflows.md), [08](08-timeouts-and-heartbeats.md)).
- **Schedule-To-Close gives up early.** When the next retry can't fit, the Service stops with "Not
  enough time to schedule next retry". The `TimeoutError` wraps the last attempt's error
  ([chapter 08](08-timeouts-and-heartbeats.md)).
- **Workflow Execution Timeout's type.** The Client gets a `TimeoutError` with `TimeoutType()`
  `StartToClose`, and deferred cleanup never runs ([chapter 08](08-timeouts-and-heartbeats.md)).
- **Invalid Activity options don't return an error.** A negative `MaximumAttempts`, or no Start-To-Close
  and no Schedule-To-Close, fails the Workflow **Task** with `BadScheduleActivityAttributes` and the
  Workflow stays Running. The page says a negative value "results in an error"
  ([chapters 07](07-retry-policies.md), [08](08-timeouts-and-heartbeats.md)).
- **Workflow Task Timeout cap.** The SDK doc comment says 1 minute. The Server caps it at 120s
  (a 5m value was stored as 120s), as the Encyclopedia says ([chapter 08](08-timeouts-and-heartbeats.md)).
- **`TemporalReportedProblems` without extra config.** The Web UI page says the dynamic config must
  be updated. On a dev server it was set after 5 failed Workflow Tasks with no config change
  ([chapter 03](03-workflow-task-vs-execution.md)).
- **Failure Converter.** `EncodeCommonAttributes: true` without a codec only moves the text into a
  readable payload; a codec is what encrypts it. Go `ApplicationError`s carry no stack trace, only
  panics do ([chapter 09](09-error-strategy.md)).
- **Test environment vs Service.** `TestWorkflowEnvironment` reports `RETRY_STATE_UNSPECIFIED` when
  retries run out; the Service reports `MaximumAttemptsReached`. Branch on Types, not on `RetryState`
  values that tests can't reproduce ([chapter 05](05-handling-errors-in-workflows.md)).
- **The Go page's sample doesn't compile.** It uses undefined `outcome`, `failure` and `YourActivity`.
  Chapter 05 has a complete version.

---

## Sources

**Encyclopedia:** [Failures and error handling](https://docs.temporal.io/encyclopedia/failures-and-error-handling) · [Application failures](https://docs.temporal.io/encyclopedia/application-failures) · [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures) · [Detecting Workflow failures](https://docs.temporal.io/encyclopedia/detecting-workflow-failures) · [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)

**Go SDK:** [Error handling](https://docs.temporal.io/develop/go/best-practices/error-handling)
