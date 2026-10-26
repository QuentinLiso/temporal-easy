# Temporal Observability with Go: a roadmap

## Why this matters, in 30 seconds

One checkout in a Temporal application is an HTTP request, a Workflow that lives for days, Activities
retried on different Workers, and maybe a Child Workflow in another service. The Event History tells you
what happened to **one** Execution. To see the system as a whole you add logs, metrics and traces, and to
connect them you **propagate context** (a trace, a tenant) across every Temporal boundary.

Replay changes the rules. Workflow code runs again on every replay, so a plain logger prints twice, a plain
counter counts twice, and a plain tracer emits duplicate spans with random IDs. The Go SDK gives you
replay-safe versions of each, plus a few traps:

- The default logger prints `DEBUG` lines to stdout; Workflow logs disappear during replay unless you ask.
- Propagated headers are stored in the Event History **in clear**, even with an encryption codec.
- The OpenTelemetry v2 plugin **panics** unless the global Tracer Provider is replay-safe.
- SDK counters export as Prometheus **gauges** unless you turn on monotonic counters.

---

## The roadmap

### Stage 1: Foundations *(core concepts, must-know)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 01 | [Observability in Temporal](01-what-is-observability.md) | Tell what the Event History, UI and CLI already show, what you must add, and why replay changes the rules. |
| 02 | [Logging](02-logging.md) | Log from Workflows and Activities without duplicates, plug in `slog` or zap, and know what the SDK adds to each line. |
| 03 | [Context propagation](03-context-propagation.md) | Carry a tenant from the Client into Workflows, Activities and Child Workflows with a `ContextPropagator`. |

### Stage 2: Daily use

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 04 | [Context propagation in depth](04-propagation-in-depth.md) | Know what crosses Signals, Queries, Updates, Schedules and Continue-As-New, combine propagators, test them, and propagate over Nexus. |
| 05 | [Metrics](05-metrics.md) | Export SDK metrics to Prometheus, tag them by owner, add replay-safe custom metrics, and alert on the right ones. |

### Stage 3: Advanced *(tracing)*

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 06 | [Tracing with OpenTelemetry](06-tracing-with-opentelemetry.md) | Set up the replay-safe Tracer Provider and plugin, add spans in Workflows and Activities, and read the trace of one checkout. |
| 07 | [Tracing options, baggage and migration](07-tracing-options-and-migration.md) | Tune the propagator, header key and baggage, survive bad headers, and migrate from the v1 interceptor. |

### Stage 4: Production

| # | Chapter | You'll be able to… |
| --- | --- | --- |
| 08 | [Observability in production](08-observability-in-production.md) | Wire logs, traces and metrics in one place, shut them down in the right order, prove it with a test, and know what to alert on. |

---

## Core concepts vs. implementation details

| **Core concepts**: understand these first | **Implementation details, options & sub-features**: learn when needed |
| --- | --- |
| Event History vs logs, metrics, traces ([01](01-what-is-observability.md)) | Shared Client options, dev server `/metrics` ([01](01-what-is-observability.md)) |
| Replay re-runs Workflow code: use replay-safe APIs ([01](01-what-is-observability.md)) | `EnableLoggingInReplay`, `workflow.GetMetricsHandler`, `temporalotel.Tracer` ([02](02-logging.md), [05](05-metrics.md), [06](06-tracing-with-opentelemetry.md)) |
| `workflow.GetLogger` / `activity.GetLogger` ([02](02-logging.md)) | `log.NewStructuredLogger`, zap via `zapslog`, SDK-added fields ([02](02-logging.md)) |
| Context travels in Temporal headers ([03](03-context-propagation.md)) | The four propagator methods, missing values, codecs don't apply ([03](03-context-propagation.md)) |
| Not every boundary carries context the same way ([04](04-propagation-in-depth.md)) | Several propagators, Plugins, `SetContextPropagators` in tests, Nexus interceptors ([04](04-propagation-in-depth.md)) |
| A metrics handler on the Client ([05](05-metrics.md)) | OTel / Prometheus / Tally, `WithTags`, `UseMonotonicCounters` ([05](05-metrics.md)) |
| One trace across Client, Workflow and Activities ([06](06-tracing-with-opentelemetry.md)) | Replay-safe provider, `AddTemporalSpans`, OTLP export ([06](06-tracing-with-opentelemetry.md)) |
| Trace context is just another header ([07](07-tracing-options-and-migration.md)) | `HeaderKey`, `TextMapPropagator`, baggage, `AllowInvalidParentSpans`, v1 → v2 ([07](07-tracing-options-and-migration.md)) |
| Set up once, shut down in order ([08](08-observability-in-production.md)) | `telemetry.Setup`, `SIGTERM` handling, checklist ([08](08-observability-in-production.md)) |

---

## Which signal do I need?

```text
What happened to ONE Execution?                      → Event History, Web UI, CLI (no code)          → 01
What did my code decide, and why?                    → logs: workflow.GetLogger / activity.GetLogger → 02
Is the system healthy? Are Workers keeping up?       → SDK metrics + alerts                          → 05
How many orders per merchant plan, per minute?       → custom metrics (low-cardinality tags only)    → 05
Where did one request spend its time, end to end?    → traces: OpenTelemetry v2 plugin               → 06
Which tenant / request is this Workflow working for?
├── Readable everywhere in Workflow and Activity code → a ContextPropagator                          → 03, 04
└── Only to connect telemetry                         → trace context + baggage (no secrets)          → 07
Crossing a Nexus boundary?                           → interceptors, not ContextPropagator           → 04
Putting it all together for production?              → one Setup, ordered shutdown, checklist        → 08
```

---

## Go API cheat sheet

| I want to… | Go API | Chapter |
| --- | --- | --- |
| Log from a Workflow | `workflow.GetLogger(ctx).Info("…", "key", v)` | 02 |
| Log from an Activity | `activity.GetLogger(ctx).Info(…)` | 02 |
| Use slog (or zap through slog) | `client.Options{Logger: log.NewStructuredLogger(slog.New(handler))}` | 02 |
| Log during replay too (debugging only) | `worker.Options{EnableLoggingInReplay: true}` | 02 |
| Propagate a value | implement `workflow.ContextPropagator` (`Inject`, `Extract`, `InjectFromWorkflow`, `ExtractToWorkflow`) | 03 |
| Register a propagator | `client.Options{ContextPropagators: []workflow.ContextPropagator{…}}` | 03 |
| Package propagators | `temporal.NewSimplePlugin(temporal.SimplePluginOptions{ContextPropagators: …})` | 04 |
| Test a propagator | `env.SetContextPropagators(…)`, `env.SetHeader(…)` | 04 |
| Emit SDK metrics | `client.Options{MetricsHandler: temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{…})}` | 05 |
| Tag all metrics | `handler.WithTags(map[string]string{"team": …})` | 05 |
| Custom metric in a Workflow | `workflow.GetMetricsHandler(ctx).Counter("…").Inc(1)` | 05 |
| Replay-safe Tracer Provider | `p := temporalotel.NewReplaySafeTracerProvider(sdktrace.WithBatcher(exp)); otel.SetTracerProvider(p)` | 06 |
| Tracing on Client and Workers | `plugin, _ := temporalotel.NewPlugin(temporalotel.PluginOptions{…})`, `client.Options{Plugins: []client.Plugin{plugin}}` | 06 |
| Span in a Workflow | `ctx, span := temporalotel.Tracer("shop").Start(ctx, "…")` (inside the Workflow function) | 06 |
| Span in an Activity or Client | `ctx, span := otel.Tracer("shop").Start(ctx, "…")` | 06 |
| Temporal's own spans | `PluginOptions{TracerOptions: tracing.TracerOptions{AddTemporalSpans: true}}` | 06 |
| Tune propagation | `PluginOptions.TextMapPropagator`, `TracerOptions.HeaderKey`, `DisableBaggage`, `AllowInvalidParentSpans` | 07 |

---

## About the code samples

- Each chapter is a **self-contained module** named `github.com/acme/shop`. Every Go block starts with
  `// file: <path>` so you can paste it into that path. Chapters 04 and 08 reuse chapter 03's
  `internal/tenancy` package (04 repeats it in a collapsed block).
- All samples were compiled, `gofmt`-checked and `go vet`-checked against **Go SDK v1.50.0**,
  **`go.temporal.io/sdk/contrib/opentelemetry-v2` v0.1.0** (experimental), `contrib/opentelemetry` v0.8.1,
  `contrib/tally` v0.2.0 and OpenTelemetry Go v1.44.0. Tests in the chapters pass.
- Every behavior was run on a dev server (Temporal CLI 1.9.1, Server 1.32.0) with in-memory span
  exporters and a real Prometheus scrape. Output shown in the chapters is real.
- Run everything locally with `temporal server start-dev` (UI at <http://localhost:8233>).

### Notes from checking the docs against the SDK

- **Sources.** The topic lists the Context propagation and OpenTelemetry v2 pages; both send readers to the
  Go SDK's [Observability](https://docs.temporal.io/develop/go/platform/observability) page, so this guide
  also uses its Logging, Metrics and Tracing sections. Its Visibility / Search Attributes part is left out.
- **The default log level.** The Go page says "the Temporal SDK core normally uses `WARN`". The Go SDK's
  default logger has no level filter and prints `DEBUG` lines to stdout ([02](02-logging.md)).
- **The docs' propagator can't tell "not set".** Its `Inject` always writes the header; with no value it
  writes JSON `null`, `Extract` turns that into a zero `Values{}`, and the Workflow's `!= nil` check is
  always true. A Worker with the propagator also sends `null` to Activities when the Client had none
  ([03](03-context-propagation.md)).
- **Pointer in, value out.** The docs' starter stores `&Values{…}` and the Workflow asserts `Values`; it
  works only because `Extract` decodes into a value type ([03](03-context-propagation.md)).
- **Headers bypass the payload codec.** With a zlib codec, inputs were stored encoded but propagator and
  tracing headers stayed `json/plain`, readable in the Event History. The docs suggest auth tokens as a use
  case: propagate an opaque reference instead ([03](03-context-propagation.md), [07](07-tracing-options-and-migration.md)).
- **Signals and Queries don't change Workflow context.** Their headers reach only worker interceptors;
  Workflow code keeps the start value. Update handlers get the caller's value. A Schedule keeps the context
  of whoever created it. Two propagators writing the same key: the last one wins silently
  ([04](04-propagation-in-depth.md)).
- **Nexus ignores `ContextPropagator`**, as the docs say; without interceptors the handler saw no tenant
  ([04](04-propagation-in-depth.md)).
- **Counters are gauges by default.** Without `UseMonotonicCounters`, SDK and custom counters scrape as
  Prometheus gauges with no `_total` suffix. If no Meter Provider is set, metrics are dropped silently. Failure
  metrics appear only after a first failure ([05](05-metrics.md)).
- **The Go page's Tally example** calls a `newPrometheusScope` helper it never shows; chapter 05 has a
  complete one. The v1 and v2 OpenTelemetry metrics handlers are identical ([05](05-metrics.md)).
- **`NewPlugin` panics instead of returning its error** when the global Tracer Provider isn't from
  `NewReplaySafeTracerProvider`, even if you only want metrics. A package-level
  `var tracer = temporalotel.Tracer(…)` panics at `init`, before `main` can install the provider
  ([05](05-metrics.md), [06](06-tracing-with-opentelemetry.md)).
- **Replay-safe, mostly.** With the Workflow cache off, no span was exported twice. But after failed
  Workflow Tasks, `RunWorkflow` and the Workflow span were exported 3 times with the same ID. A failed
  Activity marks Temporal's spans as Error but not your own Workflow span ([06](06-tracing-with-opentelemetry.md)).
- **A malformed `_tracer-data` header fails the Workflow Execution** unless `AllowInvalidParentSpans` is
  set. A `HeaderKey` mismatch between Client and Worker silently splits the trace. Workflow code has no way to
  read baggage in v0.1.0 ([07](07-tracing-options-and-migration.md)).
- **v1 and v2 are wire-compatible:** same `_tracer-data` header and W3C format; a Workflow started under the
  v1 interceptor continued its trace on a v2 Worker ([07](07-tracing-options-and-migration.md)).
- **Not documented:** the plugin adds `TraceID` and `SpanID` to `workflow.GetLogger` / `activity.GetLogger`
  lines; `temporal.SimplePluginOptions` has a `ContextPropagators` field; `WorkerStopTimeout` defaults to 0
  ([04](04-propagation-in-depth.md), [08](08-observability-in-production.md)).

---

## Sources

**Encyclopedia:** [Context Propagation](https://docs.temporal.io/encyclopedia/context-propagation)

**Go SDK:** [Context propagation](https://docs.temporal.io/develop/go/best-practices/context-propagation) ·
[OpenTelemetry v2 integration](https://docs.temporal.io/develop/go/integrations/opentelemetry-v2) ·
[Observability](https://docs.temporal.io/develop/go/platform/observability) (linked from both)
