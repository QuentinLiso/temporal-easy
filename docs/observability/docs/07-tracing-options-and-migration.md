# 07 · Tracing options, baggage and migration

> **Level:** advanced · **Read time:** ~14 min
> **Sources:** [Go SDK — OpenTelemetry v2: Configure context propagation](https://docs.temporal.io/develop/go/integrations/opentelemetry-v2#configure-context-propagation) · [Go SDK — Observability: Tracing](https://docs.temporal.io/develop/go/platform/observability#tracing) · [OpenTelemetry baggage](https://opentelemetry.io/docs/concepts/signals/baggage/) · [W3C Trace Context](https://www.w3.org/TR/trace-context/)

---

## 1. The problem

[Chapter 06](06-tracing-with-opentelemetry.md) produced one trace per checkout with the plugin's
defaults. In production, three questions come up quickly:

- *Where is the trace context stored, and what else rides along with it?* Someone wants to pass
  "which app did this order come from" to every Activity's span.
- *Which settings must match between the API and the Workers?* A team changes an option on one side and
  traces split in two.
- *How do we move from the older interceptor-based integration* without breaking Workflows that are
  already running?

This chapter goes through the plugin's propagation options, shows the header in the Event History, and
tests a migration from v1 to v2 on a dev server.

---

## 2. The options in one place

The OpenTelemetry v2 page documents four propagation settings, in two different structs. Build the plugin
in **one** function used by every process (API, Workers, cron jobs), so they can't drift apart:

```go
// file: internal/telemetry/plugin.go
package telemetry

import (
	"go.opentelemetry.io/otel/propagation"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/interceptor/tracing"
)

// NewTemporalPlugin builds the plugin with every tracing option spelled out.
// Clients and Workers that exchange Workflows must agree on HeaderKey and on
// the propagation format, so build them all from this one function.
func NewTemporalPlugin() (client.Plugin, error) {
	return temporalotel.NewPlugin(temporalotel.PluginOptions{
		TracerOptions: tracing.TracerOptions{
			AddTemporalSpans: true,
			// Default "_tracer-data", shared by all Temporal SDKs. Change it only
			// if every Client and Worker of these Workflows changes it too.
			HeaderKey: "",
			// Don't fail an Execution whose header can't be decoded (see section 6).
			AllowInvalidParentSpans: true,
		},
		// W3C Trace Context + W3C Baggage. Not the OpenTelemetry global
		// propagator: pass otel.GetTextMapPropagator() to use that one.
		TextMapPropagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
		// true: carry only the span context, drop the baggage.
		DisableBaggage: false,
	})
}
```

| Option | Where | Default | What it does |
| --- | --- | --- | --- |
| `TextMapPropagator` | `PluginOptions` | W3C Trace Context + W3C Baggage (`temporalotel.DefaultTextMapPropagator`) | Serializes the span context (and baggage) into the header. **Not** the OpenTelemetry global propagator: if you configured B3 or Jaeger propagation with `otel.SetTextMapPropagator`, pass `otel.GetTextMapPropagator()` here explicitly |
| `DisableBaggage` | `PluginOptions` | `false` | Stops carrying baggage. The span context still travels |
| `HeaderKey` | `TracerOptions` | `_tracer-data` | The Temporal header field that holds the serialized context. The same key is used by all Temporal SDKs and by the v1 Go module |
| `AllowInvalidParentSpans` | `TracerOptions` | `false` | Ignores a tracing header that can't be decoded instead of failing ([section 6](#_6-allowinvalidparentspans-a-bad-header-fails-the-workflow)) |
| `AddTemporalSpans` | `TracerOptions` | `false` | Adds Temporal's own spans ([chapter 06](06-tracing-with-opentelemetry.md#_7-what-the-trace-looks-like)) |

`TracerOptions` is `tracing.TracerOptions` from `go.temporal.io/sdk/interceptor/tracing`, a package of the
core SDK, not of the plugin module.

---

## 3. The header in the Event History

The plugin writes the context into the **Temporal header** of each call: the start request, every
scheduled Activity, Child Workflow, Signal and so on. The Service stores headers with the events, so the
`WorkflowExecutionStarted` event of every traced Workflow carries one.

This test starts the checkout from an "API" Client with a W3C baggage entry, then reads the first event of
the history and what the Activity's span recorded. It runs three configurations:

```go
// file: checkout/headers_test.go
package checkout

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/interceptor/tracing"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

func TestTracingHeaders(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	provider := temporalotel.NewReplaySafeTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	defer func() { _ = provider.Shutdown(ctx) }()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: os.Getenv("TEMPORAL_CLI"),
		LogLevel:     "error",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	cases := []struct {
		name           string
		starter, owner temporalotel.PluginOptions // API's plugin, Worker's plugin
	}{
		{"defaults", temporalotel.PluginOptions{}, temporalotel.PluginOptions{}},
		{"DisableBaggage", temporalotel.PluginOptions{DisableBaggage: true}, temporalotel.PluginOptions{DisableBaggage: true}},
		{"HeaderKey mismatch",
			temporalotel.PluginOptions{TracerOptions: tracing.TracerOptions{HeaderKey: "x-shop-trace"}},
			temporalotel.PluginOptions{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exporter.Reset()
			api := dial(t, srv, tc.starter)
			defer api.Close()
			wc := dial(t, srv, tc.owner)
			defer wc.Close()

			taskQueue := "checkout-" + tc.name
			w := worker.New(wc, taskQueue, worker.Options{})
			w.RegisterWorkflow(CheckoutWorkflow)
			w.RegisterActivity(&Activities{})
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			defer w.Stop()

			reqCtx, root := otel.Tracer("test").Start(ctx, "POST /orders")
			reqCtx, _ = withBaggage(reqCtx, "shop.channel", "mobile-app")
			run, err := api.ExecuteWorkflow(reqCtx, client.StartWorkflowOptions{ID: taskQueue, TaskQueue: taskQueue},
				CheckoutWorkflow, Order{ID: "A1001", CustomerID: "C42", AmountCents: 1800})
			if err != nil {
				t.Fatal(err)
			}
			if err := run.Get(ctx, nil); err != nil {
				t.Fatal(err)
			}
			root.End()

			// What the Activity's span recorded, and whether it joined the API's trace.
			for _, s := range exporter.GetSpans() {
				if s.Name != "payment-gateway.charge" {
					continue
				}
				channel := ""
				for _, a := range s.Attributes {
					if a.Key == "shop.channel" {
						channel = a.Value.AsString()
					}
				}
				t.Logf("Activity saw shop.channel=%q, same trace as the API: %v",
					channel, s.SpanContext.TraceID() == root.SpanContext().TraceID())
			}

			// The header the API wrote into the WorkflowExecutionStarted event.
			it := api.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			first, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			for key, p := range first.GetWorkflowExecutionStartedEventAttributes().GetHeader().GetFields() {
				t.Logf("header %q = %s", key, p.GetData())
			}
		})
	}
}

func dial(t *testing.T, srv *testsuite.DevServer, opts temporalotel.PluginOptions) client.Client {
	plugin, err := temporalotel.NewPlugin(opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.Dial(client.Options{HostPort: srv.FrontendHostPort(), Plugins: []client.Plugin{plugin}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func withBaggage(ctx context.Context, key, value string) (context.Context, error) {
	m, err := baggage.NewMember(key, value)
	if err != nil {
		return ctx, err
	}
	b, err := baggage.New(m)
	if err != nil {
		return ctx, err
	}
	return baggage.ContextWithBaggage(ctx, b), nil
}
```

The Workflow and the Activity under test:

```go
// file: checkout/workflow.go
package checkout

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "checkout"

type Order struct {
	ID          string
	CustomerID  string
	AmountCents int64
}

func CheckoutWorkflow(ctx workflow.Context, order Order) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	var a *Activities
	var chargeID string
	err := workflow.ExecuteActivity(ctx, a.ChargePayment, order).Get(ctx, &chargeID)
	return chargeID, err
}
```

```go
// file: checkout/activities.go
package checkout

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.temporal.io/sdk/activity"
)

type Activities struct{}

func (a *Activities) ChargePayment(ctx context.Context, order Order) (string, error) {
	ctx, span := otel.Tracer("github.com/acme/shop/checkout").Start(ctx, "payment-gateway.charge")
	defer span.End()

	// The baggage set by the API, three processes away, is in ctx.
	channel := baggage.FromContext(ctx).Member("shop.channel").Value()
	span.SetAttributes(attribute.String("shop.channel", channel))
	activity.GetLogger(ctx).Info("charging", "OrderID", order.ID, "Channel", channel)

	return "ch_" + order.ID, nil
}
```

```text
$ TEMPORAL_CLI=$(which temporal) go test -run TestTracingHeaders -v ./checkout
=== RUN   TestTracingHeaders/defaults
    headers_test.go:87: Activity saw shop.channel="mobile-app", same trace as the API: true
    headers_test.go:98: header "_tracer-data" = {"baggage":"shop.channel=mobile-app","traceparent":"00-f2abb827cf7bcfedb58343f3823a00d6-bf80058636dc1530-01"}
=== RUN   TestTracingHeaders/DisableBaggage
    headers_test.go:87: Activity saw shop.channel="", same trace as the API: true
    headers_test.go:98: header "_tracer-data" = {"traceparent":"00-3e350dd697f0ff5116cd36fe9f4d38e5-c609ee92b4fa6efa-01"}
=== RUN   TestTracingHeaders/HeaderKey_mismatch
    headers_test.go:87: Activity saw shop.channel="", same trace as the API: false
    headers_test.go:98: header "x-shop-trace" = {"baggage":"shop.channel=mobile-app","traceparent":"00-e1831a55a623aa54c65c7b839d962ed8-9b2004bcffb52b7a-01"}
--- PASS: TestTracingHeaders (1.28s)
```

What it shows:

- **The header is plain JSON.** The plugin encodes it with the SDK's default data converter
  (`json/plain`), not with your Client's Data Converter, so a payload codec that encrypts your Workflow
  inputs **doesn't** encrypt it. `temporal workflow show -w <id> -o json` prints it base64-encoded;
  decoded, it's exactly the line above. Anyone who can read the history can read it.
- **`traceparent`** is the W3C format: `00-<trace id>-<parent span id>-<flags>`. That's all a backend
  needs to attach the Workflow's spans to the API's trace.
- **A `HeaderKey` mismatch fails silently.** The API wrote `x-shop-trace`, the Worker looked for
  `_tracer-data`, found nothing, and started a **new trace**. No error, no log: the trace is simply cut in
  two. That's why the options live in one shared function.

---

## 4. Baggage

[Baggage](https://opentelemetry.io/docs/concepts/signals/baggage/) is a set of key-value pairs that travels
with the trace context. The API sets it once; every downstream process can read it:

```go
// file: cmd/api/baggage.go
package main

import (
	"context"

	"go.opentelemetry.io/otel/baggage"
)

// withShopBaggage adds low-risk, low-cardinality request facts to the context.
// They travel with the trace into the Workflow and every Activity, and they are
// stored in clear text in the Workflow's Event History: never put a token, an
// email address or a card number here.
func withShopBaggage(ctx context.Context, channel, experiment string) (context.Context, error) {
	ch, err := baggage.NewMember("shop.channel", channel)
	if err != nil {
		return ctx, err
	}
	exp, err := baggage.NewMember("shop.experiment", experiment)
	if err != nil {
		return ctx, err
	}
	bag, err := baggage.New(ch, exp)
	if err != nil {
		return ctx, err
	}
	return baggage.ContextWithBaggage(ctx, bag), nil
}
```

```go
// file: cmd/api/main.go
package main

import (
	"log"
	"net/http"
	"os"

	"go.opentelemetry.io/otel"
	"go.temporal.io/sdk/client"

	"github.com/acme/shop/checkout"
	"github.com/acme/shop/internal/telemetry"
)

func main() {
	// Tracer Provider setup as in chapter 06 (telemetry.SetupTracing), omitted.
	plugin, err := telemetry.NewTemporalPlugin()
	if err != nil {
		log.Fatalln(err)
	}
	c, err := client.Dial(client.Options{HostPort: os.Getenv("TEMPORAL_ADDRESS"), Plugins: []client.Plugin{plugin}})
	if err != nil {
		log.Fatalln(err)
	}
	defer c.Close()

	http.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("github.com/acme/shop/api").Start(r.Context(), "POST /orders")
		defer span.End()

		ctx, err := withShopBaggage(ctx, r.Header.Get("X-Shop-Channel"), "one-click-v2")
		if err != nil {
			http.Error(w, "bad channel", http.StatusBadRequest)
			return
		}
		order := checkout.Order{ID: r.URL.Query().Get("id"), CustomerID: "C42", AmountCents: 1800}
		if _, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: "checkout-" + order.ID, TaskQueue: checkout.TaskQueue,
		}, checkout.CheckoutWorkflow, order); err != nil {
			http.Error(w, "could not start checkout", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	log.Fatalln(http.ListenAndServe(":8080", nil))
}
```

In the Activity, `baggage.FromContext(ctx)` returns it, as `ChargePayment` does above to tag its span with
the sales channel. The plugin carried it through the Service and the Workflow without the Workflow code
knowing.

Rules for baggage:

| Do | Don't |
| --- | --- |
| Small, low-cardinality facts that help slice traces: channel, experiment, region, plan | Credentials, session tokens, API keys |
| Values you'd be fine to see in the Web UI | Personal data: email, name, address, card numbers |
| A handful of keys | Large values: the header is sent with every call and stored in the history |

The OpenTelemetry v2 page says it directly: "Do not put credentials, tokens, or personal data in baggage
since the plugin serializes it into Temporal headers that can be persisted in Workflow Event History."
Section 3 shows it stored in clear text. For data that the business logic needs (a tenant ID that
decides which database to use), don't rely on tracing baggage at all: use your own propagator
([chapter 03](03-context-propagation.md)), which you control and test.

Two limits in v0.1.0:

- **Workflow code can't read baggage.** The plugin keeps it in an unexported key of the `workflow.Context`
  and has no accessor. It flows *through* the Workflow to Activities and Child Workflows, but a Workflow
  that needs a value should receive it as an input.
- **`DisableBaggage` is all or nothing.** To drop only some keys, remove them from the context before
  calling the Temporal Client.

---

## 5. `TextMapPropagator`: matching the rest of your stack

Your HTTP services might propagate with B3 (Zipkin) or Jaeger headers. The plugin doesn't follow
`otel.SetTextMapPropagator`: its default is fixed to W3C Trace Context + Baggage. Two choices:

- Keep the default inside Temporal and only make sure your HTTP layer **extracts** the incoming format into
  `ctx`: the plugin then re-serializes the span context as W3C. This is the simplest option, because the
  header format is internal to Temporal.
- Pass `otel.GetTextMapPropagator()` (or a composite) as `TextMapPropagator` if non-Go SDKs that read the
  same Workflows expect another format. All processes must agree, as for `HeaderKey`.

Implementations must be thread-safe: the plugin shares one propagator across all Workers' goroutines.

---

## 6. `AllowInvalidParentSpans`: a bad header fails the Workflow

The v2 page describes `AllowInvalidParentSpans` as a way "to ignore errors when extracting Span Context from
Temporal headers … when migrating between tracing libraries". It's more important than it sounds.

To trigger the error, a starter Client wrote a plain string under `_tracer-data` (as a foreign tracing
library writing its own format under the same key could). A Worker with the v2 plugin then ran the
Workflow:

| Worker's `AllowInvalidParentSpans` | Result |
| --- | --- |
| `false` (default) | The Workflow Execution **failed**: `unable to decode: json: cannot unmarshal string into Go value of type map[string]string`. Status `Failed`, not a retried Workflow Task |
| `true` | The Workflow ran and completed. The broken parent was ignored and a new trace started |

A tracing problem shouldn't be able to fail an order. Set `AllowInvalidParentSpans: true` whenever
headers may come from another library, another SDK, or an older version of your own code, and keep it
during any migration.

What does **not** trigger it, from the plugin's source: a header that decodes as a JSON map of strings
but has no valid `traceparent` (for example, one written with another propagation format). The plugin's
`UnmarshalSpan` never returns an error; it falls back to "no parent" and a new trace starts, as in the
`HeaderKey` mismatch of section 3.

---

## 7. Migrating from the v1 module

Before the plugin, the Go SDK's tracing integration was an interceptor in
`go.temporal.io/sdk/contrib/opentelemetry` (still published, v0.8.1). The Go Observability page still shows
it, with the OpenTracing and Datadog interceptors:

```go
// file: internal/telemetry/legacy.go
package telemetry

import (
	"go.temporal.io/sdk/client"
	otelv1 "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

// LegacyClientOptions is the v1 setup: an interceptor, not a plugin.
func LegacyClientOptions(hostPort string) (client.Options, error) {
	tracingInterceptor, err := otelv1.NewTracingInterceptor(otelv1.TracerOptions{
		AllowInvalidParentSpans: true,
	})
	if err != nil {
		return client.Options{}, err
	}
	return client.Options{
		HostPort:     hostPort,
		Interceptors: []interceptor.ClientInterceptor{tracingInterceptor},
	}, nil
}
```

What changes, from the two modules' sources:

| | v1: `contrib/opentelemetry` | v2: `contrib/opentelemetry-v2` |
| --- | --- | --- |
| Setup | `NewTracingInterceptor` in `client.Options.Interceptors` | `NewPlugin` in `client.Options.Plugins` |
| Tracer Provider | Any (the global one, or `TracerOptions.Tracer`) | Must be `NewReplaySafeTracerProvider` |
| Temporal spans | Always created | Only with `AddTemporalSpans: true` |
| Spans during replay | Not created at all (no-op spans) | Recreated with stable IDs, not exported twice |
| Spans from Workflow code | No replay-safe Tracer | `temporalotel.Tracer` |
| Per-operation switches | `DisableSignalTracing`, `DisableQueryTracing`, `DisableUpdateTracing` | None |
| Header key, propagator | `_tracer-data`, W3C Trace Context + Baggage | **Same** |
| Metrics | `NewMetricsHandler` | `NewMetricsHandler`, or `MetricsHandlerOptions` on the plugin ([chapter 05](05-metrics.md)) |

Because the header key and format are the same, the two are compatible on the wire. This test checks it
with the real checkout. A v1 Client starts the Workflow, and a v1 Worker that registers only the Workflow
runs the first Workflow Task (which schedules `ChargePayment`). The v1 Worker is then stopped, and a v2
Worker on the same Task Queue runs the Activity and finishes the Workflow:

```go
// file: checkout/migration_test.go
package checkout

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/internal/telemetry"
)

// A Workflow starts under v1 and finishes under v2.
func TestMigrationV1ToV2(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	provider := temporalotel.NewReplaySafeTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider) // v1 works with the replay-safe provider too
	defer func() { _ = provider.Shutdown(ctx) }()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: os.Getenv("TEMPORAL_CLI"),
		LogLevel:     "error",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	// v1 era: the API and a Worker that registers only the Workflow, so it runs the
	// first Workflow Task (scheduling ChargePayment) and nothing else.
	legacyOpts, err := telemetry.LegacyClientOptions(srv.FrontendHostPort())
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := client.Dial(legacyOpts)
	if err != nil {
		t.Fatal(err)
	}
	w1 := worker.New(legacy, "checkout-migration", worker.Options{})
	w1.RegisterWorkflow(CheckoutWorkflow)
	if err := w1.Start(); err != nil {
		t.Fatal(err)
	}
	reqCtx, root := otel.Tracer("test").Start(ctx, "POST /orders")
	run, err := legacy.ExecuteWorkflow(reqCtx, client.StartWorkflowOptions{ID: "checkout-A1002", TaskQueue: "checkout-migration"},
		CheckoutWorkflow, Order{ID: "A1002", CustomerID: "C42", AmountCents: 2400})
	if err != nil {
		t.Fatal(err)
	}
	root.End()
	time.Sleep(time.Second) // let the v1 Worker run the first Workflow Task
	w1.Stop()
	legacy.Close()

	// v2 era: the plugin, same Task Queue. It runs the Activity and finishes the Workflow.
	plugin, err := telemetry.NewTemporalPlugin()
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.Dial(client.Options{HostPort: srv.FrontendHostPort(), Plugins: []client.Plugin{plugin}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w2 := worker.New(c, "checkout-migration", worker.Options{})
	w2.RegisterWorkflow(CheckoutWorkflow)
	w2.RegisterActivity(&Activities{})
	if err := w2.Start(); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()
	if err := c.GetWorkflow(ctx, run.GetID(), run.GetRunID()).Get(ctx, nil); err != nil {
		t.Fatal(err)
	}

	spans := exporter.GetSpans()
	for _, s := range spans {
		if s.SpanContext.TraceID() != root.SpanContext().TraceID() {
			t.Errorf("span %q is in another trace", s.Name)
		}
	}
	t.Logf("%d spans, one trace\n%s", len(spans), tree(spans))
}

// tree prints spans as an indented parent/child tree, ordered by start time.
func tree(spans tracetest.SpanStubs) string {
	children := map[trace.SpanID][]tracetest.SpanStub{}
	for _, s := range spans {
		children[s.Parent.SpanID()] = append(children[s.Parent.SpanID()], s)
	}
	var b strings.Builder
	var walk func(parent trace.SpanID, depth int)
	walk = func(parent trace.SpanID, depth int) {
		kids := children[parent]
		sort.Slice(kids, func(i, j int) bool { return kids[i].StartTime.Before(kids[j].StartTime) })
		for _, s := range kids {
			fmt.Fprintf(&b, "%s%s\n", strings.Repeat("  ", depth), s.Name)
			walk(s.SpanContext.SpanID(), depth+1)
		}
	}
	walk(trace.SpanID{}, 0)
	return b.String()
}
```

```text
$ TEMPORAL_CLI=$(which temporal) go test -run TestMigrationV1ToV2 -v ./checkout
    migration_test.go:95: 7 spans, one trace
        POST /orders
          StartWorkflow:CheckoutWorkflow
            RunWorkflow:CheckoutWorkflow
              StartActivity:ChargePayment
                RunActivity:ChargePayment
                  payment-gateway.charge
            RunWorkflow:CheckoutWorkflow
--- PASS: TestMigrationV1ToV2 (2.17s)
```

The first `RunWorkflow` and its `StartActivity` come from the v1 Worker's Workflow Task. The Activity ran
on the v2 Worker and still found its parent in the header v1 wrote. The second `RunWorkflow` is the v2
Worker, which replayed the history (exporting nothing for the replayed part) and completed the Workflow.

One trace, with one `RunWorkflow` span per Worker generation. A migration plan:

1. Add `AllowInvalidParentSpans: true` to the v1 options (if not set) and to the new v2 options.
2. Switch the **Tracer Provider** to `NewReplaySafeTracerProvider` everywhere. v1 works with it too: it's
   a regular `sdktrace` provider with a different ID generator.
3. Replace the interceptor with the plugin in Workers, then in Clients. Avoid registering both in one
   process: both read and write the same `_tracer-data` header.
4. Turn on `AddTemporalSpans` if you relied on v1's built-in spans.
5. Replace any custom Workflow spans with `temporalotel.Tracer`.

The other tracing interceptors mentioned on the Go Observability page follow the v1 pattern:
`go.temporal.io/sdk/contrib/opentracing` (`opentracing.NewInterceptor`) and the Datadog one in
`go.temporal.io/sdk/contrib/datadog/tracing` (`tracing.NewTracingInterceptor`). There's no replay-safe v2
plugin for them.

---

## 8. Key takeaways

- Build the plugin's options in one shared function: `HeaderKey` and the propagator must match across every
  Client and Worker, and a mismatch silently splits traces.
- The trace context lives in the `_tracer-data` header as **plain JSON**, stored in the Event History and
  not encrypted by your payload codec.
- Baggage reaches Activities (`baggage.FromContext`) but not Workflow code. Keep it small, low-cardinality
  and free of secrets and personal data. `DisableBaggage` drops all of it.
- The plugin's default propagator is W3C, not the OpenTelemetry global one. Pass
  `otel.GetTextMapPropagator()` explicitly to follow the global setting.
- Without `AllowInvalidParentSpans`, an undecodable tracing header **fails the Workflow Execution**. Turn it
  on, especially during migrations.
- v1 (interceptor) and v2 (plugin) share the header format: Workflows started under v1 continue in the same
  trace on v2 Workers.
