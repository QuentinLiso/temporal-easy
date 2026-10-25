# 05 · Metrics

> **Level:** daily use · **Read time:** ~18 min
> **Sources:** [Go SDK: Observability, Metrics](https://docs.temporal.io/develop/go/platform/observability#metrics) · [Go SDK: OpenTelemetry v2](https://docs.temporal.io/develop/go/integrations/opentelemetry-v2) · [SDK metrics reference](https://docs.temporal.io/references/sdk-metrics) · [`client.MetricsHandler`](https://pkg.go.dev/go.temporal.io/sdk/client#MetricsHandler)

---

## 1. The problem

Black Friday, 10:40. Orders are "slow". Logs ([chapter 02](02-logging.md)) tell you what **one** order
did. Traces ([chapter 06](06-tracing-with-opentelemetry.md)) tell you where **one** order spent its time.
Neither tells you whether **all** orders are slow, since when, and whether it's the Workers (no free slots),
the Temporal Service (requests failing), or the payment gateway (Activities timing out).

That's what metrics are for: counters, gauges and histograms across every Execution, cheap enough to
keep for months and to alert on. The Go SDK already measures most of what you need. You have to give it
somewhere to send the numbers, tag them so a dashboard can say **whose** Workers are struggling, and add
your own business metrics without counting each order twelve times during replay.

---

## 2. Where SDK metrics go

Every Client and every Worker created from it reports through one interface,
`client.Options.MetricsHandler`. Leave it unset and the SDK uses a no-op handler: nothing is measured
anywhere. There are three ways to fill it:

| Route | Package | When |
| --- | --- | --- |
| A. OpenTelemetry handler + any OTel exporter | `go.temporal.io/sdk/contrib/opentelemetry-v2` (`NewMetricsHandler`) | The default for new code: Prometheus, OTLP, Datadog… through the OTel SDK |
| B. The OpenTelemetry **plugin** with `MetricsHandlerOptions` | same module (`NewPlugin`) | You already use the plugin for tracing ([chapter 06](06-tracing-with-opentelemetry.md)) |
| C. Tally + its Prometheus reporter | `go.temporal.io/sdk/contrib/tally` | Existing Tally setups; the route the Go SDK page shows first |

All three implement the same interface, so Workflow and Activity code doesn't change between them.

The Go SDK page uses `temporalotel.NewMetricsHandler` from the older module,
`go.temporal.io/sdk/contrib/opentelemetry` (v0.8.1). The v2 module (v0.1.0) exports the same function;
we diffed the two handlers and the only differences are whitespace. Pick the v2 module so tracing and
metrics come from one place.

---

## 3. Route A: OpenTelemetry to Prometheus

The OpenTelemetry Prometheus exporter turns OTel instruments into a `/metrics` page. Wire it once, in the
`internal/telemetry` package that [chapter 06](06-tracing-with-opentelemetry.md) also uses:

```go
// file: internal/telemetry/metrics.go
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
)

// Ownership tags. Fixed for the life of the process: low cardinality by design.
type Owner struct {
	Team        string // "checkout"
	Service     string // "orders-worker"
	Environment string // "production"
	Region      string // "eu-west-1"
}

// Metrics is what a Worker or API process needs: a handler for client.Options,
// an HTTP handler for Prometheus to scrape, and a shutdown func.
type Metrics struct {
	Handler  client.MetricsHandler
	HTTP     http.Handler
	Shutdown func(context.Context) error
}

// NewPrometheusMetrics wires Temporal SDK metrics to a Prometheus endpoint
// through OpenTelemetry.
func NewPrometheusMetrics(owner Owner) (*Metrics, error) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))

	handler := temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{
		// Pass the Meter explicitly: the global MeterProvider is a no-op
		// until someone calls otel.SetMeterProvider.
		Meter: provider.Meter("temporal-sdk-go"),
		// Export SDK counters as Prometheus counters (_total), not gauges.
		UseMonotonicCounters: true,
		// The default panics on any Meter error. Log instead.
		OnError: func(err error) { slog.Error("temporal metrics", "error", err) },
	})

	return &Metrics{
		Handler: handler.WithTags(map[string]string{
			"team":        owner.Team,
			"service":     owner.Service,
			"environment": owner.Environment,
			"region":      owner.Region,
		}),
		HTTP:     promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		Shutdown: provider.Shutdown,
	}, nil
}
```

Three options matter more than they look:

| Option | Default | Why we set it |
| --- | --- | --- |
| `Meter` | `otel.GetMeterProvider().Meter("temporal-sdk-go")` | The global provider is a no-op until `otel.SetMeterProvider` runs. In our test, setting the global provider **after** creating the handler still worked (the OTel global delegates), but if nobody ever sets it, every metric is silently dropped. An explicit `Meter` removes the doubt |
| `UseMonotonicCounters` | `false` | See section 4: without it, Prometheus sees every SDK counter as a **gauge** |
| `OnError` | `panic(err)` | A Meter error (for example, an instrument name the exporter rejects) would crash the Worker |

The Worker serves the page next to its normal work. Chapter 01's `observability.ClientOptions` is where
`MetricsHandler` goes in a full service. Here it's set directly:

```go
// file: cmd/worker/main.go
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/internal/telemetry"
	"github.com/acme/shop/orders"
)

type fakeGateway struct{}

func (fakeGateway) Charge(ctx context.Context, orderID string, amountCents int64) error { return nil }

func main() {
	metrics, err := telemetry.NewPrometheusMetrics(telemetry.Owner{
		Team: "checkout", Service: "orders-worker", Environment: "production", Region: "eu-west-1",
	})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metrics.Shutdown(ctx)
	}()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.HTTP)
		log.Println(http.ListenAndServe(":9464", mux))
	}()

	c, err := client.Dial(client.Options{MetricsHandler: metrics.Handler})
	if err != nil {
		log.Fatalln(err)
	}
	defer c.Close()

	w := worker.New(c, "orders", worker.Options{})
	w.RegisterWorkflow(orders.FulfillOrderWorkflow)
	w.RegisterActivity(&orders.Activities{Gateway: fakeGateway{}, GatewayName: "stripe"})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln(err)
	}
}
```

---

## 4. What Prometheus actually sees

We ran the Worker of this chapter on a dev server, started three orders, and scraped the page. These are
all the Temporal families that appeared, with the type Prometheus reported, with and without
`UseMonotonicCounters`:

| Metric family | `UseMonotonicCounters: false` (default) | `UseMonotonicCounters: true` |
| --- | --- | --- |
| `temporal_request`, `temporal_request_attempt` | gauge | counter, `_total` suffix |
| `temporal_long_request`, `temporal_long_request_attempt`, `temporal_long_request_failure`, `temporal_long_request_failure_attempt` | gauge | counter, `_total` |
| `temporal_workflow_completed` | gauge | counter, `_total` |
| `temporal_workflow_task_queue_poll_succeed`, `temporal_poller_start`, `temporal_worker_start` | gauge | counter, `_total` |
| `temporal_request_latency_seconds`, `temporal_request_latency_attempt_seconds`, `temporal_long_request_latency_seconds`, `temporal_long_request_latency_attempt_seconds` | histogram | histogram |
| `temporal_workflow_task_schedule_to_start_latency_seconds`, `temporal_workflow_task_execution_latency_seconds`, `temporal_workflow_task_replay_latency_seconds`, `temporal_workflow_endtoend_latency_seconds` | histogram | histogram |
| `temporal_activity_execution_latency_seconds`, `temporal_activity_succeed_endtoend_latency_seconds` | histogram | histogram |
| `temporal_num_pollers`, `temporal_sticky_cache_size`, `temporal_worker_task_slots_available`, `temporal_worker_task_slots_used` | gauge | gauge |

Things to notice:

- **Without `UseMonotonicCounters`, counters are gauges.** The handler creates OTel `Int64UpDownCounter`s,
  which Prometheus exports as gauges without `_total`. `rate()` on a gauge is wrong, and the name changes
  when you flip the option, so **set it on day one**. The Go SDK page says the option exists from
  `go.temporal.io/sdk/contrib/opentelemetry` 0.8.0, the older module; the v2 module has it too.
- **Timers become `_seconds` histograms.** The SDK records durations; the exporter adds the unit.
- **Some families only appear when something happens.** `temporal_workflow_failed`,
  `temporal_activity_execution_failed` or `temporal_workflow_task_execution_failed` weren't on the page
  because nothing failed. An alert on a family that doesn't exist yet returns no data instead of 0, so
  write the query to survive it (for example, append `or vector(0)`).
- **`temporal_activity_schedule_to_start_latency` was missing**, although Activities ran. The SDK records
  it only for Activities it **polls** (`internal_task_pollers.go`). Ours were dispatched **eagerly** to the
  same Worker in the Workflow Task response, which skips it. Under real load, with many Workers, most
  Activities are polled, but don't build a dashboard that relies on it for every Activity.

---

## 5. Tags: who owns these Workers?

Every SDK metric already carries Temporal tags: `namespace`, `task_queue`, `workflow_type`,
`activity_type`, `worker_type`, `operation` for requests, and `client_name`. They tell you **where** in
Temporal a number comes from, not **who** runs the Workers. When five Task Queues slow down at once,
the first question is which team deployed. That's what `WithTags` in `NewPrometheusMetrics` adds. One real
line from the page, for the custom counter of section 7:

```text
shop_orders_fulfilled_total{activity_type="none",client_name="temporal_go",environment="test",namespace="default",otel_scope_name="temporal-sdk-go",otel_scope_schema_url="",otel_scope_version="",payment_method="card",region="local",service="orders-worker",task_queue="orders",team="checkout",worker_type="none",workflow_type="FulfillOrderWorkflow"} 3
```

The tags from `WithTags` (`team`, `service`, `environment`, `region`) are on every metric of the process,
SDK or custom, from the Client and from the Worker.

Keep tag values **low cardinality**. Your metrics backend stores one series per distinct combination of
tag values:

| Tag value | Distinct values | Verdict |
| --- | --- | --- |
| `team`, `service`, `environment`, `region` | a handful, fixed per process | yes |
| `payment_method`, `gateway` | a handful | yes |
| `merchant_id` | thousands, growing | **no**: put it in logs ([chapter 02](02-logging.md)) and traces |
| Workflow ID, order ID, run ID | one per Execution | **never** |

---

## 6. Route B: the OpenTelemetry plugin, and Route C: Tally

### The plugin

If you use the plugin for tracing ([chapter 06](06-tracing-with-opentelemetry.md)), set
`MetricsHandlerOptions` on it and it installs the handler on the Client for you. Two catches:

- The plugin builds the handler, so you can't call `WithTags` on it. Put the ownership tags in
  `MetricsHandlerOptions.InitialAttributes`.
- `temporalotel.NewPlugin` **panics** ("global tracer provider must be created by
  NewReplaySafeTracerProvider") if the global Tracer Provider isn't the replay-safe one, **even if you only
  want metrics**. It has an `error` return, but doesn't use it for this. The helper below turns the panic
  into an error:

```go
// file: internal/telemetry/plugin.go
package telemetry

import (
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/temporal"
)

// NewOTelPlugin is the plugin route: the plugin that does tracing (chapter 06)
// also installs the metrics handler on the Client and its Workers.
func NewOTelPlugin(meters metric.MeterProvider, owner Owner) (*temporal.SimplePlugin, error) {
	// temporalotel.NewPlugin panics unless the global TracerProvider is a
	// ReplaySafeTracerProvider, even when you only want metrics.
	if _, ok := otel.GetTracerProvider().(*temporalotel.ReplaySafeTracerProvider); !ok {
		return nil, errors.New("call SetupTracing (chapter 06) before NewOTelPlugin")
	}
	return temporalotel.NewPlugin(temporalotel.PluginOptions{
		MetricsHandlerOptions: &temporalotel.MetricsHandlerOptions{
			Meter:                meters.Meter("temporal-sdk-go"),
			UseMonotonicCounters: true,
			// The plugin builds the handler itself, so ownership tags go here
			// instead of WithTags.
			InitialAttributes: attribute.NewSet(
				attribute.String("team", owner.Team),
				attribute.String("service", owner.Service),
				attribute.String("environment", owner.Environment),
				attribute.String("region", owner.Region),
			),
		},
	})
}
```

```go
// file: internal/telemetry/plugin_test.go
package telemetry_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"

	"github.com/acme/shop/internal/telemetry"
)

func TestOTelPluginNeedsReplaySafeTracerProvider(t *testing.T) {
	meters := sdkmetric.NewMeterProvider()
	owner := telemetry.Owner{Team: "checkout", Service: "orders-worker", Environment: "test", Region: "local"}

	_, err := telemetry.NewOTelPlugin(meters, owner)
	require.Error(t, err) // temporalotel.NewPlugin itself would have panicked here

	otel.SetTracerProvider(temporalotel.NewReplaySafeTracerProvider())
	plugin, err := telemetry.NewOTelPlugin(meters, owner)
	require.NoError(t, err)
	require.NotNil(t, plugin)
}
```

### Tally

The Go SDK page's first example uses Tally with its own Prometheus reporter. It still works, and the
reporter serves `/metrics` itself on `ListenAddress`. The page calls a `newPrometheusScope` helper it
doesn't show; this is a complete version:

```go
// file: internal/telemetry/tally.go
package telemetry

import (
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	tallyprom "github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
)

// NewTallyPrometheusHandler is the older route the Go SDK page shows: Tally with
// its own Prometheus reporter, which serves /metrics on listenAddress itself.
func NewTallyPrometheusHandler(listenAddress string) (client.MetricsHandler, func() error, error) {
	cfg := tallyprom.Configuration{
		ListenAddress: listenAddress,
		TimerType:     "histogram",
	}
	reporter, err := cfg.NewReporter(tallyprom.ConfigurationOptions{
		OnError: func(err error) { fmt.Println("tally prometheus:", err) },
	})
	if err != nil {
		return nil, nil, fmt.Errorf("tally reporter: %w", err)
	}
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter:  reporter,
		Separator:       tallyprom.DefaultSeparator,
		SanitizeOptions: &sdktally.PrometheusSanitizeOptions,
	}, time.Second)
	// Rename Temporal's metrics to Prometheus conventions (timers get a _seconds suffix).
	scope = sdktally.NewPrometheusNamingScope(scope)
	return sdktally.NewMetricsHandler(scope), closer.Close, nil
}
```

`sdktally.NewPrometheusNamingScope` renames Temporal's metrics to Prometheus conventions.

---

## 7. Your own metrics, replay-safe

SDK metrics tell you about Temporal. Business metrics tell you about orders: how many started, how many
were fulfilled, how long fulfilment took, how slow the payment gateway is. Emit them through the same
handler, so they get the same tags and exporter:

| Where | Get the handler with | Replay |
| --- | --- | --- |
| Workflow | `workflow.GetMetricsHandler(ctx)` | Calls are **dropped while the Workflow replays**, so each event counts once |
| Activity | `activity.GetMetricsHandler(ctx)` | Activities aren't replayed |
| Nexus handler | `temporalnexus.GetMetricsHandler(ctx)` | Not replayed |

The Workflow counts orders and times fulfilment in Workflow time:

```go
// file: orders/workflow.go
package orders

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

type FulfillOrderInput struct {
	OrderID       string
	MerchantID    string // high cardinality: never a metric tag
	PaymentMethod string // "card", "paypal", "bank_transfer": a handful of values
	AmountCents   int64
}

func FulfillOrderWorkflow(ctx workflow.Context, in FulfillOrderInput) error {
	// Replay-safe: the SDK drops these while the Workflow replays.
	metrics := workflow.GetMetricsHandler(ctx).WithTags(map[string]string{
		"payment_method": in.PaymentMethod,
	})
	metrics.Counter("shop_orders_started").Inc(1)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var a *Activities
	if err := workflow.ExecuteActivity(ctx, a.ChargePayment, in).Get(ctx, nil); err != nil {
		metrics.Counter("shop_orders_failed").Inc(1)
		return err
	}
	if err := workflow.Sleep(ctx, 100*time.Millisecond); err != nil { // wait for the warehouse slot
		return err
	}
	if err := workflow.ExecuteActivity(ctx, a.BookShipment, in.OrderID).Get(ctx, nil); err != nil {
		metrics.Counter("shop_orders_failed").Inc(1)
		return err
	}

	metrics.Counter("shop_orders_fulfilled").Inc(1)
	// Business latency: from start to now, in Workflow time.
	metrics.Timer("shop_order_fulfillment_latency").Record(workflow.Now(ctx).Sub(workflow.GetInfo(ctx).WorkflowStartTime))
	return nil
}
```

The Activity times the gateway call, tagged with the gateway (two values, not one per merchant):

```go
// file: orders/activities.go
package orders

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
)

// PaymentGateway is the external payment provider.
type PaymentGateway interface {
	Charge(ctx context.Context, orderID string, amountCents int64) error
}

type Activities struct {
	Gateway     PaymentGateway
	GatewayName string // "stripe", "adyen"
}

func (a *Activities) ChargePayment(ctx context.Context, in FulfillOrderInput) error {
	metrics := activity.GetMetricsHandler(ctx).WithTags(map[string]string{"gateway": a.GatewayName})
	start := time.Now()
	err := a.Gateway.Charge(ctx, in.OrderID, in.AmountCents)
	metrics.Timer("shop_payment_gateway_latency").Record(time.Since(start))
	if err != nil {
		metrics.Counter("shop_payment_gateway_errors").Inc(1)
	}
	return err
}

func (a *Activities) BookShipment(ctx context.Context, orderID string) error {
	return nil
}
```

Why not a plain Prometheus counter in the Workflow? We ran three orders on a Worker with its Workflow
cache disabled (`worker.SetStickyWorkflowCacheSize(0)`), so every Workflow Task replays the Workflow from
the start. Each order took 4 Workflow Tasks:

| Counter at the top of the Workflow | Value after 3 orders |
| --- | --- |
| a package-level `int` incremented in Workflow code | **12**: once per Workflow Task |
| `workflow.GetMetricsHandler(ctx).Counter("shop_orders_started")` | **3** |

With the cache enabled replays are rarer (after a Worker restart, a cache eviction, a Query on an
evicted Workflow), so the overcount is irregular, which is worse: the numbers look plausible.

### Testing custom metrics

The OpenTelemetry SDK's `ManualReader` lets a unit test read what the Workflow emitted, through the test
environment ([Testing guide, chapter 02](../../testing/docs/02-workflow-test-environment.md)):

```go
// file: orders/workflow_test.go
package orders

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/testsuite"
)

// counterValue reads one counter from an OpenTelemetry ManualReader.
func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				var total int64
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					total += dp.Value
				}
				return total
			}
		}
	}
	return 0
}

func TestFulfillOrderEmitsBusinessMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	var ts testsuite.WorkflowTestSuite
	ts.SetMetricsHandler(temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{
		Meter: provider.Meter("temporal-sdk-go"), UseMonotonicCounters: true,
	}))
	env := ts.NewTestWorkflowEnvironment()
	var a *Activities
	env.OnActivity(a.ChargePayment, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.BookShipment, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(FulfillOrderWorkflow, FulfillOrderInput{
		OrderID: "ord-1001", MerchantID: "merchant-4521", PaymentMethod: "card", AmountCents: 12_900,
	})

	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, 1, counterValue(t, reader, "shop_orders_started"))
	require.EqualValues(t, 1, counterValue(t, reader, "shop_orders_fulfilled"))
	require.EqualValues(t, 0, counterValue(t, reader, "shop_orders_failed"))
}
```

And an end-to-end test checks the whole chain, including replay safety, against a real dev server and
the real `/metrics` page:

```go
// file: orders/e2e_test.go
package orders_test

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/internal/telemetry"
	"github.com/acme/shop/orders"
)

type okGateway struct{}

func (okGateway) Charge(context.Context, string, int64) error { return nil }

func TestMetricsAreReplaySafe(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	ctx := context.Background()
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{LogLevel: "error", Stdout: io.Discard, Stderr: io.Discard})
	require.NoError(t, err)
	defer func() { _ = srv.Stop() }()

	m, err := telemetry.NewPrometheusMetrics(telemetry.Owner{Team: "checkout", Service: "orders-worker", Environment: "test", Region: "local"})
	require.NoError(t, err)
	defer func() { _ = m.Shutdown(ctx) }()

	c, err := client.Dial(client.Options{HostPort: srv.FrontendHostPort(), MetricsHandler: m.Handler})
	require.NoError(t, err)
	defer c.Close()

	// No Workflow cache: every Workflow Task replays the Workflow from the start.
	worker.SetStickyWorkflowCacheSize(0)
	w := worker.New(c, "orders", worker.Options{})
	w.RegisterWorkflow(orders.FulfillOrderWorkflow)
	w.RegisterActivity(&orders.Activities{Gateway: okGateway{}, GatewayName: "stripe"})
	require.NoError(t, w.Start())

	for i := 1; i <= 3; i++ {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: fmt.Sprintf("fulfill-ord-%d", i), TaskQueue: "orders"},
			orders.FulfillOrderWorkflow, orders.FulfillOrderInput{OrderID: fmt.Sprintf("ord-%d", i), PaymentMethod: "card", AmountCents: 12_900})
		require.NoError(t, err)
		require.NoError(t, run.Get(ctx, nil))
	}
	w.Stop()

	scrape := httptest.NewServer(m.HTTP)
	defer scrape.Close()
	resp, err := scrape.Client().Get(scrape.URL)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)

	value := func(metric string) string {
		re := regexp.MustCompile(`(?m)^` + metric + `\{[^}]*\} (\S+)$`)
		got := re.FindStringSubmatch(string(body))
		require.NotNil(t, got, metric)
		return got[1]
	}
	t.Log(regexp.MustCompile(`(?m)^shop_orders_fulfilled_total\{.*$`).FindString(string(body)))
	t.Logf("replays: %s", value("temporal_workflow_task_replay_latency_seconds_count"))
	require.Equal(t, "3", value("shop_orders_started_total"))
	require.Equal(t, "3", value("shop_orders_fulfilled_total"))
	require.Equal(t, "3", value("temporal_workflow_completed_total"))
	for _, line := range regexp.MustCompile(`(?m)^# TYPE (temporal_\S+|shop_\S+) (\S+)$`).FindAllStringSubmatch(string(body), -1) {
		t.Logf("%s %s", line[1], line[2])
	}
}
```

```text
$ go test ./orders -run TestMetricsAreReplaySafe -v
=== RUN   TestMetricsAreReplaySafe
    e2e_test.go:69: shop_orders_fulfilled_total{activity_type="none",client_name="temporal_go",environment="test",namespace="default",otel_scope_name="temporal-sdk-go",otel_scope_schema_url="",otel_scope_version="",payment_method="card",region="local",service="orders-worker",task_queue="orders",team="checkout",worker_type="none",workflow_type="FulfillOrderWorkflow"} 3
    e2e_test.go:70: replays: 12
    e2e_test.go:75: shop_order_fulfillment_latency_seconds histogram
    e2e_test.go:75: shop_orders_fulfilled_total counter
    e2e_test.go:75: shop_orders_started_total counter
    e2e_test.go:75: shop_payment_gateway_latency_seconds histogram
    …
--- PASS: TestMetricsAreReplaySafe (4.33s)
```

Twelve Workflow Task replays, and still exactly three orders started and three fulfilled.

---

## 8. SDK metrics worth alerting on

The [SDK metrics reference](https://docs.temporal.io/references/sdk-metrics) lists every metric. These
are the ones a shop on-call rotation should watch first (names as Prometheus exports them, with
`UseMonotonicCounters: true`):

| Symptom | Metric | Alert when |
| --- | --- | --- |
| Not enough Workers: tasks wait in the queue | `temporal_workflow_task_schedule_to_start_latency_seconds`, `temporal_activity_schedule_to_start_latency_seconds` | p95 above a few hundred ms for Workflow Tasks, above your SLO for Activities |
| Workers saturated | `temporal_worker_task_slots_available` | at 0 for minutes, per `worker_type` |
| Workflow code failing (panics, non-determinism) | `temporal_workflow_task_execution_failed_total` | any sustained rate |
| Activities failing | `temporal_activity_execution_failed_total` | rate above normal, per `activity_type` |
| Workflows failing | `temporal_workflow_failed_total` | rate above normal, per `workflow_type` |
| Can't reach the Temporal Service | `temporal_request_failure_total`, `temporal_long_request_failure_total` | sustained rate, per `operation` |
| Too much replay (cache too small) | `temporal_sticky_cache_size`, `temporal_workflow_task_replay_latency_seconds` | cache size at its maximum and replay latency rising |
| Business | `shop_orders_failed_total`, `shop_payment_gateway_errors_total` | your own thresholds |

The Temporal Service has its own metrics too (server-side schedule-to-start, persistence latency). On
Temporal Cloud, they come from the Cloud metrics endpoint. [Chapter 08](08-observability-in-production.md)
puts these into dashboards.

---

## 9. Key takeaways

- Set `client.Options.MetricsHandler`. Without it, the SDK measures nothing.
- Use `temporalotel.NewMetricsHandler` (OpenTelemetry v2 module) with an explicit `Meter`; the v1 module's
  handler is identical. Tally still works.
- Set `UseMonotonicCounters: true` from the start, or Prometheus sees every SDK counter as a gauge.
- Replace the default `OnError`, which panics.
- Tag every process with `team`, `service`, `environment`, `region` via `WithTags` (or `InitialAttributes`
  on the plugin). Never tag with merchant, order or Workflow IDs.
- The OpenTelemetry plugin installs the metrics handler too, but panics without a replay-safe Tracer
  Provider, even for metrics only.
- In Workflows, emit through `workflow.GetMetricsHandler(ctx)`: it skips replay. A plain counter counted
  12 times for 3 orders.
- Some SDK metrics appear only when they happen (failures) or only for polled Activities
  (`activity_schedule_to_start_latency`). Write alerts that survive a missing series.
