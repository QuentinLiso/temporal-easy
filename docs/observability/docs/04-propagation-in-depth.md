# 04 · Context propagation in depth

> **Level:** daily use · **Read time:** ~16 min
> **Sources:** [Encyclopedia: Context Propagation](https://docs.temporal.io/encyclopedia/context-propagation) · [Go SDK: Context propagation](https://docs.temporal.io/develop/go/best-practices/context-propagation) · [`workflow.ContextPropagator`](https://pkg.go.dev/go.temporal.io/sdk/workflow#ContextPropagator) · [`interceptor` package](https://pkg.go.dev/go.temporal.io/sdk/interceptor)

---

## 1. The problem

[Chapter 03](03-context-propagation.md) moved the tenant from a starter into a Workflow, its Activities
and its Child Workflows. Real code crosses more boundaries than that:

- a support tool **Signals** a Workflow, and you want to know which merchant's agent sent it;
- a dashboard **Queries** or **Updates** a Workflow on behalf of a merchant;
- a nightly **Schedule** starts Workflows with nobody's request behind them;
- the risk team's fraud check runs behind a **Nexus** endpoint, on another team's Worker.

For each of these you need to know whether the tenant crosses, where it lands, and who can read it
afterwards. This chapter answers that, adds a second propagator, and packages everything so every team
gets the same setup.

---

## 2. The code for this chapter

This chapter's module reuses `internal/tenancy/tenant.go` and `internal/tenancy/propagator.go` from
[chapter 03](03-context-propagation.md) unchanged. They're repeated here, folded, so the module compiles on
its own:

::: details internal/tenancy/tenant.go and propagator.go (unchanged from chapter 03)

```go
// file: internal/tenancy/tenant.go
package tenancy

import (
	"context"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// Tenant is the merchant an order belongs to.
type Tenant struct {
	MerchantID string `json:"merchantId"`
	Plan       string `json:"plan"` // "starter", "growth", "enterprise"
}

// tenantKey is unexported: only this package can read or write the value.
type tenantKey struct{}

// WithTenant returns a copy of ctx carrying the tenant. Use it before c.ExecuteWorkflow.
func WithTenant(ctx context.Context, t Tenant) context.Context {
	return context.WithValue(ctx, tenantKey{}, t)
}

// FromContext reads the tenant in Activities and other non-Workflow code.
func FromContext(ctx context.Context) (Tenant, bool) {
	t, ok := ctx.Value(tenantKey{}).(Tenant)
	return t, ok
}

// WithWorkflowTenant overrides the tenant for the calls made with the returned context.
func WithWorkflowTenant(ctx workflow.Context, t Tenant) workflow.Context {
	return workflow.WithValue(ctx, tenantKey{}, t)
}

// FromWorkflow reads the tenant in Workflow code.
func FromWorkflow(ctx workflow.Context) (Tenant, bool) {
	t, ok := ctx.Value(tenantKey{}).(Tenant)
	return t, ok
}

// WorkflowLogger is workflow.GetLogger with the merchant added to every line.
func WorkflowLogger(ctx workflow.Context) log.Logger {
	logger := workflow.GetLogger(ctx)
	if t, ok := FromWorkflow(ctx); ok {
		logger = log.With(logger, "MerchantID", t.MerchantID)
	}
	return logger
}
```

```go
// file: internal/tenancy/propagator.go
package tenancy

import (
	"context"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

// HeaderKey is the Temporal header that carries the tenant. Prefix it with your
// company or service so it can't clash with another propagator's key.
const HeaderKey = "acme-shop-tenant"

type propagator struct{}

// NewPropagator returns the propagator to put in client.Options.ContextPropagators.
func NewPropagator() workflow.ContextPropagator {
	return propagator{}
}

// Inject: Client or Activity context → header.
func (propagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	t, ok := FromContext(ctx)
	if !ok {
		return nil // no tenant: write no header, so "not set" stays "not set"
	}
	return writeTenant(t, writer)
}

// InjectFromWorkflow: Workflow context → header of an Activity or Child Workflow.
func (propagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	t, ok := FromWorkflow(ctx)
	if !ok {
		return nil
	}
	return writeTenant(t, writer)
}

// Extract: header → Activity context.
func (propagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	if t, ok := readTenant(reader); ok {
		ctx = WithTenant(ctx, t)
	}
	return ctx, nil
}

// ExtractToWorkflow: header → Workflow context.
func (propagator) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	if t, ok := readTenant(reader); ok {
		ctx = WithWorkflowTenant(ctx, t)
	}
	return ctx, nil
}

func writeTenant(t Tenant, writer workflow.HeaderWriter) error {
	// The default data converter, not the Client's: a codec configured on the
	// Client is NOT applied to headers (see section 9).
	payload, err := converter.GetDefaultDataConverter().ToPayload(t)
	if err != nil {
		return err
	}
	writer.Set(HeaderKey, payload)
	return nil
}

func readTenant(reader workflow.HeaderReader) (Tenant, bool) {
	payload, ok := reader.Get(HeaderKey)
	if !ok {
		return Tenant{}, false
	}
	var t Tenant
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &t); err != nil {
		// An unreadable header is dropped, as in the docs' sample. Returning the
		// error instead would fail the Workflow Task or Activity on every attempt.
		return Tenant{}, false
	}
	return t, true
}
```

:::

The Workflow under test records which merchant each step saw, so we can check propagation step by step:

```go
// file: orders/fulfill.go
package orders

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/acme/shop/fraud"
	"github.com/acme/shop/internal/tenancy"
)

const FraudEndpoint = "fraud-endpoint"

type FulfillOrderInput struct {
	OrderID     string
	AmountCents int64
}

// FulfillOrderResult records which merchant each step saw, so tests can check propagation.
type FulfillOrderResult struct {
	Merchant        string // the Workflow
	ReservedFor     string // the Activity
	FraudCheckedFor string // the Nexus handler
}

func FulfillOrderWorkflow(ctx workflow.Context, in FulfillOrderInput) (FulfillOrderResult, error) {
	t, _ := tenancy.FromWorkflow(ctx)
	res := FulfillOrderResult{Merchant: t.MerchantID}

	tenancy.WorkflowLogger(ctx).Info("Fulfilling order",
		"OrderID", in.OrderID, "RequestID", tenancy.WorkflowRequestID(ctx))

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err := workflow.ExecuteActivity(ctx, ReserveStock, in.OrderID).Get(ctx, &res.ReservedFor); err != nil {
		return res, err
	}

	nc := workflow.NewNexusClient(FraudEndpoint, fraud.ServiceName)
	var screen fraud.ScreenResult
	err := nc.ExecuteOperation(ctx, fraud.ScreenOrder,
		fraud.ScreenInput{OrderID: in.OrderID, AmountCents: in.AmountCents},
		workflow.NexusOperationOptions{ScheduleToCloseTimeout: time.Minute}).Get(ctx, &screen)
	if err != nil {
		return res, err
	}
	if !screen.Approved {
		return res, fmt.Errorf("order %s rejected by fraud screening", in.OrderID)
	}
	res.FraudCheckedFor = screen.ScreenedFor
	return res, nil
}

// ReserveStock returns the merchant it saw.
func ReserveStock(ctx context.Context, orderID string) (string, error) {
	t, _ := tenancy.FromContext(ctx)
	return t.MerchantID, nil
}
```

---

## 3. What crosses which boundary

The SDK calls `Inject`/`InjectFromWorkflow` when it **builds** a request, and `Extract`/`ExtractToWorkflow`
when a Worker **receives** a task. In the SDK source (`internal/interceptor_header.go` and its callers), that
covers every Client call that takes a context, and every command a Workflow sends. Where the extracted
value **lands** is a different question.

We tested each boundary on a dev server (Temporal CLI 1.9.1, SDK v1.50.0). Each call used a different
merchant ID, so we could tell whose value arrived:

| Boundary | Header sent? | Where the value lands | Observed |
| --- | --- | --- | --- |
| `ExecuteWorkflow` | yes | the Workflow's root `workflow.Context` | `merchant-start` |
| Activity | yes | the Activity's `context.Context` | `merchant-start` |
| Local Activity | yes | the Local Activity's `context.Context` | `merchant-start` |
| Child Workflow | yes | the child's root `workflow.Context` | `merchant-start` |
| Continue-As-New | yes, from the `ctx` passed to `NewContinueAsNewError` | the new run's root context | `merchant-can` reached the new run |
| `SignalWithStartWorkflow` | yes | the started Workflow's root context | `merchant-sws` |
| Schedule (`ScheduleClient().Create`) | yes, **once, at creation** | every run the Schedule starts | `merchant-schedule` |
| `UpdateWorkflow` | yes | the **Update handler's** `workflow.Context` | `merchant-update`, not the start value |
| `SignalWorkflow` | yes | only the context given to a `HandleSignal` **interceptor** | the interceptor saw `merchant-signal`; Workflow code still saw `merchant-start` |
| `QueryWorkflow` | yes | only the context given to a `HandleQuery` **interceptor** | the interceptor saw `merchant-query`; the handler's closure saw `merchant-start` |
| Nexus Operation | **no** | nowhere: propagators aren't called | see section 7 |

Three rows need a closer look:

- **Signals and Queries.** A Signal arrives through a channel, and a Query handler takes no
  `workflow.Context`. Workflow code has nowhere to read the caller's value. The header still travels and
  is stored on the `WorkflowExecutionSignaled` event, and a worker interceptor can read it. If the sender
  matters to your logic, put it in the **Signal payload**.
- **Updates** are the exception: the handler gets its own `workflow.Context`, extracted from the Update's
  header. An Update handler that calls an Activity propagates the **caller's** tenant, not the starter's.
- **Schedules** capture the context of whoever called `Create`. Every run, for as long as the Schedule
  lives, carries that value. Create a Schedule from a context holding the tenant it acts for, or from a
  context with no tenant at all.

In the history of that test, the header was stored on `WorkflowExecutionStarted`,
`ActivityTaskScheduled`, `StartChildWorkflowExecutionInitiated`, `WorkflowExecutionUpdateAccepted` and
`WorkflowExecutionSignaled`. The Local Activity's marker had none: a Local Activity's header never leaves
the Worker. As [chapter 03](03-context-propagation.md) showed, every stored header is in clear text,
outside your payload codec.

---

## 4. Several propagators on one Client

The Workflow above logs a request ID next to the order. It comes from a second propagator that owns a
different header and a different context key:

```go
// file: internal/tenancy/request.go
package tenancy

import (
	"context"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

// RequestHeader carries the ID of the HTTP request that started the work, so
// logs from the API, the Workflow and its Activities can be joined on it.
const RequestHeader = "acme-shop-request-id"

type requestKey struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestKey{}, id)
}

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestKey{}).(string)
	return id
}

func WorkflowRequestID(ctx workflow.Context) string {
	id, _ := ctx.Value(requestKey{}).(string)
	return id
}

// RequestIDPropagator owns one header and one context key, nothing else.
type RequestIDPropagator struct{}

var _ workflow.ContextPropagator = RequestIDPropagator{}

func (RequestIDPropagator) Inject(ctx context.Context, w workflow.HeaderWriter) error {
	return setString(w, RequestHeader, RequestID(ctx))
}

func (RequestIDPropagator) InjectFromWorkflow(ctx workflow.Context, w workflow.HeaderWriter) error {
	return setString(w, RequestHeader, WorkflowRequestID(ctx))
}

func (RequestIDPropagator) Extract(ctx context.Context, r workflow.HeaderReader) (context.Context, error) {
	if id := getString(r, RequestHeader); id != "" {
		ctx = WithRequestID(ctx, id)
	}
	return ctx, nil
}

func (RequestIDPropagator) ExtractToWorkflow(ctx workflow.Context, r workflow.HeaderReader) (workflow.Context, error) {
	if id := getString(r, RequestHeader); id != "" {
		ctx = workflow.WithValue(ctx, requestKey{}, id)
	}
	return ctx, nil
}

func setString(w workflow.HeaderWriter, key, value string) error {
	if value == "" {
		return nil // nothing set: write no header
	}
	p, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		return err
	}
	w.Set(key, p)
	return nil
}

func getString(r workflow.HeaderReader, key string) string {
	p, ok := r.Get(key)
	if !ok {
		return ""
	}
	var s string
	if err := converter.GetDefaultDataConverter().FromPayload(p, &s); err != nil {
		return "" // unreadable: dropped, like the tenant header
	}
	return s
}
```

A Client takes both in `ContextPropagators`: `tenancy.NewPropagator()` owns `acme-shop-tenant`,
`tenancy.RequestIDPropagator{}` owns `acme-shop-request-id`. Section 5 puts them in a Plugin.

The SDK calls them in order, and they all write into the same header map. What if two of them write the
same key? We ran two propagators that both set the tenant header from the Workflow:

| Order | The Activity saw |
| --- | --- |
| tenant propagator, then the other one | the other one's value |
| the other one, then the tenant propagator | the tenant propagator's value |

The **last writer wins**, silently. Prefix your header names (`acme-shop-`) so a library's propagator
can't overwrite yours, and never let two propagators share a key.

The API that starts orders follows the rule from chapter 03: it resolves the bearer token at the edge
and propagates only identifiers.

```go
// file: cmd/api/main.go
package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/acme/shop/internal/tenancy"
	"github.com/acme/shop/orders"
)

// Authenticator turns a request's bearer token into a tenant.
// The token itself never leaves this process.
type Authenticator interface {
	TenantFor(r *http.Request) (tenancy.Tenant, error)
}

type api struct {
	temporal client.Client
	auth     Authenticator
}

func (a *api) placeOrder(w http.ResponseWriter, r *http.Request) {
	tenant, err := a.auth.TenantFor(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var in orders.FulfillOrderInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = uuid.NewString()
	}
	// Propagate opaque IDs only: they are written to Event History in clear text.
	ctx := tenancy.WithRequestID(tenancy.WithTenant(r.Context(), tenant), requestID)

	run, err := a.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "fulfill-" + in.OrderID,
		TaskQueue: "orders",
	}, orders.FulfillOrderWorkflow, in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"workflowId": run.GetID(), "requestId": requestID})
}

type staticAuth struct{}

func (staticAuth) TenantFor(*http.Request) (tenancy.Tenant, error) {
	return tenancy.Tenant{MerchantID: "m-4471", Plan: "enterprise"}, nil
}

func main() {
	plugin, err := tenancy.NewPlugin()
	if err != nil {
		log.Fatalln(err)
	}
	c, err := client.Dial(client.Options{Plugins: []client.Plugin{plugin}})
	if err != nil {
		log.Fatalln(err)
	}
	defer c.Close()

	a := &api{temporal: c, auth: staticAuth{}}
	http.HandleFunc("POST /orders", a.placeOrder)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

---

## 5. Package it as a Plugin

Every service needs the same two propagators, plus the Nexus interceptor of section 7. Forgetting one in
one process breaks propagation for that process only ([chapter 03](03-context-propagation.md), section 7).
A Plugin bundles them, so every team adds a single line to `client.Options`.
`temporal.SimplePluginOptions` has a `ContextPropagators` field for this:

```go
// file: internal/tenancy/plugin.go
package tenancy

import (
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// NewPlugin bundles everything a shop service needs to carry tenancy across
// Temporal: both propagators and the Nexus interceptor.
// Every team adds one line to its client.Options instead of three.
func NewPlugin() (*temporal.SimplePlugin, error) {
	return temporal.NewSimplePlugin(temporal.SimplePluginOptions{
		Name: "acme.shop.tenancy",
		ContextPropagators: []workflow.ContextPropagator{
			NewPropagator(),
			RequestIDPropagator{},
		},
		WorkerInterceptors: []interceptor.WorkerInterceptor{&NexusTenantInterceptor{}},
	})
}
```

Use it with `client.Options{Plugins: []client.Plugin{plugin}}`. Workers created from that Client get the
propagators and the worker interceptor too.

---

## 6. Testing a propagator without a server

The test environments take propagators and an incoming header, so you can test propagation in a unit
test ([Testing guide, chapter 02](../../testing/docs/02-workflow-test-environment.md)):

| API | What it does |
| --- | --- |
| `env.SetContextPropagators([]workflow.ContextPropagator{…})` | The propagators of the "Worker", on `TestWorkflowEnvironment` and `TestActivityEnvironment` |
| `env.SetHeader(&commonpb.Header{…})` | The header the "Client" sent. Without it, nothing is extracted |
| `ts.SetHeader(…)` on `WorkflowTestSuite` | The same header for every environment the suite creates |

`SetHeader` takes the **encoded** header, so build it the way your propagator would:

```go
// file: orders/fulfill_test.go
package orders

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/acme/shop/fraud"
	"github.com/acme/shop/internal/tenancy"
)

// tenantHeader builds the header a Client with the tenant propagator would send.
func tenantHeader(t *testing.T, tenant tenancy.Tenant) *commonpb.Header {
	p, err := converter.GetDefaultDataConverter().ToPayload(tenant)
	require.NoError(t, err)
	return &commonpb.Header{Fields: map[string]*commonpb.Payload{tenancy.HeaderKey: p}}
}

func newEnv() *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{tenancy.NewPropagator()})
	env.RegisterActivity(ReserveStock)
	env.OnNexusOperation(fraud.ServiceName, fraud.ScreenOrder, mock.Anything, mock.Anything).
		Return(&nexus.HandlerStartOperationResultSync[fraud.ScreenResult]{Value: fraud.ScreenResult{Approved: true}}, nil)
	return env
}

func TestTenantReachesWorkflowAndActivity(t *testing.T) {
	env := newEnv()
	env.SetHeader(tenantHeader(t, tenancy.Tenant{MerchantID: "m-4471", Plan: "enterprise"}))

	env.ExecuteWorkflow(FulfillOrderWorkflow, FulfillOrderInput{OrderID: "ord-1001", AmountCents: 12_900})

	require.NoError(t, env.GetWorkflowError())
	var res FulfillOrderResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, "m-4471", res.Merchant)
	require.Equal(t, "m-4471", res.ReservedFor)
}

func TestNoHeaderMeansNoTenant(t *testing.T) {
	env := newEnv() // no SetHeader: like a starter without the propagator

	env.ExecuteWorkflow(FulfillOrderWorkflow, FulfillOrderInput{OrderID: "ord-1002", AmountCents: 12_900})

	require.NoError(t, env.GetWorkflowError())
	var res FulfillOrderResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Empty(t, res.Merchant)
	require.Empty(t, res.ReservedFor)
}
```

An Activity test checks the decoding side, including the propagator's choice to drop a header it can't
read:

```go
// file: internal/tenancy/propagator_test.go
package tenancy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/acme/shop/internal/tenancy"
)

func whoAmI(ctx context.Context) (string, error) {
	t, ok := tenancy.FromContext(ctx)
	if !ok {
		return "<no tenant>", nil
	}
	return t.MerchantID + "/" + t.Plan, nil
}

func runWithHeader(t *testing.T, value any) string {
	p, err := converter.GetDefaultDataConverter().ToPayload(value)
	require.NoError(t, err)

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{tenancy.NewPropagator()})
	env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{tenancy.HeaderKey: p}})
	env.RegisterActivity(whoAmI)

	v, err := env.ExecuteActivity(whoAmI)
	require.NoError(t, err)
	var got string
	require.NoError(t, v.Get(&got))
	return got
}

func TestActivitySeesTenant(t *testing.T) {
	require.Equal(t, "m-4471/enterprise", runWithHeader(t, tenancy.Tenant{MerchantID: "m-4471", Plan: "enterprise"}))
}

func TestUnreadableHeaderIsDropped(t *testing.T) {
	// An old Worker or another team's propagator wrote a string, not a Tenant.
	require.Equal(t, "<no tenant>", runWithHeader(t, "m-4471"))
}
```

```text
$ go test -short -v ./internal/tenancy ./orders
--- PASS: TestActivitySeesTenant (0.03s)
--- PASS: TestUnreadableHeaderIsDropped (0.00s)
ok  	github.com/acme/shop/internal/tenancy	0.358s
--- PASS: TestTenantReachesWorkflowAndActivity (0.04s)
--- PASS: TestNoHeaderMeansNoTenant (0.00s)
--- SKIP: TestTenantCrossesNexus (0.00s)
ok  	github.com/acme/shop/orders	0.630s
```

If you'd rather fail than run an order with no tenant, return an error from `Extract`. The task then
fails with "failed propagating header: …" and is retried, so a bad header blocks the Workflow until a
fixed Worker is deployed.

---

## 7. Nexus: propagators don't apply, interceptors do

A Nexus call uses the Nexus protocol, whose header is a `nexus.Header` (a `map[string]string`), not a
Temporal header. `ContextPropagator` is never called for it. We ran the end-to-end test below twice:

| Plugin | Workflow saw | Activity saw | Nexus handler saw |
| --- | --- | --- | --- |
| propagators only | `m-4471` | `m-4471` | `""` |
| propagators + `NexusTenantInterceptor` | `m-4471` | `m-4471` | `m-4471` |

The interceptor has two halves:

- **Caller side.** A workflow outbound interceptor adds the tenant to
  `ExecuteNexusOperationInput.NexusHeader`, as a JSON string.
- **Handler side.** A Nexus operation inbound interceptor reads `NexusStartOperationInput.Options.Header`
  and puts the tenant back in the handler's `context.Context`.

```go
// file: internal/tenancy/nexus.go
package tenancy

import (
	"context"
	"encoding/json"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

// NexusTenantInterceptor carries the tenant into Nexus calls (caller side) and
// back into the handler's context (handler side). ContextPropagators are not
// called for Nexus, so this is the only way across.
type NexusTenantInterceptor struct {
	interceptor.WorkerInterceptorBase
}

// --- Caller side: a Workflow executes a Nexus Operation. ---

func (i *NexusTenantInterceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &nexusWorkflowInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type nexusWorkflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (w *nexusWorkflowInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return w.Next.Init(&nexusWorkflowOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}})
}

type nexusWorkflowOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
}

func (w *nexusWorkflowOutbound) ExecuteNexusOperation(ctx workflow.Context, input interceptor.ExecuteNexusOperationInput) workflow.NexusOperationFuture {
	if t, ok := FromWorkflow(ctx); ok {
		// nexus.Header is a map[string]string: encode the tenant as a JSON string.
		if b, err := json.Marshal(t); err == nil {
			if input.NexusHeader == nil {
				input.NexusHeader = nexus.Header{}
			}
			input.NexusHeader.Set(HeaderKey, string(b))
		}
	}
	return w.Next.ExecuteNexusOperation(ctx, input)
}

// --- Handler side: a Worker runs a Nexus Operation handler. ---

func (i *NexusTenantInterceptor) InterceptNexusOperation(ctx context.Context, next interceptor.NexusOperationInboundInterceptor) interceptor.NexusOperationInboundInterceptor {
	return &nexusHandlerInbound{NexusOperationInboundInterceptorBase: interceptor.NexusOperationInboundInterceptorBase{Next: next}}
}

type nexusHandlerInbound struct {
	interceptor.NexusOperationInboundInterceptorBase
}

func (n *nexusHandlerInbound) StartOperation(ctx context.Context, input interceptor.NexusStartOperationInput) (nexus.HandlerStartOperationResult[any], error) {
	if raw := input.Options.Header.Get(HeaderKey); raw != "" {
		var t Tenant
		if err := json.Unmarshal([]byte(raw), &t); err == nil {
			ctx = WithTenant(ctx, t)
		}
	}
	return n.Next.StartOperation(ctx, input)
}
```

The risk team's Nexus Service reads the tenant like any other code:

```go
// file: fraud/service.go
package fraud

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"

	"github.com/acme/shop/internal/tenancy"
)

const ServiceName = "fraud"

type ScreenInput struct {
	OrderID     string
	AmountCents int64
}

type ScreenResult struct {
	Approved bool
	// ScreenedFor is the merchant the handler saw in its context.
	ScreenedFor string
}

// ScreenOrder is owned by the risk team and runs on their Worker.
var ScreenOrder = nexus.NewSyncOperation("screen-order",
	func(ctx context.Context, in ScreenInput, _ nexus.StartOperationOptions) (ScreenResult, error) {
		t, _ := tenancy.FromContext(ctx) // set by NexusTenantInterceptor
		limit := int64(500_000)
		if t.Plan == "enterprise" {
			limit = 5_000_000
		}
		return ScreenResult{Approved: in.AmountCents < limit, ScreenedFor: t.MerchantID}, nil
	})

func NewService() (*nexus.Service, error) {
	s := nexus.NewService(ServiceName)
	return s, s.Register(ScreenOrder)
}
```

The end-to-end test starts a dev server, creates the Nexus endpoint, runs both teams' Workers with the
plugin, and checks that the tenant reached all three places:

```go
// file: orders/e2e_test.go
package orders_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/fraud"
	"github.com/acme/shop/internal/tenancy"
	"github.com/acme/shop/orders"
)

func TestTenantCrossesNexus(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	ctx := context.Background()
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{LogLevel: "error", Stdout: io.Discard, Stderr: io.Discard})
	require.NoError(t, err)
	defer func() { _ = srv.Stop() }()

	plugin, err := tenancy.NewPlugin()
	require.NoError(t, err)
	c, err := client.Dial(client.Options{HostPort: srv.FrontendHostPort(), Plugins: []client.Plugin{plugin}})
	require.NoError(t, err)
	defer c.Close()

	// The endpoint routes Nexus calls to the risk team's Task Queue.
	_, err = c.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: orders.FraudEndpoint,
			Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{
				Worker: &nexuspb.EndpointTarget_Worker{Namespace: "default", TaskQueue: "risk"},
			}},
		},
	})
	require.NoError(t, err)

	riskWorker := worker.New(c, "risk", worker.Options{})
	svc, err := fraud.NewService()
	require.NoError(t, err)
	riskWorker.RegisterNexusService(svc)
	require.NoError(t, riskWorker.Start())
	defer riskWorker.Stop()

	ordersWorker := worker.New(c, "orders", worker.Options{})
	ordersWorker.RegisterWorkflow(orders.FulfillOrderWorkflow)
	ordersWorker.RegisterActivity(orders.ReserveStock)
	require.NoError(t, ordersWorker.Start())
	defer ordersWorker.Stop()

	start := tenancy.WithRequestID(tenancy.WithTenant(ctx, tenancy.Tenant{MerchantID: "m-4471", Plan: "enterprise"}), "req-7f3a")
	run, err := c.ExecuteWorkflow(start, client.StartWorkflowOptions{ID: "fulfill-ord-1001", TaskQueue: "orders"},
		orders.FulfillOrderWorkflow, orders.FulfillOrderInput{OrderID: "ord-1001", AmountCents: 12_900})
	require.NoError(t, err)

	var res orders.FulfillOrderResult
	require.NoError(t, run.Get(ctx, &res))
	t.Logf("%+v", res)
	require.Equal(t, orders.FulfillOrderResult{
		Merchant: "m-4471", ReservedFor: "m-4471", FraudCheckedFor: "m-4471",
	}, res)
}
```

```text
$ go test ./orders -run TestTenantCrossesNexus -v
=== RUN   TestTenantCrossesNexus
    e2e_test.go:67: {Merchant:m-4471 ReservedFor:m-4471 FraudCheckedFor:m-4471}
--- PASS: TestTenantCrossesNexus (1.29s)
```

The handler interceptor covers `StartOperation`. If a handler starts a Workflow
(`temporalnexus.NewWorkflowRunOperation`), the tenant is now in its `context.Context`, so a Client with
the tenant propagator carries it into that Workflow like any other start.

---

## 8. Key takeaways

- Headers travel on every Client call and Workflow command: start, Activities, Local Activities, Child
  Workflows, Continue-As-New, Signal-With-Start, Schedules, Signals, Queries and Updates.
- Workflow code only **sees** the start value, plus the caller's value inside an **Update handler**.
  Signal and Query headers reach interceptors only, so put what you need in the payload.
- A Schedule freezes the context of whoever created it, for every run.
- Several propagators can share a Client. The last one to write a key wins, so prefix your keys.
- Bundle propagators and interceptors in a `temporal.NewSimplePlugin` so every process gets the same set.
- Nexus ignores `ContextPropagator`. Copy values into `NexusHeader` with a workflow outbound interceptor,
  and back out of `Options.Header` with a Nexus operation inbound interceptor.
- Test propagation with `SetContextPropagators` and `SetHeader`. The header you pass must be encoded.
