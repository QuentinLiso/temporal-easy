# 04 · Testing Activities

> **Level:** daily use · **Read time:** ~14 min
> **Sources:** [Go SDK: Testing Activities](https://docs.temporal.io/develop/go/best-practices/testing-suite#test-activities) · [`testsuite.TestActivityEnvironment`](https://pkg.go.dev/go.temporal.io/sdk/testsuite#TestActivityEnvironment) · [`activity` package](https://pkg.go.dev/go.temporal.io/sdk/activity)

---

## 1. The problem

Activities are where your code touches the world: payment gateways, supplier APIs, databases. They are
ordinary Go functions, but they call `activity.GetInfo`, `activity.RecordHeartbeat` or
`activity.GetHeartbeatDetails`, and those functions **panic** outside a running Activity. So you can't
call `ChargeCard(context.Background(), …)` from a plain test.

You also want to check behaviors a Worker hides from you: which error the Activity returns (retryable or
not), what progress it heartbeats, and whether a retry resumes where the last attempt stopped.

The Go SDK gives you a **test Activity environment** that runs one Activity in your test process, with a
real Activity context and no Temporal Service.

---

## 2. The Activity test environment

`s.NewTestActivityEnvironment()` (from `testsuite.WorkflowTestSuite`) returns a
`*testsuite.TestActivityEnvironment`. It doesn't start a Worker or talk to a Service: it calls your
function directly, inside a fake Activity context.

| You want to… | API |
| --- | --- |
| Make the Activity known | `env.RegisterActivity(fnOrStruct)` |
| Run it, get its result | `val, err := env.ExecuteActivity(fn, args…)`, then `val.Get(&out)` |
| Run it as a Local Activity | `env.ExecuteLocalActivity(fn, args…)` |
| See its heartbeats | `env.SetOnActivityHeartbeatListener(func(info, details) {…})` |
| Pretend a previous attempt heartbeated | `env.SetHeartbeatDetails(progress)` |
| Cancel it | `env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx})`, then cancel `ctx` |
| Test Worker shutdown | `env.SetWorkerStopChannel(ch)`, then `close(ch)` |

Activities that don't use the Activity context at all (pure functions) don't need any of this: test them
with plain Go tests (section 10).

For Workflow tests, where Activities are usually **mocked**, see
[chapter 02](02-workflow-test-environment.md) and [chapter 03](03-mocking-activities.md). This chapter tests the
real Activity code.

---

## 3. A first Activity test

`ChargeCard` charges an order through a `PaymentGateway`. The gateway is an interface, so the test can
inject a fake. The Activity builds an idempotency key from the Workflow ID and the Activity ID, so every
retry of the same Activity charges the card once.

```go
// file: payments/gateway.go
package payments

import "context"

// PaymentGateway is the card processor (Stripe, Adyen…). Tests use a fake.
type PaymentGateway interface {
	Charge(ctx context.Context, idempotencyKey string, cents int64, cardToken string) (chargeID string, err error)
}

// ErrCardDeclined is what the gateway returns for a refused card.
type ErrCardDeclined struct{ Reason string }

func (e *ErrCardDeclined) Error() string { return "card declined: " + e.Reason }
```

```go
// file: payments/activities.go
package payments

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type ChargeCardInput struct {
	OrderID   string
	Cents     int64
	CardToken string
}

type ChargeCardResult struct {
	ChargeID       string
	IdempotencyKey string
}

// Activities holds the dependencies of the payment Activities.
type Activities struct {
	Gateway PaymentGateway
}

func (a *Activities) ChargeCard(ctx context.Context, in ChargeCardInput) (ChargeCardResult, error) {
	info := activity.GetInfo(ctx)
	// Same key on every retry of this Activity: the gateway charges once.
	key := fmt.Sprintf("%s/%s", info.WorkflowExecution.ID, info.ActivityID)

	chargeID, err := a.Gateway.Charge(ctx, key, in.Cents, in.CardToken)
	var declined *ErrCardDeclined
	if errors.As(err, &declined) {
		return ChargeCardResult{}, temporal.NewNonRetryableApplicationError(
			declined.Error(), "CardDeclined", err)
	}
	if err != nil {
		return ChargeCardResult{}, fmt.Errorf("charge order %s: %w", in.OrderID, err)
	}
	return ChargeCardResult{ChargeID: chargeID, IdempotencyKey: key}, nil
}
```

The test is a testify suite, like the Workflow suites of [chapter 02](02-workflow-test-environment.md).
`SetupTest` creates a **new environment for each test**, so the fake gateway and the environment never
leak between tests:

```go
// file: payments/activities_test.go
package payments

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// fakeGateway records calls and returns a scripted answer.
type fakeGateway struct {
	keys []string
	err  error
}

func (f *fakeGateway) Charge(ctx context.Context, key string, cents int64, token string) (string, error) {
	f.keys = append(f.keys, key)
	if f.err != nil {
		return "", f.err
	}
	return "ch_123", nil
}

type ChargeCardSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env     *testsuite.TestActivityEnvironment
	gateway *fakeGateway
	acts    *Activities
}

func (s *ChargeCardSuite) SetupTest() {
	s.gateway = &fakeGateway{}
	s.env = s.NewTestActivityEnvironment()
	s.acts = &Activities{Gateway: s.gateway}
	s.env.RegisterActivity(s.acts)
}

func (s *ChargeCardSuite) Test_Success() {
	val, err := s.env.ExecuteActivity(s.acts.ChargeCard, ChargeCardInput{OrderID: "ord-42", Cents: 4999, CardToken: "tok_visa"})
	s.Require().NoError(err)

	var res ChargeCardResult
	s.Require().NoError(val.Get(&res))
	s.Equal("ch_123", res.ChargeID)
	// In the test environment the Workflow ID is "default-test-workflow-id" and the Activity ID "0".
	s.Equal("default-test-workflow-id/0", res.IdempotencyKey)
	s.Equal([]string{res.IdempotencyKey}, s.gateway.keys)
}

func (s *ChargeCardSuite) Test_DeclinedIsNonRetryable() {
	s.gateway.err = &ErrCardDeclined{Reason: "insufficient_funds"}
	_, err := s.env.ExecuteActivity(s.acts.ChargeCard, ChargeCardInput{OrderID: "ord-42", Cents: 4999, CardToken: "tok_visa"})

	var appErr *temporal.ApplicationError
	s.Require().True(errors.As(err, &appErr))
	s.Equal("CardDeclined", appErr.Type())
	s.True(appErr.NonRetryable())
}

func (s *ChargeCardSuite) Test_GatewayOutageIsRetryable() {
	s.gateway.err = errors.New("connection reset by peer")
	_, err := s.env.ExecuteActivity(s.acts.ChargeCard, ChargeCardInput{OrderID: "ord-42", Cents: 4999, CardToken: "tok_visa"})

	var appErr *temporal.ApplicationError
	s.Require().True(errors.As(err, &appErr))
	s.False(appErr.NonRetryable())
}

func TestChargeCardSuite(t *testing.T) {
	suite.Run(t, new(ChargeCardSuite))
}
```

Things to notice:

- `RegisterActivity(s.acts)` registers every exported method of the struct. `ExecuteActivity` takes
  the method value `s.acts.ChargeCard`.
- `ExecuteActivity` returns a `converter.EncodedValue`: decode it with `val.Get(&res)`, as a Client
  does with a Workflow result.
- The suite runs in about 40 ms. Nothing listens on the network.

---

## 4. What errors look like

`ExecuteActivity` returns the error the way a Workflow would see it: wrapped in an
`*temporal.ActivityError` whose cause is an `*temporal.ApplicationError`. So you assert with
`errors.As`, exactly as in Workflow code
([Error handling, chapter 04](../../error-handling/docs/04-application-errors.md)):

| The Activity returns | `errors.As(err, &appErr)` | `appErr.NonRetryable()` | `appErr.Type()` |
| --- | --- | --- | --- |
| `temporal.NewNonRetryableApplicationError(msg, "CardDeclined", err)` | true | `true` | `"CardDeclined"` |
| `fmt.Errorf("charge order %s: %w", …)` (any plain error) | true | `false` | `"wrapError"` (the Go type name) |

The two error tests in the suite above pin this down: a declined card must stop retries, a network
error must not. If someone later changes `ChargeCard` to return a plain error for declined cards, the
customer gets charged-and-retried forever, and `Test_DeclinedIsNonRetryable` fails.

The test environment runs **one attempt**. It doesn't apply a Retry Policy: retries are the Workflow's
business, tested with mocks in [chapter 03](03-mocking-activities.md).

---

## 5. What `activity.GetInfo` returns in a test

The Activity context is fake, so `activity.GetInfo(ctx)` returns fixed values. On SDK v1.50.0:

| Field | Value in `ExecuteActivity` |
| --- | --- |
| `WorkflowExecution.ID` | `"default-test-workflow-id"` |
| `WorkflowExecution.RunID` | `"default-test-run-id"` |
| `ActivityID` | `"0"` |
| `TaskQueue` | `"default-test-taskqueue"` |
| `WorkflowNamespace` | `"default-test-namespace"` |
| `Attempt` | `1` |
| `Deadline - StartedTime` | 10 minutes |

That's why `Test_Success` can assert the idempotency key `"default-test-workflow-id/0"`. If the Activity
should behave as if a Client started it directly (no Workflow), call
`env.SetExecuteActivitiesInWorkflow(false)`: the Workflow ID, Run ID and Namespace are then empty.

---

## 6. Heartbeats

`ImportSupplierCatalog` imports a supplier's catalog page by page. After each page it heartbeats its
progress. If the Worker crashes, the next attempt reads the last heartbeat and continues from there:

```go
// file: catalog/import.go
package catalog

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

// SupplierFeed pages through a supplier's product catalog.
type SupplierFeed interface {
	FetchPage(ctx context.Context, supplierID string, page int) (products []string, last bool, err error)
}

// ProductStore saves imported products.
type ProductStore interface {
	Upsert(ctx context.Context, products []string) error
}

type ImportProgress struct {
	NextPage int
	Imported int
}

type Activities struct {
	Feed  SupplierFeed
	Store ProductStore
}

// ImportSupplierCatalog imports every page of a supplier feed. It heartbeats
// after each page, and a retry resumes from the last heartbeat.
func (a *Activities) ImportSupplierCatalog(ctx context.Context, supplierID string) (int, error) {
	progress := ImportProgress{NextPage: 1}
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &progress); err != nil {
			return 0, fmt.Errorf("read heartbeat details: %w", err)
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return progress.Imported, err // canceled: stop between pages
		}
		products, last, err := a.Feed.FetchPage(ctx, supplierID, progress.NextPage)
		if err != nil {
			return progress.Imported, fmt.Errorf("fetch page %d: %w", progress.NextPage, err)
		}
		if err := a.Store.Upsert(ctx, products); err != nil {
			return progress.Imported, fmt.Errorf("store page %d: %w", progress.NextPage, err)
		}
		progress.Imported += len(products)
		progress.NextPage++
		activity.RecordHeartbeat(ctx, progress)
		if last {
			return progress.Imported, nil
		}
	}
}
```

`SetOnActivityHeartbeatListener` lets the test see what the Activity reported. Here the feed fails on page
4, and the test checks what the next attempt will resume from:

```go
// file: catalog/import_test.go
package catalog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// fakeFeed serves two products per page. It fails on page failAt (0 = never).
type fakeFeed struct {
	pages   int
	failAt  int
	delay   time.Duration
	mu      sync.Mutex
	fetched []int
}

func (f *fakeFeed) FetchPage(ctx context.Context, supplierID string, page int) ([]string, bool, error) {
	f.mu.Lock()
	f.fetched = append(f.fetched, page)
	f.mu.Unlock()
	time.Sleep(f.delay)
	if page == f.failAt {
		return nil, false, errors.New("supplier API returned 503")
	}
	return []string{"sku-a", "sku-b"}, page >= f.pages, nil
}

type memStore struct{ n int }

func (m *memStore) Upsert(ctx context.Context, products []string) error {
	m.n += len(products)
	return nil
}

type ImportSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env  *testsuite.TestActivityEnvironment
	feed *fakeFeed
	acts *Activities
}

func (s *ImportSuite) SetupTest() {
	s.feed = &fakeFeed{pages: 5}
	s.acts = &Activities{Feed: s.feed, Store: &memStore{}}
	s.env = s.NewTestActivityEnvironment()
	s.env.RegisterActivity(s.acts)
}

func (s *ImportSuite) Test_ImportsAllPages() {
	val, err := s.env.ExecuteActivity(s.acts.ImportSupplierCatalog, "acme-supplies")
	s.Require().NoError(err)

	var imported int
	s.Require().NoError(val.Get(&imported))
	s.Equal(10, imported)
	s.Equal([]int{1, 2, 3, 4, 5}, s.feed.fetched)
}

func (s *ImportSuite) Test_HeartbeatsProgressBeforeFailing() {
	s.feed.failAt = 4

	var mu sync.Mutex
	var reported []ImportProgress
	s.env.SetOnActivityHeartbeatListener(func(info *activity.Info, details converter.EncodedValues) {
		var p ImportProgress
		s.NoError(details.Get(&p))
		mu.Lock()
		reported = append(reported, p)
		mu.Unlock()
	})

	_, err := s.env.ExecuteActivity(s.acts.ImportSupplierCatalog, "acme-supplies")
	s.Require().ErrorContains(err, "supplier API returned 503")

	mu.Lock()
	defer mu.Unlock()
	s.T().Logf("heartbeats seen by the listener: %+v", reported)
	// The last heartbeat sent before the failure is what the retry will resume from.
	s.Equal(ImportProgress{NextPage: 4, Imported: 6}, reported[len(reported)-1])
}

func (s *ImportSuite) Test_ResumesFromHeartbeat() {
	// Pretend a previous attempt heartbeated after page 3, then failed.
	s.env.SetHeartbeatDetails(ImportProgress{NextPage: 4, Imported: 6})

	val, err := s.env.ExecuteActivity(s.acts.ImportSupplierCatalog, "acme-supplies")
	s.Require().NoError(err)

	var imported int
	s.Require().NoError(val.Get(&imported))
	s.Equal(10, imported)
	s.Equal([]int{4, 5}, s.feed.fetched) // pages 1 to 3 were not fetched again
}

func (s *ImportSuite) Test_StopsWhenCanceled() {
	s.feed.pages = 1000
	s.feed.delay = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	s.env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx})
	time.AfterFunc(100*time.Millisecond, cancel)

	_, err := s.env.ExecuteActivity(s.acts.ImportSupplierCatalog, "acme-supplies")

	s.Require().ErrorContains(err, "context canceled")
	s.Less(len(s.feed.fetched), 1000)
	s.T().Logf("pages fetched before cancel: %d", len(s.feed.fetched))
}

func TestImportSuite(t *testing.T) {
	suite.Run(t, new(ImportSuite))
}
```

Running it:

```text
$ go test -run 'TestImportSuite/Test_HeartbeatsProgressBeforeFailing' -v ./catalog
    import_test.go:89: heartbeats seen by the listener: [{NextPage:2 Imported:2} {NextPage:4 Imported:6}]
```

The Activity called `RecordHeartbeat` **three** times (after pages 1, 2 and 3), but the listener saw
**two**. Heartbeats are throttled in tests as in production: the first one is sent, later ones are
batched, and the latest one is flushed when the Activity fails. On success, the same Activity reports
only its first heartbeat. So:

- **Don't count heartbeats.** The Go SDK page's sample asserts `heartBeatCount == 1` on an Activity that
  heartbeats once. It works there, but breaks as soon as the Activity heartbeats in a loop.
- **Assert the last reported value**, which is what a retry will get.

---

## 7. Resuming from a heartbeat

`SetHeartbeatDetails` sets what `activity.GetHeartbeatDetails` returns, as if a previous attempt had
heartbeated it. `Test_ResumesFromHeartbeat` in the suite above pretends attempt 1 died after page 3 and
checks that attempt 2 only fetches pages 4 and 5. This is the test that proves your heartbeat payload
and your resume logic agree.

---

## 8. Cancellation

The Activity context of the test environment derives from the Worker's `BackgroundActivityContext`.
Cancel that context and the Activity sees `ctx.Done()`, as it would when its Workflow cancels it
(`Test_StopsWhenCanceled` above).

The returned error is less specific than in production. On SDK v1.50.0 it is:

```text
activity error (type: ImportSupplierCatalog, scheduledEventID: 0, startedEventID: 0, identity: ): context canceled
```

The cause is a **retryable `*temporal.ApplicationError`** with the message `context canceled`:
`temporal.IsCanceledError(err)` is `false` and `errors.Is(err, context.Canceled)` is `false`. That's why
the test uses `ErrorContains(err, "context canceled")`. What you are really testing is that the Activity
**stops** (here: after 10 of 1000 pages), not the exact error type.

The Go SDK page's cancellation sample sleeps 2 seconds before canceling. `time.AfterFunc` with 100 ms
gives the same coverage and keeps the suite fast.

---

## 9. Local Activities

A Local Activity runs inside the Workflow Task, without its own Activity Task. To test one, use
`ExecuteLocalActivity`:

```go
// file: shipping/quote.go
package shipping

import (
	"context"
	"fmt"
)

type QuoteInput struct {
	Country string
	Grams   int
}

// QuoteShipping is cheap and fast: the checkout Workflow runs it as a Local Activity.
func QuoteShipping(ctx context.Context, in QuoteInput) (int64, error) {
	switch in.Country {
	case "FR", "DE", "BE":
		return 499 + int64(in.Grams/1000)*100, nil
	case "US":
		return 1999 + int64(in.Grams/1000)*250, nil
	default:
		return 0, fmt.Errorf("no carrier ships to %q", in.Country)
	}
}
```

```go
// file: shipping/quote_test.go
package shipping

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
)

func TestQuoteShipping_LocalActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(QuoteShipping)

	val, err := env.ExecuteLocalActivity(QuoteShipping, QuoteInput{Country: "FR", Grams: 2500})
	if err != nil {
		t.Fatal(err)
	}
	var cents int64
	if err := val.Get(&cents); err != nil {
		t.Fatal(err)
	}
	if cents != 699 {
		t.Errorf("got %d cents, want 699", cents)
	}

	_, err = env.ExecuteLocalActivity(QuoteShipping, QuoteInput{Country: "AQ", Grams: 100})
	if err == nil {
		t.Fatal("expected an error for Antarctica")
	}
	t.Log(err)
}
```

---

## 10. Plain Go tests for plain Go code

Not everything needs a Temporal test environment. Business rules that don't use the Activity context
belong in plain functions with plain table tests. They run faster and fail with clearer messages:

```go
// file: payments/money.go
package payments

// ApplyDiscount returns the price after a percentage discount, rounded down to the cent.
// It's plain Go: test it with a plain Go test.
func ApplyDiscount(cents int64, percent int) int64 {
	if percent <= 0 {
		return cents
	}
	if percent >= 100 {
		return 0
	}
	return cents * int64(100-percent) / 100
}
```

```go
// file: payments/money_test.go
package payments

import "testing"

func TestApplyDiscount(t *testing.T) {
	cases := []struct {
		name    string
		cents   int64
		percent int
		want    int64
	}{
		{"no discount", 4999, 0, 4999},
		{"ten percent", 4999, 10, 4499},
		{"free", 4999, 100, 0},
		{"negative is ignored", 4999, -5, 4999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ApplyDiscount(tc.cents, tc.percent); got != tc.want {
				t.Errorf("ApplyDiscount(%d, %d) = %d, want %d", tc.cents, tc.percent, got, tc.want)
			}
		})
	}
}
```

A good split: the Activity is a thin adapter (read inputs, call the dependency, map errors). The rules
it applies are plain functions.

---

## 11. Timeouts in the Activity environment

The Go SDK page says the timeout "for the entire test can be set using `SetTestTimeout` in the Workflow
or Activity environment". In the Activity environment that's only half true on SDK v1.50.0:

- `ExecuteActivity` always runs with a fixed **10-minute** StartToClose and ScheduleToClose timeout
  (`internal/internal_workflow_testsuite.go`). `SetTestTimeout` doesn't change it.
- `SetTestTimeout` only applies to `ExecuteLocalActivity`.

An Activity that waits on `ctx.Done()` and never gets canceled hung for more than 30 seconds in a test
with `SetTestTimeout(2 * time.Second)`, until `go test -timeout 30s` killed the whole binary. To guard
against a stuck Activity, cancel it yourself (`BackgroundActivityContext` and `time.AfterFunc`) or rely
on `go test -timeout`.

Also, calling `ExecuteActivity` for an Activity you forgot to register **panics**
(`no activity is registered for taskqueue 'default-test-taskqueue'`), where the Workflow environment
returns an error ([chapter 02](02-workflow-test-environment.md)).

---

## 12. Key takeaways

- Use `NewTestActivityEnvironment` for Activities that use the Activity context. Use a plain Go test for
  plain functions.
- Inject dependencies through an Activities struct, and give the test a fake.
- Assert errors with `errors.As(err, &appErr)` and check `NonRetryable()` and `Type()`: that's what
  decides whether the Workflow retries.
- `activity.GetInfo` returns fixed values in tests (`"default-test-workflow-id"`, Activity ID `"0"`).
- Heartbeats are throttled: assert the **last** reported value, not the number of calls. Use
  `SetHeartbeatDetails` to test the resume path.
- Cancel through `BackgroundActivityContext`. The error is a plain `context canceled` application error,
  not a `CanceledError`.
- `SetTestTimeout` doesn't bound `ExecuteActivity` (fixed 10 minutes). Cancel stuck Activities yourself.
