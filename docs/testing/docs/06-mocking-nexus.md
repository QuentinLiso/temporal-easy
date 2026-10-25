# 06 · Mocking Nexus Operations

> **Level:** advanced · **Read time:** ~14 min
> **Sources:** [Go SDK: How to mock Nexus operations](https://docs.temporal.io/develop/go/best-practices/testing-suite#how-to-mock-nexus-operations) · [`testsuite.TestWorkflowEnvironment.OnNexusOperation`](https://pkg.go.dev/go.temporal.io/sdk/testsuite#TestWorkflowEnvironment.OnNexusOperation) · [`temporalnexus`](https://pkg.go.dev/go.temporal.io/sdk/temporalnexus)

---

## 1. The problem

The checkout team's Workflow asks the **risk team** whether an order is fraudulent. The risk team runs
its own Workers in its own Namespace and exposes a Nexus Service, `fraud`, with two Operations:

- `score-order`, **synchronous**: returns a score in milliseconds.
- `manual-review`, **asynchronous**: a human looks at the order, which can take days.

The checkout tests can't run the risk team's model, and shouldn't wait days for a reviewer. They need to
replace the Nexus Operations the way [chapter 03](03-mocking-activities.md) replaces Activities.

---

## 2. Mock or override

The Workflow test environment offers two ways, like for Activities:

| | Mock | Override |
| --- | --- | --- |
| API | `env.OnNexusOperation(service, op, input, options).Return(result, err)` | `env.RegisterNexusService(fakeService)` |
| You provide | A canned result per input | A handler implementation (sync function or handler Workflow) |
| Best for | Branches of **your** Workflow: approve, reject, review, failure | A realistic fake with its own logic |
| Assertions | `AssertExpectations`, `AssertNexusOperationCalled`… | Your own |

In both cases, nothing leaves the test process: there is no Nexus Endpoint, no handler Namespace, no
Service.

---

## 3. The contract and the caller

The risk team publishes a small Go package with the Service name, the input and output types, and
**operation references**: `nexus.NewOperationReference[I, O](name)` describes an Operation's name and
types without its implementation. The checkout team needs nothing else, in production or in tests:

```go
// file: fraudapi/api.go
// Package fraudapi is the contract published by the risk team. The checkout team
// imports it; the operations themselves run in the risk team's Namespace.
package fraudapi

import "github.com/nexus-rpc/sdk-go/nexus"

const ServiceName = "fraud"

type ScoreOrderInput struct {
	OrderID    string
	CustomerID string
	Cents      int64
}

type ScoreOrderOutput struct {
	Score    int    // 0 (safe) to 100 (fraud)
	Decision string // "approve", "review" or "reject"
}

type ManualReviewInput struct {
	OrderID string
	Reason  string
}

type ManualReviewOutput struct {
	Approved bool
	Reviewer string
}

// Operation references: name + input/output types, no implementation.
var (
	ScoreOrder   = nexus.NewOperationReference[ScoreOrderInput, ScoreOrderOutput]("score-order")
	ManualReview = nexus.NewOperationReference[ManualReviewInput, ManualReviewOutput]("manual-review")
)
```

The checkout Workflow calls `score-order`, and `manual-review` when the score is borderline:

```go
// file: checkout/workflow.go
package checkout

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/acme/shop/fraudapi"
)

// RiskEndpoint is the Nexus Endpoint that routes to the risk team's Namespace.
const RiskEndpoint = "risk-endpoint"

type Order struct {
	OrderID    string
	CustomerID string
	Cents      int64
}

type CheckoutResult struct {
	Status     string // "accepted" or "rejected"
	FraudScore int
	ReviewedBy string
}

func CheckoutWorkflow(ctx workflow.Context, order Order) (CheckoutResult, error) {
	risk := workflow.NewNexusClient(RiskEndpoint, fraudapi.ServiceName)
	opts := workflow.NexusOperationOptions{ScheduleToCloseTimeout: 10 * time.Minute}

	var score fraudapi.ScoreOrderOutput
	err := risk.ExecuteOperation(ctx, fraudapi.ScoreOrder, fraudapi.ScoreOrderInput{
		OrderID: order.OrderID, CustomerID: order.CustomerID, Cents: order.Cents,
	}, opts).Get(ctx, &score)
	if err != nil {
		return CheckoutResult{}, fmt.Errorf("score order: %w", err)
	}

	result := CheckoutResult{FraudScore: score.Score}
	switch score.Decision {
	case "approve":
		result.Status = "accepted"
	case "reject":
		result.Status = "rejected"
	case "review":
		// A human looks at the order: this can take days.
		reviewOpts := workflow.NexusOperationOptions{ScheduleToCloseTimeout: 7 * 24 * time.Hour}
		var review fraudapi.ManualReviewOutput
		err := risk.ExecuteOperation(ctx, fraudapi.ManualReview, fraudapi.ManualReviewInput{
			OrderID: order.OrderID, Reason: fmt.Sprintf("score %d", score.Score),
		}, reviewOpts).Get(ctx, &review)
		if err != nil {
			return CheckoutResult{}, fmt.Errorf("manual review: %w", err)
		}
		result.ReviewedBy = review.Reviewer
		result.Status = "rejected"
		if review.Approved {
			result.Status = "accepted"
		}
	default:
		return CheckoutResult{}, fmt.Errorf("unknown fraud decision %q", score.Decision)
	}
	return result, nil
}
```

`ExecuteOperation` takes `any` for the Operation and its input, so the compiler checks nothing. Pass the
operation reference rather than the name as a string: the SDK then checks at run time that the input
type matches the reference (`cannot assign argument of type …`).

---

## 4. Mocking a synchronous Operation

`OnNexusOperation` takes the Service name, the Operation (a reference, the real Operation, or its name
as a string), the expected input, and the expected `workflow.NexusOperationOptions` (`mock.Anything`
nearly always). A synchronous result is a `*nexus.HandlerStartOperationResultSync[T]`:

```go
// file: checkout/checkout_suite_test.go
package checkout

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"

	"github.com/acme/shop/fraudapi"
)

var order = Order{OrderID: "ord-7781", CustomerID: "cus-1042", Cents: 129900}

var scoreInput = fraudapi.ScoreOrderInput{OrderID: "ord-7781", CustomerID: "cus-1042", Cents: 129900}

type CheckoutSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *CheckoutSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *CheckoutSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

// mockScore makes the "score-order" operation return synchronously.
func (s *CheckoutSuite) mockScore(score int, decision string) *testsuite.MockCallWrapper {
	return s.env.OnNexusOperation(
		fraudapi.ServiceName,
		fraudapi.ScoreOrder,
		scoreInput,
		mock.Anything, // workflow.NexusOperationOptions
	).Return(
		&nexus.HandlerStartOperationResultSync[fraudapi.ScoreOrderOutput]{
			Value: fraudapi.ScoreOrderOutput{Score: score, Decision: decision},
		},
		nil,
	)
}

func (s *CheckoutSuite) result() CheckoutResult {
	s.Require().True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
	var res CheckoutResult
	s.Require().NoError(s.env.GetWorkflowResult(&res))
	return res
}

func TestCheckoutSuite(t *testing.T) {
	suite.Run(t, new(CheckoutSuite))
}
```

```go
// file: checkout/score_sync_test.go
package checkout

import (
	"github.com/stretchr/testify/mock"

	"github.com/acme/shop/fraudapi"
)

func (s *CheckoutSuite) Test_LowScore_Accepted() {
	s.mockScore(12, "approve").Once()

	s.env.ExecuteWorkflow(CheckoutWorkflow, order)

	s.Equal(CheckoutResult{Status: "accepted", FraudScore: 12}, s.result())
	s.env.AssertNexusOperationNumberOfCalls(s.T(), fraudapi.ServiceName, 1)
}

func (s *CheckoutSuite) Test_HighScore_Rejected() {
	s.mockScore(97, "reject").Once()

	s.env.ExecuteWorkflow(CheckoutWorkflow, order)

	s.Equal("rejected", s.result().Status)
	s.env.AssertNexusOperationNotCalled(s.T(), fraudapi.ServiceName, "manual-review", mock.Anything, mock.Anything)
}
```

Notes:

- The mock matches on **Service and Operation**. The Endpoint name (`risk-endpoint`) isn't part of the
  match, so the test doesn't prove you used the right Endpoint.
- `OnNexusOperation` checks the input **type** against the operation reference when you register the
  mock, and panics on a mismatch (`operation "score-order" expects input type …`).
- `AssertNexusOperationNotCalled` and `AssertNexusOperationNumberOfCalls` take the Service name, then
  (for `NotCalled`) the Operation name, input and options. The Go SDK page doesn't mention them.
- A Workflow that calls a Nexus Operation with nothing mocked or registered fails with
  `no nexus services registered`.

---

## 5. Delays and failures

`.After(d)` delays the mock's answer by `d` of **Workflow** time. To fail the Operation, return an
error as the second value of `Return`:

```go
// file: checkout/score_failure_test.go
package checkout

import (
	"errors"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"

	"github.com/acme/shop/fraudapi"
)

func (s *CheckoutSuite) Test_SlowScoring() {
	// The risk service answers after 30 s of Workflow time (no real waiting).
	s.mockScore(12, "approve").After(30 * time.Second).Once()
	start := s.env.Now()

	s.env.ExecuteWorkflow(CheckoutWorkflow, order)

	s.Equal("accepted", s.result().Status)
	s.Equal(30*time.Second, s.env.Now().Sub(start).Round(time.Second))
}

func (s *CheckoutSuite) Test_ScoringFails() {
	s.env.OnNexusOperation(fraudapi.ServiceName, fraudapi.ScoreOrder, scoreInput, mock.Anything).
		Return(nil, nexus.NewOperationFailedError("customer is on the blocklist")).Once()

	s.env.ExecuteWorkflow(CheckoutWorkflow, order)

	err := s.env.GetWorkflowError()
	var nexusErr *temporal.NexusOperationError
	s.Require().True(errors.As(err, &nexusErr), "got %v", err)
	s.Equal("score-order", nexusErr.Operation)
	s.ErrorContains(err, "customer is on the blocklist")
}
```

The Workflow receives a `*temporal.NexusOperationError` (with `Endpoint`, `Service` and `Operation`
fields). What it says about the cause depends on the error you return. On SDK v1.50.0:

| Mock returns | What the Workflow error says |
| --- | --- |
| `nexus.NewOperationFailedError("customer is on the blocklist")` | `… (type: OperationError, retryable: false): customer is on the blocklist` |
| `nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "order has no customer")` | `… handler error (BAD_REQUEST)`: **your message is gone** |
| `errors.New("risk model unavailable")` | `… handler error (INTERNAL): internal handler error`: **your message is gone** |
| `temporal.NewNonRetryableApplicationError("risk model down", …)` | `… handler error (INTERNAL): handler failed with non-retryable application error` |

So use `nexus.NewOperationFailedError` when the test asserts on the message. Also, in production a
retryable handler error (like `INTERNAL`) is retried by the caller's Service until the Operation's
ScheduleToClose timeout; in the test environment the mock's error fails the Operation at once.

---

## 6. Mocking an asynchronous Operation

An asynchronous Operation starts, returns an **operation token**, and completes later. Mock it in two
steps:

1. `OnNexusOperation(…).Return(&nexus.HandlerStartOperationResultAsync{OperationToken: "…"}, nil)`: the
   start.
2. `env.RegisterNexusAsyncOperationCompletion(service, operation, token, result, err, delay)`: the
   completion, `delay` after the start, in Workflow time.

The token is the link between the two: it must be the same string.

```go
// file: checkout/review_async_test.go
package checkout

import (
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"

	"github.com/acme/shop/fraudapi"
)

func (s *CheckoutSuite) Test_MediumScore_ManualReview() {
	s.mockScore(55, "review").Once()

	// The manual review is asynchronous: the mock returns an operation token…
	s.env.OnNexusOperation(
		fraudapi.ServiceName,
		fraudapi.ManualReview,
		fraudapi.ManualReviewInput{OrderID: "ord-7781", Reason: "score 55"},
		mock.Anything,
	).Return(&nexus.HandlerStartOperationResultAsync{OperationToken: "review-ord-7781"}, nil).Once()

	// …and the completion for that token arrives 2 days later (Workflow time).
	s.NoError(s.env.RegisterNexusAsyncOperationCompletion(
		fraudapi.ServiceName,
		"manual-review",
		"review-ord-7781",
		fraudapi.ManualReviewOutput{Approved: true, Reviewer: "j.martin"},
		nil,
		48*time.Hour,
	))
	start := s.env.Now()

	s.env.ExecuteWorkflow(CheckoutWorkflow, order)

	s.Equal(CheckoutResult{Status: "accepted", FraudScore: 55, ReviewedBy: "j.martin"}, s.result())
	s.Equal(48*time.Hour, s.env.Now().Sub(start).Round(time.Minute))
}
```

The review takes 2 days of Workflow time and no wall time. If your Workflow starts several async
Operations, give each mock its own token and register one completion per token.

---

## 7. Overriding with a fake Service

Mocks are canned answers. When you'd rather have a small working fake (here: "orders over 1,000 euros
go to review, and a reviewer approves them after 4 hours"), build a real `nexus.Service` in the test and
register it with `env.RegisterNexusService`:

- `nexus.NewSyncOperation(name, fn)` for a synchronous Operation.
- `temporalnexus.NewWorkflowRunOperation(name, handlerWorkflow, getOptions)` for an asynchronous one
  backed by a Workflow, exactly as the risk team would implement it.

```go
// file: checkout/override_test.go
package checkout

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/acme/shop/fraudapi"
)

// rulesScorer is a tiny stand-in for the risk team's model: big orders get reviewed.
var rulesScorer = nexus.NewSyncOperation(
	"score-order",
	func(ctx context.Context, in fraudapi.ScoreOrderInput, opts nexus.StartOperationOptions) (fraudapi.ScoreOrderOutput, error) {
		if in.Cents >= 100000 {
			return fraudapi.ScoreOrderOutput{Score: 60, Decision: "review"}, nil
		}
		return fraudapi.ScoreOrderOutput{Score: 5, Decision: "approve"}, nil
	},
)

// ManualReviewWorkflow plays the risk team's review queue: a reviewer answers after 4 hours.
func ManualReviewWorkflow(ctx workflow.Context, in fraudapi.ManualReviewInput) (fraudapi.ManualReviewOutput, error) {
	if err := workflow.Sleep(ctx, 4*time.Hour); err != nil {
		return fraudapi.ManualReviewOutput{}, err
	}
	return fraudapi.ManualReviewOutput{Approved: true, Reviewer: "on-call-reviewer"}, nil
}

var manualReview = temporalnexus.NewWorkflowRunOperation(
	"manual-review",
	ManualReviewWorkflow,
	func(ctx context.Context, in fraudapi.ManualReviewInput, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		return client.StartWorkflowOptions{ID: "manual-review-" + in.OrderID}, nil
	},
)

func newFakeRiskService(t *testing.T) *nexus.Service {
	svc := nexus.NewService(fraudapi.ServiceName)
	require.NoError(t, svc.Register(rulesScorer, manualReview))
	return svc
}

func TestCheckout_SmallOrder_FakeRiskService(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterNexusService(newFakeRiskService(t))
	env.RegisterWorkflow(ManualReviewWorkflow) // the handler Workflow must be registered too

	env.ExecuteWorkflow(CheckoutWorkflow, Order{OrderID: "ord-1", CustomerID: "cus-9", Cents: 2500})

	require.NoError(t, env.GetWorkflowError())
	var res CheckoutResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, CheckoutResult{Status: "accepted", FraudScore: 5}, res)
}

func TestCheckout_BigOrder_FakeRiskService(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterNexusService(newFakeRiskService(t))
	env.RegisterWorkflow(ManualReviewWorkflow)
	start := env.Now()

	env.ExecuteWorkflow(CheckoutWorkflow, Order{OrderID: "ord-2", CustomerID: "cus-9", Cents: 250000})

	require.NoError(t, env.GetWorkflowError())
	var res CheckoutResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, CheckoutResult{Status: "accepted", FraudScore: 60, ReviewedBy: "on-call-reviewer"}, res)
	require.Equal(t, 4*time.Hour, env.Now().Sub(start).Round(time.Minute))
}
```

The handler Workflow must be **registered** with `env.RegisterWorkflow`. Without it, the test binary
**panics** (`unable to find workflow type: ManualReviewWorkflow. Supported types: [CheckoutWorkflow]`),
from a goroutine that a `recover` in your test can't catch, so `go test` reports a crash instead of a
failed test.

`Register` takes several Operations at once, and the Operation names must match the references of the
contract (`"score-order"`, `"manual-review"`).

---

## 8. Key takeaways

- Depend on the other team's **contract** (Service name, types, `nexus.NewOperationReference`), not on
  their implementation. It's all your tests need.
- `OnNexusOperation` + `HandlerStartOperationResultSync` mocks a synchronous Operation.
  `HandlerStartOperationResultAsync` + `RegisterNexusAsyncOperationCompletion` mocks an asynchronous one;
  the token ties them together.
- `.After(d)` and the async completion delay are Workflow time: days of review cost nothing.
- Return `nexus.NewOperationFailedError(msg)` to test failures: handler errors and plain errors lose
  their message.
- The mock ignores the Endpoint name.
- For a working fake, register a `nexus.Service` built with `nexus.NewSyncOperation` and
  `temporalnexus.NewWorkflowRunOperation`, and register its handler Workflows too.
