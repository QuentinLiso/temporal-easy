# 03 · Mocking Activities and Child Workflows

> **Level:** daily use · **Read time:** ~15 min
> **Sources:** [Go SDK — How to mock Activities](https://docs.temporal.io/develop/go/best-practices/testing-suite#mock-activities) · [Go SDK — Debugging: Activity mocking and overriding](https://docs.temporal.io/develop/go/best-practices/debugging) · [`TestWorkflowEnvironment.OnActivity`](https://pkg.go.dev/go.temporal.io/sdk/testsuite#TestWorkflowEnvironment.OnActivity)

---

## 1. The problem

`FulfillOrderWorkflow` reserves stock, charges the card (with a Retry Policy) and starts a
`ShipOrderWorkflow` Child Workflow. The interesting tests are about **what the Workflow does when
the outside world misbehaves**:

- the payment gateway times out twice, then succeeds: does the order still go through?
- it times out on every attempt: does the Workflow give up, and **not** ship?
- the card is declined: is the charge attempted **once**, not three times?
- the warehouse takes two hours: does anything depend on that delay?

You can't make a real gateway fail on demand. Mocks can.

---

## 2. Mocking vs overriding

The docs describe two mechanisms. Both change what an Activity does in the test, **without changing
the Workflow code**:

| Mechanism | How | Use it for |
| --- | --- | --- |
| **Mocking** | `env.OnActivity(act, args…).Return(value, err)` | Fixed answers: a result, an error |
| **Overriding** | `env.OnActivity(act, args…).Return(func(ctx, args…) (…))` | Logic: check arguments, compute a result, fail on a condition |

Both are built on [testify's `mock`](https://pkg.go.dev/github.com/stretchr/testify/mock): argument
matchers, call counts and expectations work the same way.

---

## 3. The example

```go
// file: orders/activities.go
package orders

import "context"

type Inventory interface {
	Reserve(ctx context.Context, sku string, qty int) error
}

type PaymentGateway interface {
	Charge(ctx context.Context, customerID string, amountCents int64) (paymentID string, err error)
}

type Activities struct {
	Inventory Inventory
	Payments  PaymentGateway
}

func (a *Activities) ReserveInventory(ctx context.Context, sku string, qty int) error {
	return a.Inventory.Reserve(ctx, sku, qty)
}

func (a *Activities) ChargePayment(ctx context.Context, customerID string, amountCents int64) (string, error) {
	return a.Payments.Charge(ctx, customerID, amountCents)
}
```

```go
// file: shipping/workflow.go
package shipping

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

type ShipOrderInput struct {
	OrderID string
	Address string
}

// CreateLabel calls the carrier API. Its real implementation lives with the shipping team.
func CreateLabel(ctx context.Context, in ShipOrderInput) (string, error) {
	return "", nil
}

// ShipOrderWorkflow is run as a Child Workflow of FulfillOrderWorkflow.
func ShipOrderWorkflow(ctx workflow.Context, in ShipOrderInput) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var tracking string
	err := workflow.ExecuteActivity(ctx, CreateLabel, in).Get(ctx, &tracking)
	return tracking, err
}
```

```go
// file: orders/workflow.go
package orders

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/acme/shop/shipping"
)

type FulfillOrderInput struct {
	OrderID     string
	CustomerID  string
	SKU         string
	Quantity    int
	AmountCents int64
	Address     string
}

type FulfillOrderResult struct {
	PaymentID      string
	TrackingNumber string
}

func FulfillOrderWorkflow(ctx workflow.Context, in FulfillOrderInput) (FulfillOrderResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumAttempts:    3,
		},
	})
	var a *Activities

	if err := workflow.ExecuteActivity(ctx, a.ReserveInventory, in.SKU, in.Quantity).Get(ctx, nil); err != nil {
		return FulfillOrderResult{}, err
	}
	var paymentID string
	if err := workflow.ExecuteActivity(ctx, a.ChargePayment, in.CustomerID, in.AmountCents).Get(ctx, &paymentID); err != nil {
		return FulfillOrderResult{}, err
	}

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: "ship-" + in.OrderID,
	})
	var tracking string
	err := workflow.ExecuteChildWorkflow(childCtx, shipping.ShipOrderWorkflow,
		shipping.ShipOrderInput{OrderID: in.OrderID, Address: in.Address}).Get(ctx, &tracking)
	if err != nil {
		return FulfillOrderResult{}, err
	}
	return FulfillOrderResult{PaymentID: paymentID, TrackingNumber: tracking}, nil
}
```

The suite, with a helper that mocks the happy path:

```go
// file: orders/workflow_test.go
package orders_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"

	"github.com/acme/shop/orders"
	"github.com/acme/shop/shipping"
)

type FulfillOrderSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
	a   *orders.Activities // nil: only used to name the Activities in mocks
}

func (s *FulfillOrderSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *FulfillOrderSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func TestFulfillOrderSuite(t *testing.T) {
	suite.Run(t, new(FulfillOrderSuite))
}

var mugOrder = orders.FulfillOrderInput{
	OrderID: "ord_1001", CustomerID: "cus_42", SKU: "SKU-RED-MUG",
	Quantity: 2, AmountCents: 2400, Address: "1 Harbour St, Leith",
}

// mockHappyPath mocks every step with a fixed value.
func (s *FulfillOrderSuite) mockHappyPath() {
	s.env.OnActivity(s.a.ReserveInventory, mock.Anything, "SKU-RED-MUG", 2).Return(nil)
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, "cus_42", int64(2400)).Return("pay_9001", nil)
	s.env.OnWorkflow(shipping.ShipOrderWorkflow, mock.Anything, mock.Anything).Return("1Z999AA10123456784", nil)
}

func (s *FulfillOrderSuite) Test_ReturnValues() {
	s.mockHappyPath()

	s.env.ExecuteWorkflow(orders.FulfillOrderWorkflow, mugOrder)

	s.NoError(s.env.GetWorkflowError())
	var res orders.FulfillOrderResult
	s.NoError(s.env.GetWorkflowResult(&res))
	s.Equal(orders.FulfillOrderResult{PaymentID: "pay_9001", TrackingNumber: "1Z999AA10123456784"}, res)
}
```

No Activity is registered in `SetupTest`: a mocked Activity doesn't need to be. Naming it through
the nil `s.a` is enough for the environment to know its name and signature.

---

## 4. Matching arguments

`OnActivity(act, ctxMatcher, arg1, arg2…)` takes one matcher for the Activity's `context.Context`,
then one per argument:

| Matcher | Matches |
| --- | --- |
| `mock.Anything` | Any value. Always use it for the `context.Context` |
| A concrete value: `"cus_42"`, `int64(2400)` | An equal value **of the same type**. A literal `2400` is an `int`: it doesn't match an `int64` argument |
| `mock.AnythingOfType("string")` | Any value of that type |
| `mock.MatchedBy(func(in shipping.ShipOrderInput) bool { … })` | Any value the function accepts |

When the Workflow calls an Activity with arguments **no mock matches**, testify panics inside the
Activity. The environment turns that into a `*temporal.PanicError`, the Retry Policy retries it, and
the Workflow fails:

```go
// file: orders/mismatch_test.go
package orders_test

import (
	"errors"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"

	"github.com/acme/shop/orders"
)

// Arguments that match no mock: the Activity panics on every attempt.
func (s *FulfillOrderSuite) Test_UnmatchedArguments() {
	s.env.OnActivity(s.a.ReserveInventory, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, "cus_7", int64(2400)).Return("pay_9001", nil).Maybe()

	s.env.ExecuteWorkflow(orders.FulfillOrderWorkflow, mugOrder) // CustomerID is "cus_42"

	var panicErr *temporal.PanicError
	s.True(errors.As(s.env.GetWorkflowError(), &panicErr))
	s.Contains(panicErr.Error(), "mock: Unexpected Method Call")
}
```

The test log shows testify's diff, which is the fastest way to find which argument is off:

```text
mock: Unexpected Method Call
...
Diff: 0: PASS:  (*context.timerCtx=...)
	1: FAIL:  (string=cus_42) != (string=cus_7)
	2: PASS:  (int64=2400) == (int64=2400)
```

`.Maybe()` marks the mock as optional, so `AssertExpectations` in `AfterTest` doesn't also complain
that it was never called.

---

## 5. Overriding with a function

Pass a function to `Return` and it runs **instead of** the Activity, with the Activity's arguments.
The docs use it to check that the Activity gets the right parameters:

```go
// file: orders/replace_test.go
package orders_test

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/acme/shop/orders"
	"github.com/acme/shop/shipping"
)

// A replacement function: same signature as the Activity, runs instead of it.
func (s *FulfillOrderSuite) Test_ReplacementFunction() {
	s.env.OnActivity(s.a.ReserveInventory, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, customerID string, amountCents int64) (string, error) {
			s.Equal("cus_42", customerID)
			s.Positive(amountCents, "never charge zero or a refund here")
			return "pay_" + customerID, nil
		})
	s.env.OnWorkflow(shipping.ShipOrderWorkflow, mock.Anything, mock.Anything).Return("TRK-1", nil)

	s.env.ExecuteWorkflow(orders.FulfillOrderWorkflow, mugOrder)

	s.NoError(s.env.GetWorkflowError())
}
```

The docs say the framework "will validate that the signature of the mock function matches the
signature of the original Activity function". It does, but **at call time, not when you set the
mock**: `OnActivity` accepts any function, and a mismatch shows up as an ordinary, retryable
Activity error. With `amountCents int` instead of `int64`, all 3 attempts fail and the Workflow
error reads:

```text
activity error (type: ChargePayment, …): mock of ChargePayment has incorrect return function, expected func(context.Context, string, int64) (string, error), but actual is func(context.Context, string, int) (string, error)
```

---

## 6. Retries and non-retryable errors

The Workflow's Retry Policy applies to mocked Activities too, and retry delays run on the **mock
clock**. Chain several mocks for the same Activity with call counts to script a sequence:

```go
// file: orders/retry_test.go
package orders_test

import (
	"errors"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"

	"github.com/acme/shop/orders"
	"github.com/acme/shop/shipping"
)

// Two failures, then a success: the Retry Policy (3 attempts) absorbs them.
func (s *FulfillOrderSuite) Test_RetriesThenSucceeds() {
	s.env.OnActivity(s.a.ReserveInventory, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, mock.Anything, mock.Anything).
		Return("", errors.New("gateway timeout")).Twice()
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, mock.Anything, mock.Anything).
		Return("pay_9001", nil).Once()
	s.env.OnWorkflow(shipping.ShipOrderWorkflow, mock.Anything, mock.Anything).Return("TRK-1", nil)

	start := s.env.Now()
	s.env.ExecuteWorkflow(orders.FulfillOrderWorkflow, mugOrder)

	s.NoError(s.env.GetWorkflowError())
	s.env.AssertActivityNumberOfCalls(s.T(), "ChargePayment", 3)
	// Retry delays run on the mock clock: 20s of Workflow time, no real waiting.
	s.Equal(20*time.Second, s.env.Now().Sub(start))
}

// Failing on every attempt: the Workflow fails after MaximumAttempts.
func (s *FulfillOrderSuite) Test_RetriesExhausted() {
	s.env.OnActivity(s.a.ReserveInventory, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, mock.Anything, mock.Anything).
		Return("", errors.New("gateway timeout"))

	s.env.ExecuteWorkflow(orders.FulfillOrderWorkflow, mugOrder)

	var actErr *temporal.ActivityError
	s.True(errors.As(s.env.GetWorkflowError(), &actErr))
	s.env.AssertActivityNumberOfCalls(s.T(), "ChargePayment", 3)
	s.env.AssertWorkflowNotCalled(s.T(), "ShipOrderWorkflow", mock.Anything, mock.Anything)
}

// A non-retryable error stops at the first attempt, whatever the Retry Policy says.
func (s *FulfillOrderSuite) Test_NonRetryableError() {
	s.env.OnActivity(s.a.ReserveInventory, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, mock.Anything, mock.Anything).
		Return("", temporal.NewNonRetryableApplicationError("card declined", "PaymentDeclined", nil)).Once()

	s.env.ExecuteWorkflow(orders.FulfillOrderWorkflow, mugOrder)

	var appErr *temporal.ApplicationError
	s.True(errors.As(s.env.GetWorkflowError(), &appErr))
	s.Equal("PaymentDeclined", appErr.Type())
	s.env.AssertActivityNumberOfCalls(s.T(), "ChargePayment", 1)
}
```

testify consumes mocks in order: the `.Twice()` mock answers the first two calls, then the `.Once()`
mock answers the third. A fourth call would match nothing and fail as in section 4.

What the test environment did with retries (Go SDK v1.50.0):

| Observation | Production (a real Service) | Test environment |
| --- | --- | --- |
| Delay between attempts with `InitialInterval: 10s`, `BackoffCoefficient: 2` | 10s, then 20s (30s in total before attempt 3) | 10s, then **10s** (attempt 3 at +20s): the coefficient wasn't applied |
| No `MaximumAttempts` in the Retry Policy | Unlimited attempts | **10 attempts**, a test-only default (`defaultMaximumAttemptsForUnitTest`) |

So assert **how many attempts** were made (`AssertActivityNumberOfCalls`), not exact retry timings,
and set `MaximumAttempts` explicitly when the number matters.

---

## 7. Observing calls and simulating latency

Two more mock methods:

- `.Run(func(args mock.Arguments))` runs a function on each call, before returning the mocked
  values. Use it to record calls.
- `.After(d)` delays the mocked result by `d` on the **mock clock**: a two-hour warehouse costs
  nothing in wall time. It works on `OnWorkflow` mocks too.

```go
// file: orders/timing_test.go
package orders_test

import (
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/acme/shop/orders"
	"github.com/acme/shop/shipping"
)

// Run observes each call; After delays the result on the mock clock.
func (s *FulfillOrderSuite) Test_RunAndAfter() {
	var charged []int64
	s.env.OnActivity(s.a.ReserveInventory, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { charged = append(charged, args.Get(2).(int64)) }).
		After(5*time.Second). // a slow payment gateway
		Return("pay_9001", nil)
	s.env.OnWorkflow(shipping.ShipOrderWorkflow, mock.Anything, mock.Anything).
		After(2*time.Hour). // the warehouse takes a while
		Return("TRK-1", nil)

	start := s.env.Now()
	s.env.ExecuteWorkflow(orders.FulfillOrderWorkflow, mugOrder)

	s.NoError(s.env.GetWorkflowError())
	s.Equal([]int64{2400}, charged)
	s.Equal(2*time.Hour+5*time.Second, s.env.Now().Sub(start))
}
```

In `mock.Arguments`, index 0 is the context: `args.Get(2)` is `amountCents`.

`.After(d)` doesn't trigger Activity timeouts. A `ChargePayment` mock with `.After(45 * time.Second)`
completed successfully even though the Activity's `StartToCloseTimeout` is 30s. To test a timeout,
return the timeout error from the mock instead (see the
[Error handling guide](../../error-handling/docs/08-timeouts-and-heartbeats.md)).

---

## 8. Child Workflows

Child Workflows have the same two options as Activities:

| You want | Do |
| --- | --- |
| Test the parent alone | `env.OnWorkflow(shipping.ShipOrderWorkflow, mock.Anything, mock.Anything).Return("TRK-1", nil)`. The first matcher is the child's `workflow.Context`. No registration needed |
| Test parent and child together | `env.RegisterWorkflow(shipping.ShipOrderWorkflow)`, and mock the child's Activities |

```go
// file: orders/child_test.go
package orders_test

import (
	"github.com/stretchr/testify/mock"

	"github.com/acme/shop/orders"
	"github.com/acme/shop/shipping"
)

// The real Child Workflow runs; only its Activity is mocked.
func (s *FulfillOrderSuite) Test_RealChildWorkflow() {
	s.env.RegisterWorkflow(shipping.ShipOrderWorkflow)
	s.env.OnActivity(s.a.ReserveInventory, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.a.ChargePayment, mock.Anything, mock.Anything, mock.Anything).Return("pay_9001", nil)
	s.env.OnActivity(shipping.CreateLabel, mock.Anything,
		shipping.ShipOrderInput{OrderID: "ord_1001", Address: "1 Harbour St, Leith"}).
		Return("1Z999AA10123456784", nil)

	s.env.ExecuteWorkflow(orders.FulfillOrderWorkflow, mugOrder)

	s.NoError(s.env.GetWorkflowError())
	var res orders.FulfillOrderResult
	s.NoError(s.env.GetWorkflowResult(&res))
	s.Equal("1Z999AA10123456784", res.TrackingNumber)
}
```

Do one or the other. A Child Workflow that is **neither registered nor mocked** doesn't fail the
Workflow: it crashes the whole test binary, and every other test in the package with it:

```text
panic: unable to find workflow type: ShipOrderWorkflow. Supported types: [FulfillOrderWorkflow]
FAIL	github.com/acme/shop/orders	0.423s
```

(For an Activity in the same situation, the Workflow fails cleanly with "no activity is registered",
as shown in [chapter 02](02-workflow-test-environment.md).)

---

## 9. Asserting on calls

| Method | Fails the test when… |
| --- | --- |
| `AssertExpectations(t)` | A mock (not marked `.Maybe()`) was never called, or was called fewer times than `.Times(n)` |
| `AssertActivityCalled(t, "ChargePayment", mock.Anything, "cus_42", int64(2400))` | No call with those arguments |
| `AssertActivityNotCalled(t, "CreateLabel", …)` | There was such a call |
| `AssertActivityNumberOfCalls(t, "ChargePayment", 3)` | The count differs. Retries count as calls |
| `AssertWorkflowCalled` / `AssertWorkflowNotCalled` / `AssertWorkflowNumberOfCalls` | The same, for Child Workflows |

The methods take the Activity's **name** (a string), not the function. For a method on a struct,
that's the method name: `"ChargePayment"`.

A mock for `cus_999` that the Workflow never uses makes `AssertExpectations` in `AfterTest` fail the
test:

```text
workflow_testsuite.go:1328: FAIL:	ChargePayment(string,string,string)
workflow_testsuite.go:1328: FAIL: 2 out of 3 expectation(s) were met.
```

---

## 10. Key takeaways

- `OnActivity(act, mock.Anything, args…)` mocks an Activity; `.Return(values)` fixes the answer,
  `.Return(func…)` replaces the implementation. The Workflow code doesn't change.
- Match argument **types** exactly (`int64(2400)`). Arguments that match no mock make the Activity
  panic, and the Workflow fails with a `*temporal.PanicError`.
- A wrong replacement-function signature is reported when the Activity is called, as a retryable
  Activity error.
- Retry Policies apply to mocks, on the mock clock. Assert attempt counts, not timings: the test
  environment didn't apply `BackoffCoefficient` and caps unset `MaximumAttempts` at 10.
- `.Twice()`, `.Once()`, `.Times(n)` script a sequence; `.Run` observes calls; `.After(d)` adds
  latency on the mock clock but doesn't trigger timeouts.
- Child Workflows: `OnWorkflow` to mock, `RegisterWorkflow` to run for real. Forgetting both crashes
  the test binary.
