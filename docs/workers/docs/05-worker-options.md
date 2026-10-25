# 05 · Worker options: capacity, pollers and rate limits

> **Level:** daily use · **Read time:** ~15 min
> **Sources:** [Go SDK — Run a Worker](https://docs.temporal.io/develop/go/workers/run-worker-process#configure-worker-options) · [Task Routing (flow control, throttling)](https://docs.temporal.io/task-routing) · [`worker.Options`](https://pkg.go.dev/go.temporal.io/sdk/worker#Options)

---

## 1. The problem

`worker.New(c, "shipping", worker.Options{})` works on your laptop. In production the same empty
struct raises questions:

- How many Activities can one Worker run at the same time? What happens to the 1,001st?
- The carrier API you call allows **20 requests per second for the whole company**. Ten Worker
  replicas each run hundreds of Activities. Who stops them from getting you banned?
- The label printer Workers should run Activities only, never Workflow code.

The Go "Run a Worker" page answers with one sentence: *"An empty struct uses defaults that work for
most cases"*, and links to `pkg.go.dev/go.temporal.io/sdk/internal#WorkerOptions` (the `internal`
package, where the fields are actually documented). This chapter is the missing map of those fields.

---

## 2. Two separate knobs: slots and pollers

A Worker Entity has two kinds of goroutines for each Task type:

| Knob | What it is | Go option (Activity / Workflow / Nexus) |
| --- | --- | --- |
| **Pollers** | Goroutines that hold a long-poll RPC open on the Task Queue, waiting for Tasks | `MaxConcurrentActivityTaskPollers`, `MaxConcurrentWorkflowTaskPollers`, `MaxConcurrentNexusTaskPollers` |
| **Slots** | How many Tasks may **execute** at once. A poller only polls when a slot is free | `MaxConcurrentActivityExecutionSize`, `MaxConcurrentWorkflowTaskExecutionSize`, `MaxConcurrentNexusTaskExecutionSize`, `MaxConcurrentLocalActivityExecutionSize` |

This is the **flow control** the Task Routing page describes: *"A Worker that consumes from a Task
Queue asks for an Activity Task only when it has available capacity, so it is never overloaded by
request spikes."* When all slots are busy, the Worker simply stops asking. New Tasks wait in the Task
Queue backlog ([chapter 03](03-task-queues.md)), not in your process.

Rules of thumb:

- **Slots** protect your process (memory, CPU, connections to downstream systems).
- **Pollers** decide how fast free slots get filled. Two pollers are plenty for a quiet queue. A busy
  queue with many slots may need more.
- `MaxConcurrentWorkflowTaskExecutionSize` and `MaxConcurrentWorkflowTaskPollers` **can't be 1**:
  `worker.New` panics with `cannot set MaxConcurrentWorkflowTaskExecutionSize to 1` (see
  [chapter 06](06-sticky-execution.md) for why).

### Poller autoscaling (experimental)

Instead of a fixed number of pollers, you can let the SDK scale them using feedback from the Server:

```go
// file: internal/workeropts/autoscaling.go
package workeropts

import "go.temporal.io/sdk/worker"

// OrdersWorkerOptions lets the SDK scale Workflow and Activity pollers between 1 and 50.
func OrdersWorkerOptions() worker.Options {
	return worker.Options{
		WorkflowTaskPollerBehavior: worker.NewPollerBehaviorAutoscaling(worker.PollerBehaviorAutoscalingOptions{
			MinimumNumberOfPollers: 1,  // default 1
			MaximumNumberOfPollers: 50, // default 100
			InitialNumberOfPollers: 5,  // default 5
		}),
		ActivityTaskPollerBehavior: worker.NewPollerBehaviorAutoscaling(worker.PollerBehaviorAutoscalingOptions{
			MaximumNumberOfPollers: 50,
		}),
	}
}
```

`WorkflowTaskPollerBehavior`, `ActivityTaskPollerBehavior` and `NexusTaskPollerBehavior` are
**mutually exclusive** with the matching `MaxConcurrent…TaskPollers` field: setting both panics. The
SDK source also notes that if you set neither, a Namespace configured to auto-enroll Workers switches
them to autoscaling automatically.

---

## 3. Rate limits: per Worker vs per Task Queue

Slots limit **concurrency**. Rate limits cap **Activities per second**. The Task Routing page
describes both under *Throttling*:

| Option | Enforced by | Scope | Use it to… |
| --- | --- | --- | --- |
| `WorkerActivitiesPerSecond` | the SDK, in this Worker | one Worker Entity | protect this host |
| `WorkerLocalActivitiesPerSecond` | the SDK, in this Worker | Local Activities of one Worker | same, for Local Activities |
| `TaskQueueActivitiesPerSecond` | the **Temporal Service** | **all Workers** on the Task Queue | protect a downstream service shared by the whole fleet |

The important difference: with 10 replicas, `WorkerActivitiesPerSecond: 2` allows **20/s in total**,
and grows with every replica you add. `TaskQueueActivitiesPerSecond: 20` stays at 20/s however many
replicas you run, because the Service dispatches the Tasks.

Two details from the SDK source (`internal/worker.go`, v1.50.0):

- Setting `TaskQueueActivitiesPerSecond` **disables eager Activities** on this Worker, because the
  Service doesn't rate-limit eagerly dispatched Activities.
- `WorkerActivitiesPerSecond` is applied after the Worker receives the Task. Set very low, Tasks
  can wait behind the limiter long enough to hit their server-side timeouts.

---

## 4. Example: an Activity-only Worker for a rate-limited carrier API

The shipping team runs a dedicated Worker fleet that only books shipments with an external carrier.
The carrier allows 20 requests/s per account. Workflows live elsewhere (the `orders` Worker), so this
Worker never runs Workflow code.

```go
// file: shipping/carrier.go
package shipping

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// TaskQueue is the Task Queue served by the carrier Workers. The orders Workflow
// sets it in its ActivityOptions.
const TaskQueue = "shipping-carrier"

type BookShipmentInput struct {
	OrderID     string
	WarehouseID string
	Parcels     []Parcel
}

type Parcel struct {
	WeightGrams int
	LengthCm    int
	WidthCm     int
	HeightCm    int
}

type Shipment struct {
	TrackingNumber string
	LabelURL       string
}

// CarrierAPI is the external carrier (a fake in tests).
type CarrierAPI interface {
	CreateShipment(ctx context.Context, idempotencyKey string, in BookShipmentInput) (Shipment, error)
}

// ErrAddressRejected is returned by the carrier for an invalid address.
var ErrAddressRejected = errors.New("carrier rejected the delivery address")

type CarrierActivities struct {
	Carrier CarrierAPI
}

func (a *CarrierActivities) BookShipment(ctx context.Context, in BookShipmentInput) (Shipment, error) {
	info := activity.GetInfo(ctx)
	// Same key for every attempt of this Activity: the carrier deduplicates retries.
	key := fmt.Sprintf("%s-%s", info.WorkflowExecution.ID, info.ActivityID)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	s, err := a.Carrier.CreateShipment(ctx, key, in)
	if errors.Is(err, ErrAddressRejected) {
		return Shipment{}, temporal.NewNonRetryableApplicationError("address rejected", "AddressRejected", err)
	}
	return s, err
}
```

```go
// file: cmd/carrier-worker/main.go
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/acme/shop/shipping"
)

// fakeCarrier stands in for the real carrier SDK.
type fakeCarrier struct{}

func (fakeCarrier) CreateShipment(_ context.Context, key string, in shipping.BookShipmentInput) (shipping.Shipment, error) {
	return shipping.Shipment{TrackingNumber: "1Z-" + in.OrderID, LabelURL: "https://labels.example/" + key}, nil
}

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	w := worker.New(c, shipping.TaskQueue, worker.Options{
		// This fleet only runs Activities: no Workflow Task pollers, no Workflow cache.
		DisableWorkflowWorker: true,

		// Each replica runs at most 50 carrier calls at once (HTTP connections, memory).
		MaxConcurrentActivityExecutionSize: 50,
		MaxConcurrentActivityTaskPollers:   4,

		// The carrier's limit is per account, so enforce it for the whole Task Queue,
		// on the Temporal Service, whatever the number of replicas.
		TaskQueueActivitiesPerSecond: 20,
	})

	w.RegisterActivity(&shipping.CarrierActivities{Carrier: fakeCarrier{}})

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("carrier worker stopped", err)
	}
}
```

With `DisableWorkflowWorker: true`, calling `w.RegisterWorkflow` panics with `workflow worker
disabled, cannot register workflow` (checked). The opposite switch exists too:
`LocalActivityWorkerOnly: true` runs Workflow Tasks and Local Activities, but never polls for regular
Activity Tasks.

> **One Task Queue, one rate.** `TaskQueueActivitiesPerSecond` is sent by every Worker that polls the
> queue. Give all of them the same value, from shared config.

---

## 5. Other options you'll meet

| Option | Default (v1.50.0) | What it does |
| --- | --- | --- |
| `DeadlockDetectionTimeout` | 1s | Max time Workflow code may run in one Workflow Task without yielding. Longer → the Task fails with a deadlock error. Usually a sign of blocking I/O or a busy loop in Workflow code |
| `DefaultHeartbeatThrottleInterval` | 30s | How often Activity heartbeats are actually sent when the Activity has no `HeartbeatTimeout` (otherwise a bit less than that timeout) |
| `MaxHeartbeatThrottleInterval` | 60s | Upper bound for that interval |
| `DisableEagerActivities` | `false` | Eager Activities: when a Workflow Task schedules Activities on the **same** Task Queue, the Service can hand them straight back to this Worker, skipping a round trip |
| `MaxEagerActivityReservationsPerWorkflowTask` | 3 | Activity slots reserved for eager execution per Workflow Task |
| `MaxConcurrentEagerActivityExecutionSize` | 0 (bound only by Activity slots) | Cap on eager Activities running at once |
| `Identity` | the Client's identity | Shown in Event History and pollers lists ([chapter 04](04-running-a-worker.md)) |
| `EnableSessionWorker` | `false` | Worker Sessions ([chapter 08](08-sessions.md)) |
| `WorkerStopTimeout` | 0 | Graceful shutdown period ([chapter 10](10-graceful-shutdown.md)) |
| `DeploymentOptions` | off | Worker Versioning ([chapter 11](11-worker-versioning.md)) |
| `Interceptors` | none | [Chapter 09](09-interceptors.md) |

### Tuners (experimental)

`worker.Options.Tuner` replaces the four `MaxConcurrent…ExecutionSize` fields with a slot supplier:

- `worker.NewFixedSizeTuner(worker.FixedSizeTunerOptions{NumWorkflowSlots, NumActivitySlots, NumLocalActivitySlots, NumNexusSlots})`: same as the fields, in one object.
- `worker.NewResourceBasedTuner(...)`: hands out slots while CPU and memory stay under targets.

Setting a `Tuner` **and** any `MaxConcurrent…ExecutionSize` panics. Both are marked *Experimental*
in the SDK.

---

## 6. Defaults at a glance

Read from `internal/internal_worker.go` and `internal/worker.go`, SDK v1.50.0:

| Option | Default |
| --- | --- |
| `MaxConcurrentActivityExecutionSize` | 1,000 |
| `MaxConcurrentLocalActivityExecutionSize` | 1,000 |
| `MaxConcurrentWorkflowTaskExecutionSize` | 1,000 |
| `MaxConcurrentNexusTaskExecutionSize` | 1,000 |
| `MaxConcurrentActivityTaskPollers` / `…WorkflowTaskPollers` / `…NexusTaskPollers` | 2 each |
| `WorkerActivitiesPerSecond`, `WorkerLocalActivitiesPerSecond`, `TaskQueueActivitiesPerSecond` | 100,000 (effectively unlimited) |
| `StickyScheduleToStartTimeout` | 5s |
| Sticky Workflow cache (process-wide) | 10,000 Workflows |
| `MaxConcurrentSessionExecutionSize` | 1,000 |
| `DeadlockDetectionTimeout` | 1s |
| `WorkerStopTimeout` | 0 (no grace period) |

The Serverless `lambdaworker` package uses much smaller defaults ([chapter 12](12-serverless-workers.md)).
For tuning against real load, see the docs' [Worker performance](https://docs.temporal.io/develop/worker-performance)
guide: start with defaults, watch the slot and schedule-to-start metrics, then adjust.

---

## 7. Key takeaways

- **Slots** cap concurrent executions, **pollers** fetch Tasks. A Worker only polls when it has a free slot, so it can't be overloaded.
- `WorkerActivitiesPerSecond` is per Worker and multiplies with replicas. `TaskQueueActivitiesPerSecond` is enforced by the Service for **the whole queue**, and disables eager Activities.
- `DisableWorkflowWorker: true` makes an Activity-only Worker. `LocalActivityWorkerOnly: true` does the opposite.
- Poller autoscaling and Tuners are experimental and mutually exclusive with the fixed-size fields.
- Workflow code that blocks more than **1s** per Workflow Task trips the deadlock detector.
