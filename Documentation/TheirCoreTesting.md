# TheirCoreTesting

TheirCoreTesting is the test infrastructure for concurrency-sensitive code. It is small, but its behavior matters: the TheirCore tests rely on it to turn one scenario into many concurrent executions and to observe asynchronous events without sleeps or polling.

## Purpose

Ordinary unit tests often miss race conditions because they run a scenario once. `Their.stress` is designed to make tests less polite. It does three things:

1. Creates many child tasks for the same scenario.
2. Holds them at a barrier so they start the scenario at roughly the same time.
3. Applies a timeout to each child task's scenario block, so a stuck scenario fails loudly instead of hanging.

It does not replace precise state-machine tests. It amplifies them.

## `Their.stress`

```swift
try await Their.stress {
    // repeated concurrently
}

try await Their.stress(count: 10, priority: .high, timeout: .milliseconds(250)) { index in
    // repeated concurrently, with the iteration index
}
```

Defaults:

| Parameter | Default |
| --- | --- |
| `count` | `Their.stressCountDefault`, which is 50 |
| `priority` | `.medium` |
| `timeout` | `Their.stressTimeoutDefault`, which is 299 milliseconds |

The default priority is stable, so timeout diagnostics are not affected by random priority scheduling. The `priority` parameter is still an autoclosure, evaluated separately for every child task:

```swift
try await Their.stress(priority: [.high, .low, .background].randomElement()!) { ... }
```

gives each child its own random priority, while `priority: .high` gives every child `.high`.

Results are collected in completion order, not iteration order. Most tests ignore the returned array. A test that depends on the order must sort the results or include the index in them.

## Barrier Semantics

`Their.stress` creates an internal barrier, `TestQueue(count: count)`. Each child task does this:

```swift
await queue.enter()
return try await withStressTimeout(dependencies: dependencies, iteration: iteration, timeout: timeout) {
    try await block(iteration)
}
```

The parent does this:

```swift
await queue.full()
queue.open()
```

That means:

- Every child task is created and reaches `queue.enter()`.
- `enter()` suspends the child by storing its continuation.
- The parent waits in `full()` until every child has entered.
- The parent calls `open()`, which resumes every stored continuation.
- Only then does each child start its actual block.

This is neither a sleep nor polling. It is deterministic asynchronous synchronization through continuations.

The barrier makes races more likely when the block touches shared state. If the block creates an independent object in each iteration, `Their.stress` still checks repeatability and lifecycle flakiness, but it is not a shared-instance stress test. For a shared-instance stress test, create the object outside `Their.stress` and access it from every child inside the block.

## Timeout Semantics

The timeout is per child task. It starts only when that child reaches the operation-start transition after the barrier opens. It does not count task creation, initial executor delay before that transition, waiting inside `queue.enter()`, or the parent waiting in `queue.full()`. After the start gate opens, all elapsed time in the block counts, including its own suspension and later rescheduling.

The timeout child waits on an explicit operation-start gate, and the operation child opens that gate immediately before invoking the block. Executor delay before the operation child actually starts therefore cannot consume the block's budget.

If the block finishes before the timeout:

- the timeout task is cancelled;
- its sleep throws because of cancellation and it exits quietly;
- the structured race joins both children before returning the value.

If the block does not finish in time:

- the timeout task wakes up;
- the race selects `Their.StressTimeoutError` and cancels the block;
- it waits for the cancelled block to finish its `catch`, `defer` and other cooperative cleanup;
- only after that join does `Their.stress` throw `Their.StressTimeoutError`.

The thrown error fails the current Swift Testing test normally, and the rest of the run continues. A timeout still means the scenario most likely deadlocked or stopped making progress.

Swift cancellation is cooperative. A block that permanently ignores cancellation cannot be killed safely, so `Their.stress` stays in the join instead of returning while that block keeps mutating state behind the next test. Such a test relies on the outer test runner's watchdog.

If the task running `Their.stress` is cancelled while a block is active, the block and the timeout task are both cancelled, the race joins the block's cooperative cleanup, and `Their.stress` then throws `CancellationError` instead of waiting for the timeout.

The barrier itself is not timed. If a child never reaches `queue.enter()`, the run relies on the outer test runner rather than counting scheduler or barrier-fill time against an individual block.

## `Their.TestDuration`

`Their.TestDuration` lets call sites spell readable timeouts without Swift's `Duration`, which needs iOS 16:

```swift
try await Their.stress(timeout: .milliseconds(250)) { ... }
try await Their.stress(timeout: .seconds(1)) { ... }
```

It stores nanoseconds, and the live timeout uses `Task.sleep(nanoseconds:)`. The timeout meta-tests inject a manual sleeper driven by continuations, so they never consume real time to prove start or join ordering.

## Barrier Internals

`TestQueue` is internal. Its state is the stored child continuations, whether `full()` was called, the parent's continuation and whether `open()` was called. `enter()` may be called by each child before `open()`, `full()` once by the parent, and `open()` once after `full()`. Continuations are always resumed outside the lock, so resumed asynchronous work never runs while protected state is still locked.

## Recorders and Signals

Use these helpers when a test needs to observe callbacks, event sinks, lifecycle calls or deterministic gates, instead of writing a new recorder class for each test.

### `Their.TestEventRecorder`

`Their.TestEventRecorder<Event>` stores events in arrival order and lets tests wait for a count or for a matching event.

```swift
let recorder = Their.TestEventRecorder<Their.JobEvent<Int, TestError>>()

let cancel = job.subscribe(recorder.append(_:))
work.emit(.value(1))

try await recorder.waitForEventCount(1)
#expect(recorder.events == [.value(1)])
cancel()
```

Use `waitForEventCount(_:)` when a callback must have happened before the assertions continue. Use `waitForEvent(where:)` when a test needs a specific event and earlier events may arrive first. Both are `async throws`: cancelling the waiting task removes its waiter and throws `CancellationError`.

`waitForEvent(where:)` never calls the predicate while holding the recorder's non-reentrant lock. It snapshots unseen events under the lock from a local cursor, evaluates each of them once in arrival order outside the lock, then waits, cancellation-aware, for the next event. The predicate may therefore safely inspect the same recorder.

### `Their.TestCountRecorder`

`Their.TestCountRecorder` stores an integer count and resumes waiters when the count reaches the requested value. It suits dependency calls without a payload. Cancelling `waitForCount(_:)` removes the registered waiter and throws `CancellationError`.

```swift
let starts = Their.TestCountRecorder()
_ = starts.increment()
try await starts.waitForCount(1)
```

### `Their.TestCancelRecorder`

`Their.TestCancelRecorder` records cancellation calls and can produce a `Their.WorkCancel`.

```swift
let cancelRecorder = Their.TestCancelRecorder()
let cancel = cancelRecorder.cancel()

cancel()
try await cancelRecorder.waitForCancelCallsCount(1)
```

### `Their.TestWorkRecorder`

`Their.TestWorkRecorder<Value, Failure>` records a `Their.Work` start, stores its `Their.WorkReport`, lets tests emit `.value`, `.finished` or `.failure`, and records cancellation. Use it for job and hub tests that need a deterministic upstream.

```swift
let work = Their.TestWorkRecorder<Int, TestError>()
let job = Their.Job(work: work.work)
let cancel = job.subscribe { _ in }

try await work.waitForStartCallsCount(1)
work.emit(.value(1))
cancel()
try await work.waitForCancelCallsCount(1)
```

### `Their.TestJobDriver` and `Their.TestHubDriver`

Use a driver when a test wants a ready-made upstream job or hub rather than a raw work closure.

```swift
let upstream = Their.TestJobDriver<Int, TestError>()
let recorder = Their.TestEventRecorder<Their.JobEvent<Int, TestError>>()

let cancel = upstream.job.subscribe(recorder.append(_:))
upstream.emit(value: 1)

try await recorder.waitForEventCount(1)
#expect(upstream.startCallsCount == 1)
cancel()
try await upstream.waitForCancelCallsCount(1)
```

`Their.TestHubDriver<Value, Failure>` has the same shape and exposes `hub` instead of `job`. Both drivers are thin wrappers around `Their.TestWorkRecorder`, so their lifecycle semantics are the real job and hub semantics. Besides `emit(value:)`, `emit(failure:)` and `emit(_:)`, both drivers expose `emitFinished()` for the terminal successful outcome.

### `Their.TestMisuseRecorder`

`Their.TestMisuseRecorder` stores `Their.Misuse` values and exposes `handler(_:)`, so it can be passed directly as a `misuseHandler`.

```swift
let misuse = Their.TestMisuseRecorder()
let job = Their.Job(misuseHandler: misuse.handler, work: work.work)

_ = job.subscribe { _ in }
_ = job.subscribe { _ in }
try await misuse.waitForCount(1)
#expect(misuse.misuses.first?.message == "Job supports only one subscriber per lifecycle.")
```

### `Their.TestSignal`

`Their.TestSignal` is a one-shot gate with a synchronous `signal()` and an asynchronous `wait()`. It is a `final class` backed by `Their.Lock`, not an actor, so `signal()` can fire from any context, including inside a synchronous `withLock` body, a `Their.WorkCancel` closure or a `DispatchQueue` block, without an `await` or a `Task` bridge.

```swift
let signal = Their.TestSignal()

signal.signal()

try await signal.wait()
```

Use it instead of `Task.sleep` or `Task.yield` when a test needs explicit synchronization with a callback or a termination branch. `signal()` resumes every waiter that is waiting when it is called. After that, later `wait()` calls return immediately and repeated `signal()` calls are no-ops. Cancelling `wait()` removes its registered waiter and throws `CancellationError`. Waiter continuations are resumed outside the lock.

All waiters share the same discipline. Registration, satisfaction and cancellation are decided under one owner lock. An early cancellation is remembered by a per-wait token, so registering afterwards fails immediately instead of leaving an unreachable continuation behind. Ready, pending and cancelled continuations are selected under the lock and resumed after unlock, exactly once.

## How to Use This in Tests

The tests of these helpers live in `Tests/TheirCoreTestingTests`. The TheirCore tests in `Tests/TheirCoreTests` use them as shared infrastructure.

Use `Their.stress` whenever a scenario should repeat under concurrent pressure. Starting every test scenario with `try await Their.stress { ... }`, even when the objects are created inside the block, keeps ordinary behavior checks repeatable and gives lifecycle and concurrency bugs more chances to surface.

```swift
@Test func stopCancelsOnlyOnce() async throws {
    try await Their.stress {
        let cancels = Their.TestCancelRecorder()
        let job = Their.Job<Int, TestError> { _ in cancels.cancel() }
        let cancel = job.subscribe { _ in }

        cancel()
        cancel()

        #expect(cancels.cancelCallsCount == 1)
    }
}
```

For a shared-instance stress test:

```swift
@Test func sharedStateHandlesConcurrentAccess() async throws {
    let counter = Their.TestCountRecorder()

    try await Their.stress { _ in
        _ = counter.increment()
    }

    #expect(counter.count == Their.stressCountDefault)
}
```

`Their.stress` is not a substitute for a test matrix. Decide first what behavior must be true, then use `Their.stress` to run that behavior under pressure.

Keep exceptions rare and explicit. A test may stay outside `Their.stress` when it must be serialized around process-wide or static state, and the reason belongs next to the test.

Run test suites built on `Their.stress` serially, for example with `swift test --no-parallel`. Each test already fans out into concurrent iterations, and running every suite in parallel on top of that oversubscribes the cooperative thread pool until the per-iteration watchdog reports false timeouts. For the same reason, a scenario that holds a thread synchronously, for example with a `DispatchSemaphore` inside a sink, should run that held call on a Dispatch worker thread instead of inside a `Task`.
