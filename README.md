# TheirCore

*Nothing here is mine. It's all theirs.*

TheirCore is a small set of lifecycle and concurrency primitives for Swift 6: a single-owner `Their.Job`, a shared multi-subscriber `Their.Hub`, a persistent state owner `Their.Desk`, reducers that derive state from their events, `AsyncStream` adapters, a lock and a few ownership helpers. TheirCoreTesting is the test kit those primitives are verified with.

It was extracted from an app whose features are mostly written by coding agents. Agents do better with a handful of deeply tested primitives and written lifecycle rules than with a fresh mix of callback bags, actors and hand-rolled streams in every feature. So the features are theirs, and so is everything they are built on: every public name lives in the `Their` namespace.

## Requirements

- Swift 6.2 or newer.
- iOS 15, macOS 12, tvOS 15, watchOS 8 or visionOS 1.

## Installation

```swift
dependencies: [
    .package(url: "https://github.com/Stealth-O/TheirCore.git", exact: "0.4.0")
],
targets: [
    .target(name: "App", dependencies: ["TheirCore"]),
    .testTarget(name: "AppTests", dependencies: ["TheirCore", "TheirCoreTesting"])
]
```

## What's Inside

| Name | What it is |
| --- | --- |
| `Their.Job` | One finite lifecycle with one subscriber: values, then `.finished` or `.failure`. |
| `Their.Hub` | One shared lifecycle for many subscribers. The last subscriber to leave stops it. |
| `Their.Desk` | Typed events, one fixed state reducer, retained snapshots and named Job/Hub bindings. |
| `evolve`, `map`, `mapError`, `tryMap` | Derive values and state from a job or a hub through one reducer. |
| `Their.Job.merge`, `shareLatest()`, `job()` | A union of jobs, latest-value replay, and a hub-to-job bridge. |
| `stream()` | An `AsyncStream` adapter that subscribes before it returns. |
| `firstValue(cancellation:where:)` | Await one matching value, with ordinary cancellation or a bounded committed-result policy. |
| `Their.Job.once`, `Their.Job.never()` | One async operation as a job, and an inert job for defaults and previews. |
| `Their.Work`, `Their.serialized` | The producer side: report values, finish or fail, return a cancel. |
| `Their.Resource`, `Their.Lock` | Ownership of one external handle, and a mutex with the `Synchronization.Mutex` API. |
| `Their.WeakValueCache`, `Their.MainThreadOnce` | A weak read-or-create registry, and a run-once-on-main gate. |
| `Their.Misuse`, `Their.LifecycleLogging` | Misuse reports kept out of the failure channel, and DEBUG live-count diagnostics. |
| `Their.stress`, `Their.Test*` | A concurrency amplifier, signals, recorders and drivers for tests. |

## A Quick Tour

### One lifecycle, one subscriber

```swift
import TheirCore

enum LoadError: Error, Equatable {
    case offline
}

let greeting = Their.Job<String, LoadError> { report in
    report(.value("hello"))
    report(.finished)
    return {
        // Stop the source here.
    }
}

let cancel = greeting.subscribe { event in
    switch event {
    case .failure(let error):
        print("failed:", error)
    case .finished:
        print("finished")
    case .value(let value):
        print(value)
    }
}
```

The returned closure is a command and a pin at once. Calling it stops delivery and the source. Holding it keeps an rvalue pipeline such as `Their.Job(...).evolve(...).subscribe(...)` alive.

### Derive state

```swift
let totals = numbers.evolve(initial: 0) { total, value in
    total += value
    return total // Return nil to keep the new state without emitting.
}
```

The reducer runs once per value, in report order. A terminal `.finished` or `.failure` bypasses it and resets the state.

### Share one source

```swift
let prices = Their.Hub<Int, LoadError> { report in
    let feed = PriceFeed { price in
        report(.value(price))
    }
    return {
        feed.stop()
    }
}
.shareLatest()

let chart = prices.subscribe { event in /* ... */ }
let ticker = prices.subscribe { event in /* ... */ } // Same feed, starts from the latest price.
```

### Keep state on a desk

```swift
struct FeatureState: Sendable {
    var failure: LoadError?
    var total = 0
}

enum FeatureEvent: Sendable {
    case loadFailed(LoadError)
    case numberReceived(Int)
    case totalSet(Int)
}

let desk = Their.Desk<FeatureState, FeatureEvent>(FeatureState()) { state, event in
    switch event {
    case .totalSet(let total): state.total = total
    case .numberReceived(let value): state.total += value
    case .loadFailed(let failure): state.failure = failure
    }
}

desk.send(.totalSet(10))
desk.bind(numbers, id: "numbers") { event in
    switch event {
    case .value(let value): return .numberReceived(value)
    case .failure(let failure): return .loadFailed(failure)
    case .finished: return nil
    }
}

let cancel = desk.changes.subscribe { event in /* Consume snapshots. */ }
cancel()                         // State and bindings keep running.
let snapshot = desk.current      // Read-only value snapshot.
desk.unbind("numbers")            // Stops this binding; keeps its last state.
```

`Desk<State, Event>` fixes one reducer at construction. Its only state transition path is `Event → reducer → State`: `send` queues a typed event, and `bind` maps a Job or Hub input into the same event type. Bindings never receive mutable state or install another reducer. A mapping can return `nil` to ignore an input without reduction or publication. A source terminal still retires its binding, whether it maps to an event or to `nil`.

Binding another source under the same id cancels the old subscription and fences its queued events before reduction. An event already claimed by the reducer can finish. Retain Desk in the feature owner; it owns binding cancellations even when their returned handles are discarded. Use weak captures when a stored reducer, source mapping or observer refers back to that owner.

Desk reuses `Hub.evolve` for FIFO reduction and keeps an internal observer until Desk release. The existing evolution still resets at the end of its own lifecycle. Desk adds state ownership rather than another reducer engine. Keep the reducer and source mappings pure and short; database writes and other effects belong in Jobs or application services. State must have value semantics: `Sendable` alone does not stop a caller from mutating shared reference storage.

There is no implicit scheduler or actor confinement. An uncontended `send` drains inline; a concurrent or reentrant call can return while its event is queued. The current drainer processes events in queue admission order. Source mappings execute before admission and can run concurrently across bindings. `current` is updated before snapshot publication, but a concurrent send can make it newer than an observer's captured snapshot. Releasing Desk cancels bindings and finishes current observers; retaining `changes` does not keep Desk alive.

### Streams and one-shot work

```swift
let profile = Their.Job<Profile, LoadError>.once(failure: { _ in .offline }) {
    try await api.loadProfile()
}

for await event in profile.stream() {
    // .value(profile), then .finished; or .failure(.offline).
}
```

### Await one result

```swift
let profileValue = try await profile.firstValue()
let receipt = try await submittedCommand.firstValue(cancellation: .awaitResult)
```

The default cancels its subscription when the waiting task is cancelled.
`awaitResult` keeps a bounded operation pinned until a matching value or terminal
event so cancellation cannot discard an acknowledgement. An already-cancelled
task never starts either policy. A failure is thrown unchanged; finishing without
a match throws `Their.JobValueUnavailable`. The method does not clear the task's
cancellation flag or impose a timeout. Use `awaitResult` only when the source has
its own bounded outcome; ordinary reads use the default.

### Testing

```swift
import Testing
import TheirCore
import TheirCoreTesting

@Test func totalsAccumulate() async throws {
    try await Their.stress {
        let upstream = Their.TestJobDriver<Int, LoadError>()
        let totals = Their.TestEventRecorder<Their.JobEvent<Int, LoadError>>()
        let cancel = upstream.job
            .evolve(initial: 0) { total, value in
                total += value
                return total
            }
            .subscribe(totals.append(_:))

        upstream.emit(value: 1)
        upstream.emit(value: 2)
        upstream.emitFinished()

        try await totals.waitForEventCount(3)
        #expect(totals.events == [.value(1), .value(3), .finished])
        cancel()
    }
}
```

`Their.stress` runs the block 50 times concurrently behind a barrier, each iteration under its own deadlock watchdog. Recorders wait on continuations, never on sleeps.

## How It Behaves

- Each primitive is a small state machine behind one lock. Sinks, transforms, cancellation and cleanup run outside that lock; the weak cache's read-or-create factory deliberately runs inside its cache lock.
- Reports enter a FIFO queue that one caller drains at a time, so a terminal event never overtakes values reported before it.
- Nothing hops threads. Callbacks run on the thread that reported the event, so hop to the main actor yourself before touching UI.
- Subscribing twice to a `Their.Job`, or another incorrect use, is reported as `Their.Misuse`. The default handler stops the process.

## 0.4.0

Adds `Their.Job.firstValue(cancellation:where:)`, `Their.JobAwaitCancellation`
and `Their.JobValueUnavailable`. The adapter uses the existing Job subscription,
Lock and Resource owners, preserving single-subscriber enforcement and exact
cleanup even for synchronous results or a late-returned cancel handle. Existing
Job, Hub, Desk and stream call sites remain unchanged.

Validated with Swift 6.3.3: **13 targeted tests in 2 suites**, the complete
**474 tests in 38 suites**, and a Release build. All scenarios use `Their.stress`.
Coverage includes cancellation before registration, synchronous late handles,
cancellation/value races, retained acknowledgements and failures, predicate
filtering, empty finish and reentrant teardown. Public-consumer compilation
verifies the names without `@testable import`. The installation example targets
0.4.0; publishing this candidate remains the release step after review.

## 0.3.0

Makes Desk's state transitions explicit: `Their.Desk<State, Event>` requires one reducer at construction, `send(Event)` replaces `update`, and Job/Hub bindings only map source inputs to `Event?`. The same FIFO and fixed reducer process every accepted event. Initial replay and ignored inputs do not invoke the reducer. Cancellation, named replacement and Desk lifetime keep their existing guarantees.

This is a breaking Desk API change. Move former `update` closures and per-binding state mutations into cases of one feature event and its constructor reducer. Replace each `update` call with `send`, and each binding reducer with a source-to-event mapping. There is no mutable-state compatibility entry point. Existing consumers pinned to 0.2.0 can migrate separately; Job, Hub, `evolve` and TheirCoreTesting retain their contracts.

Validated with Swift 6.3.3: **461 tests in 37 suites** and a Release build. The regression coverage includes mixed direct/Job/Hub FIFO delivery, ignored terminals, replacement during mapping, reentrant mapping and release of mapping captures while typed events remain queued, alongside the existing Desk lifecycle scenarios. Public-consumer compilation verifies the new API; compiler checks also reject `update`, mutable binding reducers, assignment to `current` or `changes`, and events of the wrong type.

## 0.2.0

Adds `Their.Desk<State>` with `current`, `changes`, pure `update`, Job/Hub `bind(id:)` and `unbind`. Its state and bindings survive periods with no UI subscribers. Named replacement and cancellation fence reducers at application time; an already claimed reducer can finish. Bindings use `Their.Resource` to release a cancel returned after cancellation or synchronous terminal delivery exactly once.

Validated with Swift 6.3.3: **455 tests in 37 suites** and a Release build. The 20 added scenarios include public consumer compilation, synchronous terminals, queued stale events, concurrent updates, reentrant cancellation and snapshot destruction, independent ids, source sharing and Desk release while external handles remain retained. All run under `Their.stress`.

## 0.1.1

This release brings the current Core and CoreTesting lifecycle implementation into the package while preserving the `Their` API:

- A job's sink slot closes permanently before teardown, so reentrant subscription cannot install a rejected sink.
- Cancellation during an evolution's failure mapping or a merge's upstream teardown suppresses a terminal callback that has not yet been claimed.
- A merge stops starting sources as soon as a failure is queued, while still delivering earlier queued values first. A cancel returned late is invoked when the source subscription returns.
- Weak-cache entries, replaced logging outputs, diagnostic hooks and test-recorder reports release their retired captures after unlocking.
- Internal hub state waits complete once on either cancellation or the requested transition.

Validated with Swift 6.3.3: 435 tests in 36 suites, a release build, and an unchanged public symbol graph for both libraries (204 symbols compared with 0.1.0).

## Running the Tests

```sh
swift test --no-parallel
```

Every test already runs its scenario many times concurrently through `Their.stress`, with a short watchdog against deadlocks. The default in-process parallelism of Swift Testing stacks the whole suite on top of that, oversubscribes the cooperative thread pool and turns the watchdog into false timeouts.

## Documentation

- [TheirCore](Documentation/TheirCore.md): state machines, ordering, cancellation, threading and the lifetime contract.
- [TheirCoreTesting](Documentation/TheirCoreTesting.md): the stress runner, its barrier and timeout, recorders and signals.

## License

MIT. See [LICENSE](LICENSE).
