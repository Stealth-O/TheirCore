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
    .package(url: "https://github.com/Stealth-O/TheirCore.git", from: "0.2.0")
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
| `Their.Desk` | Retained state, latest snapshots and named Job/Hub bindings, independent of UI subscribers. |
| `evolve`, `map`, `mapError`, `tryMap` | Derive values and state from a job or a hub through one reducer. |
| `Their.Job.merge`, `shareLatest()`, `job()` | A union of jobs, latest-value replay, and a hub-to-job bridge. |
| `stream()` | An `AsyncStream` adapter that subscribes before it returns. |
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
struct ScreenState: Sendable {
    var total = 0
    var failure: LoadError?
}

let desk = Their.Desk(ScreenState())
desk.update { $0.total = 10 }
desk.bind(numbers, id: "numbers") { state, event in
    switch event {
    case .value(let value): state.total += value
    case .failure(let failure): state.failure = failure
    case .finished: break
    }
}

let cancelUI = desk.changes.subscribe { event in /* Render on your UI actor. */ }
cancelUI()                       // State and bindings keep running.
let snapshot = desk.current
desk.unbind("numbers")            // Stops this binding; keeps its last state.
```

`numbers` can be a Job or a Hub. Binding another source under the same id cancels the old subscription and fences its queued reducers. A source's terminal event reaches the reducer and retires that binding, while the Desk remains usable. Retain Desk in the feature owner; it owns binding cancellations even when their returned handles are discarded. Use weak captures when a stored reducer or observer refers back to that owner.

Desk reuses `Hub.evolve` for FIFO reduction and keeps an internal observer until Desk release. The existing evolution still resets at the end of its own lifecycle. Desk adds state ownership rather than another reducer engine. Reducers stay pure; database writes and other effects belong in Jobs or application services.

There is no implicit scheduler. Serial, uncontended updates drain inline; a concurrent or reentrant call can return while its update is queued. When bridging to an actor, reading `desk.current` after the hop avoids rendering a captured snapshot that has already been superseded. Releasing Desk cancels bindings and finishes current observers; retaining `changes` does not keep Desk alive.

### Streams and one-shot work

```swift
let profile = Their.Job<Profile, LoadError>.once(failure: { _ in .offline }) {
    try await api.loadProfile()
}

for await event in profile.stream() {
    // .value(profile), then .finished; or .failure(.offline).
}
```

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
