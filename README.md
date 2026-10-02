# TheirCore

*Nothing here is mine. It's all theirs.*

TheirCore is a small set of lifecycle and concurrency primitives for Swift 6: a single-owner `Their.Job`, a shared multi-subscriber `Their.Hub`, reducers that derive state from their events, `AsyncStream` adapters, a lock and a few ownership helpers. TheirCoreTesting is the test kit those primitives are verified with.

It was extracted from an app whose features are mostly written by coding agents. Agents do better with a handful of deeply tested primitives and written lifecycle rules than with a fresh mix of callback bags, actors and hand-rolled streams in every feature. So the features are theirs, and so is everything they are built on: every public name lives in the `Their` namespace.

## Requirements

- Swift 6.2 or newer.
- iOS 15, macOS 12, tvOS 15, watchOS 8 or visionOS 1.

## Installation

```swift
dependencies: [
    .package(url: "https://github.com/Stealth-O/TheirCore.git", from: "0.1.1")
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
