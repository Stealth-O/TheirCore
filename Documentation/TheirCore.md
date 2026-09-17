# TheirCore

TheirCore holds the lifecycle and concurrency primitives: `Their.Job`, `Their.Hub`, the evolution operators, the stream adapters, `Their.Lock` and a few ownership helpers. It is small on purpose, and it is meant to show the engineering depth expected from any stateful service built on top of it.

Passing tests is not the goal by itself. The goal is to make every important behavior explicit: state, lifecycle, resource ownership, observable events, ignored branches and terminal transitions.

## Why It Exists

TheirCore was extracted from an app whose features are mostly written by coding agents. It exists to make that code repeatable, not just reusable. A small set of owned primitives is preferred over external reactive frameworks and over one-off lifecycle code in each feature. The tradeoff is deliberate: more explicit code is acceptable when it brings owned cancellation semantics, deterministic tests and documented lifecycle behavior.

New features start by expressing lifecycle, event delivery, shared subscriptions, state evolution and synchronization through these primitives. If that expression is awkward once, local glue is fine. If the same awkward shape repeats, the next step is a new operator or primitive, tested deeply and documented here, and only then reused.

The success criterion is not the fewest abstractions. It is that new features need no new lifecycle rules. That is the joke in the name too: nothing here is mine, it's all theirs.

## Design Intent

Stream-first code is hard to test deeply. `AsyncStream` is a good integration surface, but when the actual behavior lives only inside a stream closure it becomes difficult to inspect state, force lifecycle branches, prove resource ownership and test late events.

The preferred pattern is:

1. Put the real lifecycle and state machine in a small `JobEngine`, `HubEngine` or service.
2. Drive it with sinks, reports or explicit dependency closures.
3. Test the state machine directly across its complete state matrix.
4. Add public adapters such as sinks or `AsyncStream` on top.
5. Test the adapter contract separately.

That is why the one-owner lifecycle stack is layered as:

- `JobEngine`: the internal lifecycle engine behind `Their.Job`. Its states, ordering and cancellation model live in `Sources/TheirCore/Job/Engine/JobEngine.swift`.
- `Their.Work`: the upstream closure that starts source work and returns a cancel closure.
- `Their.WorkCancel`: the closure that stops source work and releases what it owns.
- `Their.WorkOutput`: the three-outcome report alphabet. A `.value`, a terminal successful `.finished`, or a terminal `.failure`. `.finished` is how a producer says that no more values will ever be reported.
- `Their.WorkReport`: the closure through which work reports `Their.WorkOutput` values.
- `Their.Resource`: a small owner for one SDK or resource handle that must be released exactly once.
- `Their.Job`: the public sink-based facade.
- `stream()`: an `AsyncStream` adapter over that facade.

The stream is not the foundation. The state machine is.

## Quality Bar

Any service with meaningful state needs a written or obvious state matrix before it is complete. At minimum, check these dimensions:

- Initial states.
- Active and running states.
- Terminal states.
- All public methods in every state.
- All dependency callbacks in every state.
- Cancellation and stop behavior.
- Failure behavior.
- Deinit behavior when idle, active and terminated.
- Late upstream callbacks after stop, failure or termination.
- Duplicate calls such as repeated start, stop, cancel or subscribe.
- Observable effects: callback events, stream `yield`, stream `finish`, dependency calls, state changes and ignored branches.

Tests should prove the model, not just call methods.

## Job Model

`Their.Job` represents one resource lifecycle: a finite event-producing lifecycle with one start, one active resource and one terminal end. It does not mean a generic Swift task or a background queue item.

A lifecycle ends in one of three ways: a terminal successful `.finished`, a terminal `.failure`, or silently through an owner-side command such as `cancel`, `stop` or `deinit`. Owner-initiated termination produces no event, because the owner already knows. Producer-initiated termination is an ordered, observable event that never overtakes values reported before it.

The behavioral contracts live on the types, so this document only points at them and cannot drift from the code:

- `JobEngine`: the internal lifecycle engine. It owns the states, FIFO report ordering, the `stop` / `terminate` / `deinit` control commands and the difference in intent between `stop` and `terminate`. Header in `Sources/TheirCore/Job/Engine/JobEngine.swift`; tested in `JobEngineTests`.
- `InputQueue`: internal FIFO storage behind the shared `DrainQueue`, with append order, amortized O(1) `popFirst()` and `pending` inspection before `clear()`. It always lives inside a record protected by `Their.Lock`. Header in `Sources/TheirCore/InputQueue.swift`; tested in `InputQueueTests`.
- `Their.Job`: the public single-subscriber sink facade. It owns subscription, single-subscriber misuse, events, cancel and pin, and deinit. Header in `Sources/TheirCore/Job/Job/Job.swift`; tested in `JobTests`.
- `evolve` / `map` / `mapError`: sink-level evolution. The root `evolve(failure:initial:value:)` owns the model: owned `State`, `nil` suppression, failure mapping, the single lifecycle, misuse forwarding, terminal reset, cancel and deinit, and the race of a terminal event arriving before the upstream cancel is stored. The failure-preserving `evolve(initial:_:)`, `map` and `mapError` are thin wrappers. Execution mirrors `JobEngine`: inputs enter a FIFO queue, a single drainer reduces them in order, and the transform and the `failure` mapping run outside the internal lock, so a transform may synchronously cancel its own subscription without deadlocking. An upstream terminal `.finished` or `.failure` is FIFO-ordered behind earlier values and never overtakes them; only a concurrent cancel drops an in-flight emission. Both terminal events bypass the transform: `.finished` is forwarded as is and `.failure` is mapped through `failure`. The transform sees only values, and a derived "last word" belongs to a materialized pipeline, not to `evolve`. Header in `Sources/TheirCore/Job/Evolve/JobEvolve.swift`; tested in `JobEvolutionTests`.
- `tryMap`: a value map through a throwing transform. Each upstream `.value` runs `transform`, a returned value is emitted, and a thrown error is mapped through `onThrow` into the derived job's `Failure` and delivered as a terminal `.failure` that also cancels the still-live upstream. The failure type is unchanged, and an upstream `.failure` is forwarded as is, so normalise it first with `mapError` when the domain failure differs: `upstream.mapError { … }.tryMap({ try decode($0) }) { … }`. It is built on the root `evolve` machinery, with one FIFO queue, a single drainer, `transform`, `onThrow` and the sink outside the lock, FIFO-ordered terminals, single-lifecycle misuse and the cancel pin. That makes it the canonical replacement for per-feature adapters that switch over the upstream event and wrap a decode in `do` / `catch`. Header in `Sources/TheirCore/Job/Evolve/JobTryMap.swift`; tested in `JobTryMapTests`.
- `Their.Job.merge`: dynamic merging of same-typed upstreams. Callers map feature-specific upstreams into one `Value` and `Failure` first, then use `Their.Job.merge([...])` for dynamic collections or `Their.Job.merge(job1, job2)` for fixed inputs. The merged job owns N subscriptions, one FIFO input queue, FIFO-ordered terminal failure propagation, cancellation of all started upstreams, synchronous cleanup when a terminal event arrives before a cancel is stored, and single-lifecycle misuse. A failure never overtakes values queued before it, and inputs queued after it are dropped. Terminal rules follow one invariant: the union is alive while any upstream can still produce an event. The first `.failure` terminates everything, the `.finished` of the last live upstream terminates with `.finished`, and an individual upstream `.finished` is silent downstream and releases that upstream's cancel slot. It is the preferred operator for "several independent jobs become one event stream, then a reducer derives state". It is not `combineLatest` and does not invent a readiness policy. Header in `Sources/TheirCore/Job/Merge/JobMerge.swift`; tested in `JobMergeTests`.
- `Their.Job.never()`: an inert silent job for dependency defaults, previews and tests. It starts one empty `Their.Work`, never reports `.value`, `.failure` or `.finished`, and relies on normal owner-side cancel and deinit to stop the lifecycle. Misuse and the cancel pin follow the standard `Their.Job` contract. Header in `Sources/TheirCore/Job/Never/JobNever.swift`; tested in `JobNeverTests`.
- `stream()`: an `AsyncStream` adapter with one eagerly started lifecycle per `stream()` call. It subscribes inline before returning, so a caller can neither miss an immediate live event nor observe a job that has not started. Copies and iterators share the stream context and its subscription pin; releasing the last owner or cancelling iteration runs termination and cancels that subscription. A synchronous terminal event is safe even when `subscribe(_:)` returns its cancel afterwards: `.finished` or `.failure` is yielded, the stream finishes, and the late-returned cancel is invoked exactly once. The adapter intentionally uses the unbounded buffer of `AsyncStream` to preserve every event, so callers retaining a stream must consume it promptly. Build streams from `Their.Job(...).stream()`; there is no separate stream factory. Header in `Sources/TheirCore/Job/Stream/JobStreamAdapter.swift`; tested in `JobStreamTests` and `SyncSubscribeRegressionTests`.
- `Their.Job.once { }` / `Their.Job.once(failure:) { }`: one-shot factories. Each wraps one async operation into a finite `Their.Job` that owns one `Task`, reports one `.value` and then the terminal `.finished` on success, and cancels the `Task` through standard cooperative cancellation from `Their.WorkCancel`. The throwing overload maps a thrown error through `failure` into the terminal `.failure`, mirroring `evolve(failure:)`; the non-throwing overload returns `Their.Job<Value, Never>`. Typed `throws(Failure)` is deliberately not used, because closure conversions into a generic typed-throws parameter are still rejected by the compiler. This is the canonical shape for fetches, clear-style commands and other single-result effects. Header in `Sources/TheirCore/Job/Once/JobOnce.swift`; tested in `JobOnceTests`, including the success path of the throwing overload and a cancel that maps `CancellationError` through `failure` without delivering anything to the cancelled subscriber.
- `Their.Work` / `Their.WorkOutput` / `Their.WorkReport`: the upstream work closure and its three-outcome reporting closure. Downstream sinks stay separate from upstream reporting. Wrap a genuinely concurrent producer with `Their.serialized(_:)`. Headers in `Work.swift`, `WorkOutput.swift`, `WorkReport.swift` and `Serialized.swift`. `Their.serialized(_:)` is tested in `SerializedTests`: call-order FIFO through the `Task` chain, a report queued behind an in-flight delivery becoming a no-op once the wrapper is cancelled, pinned through the DEBUG-only `SerializedWorkTestHooks.didSkipQueuedReport` task-local, and a single upstream cancel on repeated wrapper cancel. It is also stressed in `TheirCoreSharedStressTests`.
- `Their.Misuse`: incorrect use of a primitive. It is kept outside `Their.JobEvent` and `Their.HubEvent` so the value and failure channels keep their domain meaning, and it is delivered to a `Their.MisuseHandler`, by default `Their.MisuseHandlers.fatal`. Header in `Sources/TheirCore/Misuse.swift`.

Usage guidance: treat `evolve` as the default shape for pure event-to-state logic, and keep side effects such as SDK calls, persistence or cancelling other resources outside the transform, in a thin `Their.Work` or runner layer. If a restartable subscription is needed, add a wrapper that builds a fresh `Their.Job` per lifecycle instead of weakening the one-lifecycle contract.

## Hub Model

`Their.Hub` is the shared, multi-subscriber peer of `Their.Job`: one shared lifecycle that many subscribers join. `HubEngine` is its internal engine. Both are plain `final class: Sendable` types. TheirCore uses no actors for jobs and hubs; all synchronization goes through `Their.Lock`. A hub is intentionally different from a job, which is single-lifecycle and single-subscriber.

As with `Their.Job`, the behavioral contracts live on the types:

- `HubEngine`: broadcast and snapshot ordering, the three-phase start, restart after a synchronous terminal `.finished` or `.failure`, last-subscriber teardown, the deinit cancel chain and generation fencing. Every engine generation carries a token, and events from a detached previous engine, one still running for a moment before its `stop()` lands, are dropped instead of reaching or tearing down a newer lifecycle. A terminal `.finished` and a terminal `.failure` tear the shared lifecycle down identically: re-snapshot, broadcast to every attached subscriber, reset. A later subscriber starts a fresh generation. Header in `Sources/TheirCore/Hub/Engine/HubEngine.swift`; tested in `HubEngineTests`.
- `Their.Hub`: the public multi-subscriber facade. It owns the synchronous `subscribe`, the `Their.HubEvent` / `Their.HubSink` vocabulary shared with the engine, live-only delivery, the three effects of the cancel pin and the timing of last-subscriber teardown. Header in `Sources/TheirCore/Hub/Hub/Hub.swift`; tested in `HubTests`.
- `evolve` / `map` / `mapError` on a hub: shared evolution. One `State` is shared by the active lifecycle, a restart takes a fresh generation token that drops stale callbacks, a terminal event is broadcast and resets everything, and the `replayLatest` seam supports `shareLatest()`. The root `evolve` owns the model; the rest are thin wrappers. Execution mirrors `JobEngine`: inputs, both upstream events and replay requests, enter a FIFO queue, a single drainer reduces them in order, and the transform and the `failure` mapping run outside the internal lock, so a transform may synchronously cancel a derived subscription without deadlocking. The recipients of a value are snapshotted when its input is dequeued, before the transform, so a subscriber joining mid-transform does not see that in-flight value. The base hub stays live-only, and `shareLatest()` replays such a value through the queue. An upstream terminal `.finished` or `.failure` is FIFO-ordered behind earlier values and never overtakes them. Both terminal events bypass the transform: `.finished` is broadcast as is and `.failure` is mapped through `failure`. Either one clears subscribers, replay storage and the shared `State`, and a later subscription starts a fresh lifecycle. Header in `Sources/TheirCore/Hub/Evolve/HubEvolve.swift`; tested in `HubEvolutionTests`.
- `shareLatest()`: explicit late-subscriber replay. It stores only successful values and clears them on a terminal `.finished` or `.failure` and after the last subscriber leaves. Replay is delivered through the FIFO queue of the evolution, so it is ordered with live broadcasts, and a late joiner receives the latest value exactly once: never a stale value after a newer broadcast it already saw, and never a duplicate. The base hub is intentionally live-only; replay belongs here, never in `HubEngine`. Header in `Sources/TheirCore/Hub/Evolve/HubShareLatest.swift`; tested in `HubEvolutionTests`.
- `stream()` / `job()` on a hub: adapters that add one subscriber on the shared lifecycle and follow the same pin contract as the job stream. The hub stream joins inline before returning, so an already active generation cannot emit or terminate through a gap before the stream subscriber is attached. Releasing the last owner of the shared stream context removes the subscriber and can synchronously tear the generation down when it was the last subscriber. Headers in `HubStream.swift` and `HubJob.swift`; tested in `HubStreamTests`, `SyncSubscribeRegressionTests` and `HubJobTests`, including resubscribe after cancel or after a terminal event, which reports `Their.Misuse` without rejoining the hub.

Delivery order between subscribers of one event is unspecified. The engine registry is a dictionary keyed by `UUID`, so two sinks may receive the same value in either order. Each subscriber still sees events in emit order, and consumer code must not encode subscription order across sinks.

The threading caveats in [Execution and Threading](#execution-and-threading) apply equally to hubs. `Their.HubCancel` cuts delivery synchronously but does not prove that SDK teardown finished, an already running subscriber callback is not interrupted, and derived wrappers do not copy active root `.topLevel` logging.

## Execution and Threading

TheirCore is scheduler-less: effects run synchronously on whichever thread pushed the triggering input. The engines never hop to main and never dispatch to a queue. The chain runs in two seams. One is where the producer calls `report(...)`, inside your `Their.Work` or SDK callback. The other is where you call `subscribe`, `cancel` or `emit`, inline on your thread; by the time `subscribe` returns, `work(report:)` has already run. The first caller that finds the queue empty becomes the drainer and replays every queued input, including inputs enqueued by other threads, so a slow sink stalls whoever happened to become the drainer.

- Trace the trigger thread instead of assuming main. Hop explicitly before touching UI, with `Task { @MainActor in }` or `await MainActor.run`, and never with `MainActor.assumeIsolated`.
- Keep `evolve` transforms and sinks cheap and non-blocking, because they run under the drainer. No I/O, disk access, `DispatchQueue.sync`, semaphores or `Task.value` waits inside them. Transforms run outside the locks, so a transform may synchronously cancel its own subscription and the re-entrant input joins the drain in progress, but it still blocks the drainer for its duration.
- Route heavy work such as large blob parsing, decoding or database writes from the sink to an effect boundary: a `Task`, a background queue or an actor. Keep `evolve` as `State + Event -> Output?`.
- Use `Their.serialized(_:)` only for a genuinely multi-threaded producer that needs a deterministic order or post-cancel suppression. The engine FIFO already makes concurrent reports safe, so do not wrap a serial SDK delegate queue.
- `Their.Lock` is non-reentrant, with tiny critical sections. Never re-enter the same lock or `Their.WeakValueCache` from inside its own `withLock` closure. Compute effects under the lock and run them after release.
- A sink can still observe one value racing with `cancel()`, because an already running drainer callback is not interrupted, so make sinks idempotent. `cancel()` cuts delivery and invokes the upstream cancel, but does not prove that SDK teardown finished.

## Shared Internal Mechanics

The primitives reuse three internal building blocks:

- `SubscriptionPin<Owner>` holds the originating job or hub for the returned cancel closure. Explicit release detaches the owner under its lock and releases it after unlock. Dropping the pin releases ownership through ARC; the pin itself issues no cancellation command.
- `makeSubscriptionStream` is the common eager sink-to-stream bridge. A `Their.Resource<Their.WorkCancel>` owns the returned cancel and handles termination that happens before that cancel is returned. The job and hub adapters supply their own terminal test; unbounded buffering, terminal-then-finish and shared stream and iterator ownership stay the same.
- `DrainQueue<Input>` combines `InputQueue` storage with the single-drainer claim. It lives inside the existing owner lock in `JobEngine`, job evolution, hub evolution, merge and lifecycle logging. `append` appends and claims an idle drain atomically, and `popFirst` releases the claim only when the queue is empty or the owner is inactive. Popping the last input keeps the claim while that input's callback runs. `takePending` returns detached storage while preserving the claim, so cancel or restart cannot create a second drainer during an in-flight callback.

Each operator still owns lifecycle eligibility, terminal decisions and its state snapshot. Dequeue and the snapshot of state, subscribers and generation stay in one critical section, while transforms and effects run after unlock. `DrainQueue` introduces no extra lock, scheduler, task or callback runner. Fully detached records retain discarded inputs, sinks and state through post-lock cleanup. Hub evolution uses one record reset for cancel, terminal, last unsubscribe and fresh start, preserving its drainer claim and generation token.

`JobEngine` shares one state transition for `stop`, `terminate` and `deinit`, with an explicit diagnostic table that preserves their differences and effect order. A successful finish and a failure share one terminal transition. `HubEngine` uses `Their.HubEvent` and `Their.HubSink` directly, and its mapping from `JobEngineEvent` filters internal diagnostics. The public job and hub events remain distinct.

| Behavior | Tests |
| --- | --- |
| Pin release, ARC ownership, concurrent release and destructor reentry | `SubscriptionPinTests`, job and hub subscription lifetime tests |
| Eager stream creation, synchronous terminal, late-returned cancel, last stream or iterator owner | `SubscriptionStreamTests`, `JobStreamTests`, `HubStreamTests`, `SyncSubscribeRegressionTests` |
| Queue ownership, empty or inactive dequeue, detach and restart, optional nil inputs | `DrainQueueTests`: all 256 length-four action traces against a reference model, plus concurrent producers and payload destruction |
| Per-operator FIFO, terminal ordering and lifetime | Engine, evolution, merge, logging and lifetime suites |
| Hub restart while the previous transform is still running | `evolveRestartDuringTransformKeepsOneDrainerAndResetsState` |
| All four engine states across stop, terminate, deinit and mixed commands | `JobEngineTests`, including exact cancel and diagnostic ordering and silent active deinit |
| `Their.serialized(_:)` chain order, queued-report no-op after cancel, single upstream cancel | `SerializedTests`, `TheirCoreSharedStressTests` |

The `map` and `evolve` wrappers use their root evolution. `Their.serialized(_:)` keeps its asynchronous `Task` chain; replacing it with inline draining would change timing rather than simply share implementation.

## Lifecycle Diagnostics

`Their.LifecycleLogging` is the only supported lifecycle diagnostic surface. The detailed contract lives in the header in `Sources/TheirCore/LifecycleLogging.swift`: a no-op outside DEBUG, counting of root `.topLevel` jobs and hubs only, keyed by `(file, line, label)`, the `~~| [label] (N)` output, `showOrigin`, `withoutTopLevel` for derived wrappers, the process-wide store behind `Their.Lock` and the test hooks. It is tested in the single serialized `LifecycleLoggingTests` suite.

The store appends each line together with the current output sink to one FIFO while holding its lock, then one drainer invokes sinks outside that lock. This keeps concurrent count lines in enqueue order and lets an output sink log reentrantly without recursive locking or overtaking its current line. A concurrent caller that is not the drainer may therefore return while its queued sink invocation is still pending. Because the output hook and counts are process-wide, every test that activates or resets lifecycle logging belongs in the same serialized suite; `Their.stress(count: 1)` alone does not serialize separate Swift Testing methods.

Keep it intentionally narrow. Do not add broad tracing of subscribe, cancel, work, failure or internal engine events without a concrete debugging need and a new tested contract. Derived and internal wrappers must never copy active root `.topLevel` logging. Hub-derived wrappers (`evolve`, `map`, `shareLatest`, `job()`) pass `logging.withoutTopLevel`, so disabled metadata with `options: []` flows through as inert metadata. Job-derived wrappers (`evolve`, `map`, `tryMap`, `merge`) carry no logging metadata at all. Both keep active `.topLevel` counting at root construction sites only.

## Synchronization

`Their.Resource<Value>` is the canonical owner of one externally created handle, such as an SDK listener registration, an observer token or a cancellable handle. The detailed contract lives in the header in `Sources/TheirCore/Resource.swift`: set once, release exactly once, a set after cancel and a second set both release immediately, `deinit` cancels, and release runs outside the lock.

Use `Their.Resource` inside `Their.Work` implementations and SDK boundaries where a cancel closure may be returned before the external registration is stored. Do not build local optional-registration boxes or callback bags for this one-handle ownership shape. `Their.Resource` is not a lifecycle abstraction by itself; jobs, hubs and `evolve` still own event delivery and state evolution.

`Their.Lock<Value>` is the canonical synchronization primitive:

- It is a mutex backed by `os_unfair_lock`, stored in an internal `LockStorage` class.
- It supports noncopyable values with `Value: ~Copyable`.
- `withLock` and `withLockIfAvailable` mirror `Synchronization.Mutex`, so the storage can move onto the standard library `Mutex` once iOS 18 and macOS 15 are the deployment floor, without touching call sites. The behavioral contract is pinned in `LockTests`: result passthrough, in-out mutation, typed-error propagation, lock release on `throw`, and `withLockIfAvailable` returning `nil` without running the body while the lock is held.
- Prefer explicit named state under `Their.Lock` over ad hoc mutable boxes, so the protected data model stays visible.

`Their.WeakValueCache<Key, Value>` is the canonical synchronized weak-value cache for registries that need one read-or-create operation. The detailed contract lives in the header in `Sources/TheirCore/WeakValueCache.swift`: `value(forKey:orInsert:)` as the single read-or-create step, the `job(forKey:orInsert:)` helper from a cached hub to a fresh `Their.Job` with explicit `shareLatest()` replay, weak storage, lazy pruning on the insert path, the non-reentrant `orInsert` and backing storage shared between copies. It is a cache primitive, not a registry policy: the choice of key and the meaning of the cached object stay with the owner.

`Their.MainThreadOnce` is the canonical run-once-on-main gate. It runs one injected `work` effect exactly once, always on the main thread, and blocks every caller from any thread until that work has finished. Use it for synchronous "configure the SDK before first touch" entry points instead of a `static let` paired with `DispatchQueue.main.sync`, which can deadlock when a background caller holds the lazy-init token while the main thread waits on the same token. `work`, `isMainThread` and `runOnMain` are injected, with `Thread.isMainThread` and `DispatchQueue.main.async` as production defaults, so the gate stays SDK-agnostic and deterministically testable. The header in `Sources/TheirCore/MainThreadOnce.swift` owns the detailed contract: the `idle -> (configuring | waitingForMain) -> configured` phase machine, main-thread-only `work`, a single main hop, re-entrancy handled by `returnNow`, an atomic phase check and waiter enqueue under `Their.Lock` with no lost wakeup, and `work` and waiter signals running outside the lock. It is tested in `MainThreadOnceTests`, including the four pairings of main and non-main callers and a mixed-caller stress that asserts `work` runs exactly once.

### `@unchecked Sendable` Registry

TheirCore normally avoids `@unchecked Sendable`. Every exception below documents why the type is safe and which invariant the compiler cannot see.

- `Their.Lock<Value>` in `Lock.swift`: a conditional `@unchecked Sendable where Value: ~Copyable`. It is justified because `LockStorage<Value>` wraps an `os_unfair_lock`, and every access to `value` goes through `withLock` or `withLockIfAvailable`, which serialize reads and writes. It is required because Swift cannot infer `Sendable` for `~Copyable` values.
- `WeakValueCacheBox<Object>` in `WeakValueCache.swift`: an unconditional `@unchecked Sendable`. It is justified because the weak reference is only ever read or written while the lock of the enclosing `WeakValueCacheStorage` is held. The compiler cannot see this invariant, because the lock lives on the storage, not on the box.

A new low-level primitive that needs `@unchecked Sendable` gets the same kind of one-paragraph justification here and a comment next to the type.

## Testing Expectations

The tests are intentionally deep. They use `Their.stress` from TheirCoreTesting to repeat each scenario under concurrent pressure. Tests for the TheirCoreTesting helpers themselves live in `Tests/TheirCoreTestingTests`, and `Tests/TheirCorePublicAPITests` uses the package without `@testable import`, the way a consumer does.

The waiters in TheirCoreTesting, `Their.TestSignal` and the `waitFor...` methods of recorders and drivers, are cancellation-aware and throwing. Always call them with `try await` inside the surrounding `try await Their.stress { ... }` scenario. A per-iteration timeout cancels and structurally joins the user block before `Their.stress` throws, so timed-out test code cannot resume after the test has returned. This requires awaited operations to cooperate with cancellation, which the shared waiters do.

A new service should normally come with:

- Direct tests of the internal engine or state machine when one exists.
- Public contract tests of each facade or adapter, including sink and stream subscribers sharing the same hub.
- Tests for lifecycle and deinit behavior.
- Tests for terminal failure and late callbacks.
- Tests for repeated calls.
- Tests that a second `subscribe` after cancel or after a terminal event reports `Their.Misuse` without starting a second lifecycle. This is pinned for `Their.Job`, `evolve`, `tryMap`, `merge` and `job()`.
- Tests for ignored or no-op branches when those branches are observable.
- Tests that dependency start and cancel calls happen exactly once when that matters.

To prove that an event does not happen, do not use `sleep`, and do not use polling loops such as `while !predicate { await Task.yield() }`. Use deterministic signals, explicit state, callback recorders, actor gates or test-only hooks under `#if DEBUG`.

When teardown is asynchronous, prefer an explicit wait hook over polling. For internal engines such as `JobEngine` and `HubEngine`, that hook can live directly on the internal type under `#if DEBUG`. Public facades should not leak internal state just to satisfy tests.

Do not write tests that require `Their.WorkCancel` or `Their.HubCancel` to synchronously finish SDK-owned asynchronous teardown. The cancellation semantics are: subscriber delivery is cut off immediately and the upstream cancel closure is invoked before returning, while any asynchronous work started by that closure may finish later.

A scenario that holds a drainer inside a sink or transform, for example with a `DispatchSemaphore`, runs the held call through the test target's `BlockingWork` helper on a Dispatch worker thread. The cooperative thread pool of Swift concurrency is only as wide as the CPU count, so holds taken from several suites at once would starve the very continuation that releases them.

### Lifetime Cleanup Contract

The cleanup paths covered here are subscription pins and sink slots, `JobEngine` input cleanup, job evolution, hub evolution and merge. They detach retired callbacks, state and discarded inputs under the corresponding lock, then release those references after unlocking, so destructors may synchronously re-enter cancellation. Releasing the last facade reference of a job or hub subscription pin must not run upstream teardown under the pin lock.

A hub subscription drops its sink reference on cancel, finish or failure. Keeping an inert cancel handle does not keep sink captures alive. A callback or transform already in flight may retain its own snapshot until it returns; cancellation does not interrupt it or wait for SDK-owned asynchronous teardown. Repeated cancellation stays safe, and terminal and value FIFO ordering and generation fencing are preserved.

`InputQueue` clears each consumed slot while the element it returns is retained, so compaction cannot destroy previously consumed payloads inside an owner's lock. Its `pending` property creates an ordered snapshot. Owners discarding pending entries retain the detached queue through post-lock cleanup. `JobEngine` also keeps ignored dequeued inputs alive outside reduction and carries discarded queues alongside cleanup effects; cancels returned by `start` are still invoked exactly once. Job evolution, hub evolution and merge retain detached records across teardown and terminal delivery, and evolution commits retain replaced state and replay owners until after unlock.

Regression coverage:

- `JobSubscriptionLifetimeTests` and `HubSubscriptionLifetimeTests`: the real public cancel and pin paths, weak task-local lock probes, and upstream teardown re-entering the same cancel.
- `JobEvolutionLifetimeTests`, `HubEvolutionLifetimeTests` and `JobMergeLifetimeTests`: destructors of state, replay values, sinks and discarded inputs across cancel, finish, failure, transform failure, unsubscribing a subscriber that is not the last, replacement and reentrant cleanup. DEBUG-only construction seams use the actual private state machines without exposing public facade state.
- `HubCaptureLifetimeTests` and `HubEngineSubscriptionTests`: captures released while the cancel handle remains alive, terminal cleanup, callback-driven cancellation, and captures retained only until an in-flight callback returns.
- `InputQueueTests` and `JobEngineLifetimeTests`: ownership of consumed payloads through compaction, including optional nil values, discarded and ignored inputs, and destructor reentry into stop and terminate.

Weak lifetime observations in these regressions use `@Sendable` closures with weak captures that return only a Boolean. This keeps observation non-owning without mutable weak locals in assertion macros or artificial writes to silence compiler warnings; deinit counters remain independent checks of cleanup.

### Running the Suite

```sh
swift test --no-parallel
```

Every test already runs its scenario many times concurrently through `Their.stress`, with a short per-iteration watchdog against deadlocks. The default in-process parallelism of Swift Testing stacks the whole suite on top of that, oversubscribes the cooperative thread pool and turns the watchdog into false timeouts.

## Known Gaps and Missing Operators

`Their.Job` and `Their.Hub` are the two primitives. Their axis is subscriber cardinality, one owner or many, and there is no third value on that axis, so a third peer primitive is not the gap. The gaps are operators, in priority order:

- Multi-upstream readiness operators such as `combineLatest`, `withLatestFrom` or `zip`. `Their.Job.merge` covers the union of same-typed event streams and should be paired with `evolve` for reducer-owned state. Readiness policies such as "wait for the latest value of every upstream", "sample another upstream" or "pair values by position" are still absent. Add them as operators with full lifecycle tests when a second real case needs that specific policy, not as local callback bags.
- Materialization with `materialize()` and `dematerialize()`, for reducer-owned terminal policy such as "the first finish of an important source terminates the whole merged pipeline" or errors as data from sources you do not own. Add the pair on top of `.finished`: `materialize` duplicates the upstream terminal event as a final data marker and then genuinely finishes, and `dematerialize` turns marker output back into a real terminal event and cancels its upstream, which cascades through `merge`. Keep `evolve` untouched: a "last word" or flush belongs to the materialized output alphabet, not to a second transform parameter.
- `first()`: take the first matching value, then finish and cancel the upstream. It is cheap once a second real case appears; do not bake it into `merge` as a policy flag.
- Restart and retry: deferred by design. Add a wrapper that builds a fresh `Their.Job` per lifecycle instead of weakening the one-lifecycle contract.
- Time operators such as debounce, throttle, timer or delay are intentionally absent, because there is no scheduler model. Build them as an explicit `Their.Work` that schedules `Task.sleep`, not as engine state, so executor dependencies do not leak into the engine.
