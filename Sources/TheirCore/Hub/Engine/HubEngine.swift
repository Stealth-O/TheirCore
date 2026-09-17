import Foundation

/// Internal shared-lifecycle engine behind `Hub`: one active upstream
/// `JobEngine` multiplexed to many subscribers.
///
/// A `HubEngine` owns at most one inner `JobEngine` at a time. The first
/// subscriber starts that engine, additional subscribers attach to it, and the
/// last subscriber to leave stops it. After it stops — whether by
/// last-unsubscribe, terminal end or terminal failure — a later subscriber
/// starts a fresh engine. `Hub` adds the public sink surface and the cancel/pin
/// contract on top; this type owns the shared-lifecycle, broadcast, fencing and
/// restart model.
///
/// State: all mutable state lives behind one `Lock` (see the `state` property),
/// so the lock-order story is trivial — `subscribe`, `unsubscribe`, `handle`
/// and `deinit` all mutate the same lock atomically.
///
/// Broadcast and snapshots: each upstream event snapshots the subscriber list
/// and emits with the lock released.
/// - `.value` uses the snapshot taken when the sink fires, so a subscriber that
///   joins after that snapshot but before the broadcast runs does not see this
///   in-flight value — the base hub is live-only.
/// - A terminal `.finished` / `.failure` re-snapshots under the lock before clearing
///   subscribers, so every subscriber attached to the terminating lifecycle
///   receives the terminal event, including one that joined after the
///   sink-fire snapshot. A terminal outcome also nulls the engine, so the next
///   subscriber starts fresh.
/// - Broadcast order follows the order the inner `JobEngine` emits events: that
///   engine serializes delivery through its single drainer, so the sink is never
///   called concurrently and no per-event `Task` chain is needed here. A
///   multi-threaded producer that needs a deterministic domain order establishes
///   it upstream or wraps its `Work` with `Their.serialized(_:)`.
/// - Delivery order *between* subscribers of one event is unspecified: the
///   registry is a dictionary keyed by `UUID`, so two subscribers may receive
///   the same value in either order. Per-subscriber order (events in emit
///   order) is the guarantee; domain code must not depend on subscription
///   order across sinks.
///
/// Generation fencing: every engine generation takes a token from
/// `tokenCounter`, the sink built for that engine carries it, and both
/// `snapshotSubscribers(token:)` and the terminal branches of `handle` accept
/// events only while `activeToken` still equals that token. `activeToken`
/// moves to a fresh value on every start and restart, and becomes `nil` on a
/// terminal outcome (`.finished` / `.failure`), on last-unsubscribe and on the
/// empty-registry start commit. A detached previous engine that is still
/// running for a moment
/// before its `stop()` lands therefore cannot deliver values to — or tear
/// down — a newer lifecycle: its events are dropped under the lock.
///
/// Subscribe is three-phase (detailed on `subscribe(_:)`): register under the
/// lock; create and `start()` the inner engine outside the lock, so a
/// synchronous `report` can re-enter the lock safely; then commit under the
/// lock. A single starter is elected by the `isStarting` flag, so concurrent
/// subscribers attach to one engine instead of racing to start duplicates. If
/// the engine ends or fails synchronously during start, the starter loops and
/// starts a fresh engine for subscribers that re-subscribed from the terminal sink; if
/// no subscriber remains, it stops the engine without installing it.
///
/// Lifetime: `deinit` nulls the inner engine; ARC then drops the `JobEngine`,
/// whose own `deinit` cancels the active `WorkCancel`. The engine reference is
/// always extracted out of the lock before release, because the engine's
/// teardown can call back through the sink into `snapshotSubscribers(token:)`
/// and the non-reentrant `os_unfair_lock` would otherwise deadlock.
///
/// Threading: every step runs synchronously on the thread that drives it — the
/// upstream drainer's thread for broadcasts, the caller's thread for
/// subscribe/unsubscribe. The engine adds no `main` hop of its own, critical
/// sections are tiny, and subscriber callbacks always run with the lock
/// released, so a callback may overlap a concurrent cancel and must stay
/// lightweight.
///
/// DEBUG hooks (`afterSnapshotForTests`, `beforeDetachedEngineStopForTests`,
/// `waitForStateForTests`) exist only for TheirCore tests and are never exposed
/// through the public `Hub` facade.
final class HubEngine<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    private let misuseHandler: Their.MisuseHandler
    private let misuseLocation: Their.MisuseLocation
    /// All mutable HubEngine state lives behind one `Lock`:
    /// - `activeToken`: generation token of the engine that is currently allowed to deliver events — the one being
    ///   started by the in-progress starter, or the installed one — or `nil` when no legitimate engine exists.
    /// - `isStarting`: another caller is currently inside the three-phase `subscribe` for this lifecycle.
    /// - `isStartingTerminated`: the starting engine reached `.finished` / `.failure` before Phase 3 could commit it.
    /// - `jobEngine`: the currently-running underlying `JobEngine`, or `nil` when idle or while starting.
    /// - `subscribers`: registered downstream subscribers keyed by `UUID`.
    /// - `tokenCounter`: monotonically increasing source of generation tokens.
    /// - `afterSnapshotForTests` / `beforeDetachedEngineStopForTests` / `stateWaiters` (DEBUG): test hooks.
    ///
    /// Keeping everything under one lock keeps the lock-order story trivial: `notifyStateWaitersForTests` can
    /// re-check the current state and drain matching waiters atomically against `subscribe`/`unsubscribe`/`handle`
    /// state changes.
    private let state: Their.Lock<HubEngineLifecycleState<Value, Failure>>
    private let work: Their.Work<Value, Failure>

    init(
        misuseHandler: @escaping Their.MisuseHandler = Their.MisuseHandlers.fatal,
        misuseLocation: Their.MisuseLocation = .init(),
        work: @escaping Their.Work<Value, Failure>
    ) {
        self.misuseHandler = misuseHandler
        self.misuseLocation = misuseLocation
        self.state = Their.Lock(HubEngineLifecycleState())
        self.work = work
    }

    /// Releases the underlying `JobEngine`. When the engine is still active,
    /// the cancel of in-flight upstream work happens transitively through
    /// `JobEngine.deinit`: nulling the reference is the only strong owner this
    /// engine holds, so ARC drops `JobEngine`, whose own `deinit` cancels the
    /// active `WorkCancel`. The documented guarantee that deinit of an active
    /// `HubEngine` cancels the active underlying `JobEngine` depends on that chain.
    ///
    /// Subscriber bookkeeping is intentionally not cleared here: by the time
    /// `HubEngine.deinit` runs there are no callers that can observe state, so
    /// the dictionary will be released along with the engine itself.
    deinit {
        // Extract jobEngine to a local before releasing the lock so its `deinit`
        // (which calls back into `HubEngine` via the sink) runs OUTSIDE this state
        // lock. `os_unfair_lock` is not reentrant — releasing the engine inside
        // the lock would deadlock when the engine's sink calls
        // `snapshotSubscribers(token:)` and tries to re-acquire the same lock.
        let oldEngine: JobEngine<Value, Failure>? = state.withLock { lifecycle in
            let engine = lifecycle.jobEngine
            lifecycle.activeToken = nil
            lifecycle.jobEngine = nil
            return engine
        }
        _ = oldEngine
    }

#if DEBUG
    private func afterSnapshotForTests() -> (@Sendable () -> Void)? {
        state.withLock { lifecycle in
            lifecycle.afterSnapshotForTests
        }
    }
#endif

#if DEBUG
    private func beforeDetachedEngineStopForTests() -> (@Sendable () -> Void)? {
        state.withLock { lifecycle in
            lifecycle.beforeDetachedEngineStopForTests
        }
    }
#endif

    func getState() -> HubEngineState {
        state.withLock { lifecycle in
            lifecycle.currentState
        }
    }

    /// Broadcasts an upstream `JobEngineEvent` from the engine generation identified by `token`. Values use the
    /// subscriber snapshot taken at sink-fire time, so a subscriber that joins after the upstream value but before
    /// the broadcast runs will not see that in-flight value. A terminal `.finished` / `.failure` re-snapshots under the
    /// state lock via `takeTerminatingLifecycle(token:)` — re-checking the generation token, so a detached previous
    /// engine cannot tear down a newer lifecycle — before clearing subscribers, so every subscriber attached to the
    /// terminating lifecycle receives the terminal event.
    ///
    /// A terminal outcome both clears the engine state and emits to subscribers. To avoid holding the lock while
    /// calling sink callbacks, state mutation happens inside `withLock` and the actual subscription emits happen
    /// outside the lock.
    private func handle(
        event: JobEngineEvent<Value, Failure>,
        subscriptions: [HubEngineSubscription<Value, Failure>],
        token: Int
    ) {
        let terminal: Their.HubEvent<Value, Failure>
        switch event {
        case .finished:
            terminal = .finished
        case .failure(let failure):
            terminal = .failure(failure)
        case .message:
            return
        case .value(let value):
            for subscription in subscriptions {
                subscription.emit(.value(value))
            }
            return
        }
        guard let output = takeTerminatingLifecycle(token: token) else {
            return
        }
        withExtendedLifetime(output.oldEngine) {
            for subscription in output.subscriptions {
                subscription.emit(terminal)
            }
        }
#if DEBUG
        notifyStateWaitersForTests()
#endif
    }

    /// Builds the `JobEngineSink` used by the inner `JobEngine` that this `HubEngine` owns for the engine
    /// generation identified by `token`.
    ///
    /// Each upstream event snapshots the current subscriber list — empty when the generation is no longer the
    /// active one — and broadcasts synchronously on the caller's thread. The inner `JobEngine` serializes delivery
    /// through its single drainer and calls this sink in the order it emits events, so broadcast ordering follows
    /// that emit order — no per-event `Task` chain is needed here. A multi-threaded producer that needs a
    /// deterministic domain order establishes it upstream or wraps its `Work` with `Their.serialized(_:)`.
    private static func makeSink(
        object: HubEngine<Value, Failure>,
        token: Int
    ) -> JobEngineSink<Value, Failure> {
        { [weak object] event in
            guard let object else {
                return
            }
            let subscriptions = object.snapshotSubscribers(token: token)
#if DEBUG
            object.afterSnapshotForTests()?()
#endif
            object.handle(
                event: event,
                subscriptions: subscriptions,
                token: token
            )
        }
    }

#if DEBUG
    private func notifyStateWaitersForTests() {
        let readyWaiters = state.withLock { lifecycle in
            lifecycle.takeWaiters(matching: lifecycle.currentState)
        }
        readyWaiters.forEach { $0.continuation.resume() }
    }
#endif

#if DEBUG
    func setAfterSnapshotForTests(_ hook: (@Sendable () -> Void)?) {
        state.withLock { lifecycle in
            lifecycle.afterSnapshotForTests = hook
        }
    }
#endif

#if DEBUG
    func setBeforeDetachedEngineStopForTests(_ hook: (@Sendable () -> Void)?) {
        state.withLock { lifecycle in
            lifecycle.beforeDetachedEngineStopForTests = hook
        }
    }
#endif

    /// Snapshots the current subscriber list under the Lock, or returns an empty list when the generation
    /// identified by `token` is no longer the active one. Called from `makeSink` at sink-fire time.
    func snapshotSubscribers(token: Int) -> [HubEngineSubscription<Value, Failure>] {
        state.withLock { lifecycle in
            guard lifecycle.activeToken == token else {
                return []
            }
            return Array(lifecycle.subscribers.values)
        }
    }

    /// Three-phase subscriber registration.
    ///
    /// Phase 1 (under lock): add the subscriber to the registry. If no engine is running and no other subscribe
    /// is currently starting one, mark `isStarting = true`, take a fresh generation token as `activeToken` and
    /// prepare to start an engine.
    ///
    /// Phase 2 (outside lock): create the `JobEngine` whose sink carries this generation's token and call
    /// `JobEngine.start()`. This calls `work(report:)`, which may synchronously invoke `report` (e.g. an SDK
    /// preflight that emits an immediate failure). Because we are outside the lock, the resulting `makeSink`
    /// chain — `handle(event:subscriptions:token:)` over `snapshotSubscribers(token:)` — can safely re-acquire
    /// the same `Lock` without deadlocking.
    ///
    /// Phase 3 (under lock): clear `isStarting`. If a terminal outcome (`.finished` / `.failure`) happened during
    /// Phase 2, stop that engine. When a subscriber joined reentrantly from the terminal sink, the original
    /// starter immediately loops and starts a fresh engine — with a fresh token — for those new subscribers. If
    /// the registry is empty, the engine is not installed: `activeToken` is invalidated and the caller below
    /// stops the detached engine, whose late events are already fenced off by the token check. If no terminal
    /// outcome happened, install `lifecycle.jobEngine = job` so future subscribers and `unsubscribe` see it.
    /// `JobEngine.stop()` is idempotent against `.terminated`, so calling it on an engine that already terminated
    /// via a synchronous terminal report is a no-op debug message.
    func subscribe(
        _ sink: @escaping Their.HubSink<Value, Failure>
    ) -> Their.HubCancel {
        let id = UUID()
        let subscription = HubEngineSubscription(sink: sink)
        let start: (shouldStart: Bool, token: Int) = state.withLock { lifecycle in
            lifecycle.subscribers[id] = subscription
            guard lifecycle.jobEngine == nil, lifecycle.isStarting == false else {
                return (false, 0)
            }
            lifecycle.isStarting = true
            lifecycle.isStartingTerminated = false
            lifecycle.tokenCounter += 1
            lifecycle.activeToken = lifecycle.tokenCounter
            return (true, lifecycle.tokenCounter)
        }
        var shouldStartNewEngine = start.shouldStart
        var token = start.token
        while shouldStartNewEngine {
            let job = JobEngine(
                misuseHandler: misuseHandler,
                misuseLocation: misuseLocation,
                sink: Self.makeSink(object: self, token: token),
                work: work
            )
            _ = job.start()
            let result: (
                engineToStop: JobEngine<Value, Failure>?,
                shouldRestart: Bool,
                restartToken: Int
            ) = state.withLock { lifecycle in
                lifecycle.isStarting = false
                let didTerminateWhileStarting = lifecycle.isStartingTerminated
                lifecycle.isStartingTerminated = false
                if lifecycle.subscribers.isEmpty {
                    // All subscribers either left or were cleared by a terminal outcome that handle() observed
                    // through the sink. Don't install the engine; invalidate its generation so any late event it
                    // still produces is fenced off, and let the caller below stop it.
                    if lifecycle.activeToken == token {
                        lifecycle.activeToken = nil
                    }
                    return (job, false, 0)
                }
                if didTerminateWhileStarting {
                    lifecycle.isStarting = true
                    lifecycle.tokenCounter += 1
                    lifecycle.activeToken = lifecycle.tokenCounter
                    return (job, true, lifecycle.tokenCounter)
                }
                lifecycle.jobEngine = job
                return (nil, false, 0)
            }
            if let engineToStop = result.engineToStop {
#if DEBUG
                beforeDetachedEngineStopForTests()?()
#endif
                engineToStop.stop()
            }
            shouldStartNewEngine = result.shouldRestart
            token = result.restartToken
        }
#if DEBUG
        notifyStateWaitersForTests()
#endif
        return { [subscription, weak self] in
            subscription.cancel()
            self?.unsubscribe(id: id)
        }
    }

    /// Tears down the lifecycle identified by `token` for a terminal event (`.finished` / `.failure`): re-snapshots the
    /// subscribers under the state lock — re-checking the generation token, so a detached previous engine cannot
    /// tear down a newer lifecycle — clears the engine state and returns the subscriptions to emit to. Returns
    /// `nil` when the generation is no longer active.
    ///
    /// Same lock-reentry concern as `deinit`: the engine is extracted out of the state lock so its release (and
    /// any callback its `deinit` triggers via the sink → `snapshotSubscribers(token:)` path) does not re-acquire
    /// the same lock; the caller drops `oldEngine` after emitting.
    private func takeTerminatingLifecycle(token: Int) -> (
        oldEngine: JobEngine<Value, Failure>?,
        subscriptions: [HubEngineSubscription<Value, Failure>]
    )? {
        state.withLock { lifecycle in
            guard lifecycle.activeToken == token else {
                return nil
            }
            let engine = lifecycle.jobEngine
            let subscriptions = Array(lifecycle.subscribers.values)
            lifecycle.activeToken = nil
            lifecycle.jobEngine = nil
            if lifecycle.isStarting {
                lifecycle.isStartingTerminated = true
            }
            lifecycle.subscribers = [:]
            return (engine, subscriptions)
        }
    }

    /// Unsubscribes the given subscriber id. If the registry becomes empty and the engine is installed (Phase 3
    /// of `subscribe` already happened), invalidates its generation token and stops it; late events the detached
    /// engine still produces before `stop()` lands are fenced off by the token check. If `isStarting == true`,
    /// the engine is not yet in `lifecycle.jobEngine`; `subscribe`'s Phase 3 will observe the empty registry and
    /// stop the engine instead.
    func unsubscribe(id: UUID) {
        let jobEngineToStop: JobEngine<Value, Failure>? = state.withLock { lifecycle in
            guard lifecycle.subscribers.removeValue(forKey: id) != nil else {
                return nil
            }
            guard lifecycle.subscribers.isEmpty,
                  lifecycle.isStarting == false,
                  let jobEngine = lifecycle.jobEngine
            else {
                return nil
            }
            lifecycle.activeToken = nil
            lifecycle.jobEngine = nil
            return jobEngine
        }
        if let jobEngineToStop {
#if DEBUG
            beforeDetachedEngineStopForTests()?()
#endif
            jobEngineToStop.stop()
        }
#if DEBUG
        notifyStateWaitersForTests()
#endif
    }

#if DEBUG
    func waitForStateForTests(_ state: HubEngineState) async {
        await withCheckedContinuation { continuation in
            let shouldResume: Bool = self.state.withLock { lifecycle in
                if lifecycle.currentState == state {
                    return true
                }
                lifecycle.stateWaiters.append(
                    HubEngineStateWaiter(continuation: continuation, state: state)
                )
                return false
            }
            if shouldResume {
                continuation.resume()
            }
        }
    }
#endif
}

#if DEBUG
struct HubEngineStateWaiter: Sendable {

    let continuation: CheckedContinuation<Void, Never>
    let state: HubEngineState
}
#endif

private struct HubEngineLifecycleState<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    var activeToken: Int?
#if DEBUG
    var afterSnapshotForTests: (@Sendable () -> Void)?
#endif
#if DEBUG
    var beforeDetachedEngineStopForTests: (@Sendable () -> Void)?
#endif
    var currentState: HubEngineState {
        HubEngineState(
            isRunning: jobEngine != nil,
            subscribersCount: subscribers.count
        )
    }
    var isStarting: Bool = false
    var isStartingTerminated: Bool = false
    var jobEngine: JobEngine<Value, Failure>?
#if DEBUG
    var stateWaiters: [HubEngineStateWaiter] = []
#endif
    var subscribers: [UUID: HubEngineSubscription<Value, Failure>] = [:]
    var tokenCounter = 0

#if DEBUG
    mutating func takeWaiters(matching state: HubEngineState) -> [HubEngineStateWaiter] {
        var ready = [HubEngineStateWaiter]()
        var pending = [HubEngineStateWaiter]()
        for waiter in stateWaiters {
            if waiter.state == state {
                ready.append(waiter)
            } else {
                pending.append(waiter)
            }
        }
        stateWaiters = pending
        return ready
    }
#endif
}
