import Foundation

/// One queued input of an evolved `Hub` lifecycle: a replay request for one
/// late joiner, or an upstream event.
private enum EvolvedHubInput<Input: Sendable, InputFailure: Swift.Error & Sendable>: Sendable {

    case replay(UUID)
    case upstream(Their.HubEvent<Input, InputFailure>)
}

private struct EvolvedHubRecord<
    Input: Sendable,
    InputFailure: Swift.Error & Sendable,
    Value: Sendable,
    State: Sendable,
    Failure: Swift.Error & Sendable
>: Sendable {

    var cancel: Their.HubCancel?
    var inputs = DrainQueue<EvolvedHubInput<Input, InputFailure>>()
    var isRunning = false
    var latestValue: Value?
    /// Subscriber ids that joined a live `replayLatest` lifecycle and have not
    /// yet received any value. A processed `.replay` delivers `latestValue` to
    /// ids still in this set; a live broadcast removes every recipient, so a
    /// joiner whose replay is still queued behind that broadcast does not
    /// receive the same value twice.
    var pendingReplays = Set<UUID>()
    var state: State
    var subscribers = [UUID: HubEngineSubscription<Value, Failure>]()
    /// Generation token incremented per upstream lifecycle start so that
    /// late callbacks delivered after a lifecycle restart (last subscriber
    /// unsubscribed, then a new first subscriber arrived) are dropped at
    /// enqueue time and stale in-flight transforms are dropped at commit
    /// time. The token is `Int` for simplicity; overflow is not a practical
    /// concern for this counter.
    var token = 0

    /// Detaches all lifecycle owners for post-lock cleanup. The current
    /// drainer keeps its claim across reset/restart, and the generation token
    /// advances only when a new upstream subscription starts.
    mutating func reset(initialState: State) -> Self {
        let retired = self
        _ = inputs.takePending()
        self = Self(
            inputs: inputs,
            state: initialState,
            token: token
        )
        return retired
    }
}

/// Owner of one shared evolved `Hub` lifecycle. Mirrors the `JobEngine`
/// execution model: inputs — upstream events and replay requests — enter a
/// private FIFO queue under the `Lock`, a single drainer reduces them in
/// append order, and all user code (the `transform`, the `failure` mapping and
/// subscriber sinks) runs with the lock released. The drainer dequeues an
/// input together with a copy of `State`, the generation token and the
/// recipient snapshot — the subscribers registered at dequeue time — runs the
/// transform on the copy, and commits only while the same generation is still
/// running. A subscriber that joins mid-transform is not in the recipient
/// snapshot, so the in-flight value does not reach it. Upstream events are
/// processed by this single drainer, so a terminal event (`.finished` / `.failure`) is
/// FIFO-ordered behind earlier values and can never interrupt a transform;
/// only a cancel-driven teardown (last unsubscribe, `cancel()`) or a restart
/// can race an in-flight transform, and the commit guard then drops that
/// emission and its state write instead of corrupting the new generation.
private final class EvolvedHubState<
    Input: Sendable,
    InputFailure: Swift.Error & Sendable,
    Value: Sendable,
    State: Sendable,
    Failure: Swift.Error & Sendable
>: Sendable {

    private let failure: @Sendable (InputFailure) -> Failure
    private let initialState: State
    private let lock: Their.Lock<EvolvedHubRecord<Input, InputFailure, Value, State, Failure>>
    private let replayLatest: Bool
    private let transform: @Sendable (inout State, Input) -> Value?
    private let upstream: Their.Hub<Input, InputFailure>

    init(
        failure: @escaping @Sendable (InputFailure) -> Failure,
        initialState: State,
        replayLatest: Bool,
        transform: @escaping @Sendable (inout State, Input) -> Value?,
        upstream: Their.Hub<Input, InputFailure>
    ) {
        self.failure = failure
        self.initialState = initialState
        lock = Their.Lock(
            EvolvedHubRecord(
                state: initialState
            )
        )
        self.replayLatest = replayLatest
        self.transform = transform
        self.upstream = upstream
    }

    func cancel() {
        let retired = lock.withLock { record in
            record.reset(initialState: initialState)
        }
        // State, replay values, queued inputs and subscriber captures can run
        // user destructors. Keep every detached owner alive until after unlock.
        withExtendedLifetime(retired) {
            retired.cancel?()
        }
    }

    private func drain() {
        while true {
            let step: (
                input: EvolvedHubInput<Input, InputFailure>,
                snapshot: [UUID: HubEngineSubscription<Value, Failure>],
                state: State,
                token: Int
            )? = lock.withLock { record in
                guard let input = record.inputs.popFirst(isActive: record.isRunning) else {
                    return nil
                }
                return (input, record.subscribers, record.state, record.token)
            }
            guard let step else {
                return
            }
            process(
                step.input,
                snapshot: step.snapshot,
                state: step.state,
                token: step.token
            )
        }
    }

    func handle(_ event: Their.HubEvent<Input, InputFailure>, token: Int) {
        let shouldDrain: Bool = lock.withLock { record in
            guard record.isRunning, record.token == token else {
                return false
            }
            return record.inputs.append(.upstream(event))
        }
        guard shouldDrain else {
            return
        }
        drain()
    }

    #if DEBUG
    func isLockAvailableForTests() -> Bool {
        lock.withLockIfAvailable { _ in true } ?? false
    }
    #endif

    private func process(
        _ input: EvolvedHubInput<Input, InputFailure>,
        snapshot: [UUID: HubEngineSubscription<Value, Failure>],
        state: State,
        token: Int
    ) {
        switch input {
        case .replay(let id):
            let emission: (subscription: HubEngineSubscription<Value, Failure>, value: Value)? = lock.withLock { record in
                guard record.isRunning, record.token == token else {
                    return nil
                }
                guard record.pendingReplays.remove(id) != nil else {
                    return nil
                }
                guard let value = record.latestValue, let subscription = record.subscribers[id] else {
                    return nil
                }
                return (subscription, value)
            }
            guard let emission else {
                return
            }
            emission.subscription.emit(.value(emission.value))
        case .upstream(.finished):
            guard let retired = takeTerminatingRecord(token: token) else {
                return
            }
            withExtendedLifetime(retired) {
                for subscription in retired.subscribers.values {
                    subscription.emit(.finished)
                }
            }
        case .upstream(.failure(let inputFailure)):
            guard let retired = takeTerminatingRecord(token: token) else {
                return
            }
            withExtendedLifetime(retired) {
                let mappedFailure = failure(inputFailure)
                for subscription in retired.subscribers.values {
                    subscription.emit(.failure(mappedFailure))
                }
            }
        case .upstream(.value(let input)):
            var evolvingState = state
            let output = transform(&evolvingState, input)
            let commit: (
                emission: (subscriptions: [HubEngineSubscription<Value, Failure>], value: Value)?,
                previousState: State,
                previousValue: Value?
            )? = lock.withLock { record in
                guard record.isRunning, record.token == token else {
                    return nil
                }
                let previousState = record.state
                let previousValue = record.latestValue
                record.state = evolvingState
                guard let output else {
                    return (nil, previousState, previousValue)
                }
                if replayLatest {
                    record.latestValue = output
                }
                // The recipients are the subscribers captured when this input
                // was dequeued — before the transform — so a subscriber that
                // joined mid-transform does not see this in-flight value, and
                // the evolution stays live-only. Pending replays are dropped
                // only for those recipients (a replay would now be a
                // duplicate for them); a mid-transform joiner keeps its queued
                // replay and receives the new `latestValue` through it.
                record.pendingReplays.subtract(snapshot.keys)
                return ((Array(snapshot.values), output), previousState, previousValue)
            }
            // In particular, replacing latestValue may release the last
            // reference to a previous output whose destructor calls cancel.
            defer { withExtendedLifetime(commit) {} }
            guard let emission = commit?.emission else {
                return
            }
            for subscription in emission.subscriptions {
                subscription.emit(.value(emission.value))
            }
        }
    }

    func subscribe(
        _ sink: @escaping Their.HubSink<Value, Failure>
    ) -> Their.HubCancel {
        let id = UUID()
        let subscription = HubEngineSubscription(sink: sink)
        let registration: (
            retired: EvolvedHubRecord<Input, InputFailure, Value, State, Failure>?,
            shouldDrain: Bool,
            start: Bool,
            token: Int
        ) = lock.withLock { record in
            let start = record.isRunning == false
            let retired = start ? record.reset(initialState: initialState) : nil
            if start {
                record.isRunning = true
                record.token += 1
            }
            record.subscribers[id] = subscription
            var shouldDrain = false
            if start == false, replayLatest {
                record.pendingReplays.insert(id)
                shouldDrain = record.inputs.append(.replay(id))
            }
            return (retired, shouldDrain, start, record.token)
        }
        defer { withExtendedLifetime(registration.retired) {} }
        if registration.shouldDrain {
            drain()
        }
        if registration.start {
            let token = registration.token
            let cancel = upstream.subscribe { [weak self] event in
                self?.handle(event, token: token)
            }
            let shouldCancelImmediately = lock.withLock { record in
                guard record.isRunning, record.token == token else {
                    return true
                }
                record.cancel = cancel
                return false
            }
            if shouldCancelImmediately {
                cancel()
            }
        }
        return { [subscription, weak self] in
            subscription.cancel()
            self?.unsubscribe(id: id)
        }
    }

    /// Tears down the running generation identified by `token` for an upstream
    /// terminal event (`.finished` / `.failure`): clears the lifecycle under the lock —
    /// including replay storage and shared `State` — and returns the detached
    /// record. Its owners remain alive until the terminal snapshot is delivered
    /// outside the lock. Returns `nil` if the generation is no longer running.
    private func takeTerminatingRecord(token: Int) -> EvolvedHubRecord<Input, InputFailure, Value, State, Failure>? {
        lock.withLock { record in
            guard record.isRunning, record.token == token else {
                return nil
            }
            return record.reset(initialState: initialState)
        }
    }

    func unsubscribe(id: UUID) {
        let removal: (
            retired: EvolvedHubRecord<Input, InputFailure, Value, State, Failure>?,
            subscription: HubEngineSubscription<Value, Failure>
        )? = lock.withLock { record in
            guard let subscription = record.subscribers.removeValue(forKey: id) else {
                return nil
            }
            record.pendingReplays.remove(id)
            guard record.subscribers.isEmpty else {
                return (nil, subscription)
            }
            return (record.reset(initialState: initialState), subscription)
        }
        withExtendedLifetime(removal) {
            removal?.retired?.cancel?()
        }
    }
}

#if DEBUG
/// Constructs the real private evolution with weak observation links, so the
/// test seam neither pins its lifetime nor needs to reproduce a recursive-lock
/// trap when a regression moves a destructor back under the lock.
func makeEvolvedHubForTests<
    Input: Sendable,
    Value: Sendable,
    State: Sendable,
    Failure: Swift.Error & Sendable
>(
    initial initialState: State,
    replayLatest: Bool = false,
    upstream: Their.Hub<Input, Failure>,
    value: @escaping @Sendable (inout State, Input) -> Value?
) -> (
    cancelState: Their.HubCancel,
    hub: Their.Hub<Value, Failure>,
    isLockAvailable: @Sendable () -> Bool
) {
    let state = EvolvedHubState<Input, Failure, Value, State, Failure>(
        failure: { $0 },
        initialState: initialState,
        replayLatest: replayLatest,
        transform: value,
        upstream: upstream
    )
    return (
        cancelState: { [weak state] in state?.cancel() },
        hub: Their.Hub(
            logging: upstream.logging.withoutTopLevel,
            misuseHandler: upstream.misuseHandler,
            misuseLocation: Their.MisuseLocation(),
            onDeinit: state.cancel,
            onSubscribe: state.subscribe(_:)
        ),
        isLockAvailable: { [weak state] in state?.isLockAvailableForTests() ?? true }
    )
}
#endif

public extension Their.Hub {

    /// Root shared stateful evolution: derives a new `Hub` whose values are
    /// produced from the upstream `Hub` through one shared evolution `State`.
    /// This is the canonical shared sink-level evolution; the failure-preserving
    /// `evolve(initial:_:)` overload and the stateless `map` / `mapError` shapes
    /// are thin convenience wrappers over it, so this header owns the contract
    /// they all inherit.
    ///
    /// Transform and shared state: while the derived hub lifecycle is active, one
    /// `State` seeded from `initial` is shared by every subscriber. Each upstream
    /// `.value` calls `value(&state, value)`; returning a `NewValue` broadcasts
    /// it to the current subscribers, returning `nil` suppresses the broadcast
    /// while keeping the state mutation. `failure` maps the upstream `Failure` into the
    /// derived `NewFailure`.
    ///
    /// Execution: inputs — upstream events and `replayLatest` replay requests —
    /// enter a private FIFO queue and a single drainer reduces them in order;
    /// the `value` transform, the `failure` mapping and subscriber sinks all run
    /// with the internal lock released, so the transform may safely cancel a
    /// derived subscription synchronously — re-entrant inputs join the
    /// in-progress drain instead of deadlocking. A value's recipients are the
    /// subscribers registered when its input is dequeued, before the transform
    /// runs, so a subscriber joining mid-transform does not see that in-flight
    /// value (`shareLatest` replays it through the queue instead). An upstream
    /// terminal event (`.finished` / `.failure`) is processed by the same drainer, so
    /// it is FIFO-ordered behind earlier values and never overtakes or
    /// interrupts them; only a cancel-driven teardown or restart can land while
    /// a transform is in flight, and that drops the in-flight emission and its
    /// state write. Replaced and detached state, replay values, queued inputs,
    /// upstream handles and subscriber captures are released outside the lock;
    /// their destructors may synchronously cancel a derived subscription.
    ///
    /// Multi-subscriber lifecycle and restart: the first subscriber starts the
    /// upstream subscription with a fresh `State`; later subscribers join the
    /// same live state. When the last subscriber unsubscribes, the upstream
    /// subscription is cancelled, queued inputs are dropped and `State` is
    /// reset; a later first subscriber starts a brand-new lifecycle with a fresh
    /// `State`. Each start bumps a generation token, so a late upstream callback
    /// delivered after a restart is dropped instead of corrupting the new
    /// generation.
    ///
    /// Terminal end: an upstream `.finished` is broadcast once to the current
    /// subscribers, bypassing the transform, then the lifecycle terminates
    /// exactly like terminal failure below — including clearing any
    /// `replayLatest` storage, so a later first subscriber starts a fresh
    /// lifecycle with no replay.
    ///
    /// Terminal failure: an upstream `.failure` is mapped through `failure`, broadcast
    /// once to the current subscribers, then the lifecycle terminates — the
    /// subscribers are cleared, the upstream cancel is released, queued inputs
    /// are dropped and `State` is reset. A later subscription starts a fresh
    /// lifecycle.
    ///
    /// Cancel, deinit and logging: the derived `Hub` is built on the secondary
    /// `Hub` initializer, so it carries an explicit `onDeinit` that cancels the
    /// shared upstream subscription and resets state, and it inherits the `Hub`
    /// cancel/pin contract. Derived hubs do not copy the root's active `.topLevel`
    /// `LifecycleLogging` (they pass `logging.withoutTopLevel`).
    ///
    /// Replay: the base `evolve` is live-only. The shared internal seam takes a
    /// `replayLatest` flag that stores the latest broadcast value and replays it
    /// to a late joiner through the same FIFO queue, so replay is ordered with
    /// live broadcasts and delivered exactly once; `shareLatest()` is the only
    /// operator that enables it. Tested in `HubEvolutionTests`.
    ///
    /// - Parameters:
    ///   - failure: Transforms an upstream `Failure` into the derived hub's failure.
    ///   - initialState: Initial state, reset whenever the shared lifecycle ends.
    ///   - value: Updates `State` and returns the next derived value, or `nil`
    ///     to suppress the broadcast.
    func evolve<NewValue: Sendable, NewFailure: Swift.Error & Sendable, State: Sendable>(
        fileID: String = #fileID,
        failure: @escaping @Sendable (Failure) -> NewFailure,
        function: String = #function,
        initial initialState: State,
        line: UInt = #line,
        value: @escaping @Sendable (inout State, Value) -> NewValue?
    ) -> Their.Hub<NewValue, NewFailure> {
        evolve(
            fileID: fileID,
            failure: failure,
            function: function,
            initial: initialState,
            line: line,
            replayLatest: false,
            value: value
        )
    }

    /// Shared stateful evolution that keeps the upstream failure type unchanged.
    /// Convenience shape over the root `evolve`; see its header for the shared
    /// lifecycle, restart, cancellation and terminal contract.
    func evolve<NewValue: Sendable, State: Sendable>(
        fileID: String = #fileID,
        function: String = #function,
        initial initialState: State,
        line: UInt = #line,
        _ transform: @escaping @Sendable (inout State, Value) -> NewValue?
    ) -> Their.Hub<NewValue, Failure> {
        evolve(
            fileID: fileID,
            failure: { $0 },
            function: function,
            initial: initialState,
            line: line,
            value: transform
        )
    }

    /// Internal seam shared by every public `Hub` evolution operator.
    internal func evolve<NewValue: Sendable, NewFailure: Swift.Error & Sendable, State: Sendable>(
        fileID: String,
        failure: @escaping @Sendable (Failure) -> NewFailure,
        function: String,
        initial initialState: State,
        line: UInt,
        replayLatest: Bool,
        value: @escaping @Sendable (inout State, Value) -> NewValue?
    ) -> Their.Hub<NewValue, NewFailure> {
        let misuseLocation = Their.MisuseLocation(
            fileID: fileID,
            function: function,
            line: line
        )
        let state = EvolvedHubState(
            failure: failure,
            initialState: initialState,
            replayLatest: replayLatest,
            transform: value,
            upstream: self
        )
        return Their.Hub<NewValue, NewFailure>(
            logging: logging.withoutTopLevel,
            misuseHandler: misuseHandler,
            misuseLocation: misuseLocation,
            onDeinit: state.cancel,
            onSubscribe: { sink in
                state.subscribe(sink)
            }
        )
    }
}
