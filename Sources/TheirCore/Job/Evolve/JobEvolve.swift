import Foundation

/// Single-consumer lifecycle marker for an `EvolvedJobRecord`. Kept private
/// here rather than as a TheirCore-wide enum because no other type depends on it;
/// `JobEngineState` has its own `idle`/`active(cancel:)`/`terminated`.
private enum EvolvedJobLifecycleState: Sendable {

    case pending
    case started
    case terminated
    case terminating
}

/// Post-lock action computed by the value branch of `EvolvedJobState.process`.
/// Carries the value sink (and, for a value-driven failure, the live upstream cancel)
/// out of the lock so the emission and the upstream teardown run with the lock
/// released. A terminal sink stays cancellable until teardown has finished.
private enum EvolvedJobValueAction<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    case emit(sink: Their.JobSink<Value, Failure>, value: Value)
    case failure(cancel: Their.WorkCancel?, failure: Failure)
}

/// Internal vocabulary of an evolution's value transform. Public `evolve` only
/// ever produces `.emit` / `.suppress`; the `.failure` outcome is the seam that lets
/// `tryMap` turn one upstream value into a terminal failure that also
/// cancels the still-live upstream. Kept here so the shared `EvolvedJobState`
/// owns a single value-processing path for both shapes.
enum EvolvedJobValueOutcome<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    case emit(Value)
    case failure(Failure)
    case suppress
}

private struct EvolvedJobRecord<
    Input: Sendable,
    InputFailure: Swift.Error & Sendable,
    Value: Sendable,
    State: Sendable,
    Failure: Swift.Error & Sendable
>: Sendable {

    var cancel: Their.WorkCancel?
    var inputs = DrainQueue<Their.JobEvent<Input, InputFailure>>()
    var lifecycleState = EvolvedJobLifecycleState.pending
    var sink: Their.JobSink<Value, Failure>?
    var state: State

    /// Detach retired ownership for post-lock release. Terminal processing
    /// keeps a cancellable sink in the owner until mapping/teardown finishes;
    /// cancellation closes it immediately instead.
    mutating func terminate(initialState: State, pendingTerminal: Bool = false) -> Self {
        let detached = self
        _ = inputs.takePending()
        self = Self(
            inputs: inputs,
            lifecycleState: pendingTerminal ? .terminating : .terminated,
            sink: pendingTerminal ? detached.sink : nil,
            state: initialState
        )
        return detached
    }
}

/// Owner of one evolved `Job` lifecycle. Mirrors the `JobEngine` execution
/// model: upstream events enter a private FIFO queue under the `Lock`, a
/// single drainer reduces them in append order, and all user code — the
/// `transform`, the `failure` mapping and the downstream sink — runs with the
/// lock released. The drainer copies `State` out, runs the transform on the
/// copy, and commits the result back only while the lifecycle is still
/// `.started`. Upstream events are processed by this single drainer, so a
/// terminal event (`.finished` / `.failure`) is FIFO-ordered behind earlier values and
/// can never interrupt a transform; only a concurrent `cancel()` can land
/// mid-transform, and the commit guard then drops that emission and its state
/// write instead of resurrecting stale state.
///
/// The transform returns an `EvolvedJobValueOutcome`: `.emit` and `.suppress`
/// are the `evolve`/`map` shapes, while `.failure` is the `tryMap` seam that
/// turns one upstream value into a terminal failure. A value-driven `.failure`
/// terminates the lifecycle and additionally cancels the still-live upstream —
/// unlike an upstream `.finished` / `.failure`, where the source has already terminated
/// itself and its stored cancel is merely dropped.
/// Terminal processing first enters `.terminating`: inputs close, while the
/// owner retains a cancellable terminal sink. Mapping and teardown through an
/// already-stored upstream cancel run before the final sink claim; a cancel
/// during either suppresses delivery. A not-yet-stored cancel is invoked when
/// upstream subscription returns, without delaying terminal delivery.
/// Claiming that sink changes the state to `.terminated` under the same lock.
private final class EvolvedJobState<
    Input: Sendable,
    InputFailure: Swift.Error & Sendable,
    Value: Sendable,
    State: Sendable,
    Failure: Swift.Error & Sendable
>: Sendable {

    private let failure: @Sendable (InputFailure) -> Failure
    private let initialState: State
    private let lock: Their.Lock<EvolvedJobRecord<Input, InputFailure, Value, State, Failure>>
    private let misuseHandler: Their.MisuseHandler
    private let misuseLocation: Their.MisuseLocation
    private let transform: @Sendable (inout State, Input) -> EvolvedJobValueOutcome<Value, Failure>
    private let upstream: Their.Job<Input, InputFailure>

    init(
        failure: @escaping @Sendable (InputFailure) -> Failure,
        initialState: State,
        transform: @escaping @Sendable (inout State, Input) -> EvolvedJobValueOutcome<Value, Failure>,
        misuseHandler: @escaping Their.MisuseHandler,
        misuseLocation: Their.MisuseLocation,
        upstream: Their.Job<Input, InputFailure>
    ) {
        self.failure = failure
        self.initialState = initialState
        lock = Their.Lock(
            EvolvedJobRecord(
                state: initialState
            )
        )
        self.misuseHandler = misuseHandler
        self.misuseLocation = misuseLocation
        self.transform = transform
        self.upstream = upstream
    }

    func cancel() {
        let detached: EvolvedJobRecord<Input, InputFailure, Value, State, Failure>? = lock.withLock { record in
            switch record.lifecycleState {
            case .pending, .terminated:
                return nil
            case .started, .terminating:
                return record.terminate(initialState: initialState)
            }
        }
        withExtendedLifetime(detached) {
            detached?.cancel?()
        }
    }

    private func drain() {
        while true {
            let step: (input: Their.JobEvent<Input, InputFailure>, state: State)? = lock.withLock { record in
                guard let input = record.inputs.popFirst(isActive: record.lifecycleState == .started) else {
                    return nil
                }
                return (input, record.state)
            }
            guard let step else {
                return
            }
            process(step.input, state: step.state)
        }
    }

    func handle(_ event: Their.JobEvent<Input, InputFailure>) {
        let shouldDrain: Bool = lock.withLock { record in
            guard record.lifecycleState == .started else {
                return false
            }
            return record.inputs.append(event)
        }
        guard shouldDrain else {
            return
        }
        drain()
    }

    #if DEBUG
    /// Nonblocking observation for destructor-lifetime tests. Unlike calling
    /// `cancel` reentrantly, this can report a held lock without stopping the
    /// test runner on `os_unfair_lock`'s recursive-lock trap.
    func isLockAvailableForTests() -> Bool {
        lock.withLockIfAvailable { _ in true } ?? false
    }
    #endif

    private func process(_ event: Their.JobEvent<Input, InputFailure>, state: State) {
        // A value commit may replace the last stored reference to the previous
        // State. Keep the dequeued copy alive through every post-lock effect.
        defer { withExtendedLifetime(state) {} }
        switch event {
        case .finished:
            let detached = takeStartedRecord()
            withExtendedLifetime(detached) {
                takeTerminalSink()?(.finished)
            }
        case .failure(let inputFailure):
            guard let detached = takeStartedRecord() else {
                return
            }
            withExtendedLifetime(detached) {
                let mappedFailure = failure(inputFailure)
                takeTerminalSink()?(.failure(mappedFailure))
            }
        case .value(let input):
            var detached: EvolvedJobRecord<Input, InputFailure, Value, State, Failure>?
            var evolvingState = state
            let outcome = transform(&evolvingState, input)
            let action: EvolvedJobValueAction<Value, Failure>? = lock.withLock { record in
                guard record.lifecycleState == .started else {
                    return nil
                }
                switch outcome {
                case .emit(let value):
                    record.state = evolvingState
                    guard let sink = record.sink else {
                        return nil
                    }
                    return .emit(sink: sink, value: value)
                case .failure(let failure):
                    detached = record.terminate(initialState: initialState, pendingTerminal: true)
                    return .failure(cancel: detached?.cancel, failure: failure)
                case .suppress:
                    record.state = evolvingState
                    return nil
                }
            }
            withExtendedLifetime(detached) {
                guard let action else {
                    return
                }
                switch action {
                case .emit(let sink, let value):
                    sink(.value(value))
                case .failure(let cancel, let failure):
                    cancel?()
                    takeTerminalSink()?(.failure(failure))
                }
            }
        }
    }

    func subscribe(
        _ sink: @escaping Their.JobSink<Value, Failure>
    ) -> Their.WorkCancel? {
        let shouldStart = lock.withLock { record in
            guard record.lifecycleState == .pending else {
                return false
            }
            record.lifecycleState = .started
            record.sink = sink
            record.state = initialState
            return true
        }
        guard shouldStart else {
            misuseHandler(
                .init(
                    message: "Evolved Job supports only one subscriber per lifecycle.",
                    origin: .init(),
                    trace: [misuseLocation]
                )
            )
            return nil
        }
        let cancel = upstream.subscribe { [weak self] event in
            self?.handle(event)
        }
        let shouldCancelImmediately = lock.withLock { record in
            guard record.lifecycleState == .started else {
                return true
            }
            record.cancel = cancel
            return false
        }
        if shouldCancelImmediately {
            cancel()
        }
        return { [weak self] in
            self?.cancel()
        }
    }

    private func takeStartedRecord() -> EvolvedJobRecord<Input, InputFailure, Value, State, Failure>? {
        lock.withLock { record in
            guard record.lifecycleState == .started else {
                return nil
            }
            return record.terminate(initialState: initialState, pendingTerminal: true)
        }
    }

    /// Claim delivery only after user mapping/teardown has returned. A cancel
    /// during that work changes .terminating to .terminated and clears the sink.
    /// Once claimed, the callback has the same in-flight ownership as a value.
    private func takeTerminalSink() -> Their.JobSink<Value, Failure>? {
        lock.withLock { record in
            guard record.lifecycleState == .terminating else {
                return nil
            }
            let sink = record.sink
            record.lifecycleState = .terminated
            record.sink = nil
            return sink
        }
    }
}

#if DEBUG
/// Builds the real evolution state and standard `Job` ownership path while
/// exposing only a nonblocking lock probe to `@testable` lifetime tests. The
/// private state type and public `Job` facade remain unchanged.
func makeEvolvedJobForTests<
    Input: Sendable,
    Value: Sendable,
    State: Sendable,
    Failure: Swift.Error & Sendable
>(
    initial initialState: State,
    upstream: Their.Job<Input, Failure>,
    value: @escaping @Sendable (inout State, Input) -> Value?
) -> (isLockAvailable: @Sendable () -> Bool, job: Their.Job<Value, Failure>) {
    let misuseLocation = Their.MisuseLocation()
    let state = EvolvedJobState<Input, Failure, Value, State, Failure>(
        failure: { $0 },
        initialState: initialState,
        transform: { state, input in
            guard let output = value(&state, input) else {
                return .suppress
            }
            return .emit(output)
        },
        misuseHandler: upstream.misuseHandler,
        misuseLocation: misuseLocation,
        upstream: upstream
    )
    return (
        isLockAvailable: state.isLockAvailableForTests,
        job: Their.Job(
            misuseHandler: upstream.misuseHandler,
            misuseLocation: misuseLocation,
            onDeinit: state.cancel,
            onSubscribe: state.subscribe(_:)
        )
    )
}
#endif

public extension Their.Job {

    /// Root stateful evolution: derives a new `Job` whose values are produced
    /// from the upstream `Job` through one owned evolution `State`. This is the
    /// canonical sink-level evolution; the failure-preserving
    /// `evolve(initial:_:)` overload and the stateless `map` / `mapError`
    /// shapes are thin convenience wrappers over it, so this header owns the
    /// contract they all inherit.
    ///
    /// Transform and state: the derived lifecycle owns one `State` seeded from
    /// `initial`. Each upstream `.value` calls `value(&state, value)`; returning
    /// a `NewValue` emits it downstream, returning `nil` suppresses emission
    /// while keeping the state mutation. `failure` maps the upstream `Failure` into the
    /// derived `NewFailure`.
    ///
    /// Execution: upstream events enter a private FIFO queue and a single
    /// drainer reduces them in order; the `value` transform, the `failure` mapping
    /// and the downstream sink all run with the internal lock released, so the
    /// transform may safely cancel the derived subscription synchronously —
    /// re-entrant inputs join the in-progress drain instead of deadlocking. An
    /// upstream terminal event (`.finished` / `.failure`) is processed by the same
    /// drainer, so it is FIFO-ordered behind earlier values and never overtakes
    /// or interrupts them; only a cancel that lands while the transform is in
    /// flight drops that emission and its state write.
    ///
    /// Lifecycle: the derived `Job` is single-subscriber and single-lifecycle,
    /// like any other `Job`. The first `subscribe` installs the sink and
    /// subscribes upstream; a second `subscribe`, or any `subscribe` after the
    /// lifecycle ended, reports a `Misuse` (forwarded to the upstream's own
    /// `MisuseHandler`, with this evolution's creation location in the trace) and
    /// returns an inert cancel without starting a second upstream subscription.
    ///
    /// Terminal end: an upstream `.finished` bypasses the transform and closes the
    /// lifecycle through the same terminal transition as failure below, then
    /// claims the downstream sink for one `.finished`. The transform never sees
    /// terminal events; a derived "last word" belongs to a materialized pipeline,
    /// not to `evolve`.
    ///
    /// Terminal failure: an upstream `.failure` closes input, clears the upstream
    /// cancel, drops queued inputs and resets `State` to `initial`. It then
    /// maps through `failure` outside the lock and claims the downstream sink for
    /// one failure. A cancel during mapping clears the pending sink and
    /// suppresses that terminal callback. Value-driven failures from
    /// `tryMap` use the same claim after teardown through an already-stored
    /// upstream cancel, so cancellation reentered from that teardown suppresses
    /// delivery. If the upstream cancel is not stored yet, the failure may be
    /// delivered first; the late-returned cancel is invoked when upstream
    /// subscription returns and cannot suppress an already-delivered callback.
    /// A callback already claimed by the drainer may finish during concurrent
    /// cancellation.
    /// Later values, ends or failures are suppressed.
    ///
    /// Cancel and deinit: cancelling the derived subscription (or releasing the
    /// derived `Job`) cancels the upstream subscription and resets `State`, and
    /// it inherits `Job`'s cancel/pin contract, so a retained cancel keeps a
    /// `Their.Job(...).evolve(...).subscribe(...)` chain alive. If an upstream
    /// terminal event arrives reentrantly during `subscribe`, before the upstream
    /// cancel closure has been stored, that cancel is invoked immediately rather
    /// than dropped, so no upstream resource leaks.
    /// State, subscriber captures and queued inputs detached by cancellation or
    /// termination are released outside the internal lock; their destructors
    /// may safely reenter cancellation. A State copy or sink already used by an
    /// in-flight transform/callback remains alive until that operation returns.
    ///
    /// Prefer `evolve` over a custom `Job` for any pure event-to-state
    /// derivation, and keep side effects (SDK calls, persistence, cancelling
    /// other resources) outside the transform. Tested in `JobEvolutionTests`.
    ///
    /// - Parameters:
    ///   - failure: Transforms an upstream `Failure` into the derived job's failure.
    ///   - initialState: Initial state, reset after the derived lifecycle ends.
    ///   - value: Updates `State` and returns the next derived value, or `nil`
    ///     to suppress emission.
    func evolve<NewValue: Sendable, NewFailure: Swift.Error & Sendable, State: Sendable>(
        fileID: String = #fileID,
        failure: @escaping @Sendable (Failure) -> NewFailure,
        function: String = #function,
        initial initialState: State,
        line: UInt = #line,
        value: @escaping @Sendable (inout State, Value) -> NewValue?
    ) -> Their.Job<NewValue, NewFailure> {
        let misuseLocation = Their.MisuseLocation(
            fileID: fileID,
            function: function,
            line: line
        )
        return evolveOutcome(
            failure: failure,
            initialState: initialState,
            misuseLocation: misuseLocation
        ) { state, input in
            guard let output = value(&state, input) else {
                return .suppress
            }
            return .emit(output)
        }
    }

    /// Stateful evolution that keeps the upstream failure type unchanged.
    /// Convenience shape over the root `evolve`; see its header for the
    /// lifecycle, cancellation and terminal contract.
    func evolve<NewValue: Sendable, State: Sendable>(
        fileID: String = #fileID,
        function: String = #function,
        initial initialState: State,
        line: UInt = #line,
        _ transform: @escaping @Sendable (inout State, Value) -> NewValue?
    ) -> Their.Job<NewValue, Failure> {
        evolve(
            fileID: fileID,
            failure: { $0 },
            function: function,
            initial: initialState,
            line: line,
            value: transform
        )
    }

    /// Internal construction seam shared by `evolve` and `tryMap`. Builds
    /// one `EvolvedJobState` from an outcome-returning transform so both the
    /// `.emit` / `.suppress` evolutions and the `.failure` value-to-terminal map run
    /// on the same single-drainer machinery. Not public: `EvolvedJobValueOutcome`
    /// is an internal vocabulary and the public surface stays `evolve` / `map` /
    /// `tryMap`.
    internal func evolveOutcome<NewValue: Sendable, NewFailure: Swift.Error & Sendable, State: Sendable>(
        failure: @escaping @Sendable (Failure) -> NewFailure,
        initialState: State,
        misuseLocation: Their.MisuseLocation,
        outcome: @escaping @Sendable (inout State, Value) -> EvolvedJobValueOutcome<NewValue, NewFailure>
    ) -> Their.Job<NewValue, NewFailure> {
        let state = EvolvedJobState(
            failure: failure,
            initialState: initialState,
            transform: outcome,
            misuseHandler: misuseHandler,
            misuseLocation: misuseLocation,
            upstream: self
        )
        return Their.Job<NewValue, NewFailure>(
            misuseHandler: misuseHandler,
            misuseLocation: misuseLocation,
            onDeinit: state.cancel,
            onSubscribe: state.subscribe(_:)
        )
    }
}
