import Foundation

/// Internal engine for one finite event-producing lifecycle, with FIFO report
/// ordering.
///
/// All mutable state lives behind a single `Their.Lock`, and user code never runs
/// under that lock. Each report is appended to a private FIFO queue; a single
/// drainer then reduces inputs one at a time and runs the resulting effects
/// (`WorkCancel` / sink callbacks) after releasing the lock.
/// Dequeued inputs and discarded queues are retained through post-lock effects
/// so payload destructors can safely re-enter report/stop/terminate as well.
///
/// States:
/// - `.idle`: `start()` has not claimed the lifecycle.
/// - `.starting`: `start()` is calling `work(report:)` outside the lock, so
///   `work` may report synchronously. Values reported here are delivered; a
///   terminal outcome (`.finished` / `.failure`) terminates the lifecycle before
///   `work` returns its cancel.
/// - `.active(cancel:)`: `work` returned its `WorkCancel`; lifecycle is running.
/// - `.terminated`: terminal. Late reports produce ignored debug messages.
///
/// Ordering: the linearization point is the append under `state`. Reports are
/// delivered in append order and a later caller never overtakes an earlier
/// queued input. Successful end and failure are report inputs, so they stay
/// ordered relative to values. The cancel returned by `work` is queued as
/// `.startReturned`, so a synchronous terminal outcome during `.starting` is
/// processed before that cancel is committed: the `.startReturned` input then
/// observes `.terminated` and invokes the cancel.
///
/// `stop()` / `terminate()` / `deinit` are immediate control commands, not
/// queued inputs. They clear the queue, move an active lifecycle to
/// `.terminated`, and invoke the active `WorkCancel` before returning. Queued
/// values are dropped, but any queued `.startReturned` cancel is preserved and
/// invoked so a `work` resource cannot leak after the queue is cleared. A sink
/// callback already running on the drainer is not interrupted and may overlap a
/// control effect, so `sink` and `WorkCancel` must tolerate concurrent
/// invocation.
///
/// All three control paths share one transition and cleanup reducer. They keep
/// distinct diagnostic policies: `stop()` means an outside owner asked the
/// lifecycle to end, while `terminate()` means an internal/lifecycle path
/// decided it is done. Active `deinit` is silent; starting `deinit` reports its
/// special diagnostic. Sharing the mechanics does not merge these contracts.
///
/// Threading: effects run synchronously on the thread that enqueues
/// the triggering input — the engine never hops to `main` or dispatches to a
/// queue, so it adds no main-thread work of its own and runs wherever the
/// caller already is. The `Lock` is an `os_unfair_lock` with tiny critical
/// sections (append / pop / one reduce step) and user code always runs outside
/// it. The first caller to find the queue idle becomes the drainer and delivers
/// every input queued while it drains, including ones appended by other
/// threads; concurrent callers append and return immediately. Because the lock
/// is released before effects run, a `sink` callback may safely re-enter the
/// engine (`emit` / `stop` / `terminate`): a re-entrant report joins the
/// in-progress drain instead of starting a nested one.
///
/// `start()` returns `true` when it claimed the `.idle` lifecycle and invoked
/// `work`; it returns `false` (and reports a `Misuse`) when the engine is not
/// idle, so `work` runs at most once.
final class JobEngine<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    private let misuseHandler: Their.MisuseHandler
    private let misuseLocation: Their.MisuseLocation
    private let sink: JobEngineSink<Value, Failure>
    private let state: Their.Lock<JobEngineRecord<Value, Failure>>
    private let work: Their.Work<Value, Failure>

    /// - Important: The `work` and `state` defaults exist so tests can construct
    ///   a `JobEngine` directly in `.idle`, `.starting`, `.active(cancel:)` or
    ///   `.terminated` without supplying real upstream work. Production callers
    ///   always pass both; treat the defaults as test scaffolding.
    init(
        misuseHandler: @escaping Their.MisuseHandler = Their.MisuseHandlers.fatal,
        misuseLocation: Their.MisuseLocation = .init(),
        sink: @escaping JobEngineSink<Value, Failure>,
        state: JobEngineState = .idle,
        work: @escaping Their.Work<Value, Failure> = { _ in {} }
    ) {
        self.misuseHandler = misuseHandler
        self.misuseLocation = misuseLocation
        self.sink = sink
        self.state = Their.Lock(.init(engineState: state))
        self.work = work
    }

    deinit {
        control(.deinitialize)
    }

    private func control(_ kind: JobEngineControlKind) {
        perform(reduceControl(kind))
    }

    private func drain() {
        while true {
            let step: (effects: [JobEngineEffect<Value, Failure>], input: JobEngineInput<Value, Failure>)? = state.withLock { record in
                guard let input = record.inputs.popFirst() else {
                    return nil
                }
                return (Self.reduce(input, state: &record.engineState), input)
            }
            guard let step else {
                return
            }
            // Even an ignored input may own a destructor with callbacks.
            withExtendedLifetime(step.input) {
                perform(step.effects)
            }
        }
    }

    func emit(failure: Failure) {
        enqueue(.report(.failure(failure)))
    }

    func emit(value: Value) {
        enqueue(.report(.value(value)))
    }

    func emitFinished() {
        enqueue(.report(.finished))
    }

    private func enqueue(_ input: JobEngineInput<Value, Failure>) {
        let shouldDrain = state.withLock { record in
            record.inputs.append(input)
        }
        guard shouldDrain else {
            return
        }
        drain()
    }

    func getState() -> JobEngineState {
        state.withLock { record in
            record.engineState
        }
    }

    #if DEBUG
    func isLockAvailableForTests() -> Bool {
        state.withLockIfAvailable { _ in true } ?? false
    }
    #endif

    private func makeReport() -> Their.WorkReport<Value, Failure> {
        { [weak self] output in
            self?.enqueue(.report(output))
        }
    }

    private func perform(_ effects: [JobEngineEffect<Value, Failure>]) {
        for effect in effects {
            switch effect {
            case .cancel(let cancel):
                cancel()
            case .releaseInputs(let inputs):
                withExtendedLifetime(inputs) {}
            case .sink(let event):
                sink(event)
            }
        }
    }

    private static func reduce(
        _ input: JobEngineInput<Value, Failure>,
        state: inout JobEngineState
    ) -> [JobEngineEffect<Value, Failure>] {
        switch input {
        case .report(.finished):
            return reduceTerminal(.finished, ignoredMessage: .emitFinishedIgnoredBecauseStateIsNotActive, state: &state)
        case .report(.failure(let failure)):
            return reduceTerminal(.failure(failure), ignoredMessage: .emitFailureIgnoredBecauseStateIsNotActive, state: &state)
        case .report(.value(let value)):
            switch state {
            case .active, .starting:
                return [.sink(.value(value))]
            case .idle, .terminated:
                return [.sink(.message(.emitOutputIgnoredBecauseStateIsNotActive))]
            }
        case .startReturned(let cancel):
            switch state {
            case .starting:
                state = .active(cancel: cancel)
                return []
            case .terminated:
                return [.cancel(cancel)]
            // Unreachable: `.startReturned` is enqueued once by `start()` right
            // after `work` returns, when the engine is either still `.starting`
            // or already `.terminated` (a control command or failure raced in).
            // `start()` moved it out of `.idle`, and only this input produces
            // `.active`, so neither state can be observed here.
            case .active, .idle:
                return []
            }
        }
    }

    private func reduceControl(_ kind: JobEngineControlKind) -> [JobEngineEffect<Value, Failure>] {
        state.withLock { record in
            let pendingStartCancelEffects = record.takeInputCleanupEffects()
            let messages = kind.messages
            let message: JobEngineMessage?
            var effects: [JobEngineEffect<Value, Failure>] = []
            switch record.engineState {
            case .active(let cancel):
                record.engineState = .terminated
                effects.append(.cancel(cancel))
                message = messages.active
            case .starting:
                record.engineState = .terminated
                message = messages.starting
            case .idle, .terminated:
                message = messages.inactive
            }
            effects += pendingStartCancelEffects
            if let message {
                effects.append(.sink(.message(message)))
            }
            return effects
        }
    }

    private static func reduceTerminal(
        _ event: JobEngineEvent<Value, Failure>,
        ignoredMessage: JobEngineMessage,
        state: inout JobEngineState
    ) -> [JobEngineEffect<Value, Failure>] {
        switch state {
        case .active(let cancel):
            state = .terminated
            return [.cancel(cancel), .sink(event)]
        case .starting:
            state = .terminated
            return [.sink(event)]
        case .idle, .terminated:
            return [.sink(.message(ignoredMessage))]
        }
    }

    func start() -> Bool {
        let shouldStart = state.withLock { record in
            switch record.engineState {
            case .idle:
                record.engineState = .starting
                return true
            case .active, .starting, .terminated:
                return false
            }
        }
        guard shouldStart else {
            misuseHandler(
                Their.Misuse(
                    message: "JobEngine cannot start because it is not idle.",
                    origin: .init(),
                    trace: [misuseLocation]
                )
            )
            return false
        }
        let report = makeReport()
        let cancel = work(report)
        enqueue(.startReturned(cancel))
        return true
    }

    func stop() {
        control(.stop)
    }

    func terminate() {
        control(.terminate)
    }
}

private enum JobEngineControlKind: Sendable {

    case deinitialize
    case stop
    case terminate

    var messages: (active: JobEngineMessage?, inactive: JobEngineMessage, starting: JobEngineMessage) {
        switch self {
        case .deinitialize:
            return (nil, .deinitIgnoredBecauseStateIsNotActive, .deinitStoppedWhileStarting)
        case .stop:
            return (.stopStopped, .stopIgnoredBecauseStateIsNotActive, .stopStopped)
        case .terminate:
            return (.terminateStopped, .terminateIgnoredBecauseStateIsNotActive, .terminateStopped)
        }
    }
}

private enum JobEngineEffect<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    case cancel(Their.WorkCancel)
    case releaseInputs(InputQueue<JobEngineInput<Value, Failure>>)
    case sink(JobEngineEvent<Value, Failure>)
}

private enum JobEngineInput<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    case report(Their.WorkOutput<Value, Failure>)
    case startReturned(Their.WorkCancel)
}

private struct JobEngineRecord<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    var engineState: JobEngineState
    var inputs = DrainQueue<JobEngineInput<Value, Failure>>()

    mutating func takeInputCleanupEffects() -> [JobEngineEffect<Value, Failure>] {
        let inputs = self.inputs.takePending()
        // Retain all discarded payloads in the post-lock effects, not only
        // start-returned cancels. Their destructors may report or stop again.
        var effects: [JobEngineEffect<Value, Failure>] = [.releaseInputs(inputs)]
        for input in inputs.pending {
            switch input {
            case .report:
                break
            case .startReturned(let cancel):
                effects.append(.cancel(cancel))
            }
        }
        return effects
    }
}
