import Foundation

private enum MergedJobLifecycleState: Sendable {

    case pending
    case started
    case terminated
    case terminating
}

private enum MergedJobProcessOutput<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    case finished(endedCancel: Their.WorkCancel?, record: MergedJobRecord<Value, Failure>)
    case failure(failure: Failure, record: MergedJobRecord<Value, Failure>)
    case release(Their.WorkCancel?)
    case value(sink: Their.JobSink<Value, Failure>, value: Value)
}

private struct MergedJobInput<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    let event: Their.JobEvent<Value, Failure>
    let upstreamIndex: Int
}

private struct MergedJobRecord<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    var cancels: [Their.WorkCancel?]
    var endedCount = 0
    var endedUpstreams: [Bool]
    var hasPendingFailure = false
    var inputs = DrainQueue<MergedJobInput<Value, Failure>>()
    var lifecycleState = MergedJobLifecycleState.pending
    var sink: Their.JobSink<Value, Failure>?

    init(upstreamCount: Int) {
        cancels = Array(repeating: nil, count: upstreamCount)
        endedUpstreams = Array(repeating: false, count: upstreamCount)
    }

    /// Moves retired ownership out for post-lock release. A pending terminal
    /// keeps its sink cancellable in the owner until upstream teardown returns.
    mutating func takeTerminated(pendingTerminal: Bool = false) -> Self {
        let detached = self
        cancels = Array(repeating: nil, count: cancels.count)
        _ = inputs.takePending()
        lifecycleState = pendingTerminal ? .terminating : .terminated
        sink = pendingTerminal ? detached.sink : nil
        return detached
    }
}

/// Owner of one merged `Job` lifecycle. Upstream events from every source enter
/// one FIFO queue, and a single drainer delivers them in append order.
/// Enqueuing failure closes the subscription loop without changing that FIFO.
/// Terminal processing enters `.terminating` until post-lock teardown returns;
/// cancellation can clear the pending terminal sink before its final claim.
private final class MergedJobState<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    private let lock: Their.Lock<MergedJobRecord<Value, Failure>>
    private let misuseHandler: Their.MisuseHandler
    private let misuseLocation: Their.MisuseLocation
    private let upstreams: [Their.Job<Value, Failure>]

    init(
        misuseHandler: @escaping Their.MisuseHandler,
        misuseLocation: Their.MisuseLocation,
        upstreams: [Their.Job<Value, Failure>]
    ) {
        lock = Their.Lock(.init(upstreamCount: upstreams.count))
        self.misuseHandler = misuseHandler
        self.misuseLocation = misuseLocation
        self.upstreams = upstreams
    }

    func cancel() {
        let detached: MergedJobRecord<Value, Failure>? = lock.withLock { record in
            switch record.lifecycleState {
            case .pending, .terminated:
                return nil
            case .started, .terminating:
                return record.takeTerminated()
            }
        }
        withExtendedLifetime(detached) {
            detached?.cancels.forEach { $0?() }
        }
    }

    private func drain() {
        while true {
            let input: MergedJobInput<Value, Failure>? = lock.withLock { record in
                guard let input = record.inputs.popFirst(isActive: record.lifecycleState == .started) else {
                    return nil
                }
                return input
            }
            guard let input else {
                return
            }
            process(input)
        }
    }

    func handle(_ event: Their.JobEvent<Value, Failure>, upstreamIndex: Int) {
        let shouldDrain = lock.withLock { record in
            guard record.lifecycleState == .started else {
                return false
            }
            if case .failure = event {
                // Accepting failure stops new source starts immediately, while
                // delivery still waits behind every earlier queued value.
                record.hasPendingFailure = true
            }
            return record.inputs.append(
                MergedJobInput(
                    event: event,
                    upstreamIndex: upstreamIndex
                )
            )
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

    private func process(_ input: MergedJobInput<Value, Failure>) {
        let output: MergedJobProcessOutput<Value, Failure>? = lock.withLock { record in
            guard record.lifecycleState == .started else {
                return nil
            }
            switch input.event {
            case .finished:
                // An upstream delivers at most one terminal event, so each
                // `.finished` input counts exactly one upstream. The ended
                // upstream's cancel slot is extracted for outside-lock release — its
                // lifecycle is already terminated, keeping the cancel would
                // only pin a dead chain until the merged terminal.
                let endedCancel = record.cancels[input.upstreamIndex]
                record.cancels[input.upstreamIndex] = nil
                record.endedCount += 1
                record.endedUpstreams[input.upstreamIndex] = true
                guard record.endedCount == upstreams.count else {
                    return .release(endedCancel)
                }
                return .finished(
                    endedCancel: endedCancel,
                    record: record.takeTerminated(pendingTerminal: true)
                )
            case .failure(let failure):
                // Detach every upstream cancel for unconditional teardown.
                // Keep terminal delivery cancellable until that work returns.
                return .failure(
                    failure: failure,
                    record: record.takeTerminated(pendingTerminal: true)
                )
            case .value(let value):
                guard let sink = record.sink else {
                    return nil
                }
                return .value(
                    sink: sink,
                    value: value
                )
            }
        }
        guard let output else {
            return
        }
        switch output {
        case .finished(let endedCancel, let record):
            withExtendedLifetime((endedCancel, record)) {
                record.cancels.forEach { $0?() }
                takeTerminalSink()?(.finished)
            }
        case .failure(let failure, let record):
            withExtendedLifetime(record) {
                record.cancels.forEach { $0?() }
                takeTerminalSink()?(.failure(failure))
            }
        case .release(let cancel):
            withExtendedLifetime(cancel) {}
        case .value(let sink, let value):
            sink(.value(value))
        }
    }

    func subscribe(
        _ sink: @escaping Their.JobSink<Value, Failure>
    ) -> Their.WorkCancel? {
        guard upstreams.isEmpty == false else {
            misuseHandler(
                .init(
                    message: "Merged Job requires at least one upstream.",
                    origin: .init(),
                    trace: [misuseLocation]
                )
            )
            return nil
        }
        let shouldStart = lock.withLock { record in
            guard record.lifecycleState == .pending else {
                return false
            }
            record.cancels = Array(repeating: nil, count: upstreams.count)
            record.endedCount = 0
            record.endedUpstreams = Array(repeating: false, count: upstreams.count)
            _ = record.inputs.takePending()
            record.lifecycleState = .started
            record.sink = sink
            return true
        }
        guard shouldStart else {
            misuseHandler(
                .init(
                    message: "Merged Job supports only one subscriber per lifecycle.",
                    origin: .init(),
                    trace: [misuseLocation]
                )
            )
            return nil
        }
        for index in upstreams.indices {
            let shouldSubscribe = lock.withLock { record in
                record.lifecycleState == .started && record.hasPendingFailure == false
            }
            guard shouldSubscribe else {
                break
            }
            let cancel = upstreams[index].subscribe { [weak self] event in
                self?.handle(event, upstreamIndex: index)
            }
            let shouldCancelImmediately = lock.withLock { record in
                // An upstream that already delivered its `.finished` keeps its
                // cancel slot released: storing the cancel here again would
                // resurrect the pin of a dead chain. Invoking the cancel
                // instead is a no-op on the terminated upstream and drops the
                // pin synchronously.
                guard record.lifecycleState == .started,
                      record.hasPendingFailure == false,
                      record.endedUpstreams[index] == false
                else {
                    return true
                }
                record.cancels[index] = cancel
                return false
            }
            if shouldCancelImmediately {
                cancel()
            }
        }
        return { [weak self] in
            self?.cancel()
        }
    }

    /// Upstream teardown may cancel this subscription reentrantly. Claim the
    /// terminal sink only after it returns, while still using the owner lock.
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
/// Constructs the real merge state and Job ownership path with a weak,
/// nonblocking lock probe for destructor-reentry regression tests.
func makeMergedJobForTests<Value: Sendable, Failure: Swift.Error & Sendable>(
    _ jobs: [Their.Job<Value, Failure>]
) -> (isLockAvailable: @Sendable () -> Bool, job: Their.Job<Value, Failure>) {
    let misuseHandler = jobs.first?.misuseHandler ?? Their.MisuseHandlers.fatal
    let misuseLocation = Their.MisuseLocation()
    let state = MergedJobState(
        misuseHandler: misuseHandler,
        misuseLocation: misuseLocation,
        upstreams: jobs
    )
    return (
        isLockAvailable: { [weak state] in state?.isLockAvailableForTests() ?? true },
        job: Their.Job(
            misuseHandler: misuseHandler,
            misuseLocation: misuseLocation,
            onDeinit: state.cancel,
            onSubscribe: state.subscribe(_:)
        )
    )
}
#endif

public extension Their.Job {

    /// Merges a dynamic set of same-typed upstream jobs into one single-subscriber
    /// `Job`. Every upstream must already share this job's `Value` and `Failure`
    /// types; callers should use `map` / `mapError` first when combining
    /// domain-specific sources into one input enum.
    ///
    /// Subscription starts upstream jobs once, in array order, before
    /// `subscribe` returns, stopping if failure or cancellation intervenes.
    /// Successful values from all upstreams enter one FIFO
    /// queue and are delivered downstream in append order; values from the same
    /// upstream keep their emission order.
    ///
    /// Terminal end: the merged job ends when the last upstream ends — a union
    /// is alive while any upstream can still produce an event. An individual
    /// upstream `.finished` is silent downstream: it releases that upstream's cancel
    /// slot (the chain is already terminated, keeping the cancel would only pin
    /// it) and shrinks the live set. The `.finished` of the last live upstream is
    /// terminal: it is FIFO-ordered behind values queued before it, delivers
    /// one downstream `.finished` and tears the merged lifecycle down. A synchronous
    /// end is counted when dequeued, including while subscription is in progress;
    /// the remaining upstreams must still start before all can have ended.
    ///
    /// Terminal failure: a failure from any upstream is a queued input processed
    /// by the same single drainer, so values enqueued before it are still
    /// delivered first and the failure never overtakes them; inputs queued after
    /// the failure are dropped. Processing the failure cancels every upstream
    /// whose cancel is already stored exactly once, then claims its terminal
    /// sink. Cancellation reentered from that teardown clears the pending sink
    /// and suppresses that callback. Enqueuing a failure stops further
    /// subscription-loop iterations immediately, even while an earlier callback
    /// holds the drainer; the current upstream's late-returned cancel is invoked
    /// immediately when its `subscribe` returns, which can be after the terminal
    /// callback, so that teardown cannot suppress delivery. Late values or
    /// failures after cancel or terminal failure are ignored; as everywhere in
    /// TheirCore, a downstream sink callback already claimed by the drainer is not
    /// interrupted and may overlap a concurrent cancel.
    ///
    /// Misuse: `Job.merge([])` is invalid and reports `Misuse` on subscribe.
    /// Like every `Job`, the merged job is single-subscriber and
    /// single-lifecycle; a second subscribe or any subscribe after termination
    /// reports `Misuse` and returns a non-pinning inert cancel. The merged
    /// wrapper inherits the first upstream's `MisuseHandler` (or
    /// `MisuseHandlers.fatal` for an empty list) unless the explicit-handler
    /// overload is used. An upstream that cannot be subscribed — already
    /// claimed, already terminated, or passed twice in the same list — reports
    /// its own `Misuse` through its own handler and contributes no events; the
    /// merged job continues with the remaining upstreams. Such an upstream also
    /// never contributes an `.finished`, so a merged job containing one cannot end
    /// successfully — consistent with "contributes no events", and irrelevant
    /// in production where the default `MisuseHandler` is fatal.
    static func merge(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line,
        _ jobs: [Their.Job<Value, Failure>]
    ) -> Their.Job<Value, Failure> {
        let misuseHandler = jobs.first?.misuseHandler ?? Their.MisuseHandlers.fatal
        return merge(
            fileID: fileID,
            function: function,
            line: line,
            misuseHandler: misuseHandler,
            jobs
        )
    }

    /// Merges jobs using an explicit `MisuseHandler` for the merged wrapper.
    static func merge(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line,
        misuseHandler: @escaping Their.MisuseHandler,
        _ jobs: [Their.Job<Value, Failure>]
    ) -> Their.Job<Value, Failure> {
        let misuseLocation = Their.MisuseLocation(
            fileID: fileID,
            function: function,
            line: line
        )
        let state = MergedJobState(
            misuseHandler: misuseHandler,
            misuseLocation: misuseLocation,
            upstreams: jobs
        )
        return Their.Job<Value, Failure>(
            misuseHandler: misuseHandler,
            misuseLocation: misuseLocation,
            onDeinit: state.cancel,
            onSubscribe: state.subscribe(_:)
        )
    }

    /// Variadic convenience over `Job.merge(_:)`.
    static func merge(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line,
        _ jobs: Their.Job<Value, Failure>...
    ) -> Their.Job<Value, Failure> {
        merge(
            fileID: fileID,
            function: function,
            line: line,
            jobs
        )
    }
}
