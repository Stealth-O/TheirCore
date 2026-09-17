import Foundation

public extension Their.Job {

    /// Adapts this `Job` into an `AsyncStream` of `JobEvent`s. This is the
    /// canonical stream adapter — there is no separate stream factory; build
    /// streams from `Their.Job(...).stream()`.
    ///
    /// One lifecycle per stream value: the stream subscribes to this existing
    /// `Job` (it does not create a second lifecycle primitive), so one returned
    /// `AsyncStream` is backed by one lifecycle. Copying the `AsyncStream` value
    /// does not start a new lifecycle, and multiple iterators over the same value
    /// share stream state and compete for elements — this is not broadcast
    /// subscription. Because `Job` is one-lifecycle and single-subscriber, a
    /// caller that needs another independent stream must create another `Job`.
    ///
    /// Events: each `.value` is yielded; a terminal `.finished` or `.failure` is
    /// yielded and then finishes the stream, so `for await` ends naturally on
    /// both successful and failed completion.
    ///
    /// Eager start and pin: creating the stream subscribes inline, before
    /// `stream()` returns. The stored `WorkCancel` retains the originating root
    /// or derived `Job`, so rvalue pipelines like
    /// `for await event in Their.Job(...).evolve(...).stream()` keep the upstream
    /// alive while any stream copy or iterator owns the shared stream context.
    /// Releasing its last owner or cancelling suspended iteration runs
    /// `onTermination`, which releases the cancel and pin synchronously. If a
    /// terminal event arrives inside `subscribe(_:)`, termination is recorded
    /// first and the cancel returned afterwards is invoked exactly once.
    ///
    /// Buffering: the adapter deliberately uses `AsyncStream`'s default
    /// unbounded buffer so no `JobEvent` is dropped. A retained stream must be
    /// consumed promptly, reach a terminal event or be released; retaining it
    /// without consuming a live producer lets buffered events accumulate.
    func stream() -> AsyncStream<Their.JobEvent<Value, Failure>> {
        makeSubscriptionStream(
            isTerminal: { event in
                switch event {
                case .finished, .failure:
                    return true
                case .value:
                    return false
                }
            },
            subscribe: subscribe(_:)
        )
    }
}
