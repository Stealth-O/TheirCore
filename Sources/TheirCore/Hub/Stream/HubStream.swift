import Foundation

public extension Their.Hub {

    /// Adapts this `Hub` into an `AsyncStream` of `HubEvent`s. The stream is one
    /// subscriber on the shared lifecycle: it joins this `Hub` like any other
    /// subscriber, so it follows the base-`Hub` live-only rule (use
    /// `shareLatest()` upstream for replay). Copying the `AsyncStream` value does
    /// not add a subscriber, and multiple iterators over the same value share
    /// stream state and compete for elements — this is not a second subscription.
    ///
    /// Events: each `.value` is yielded; a terminal `.finished` or `.failure` is
    /// yielded and then finishes the stream, so `for await` ends naturally on
    /// both successful and failed completion.
    ///
    /// Eager start and pin: creating the stream subscribes inline, before
    /// `stream()` returns. The stored `HubCancel` retains the originating root
    /// or derived `Hub`, so rvalue pipelines like
    /// `for await event in Their.Hub(...).evolve(...).stream()` keep the upstream
    /// alive while any stream copy or iterator owns the shared stream context.
    /// Releasing its last owner or cancelling suspended iteration runs
    /// `onTermination`, which releases the cancel and pin synchronously and,
    /// when this was the last subscriber, stops the shared lifecycle. If a
    /// terminal event arrives inside `subscribe(_:)`, termination is recorded
    /// first and the cancel returned afterwards is invoked exactly once.
    ///
    /// Buffering: the adapter deliberately uses `AsyncStream`'s default
    /// unbounded buffer so no `HubEvent` is dropped. A retained stream must be
    /// consumed promptly, reach a terminal event or be released; retaining it
    /// without consuming a live producer lets buffered events accumulate.
    func stream() -> AsyncStream<Their.HubEvent<Value, Failure>> {
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
