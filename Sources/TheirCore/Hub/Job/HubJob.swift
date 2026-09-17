import Foundation

struct HubJobState: Sendable {

    var cancel: Their.HubCancel?
    var isSubscribed = false
    var isTerminal = false
}

/// Reference wrapper around a `Lock<HubJobState>`, used because `Lock` is
/// `~Copyable` and the protected state must be shared by reference across
/// the subscribe / cancel / report closures captured by `Hub.job()`. Same
/// indirection pattern as `WeakValueCacheStorage`.
final class HubJobStateStorage: Sendable {

    let lock = Their.Lock(HubJobState())
}

public extension Their.Hub {

    /// Adapts this shared `Hub` into a single-subscriber `Job` facade — the
    /// canonical bridge from a hot multi-subscriber `Hub` to a cold one-consumer
    /// `Job`. The returned `Job` retains the source `Hub` while it is alive, so a
    /// caller can drop the originating `Hub` reference and keep observing events;
    /// derived from the root `Hub`, it does not copy active `.topLevel`
    /// `LifecycleLogging`.
    ///
    /// Lifecycle: the returned `Job` follows the standard `Job` one-lifecycle,
    /// single-subscriber contract. The first `subscribe` installs the sink and
    /// joins the hub; a second concurrent or later `subscribe` on the same `Job`
    /// reports a `Misuse` (forwarded to the configured `MisuseHandler`, default
    /// `MisuseHandlers.fatal`, with this call's creation location in the trace)
    /// and returns an inert cancel.
    ///
    /// Events and termination: hub `.value`/`.finished`/`.failure` map one-to-one onto
    /// the `Job`. A hub `.finished` or `.failure` is delivered once and makes the
    /// bridge terminal — later hub events are dropped. If a terminal event
    /// arrives reentrantly during `subscribe`, before the hub cancel is stored,
    /// that cancel is invoked immediately so the hub subscription does not
    /// leak.
    ///
    /// Cancel and deinit: cancelling the returned cancel (or releasing the `Job`)
    /// clears the sink and unsubscribes from the hub synchronously; events that
    /// arrive after cancel or terminal failure are dropped by the bridge.
    func job(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line
    ) -> Their.Job<Value, Failure> {
        let hub = self
        let logging = self.logging.withoutTopLevel
        let misuseHandler = self.misuseHandler
        let misuseLocation = Their.MisuseLocation(
            fileID: fileID,
            function: function,
            line: line
        )
        let state = HubJobStateStorage()
        let cancelSubscription: Their.WorkCancel = {
            let cancel: Their.HubCancel? = state.lock.withLock { state in
                guard state.isTerminal == false else {
                    return nil
                }
                state.isSubscribed = false
                state.isTerminal = true
                let cancel = state.cancel
                state.cancel = nil
                return cancel
            }
            cancel?()
        }
        return Their.Job(
            logging: logging,
            misuseHandler: misuseHandler,
            misuseLocation: misuseLocation,
            onDeinit: cancelSubscription,
            onSubscribe: { sink in
                let shouldSubscribe = state.lock.withLock { state in
                    guard state.isSubscribed == false, state.isTerminal == false else {
                        return false
                    }
                    state.isSubscribed = true
                    return true
                }
                guard shouldSubscribe else {
                    misuseHandler(
                        .init(
                            message: "Job supports only one subscriber per lifecycle.",
                            origin: .init(),
                            trace: [misuseLocation]
                        )
                    )
                    return nil
                }
                let cancel = hub.subscribe { event in
                    Self.report(
                        event: event,
                        sink: sink,
                        state: state
                    )
                }
                let shouldCancel = state.lock.withLock { state in
                    guard state.isSubscribed, state.isTerminal == false else {
                        return true
                    }
                    state.cancel = cancel
                    return false
                }
                if shouldCancel {
                    cancel()
                }
                return cancelSubscription
            }
        )
    }

    private static func report(
        event: Their.HubEvent<Value, Failure>,
        sink: @escaping Their.JobSink<Value, Failure>,
        state: HubJobStateStorage
    ) {
        switch event {
        case .finished:
            let shouldSend = state.lock.withLock { state in
                guard state.isSubscribed, state.isTerminal == false else {
                    return false
                }
                state.cancel = nil
                state.isSubscribed = false
                state.isTerminal = true
                return true
            }
            if shouldSend {
                sink(.finished)
            }
        case .failure(let failure):
            let shouldSend = state.lock.withLock { state in
                guard state.isSubscribed, state.isTerminal == false else {
                    return false
                }
                state.cancel = nil
                state.isSubscribed = false
                state.isTerminal = true
                return true
            }
            if shouldSend {
                sink(.failure(failure))
            }
        case .value(let value):
            let shouldSend = state.lock.withLock { state in
                state.isSubscribed && state.isTerminal == false
            }
            if shouldSend {
                sink(.value(value))
            }
        }
    }
}
