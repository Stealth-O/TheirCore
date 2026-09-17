import Foundation

/// One `HubEngine` subscriber slot. `emit` delivers each event until `cancel()`
/// — or a delivered terminal `.finished` / `.failure` — deactivates it, after which
/// further events are dropped. The optional sink is the whole state: non-nil
/// means active, nil means cancelled or terminal. Cancel and terminal delivery
/// remove the slot's sink ownership while locked, then release its captures
/// outside the lock. A callback already taken by `emit` may finish after cancel
/// and keeps its captures alive until it returns. The owning engine serializes
/// events; the slot only synchronizes cancellation against that delivery.
final class HubEngineSubscription<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    private let sink: Their.Lock<Their.HubSink<Value, Failure>?>

    init(
        sink: @escaping Their.HubSink<Value, Failure>
    ) {
        self.sink = Their.Lock(sink)
    }

    func cancel() {
        let releasedSink = sink.withLock { stored in
            let released = stored
            stored = nil
            return released
        }
        // Capture deinit may synchronously re-enter cancel on this same slot.
        withExtendedLifetime(releasedSink) {}
    }

    func emit(_ event: Their.HubEvent<Value, Failure>) {
        let callback: Their.HubSink<Value, Failure>? = sink.withLock { stored in
            guard let callback = stored else {
                return nil
            }
            switch event {
            case .finished, .failure:
                stored = nil
            case .value:
                break
            }
            return callback
        }
        callback?(event)
    }
}
