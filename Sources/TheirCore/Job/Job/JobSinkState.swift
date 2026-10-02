import Foundation

/// Reference-typed owner of a single-use `JobSink` slot protected by a TheirCore `Lock`. The class is `Sendable` because its
/// only stored property is a `Lock`, which is `@unchecked Sendable` for `~Copyable` values by construction; no
/// additional `@unchecked` is needed at this layer.
///
/// Same shape as `HubJobStateStorage` / `WeakValueCacheStorage`: a thin class wrapper that exists only so the
/// `~Copyable` `Lock` can be shared by reference across the closures captured by `Job` and its `Work`/cancel paths.
/// Taking/clearing the sink permanently closes the slot before releasing its
/// captures. A destructor reentering subscribe cannot temporarily install a new
/// sink while the old engine is still completing cancellation.
final class JobSinkState<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    private let sink = Their.Lock<JobSinkSlot<Value, Failure>>(.open)

    func clearSink() {
        let detached = takeSink()
        // A captured object's destructor may reenter subscription cancellation.
        // Taking the closure keeps that destruction outside the sink lock.
        withExtendedLifetime(detached) {}
    }

    func getSink() -> Their.JobSink<Value, Failure>? {
        sink.withLock { sink in
            guard case .subscribed(let callback) = sink else {
                return nil
            }
            return callback
        }
    }

    func setSinkIfEmpty(
        _ sink: @escaping Their.JobSink<Value, Failure>
    ) -> Bool {
        self.sink.withLock { currentSink in
            guard case .open = currentSink else {
                return false
            }
            currentSink = .subscribed(sink)
            return true
        }
    }

    func takeSink() -> Their.JobSink<Value, Failure>? {
        sink.withLock { sink in
            let callback: Their.JobSink<Value, Failure>?
            switch sink {
            case .closed, .open:
                callback = nil
            case .subscribed(let current):
                callback = current
            }
            sink = .closed
            return callback
        }
    }
}

private enum JobSinkSlot<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    case closed
    case open
    case subscribed(Their.JobSink<Value, Failure>)
}
