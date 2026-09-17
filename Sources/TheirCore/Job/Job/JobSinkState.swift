import Foundation

/// Reference-typed owner of a single `JobSink` slot protected by a `Their.Lock`. The class is `Sendable` because its
/// only stored property is a `Lock`, which is `@unchecked Sendable` for `~Copyable` values by construction; no
/// additional `@unchecked` is needed at this layer.
///
/// Same shape as `HubJobStateStorage` / `WeakValueCacheStorage`: a thin class wrapper that exists only so the
/// `~Copyable` `Lock` can be shared by reference across the closures captured by `Job` and its `Work`/cancel paths.
final class JobSinkState<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    private let sink = Their.Lock<Their.JobSink<Value, Failure>?>(nil)

    func clearSink() {
        let detached = takeSink()
        // A captured object's destructor may reenter subscription cancellation.
        // Taking the closure keeps that destruction outside the sink lock.
        withExtendedLifetime(detached) {}
    }

    func getSink() -> Their.JobSink<Value, Failure>? {
        sink.withLock { sink in
            sink
        }
    }

    func setSinkIfEmpty(
        _ sink: @escaping Their.JobSink<Value, Failure>
    ) -> Bool {
        self.sink.withLock { currentSink in
            guard currentSink == nil else {
                return false
            }
            currentSink = sink
            return true
        }
    }

    func takeSink() -> Their.JobSink<Value, Failure>? {
        sink.withLock { sink in
            let currentSink = sink
            sink = nil
            return currentSink
        }
    }
}
