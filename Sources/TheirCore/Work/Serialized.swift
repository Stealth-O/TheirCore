import Foundation

extension Their {

    /// Wraps an upstream `Work` so its `WorkReport` is serialized through a `Task` chain that TheirCore owns.
    ///
    /// The default `WorkReport` contract is: the upstream calls `report` from a serial context (its delegate queue,
    /// `OperationQueue`, etc.). If your upstream can legitimately call `report` from multiple threads concurrently —
    /// rare in practice for iOS SDKs, but possible for custom multi-thread producers — wrap it with `Their.serialized(_:)`.
    ///
    /// The wrapper installs one `Lock<Task<Void, Never>?>` per `Work` invocation. Each `report` call appends a new
    /// `Task` to that chain via `await previousTask?.value`, so the inner `JobEngine`/`HubEngine` receives events in
    /// the order `serialReport` was called even if the wrapped upstream fires from many threads at once.
    /// Calling the returned `WorkCancel` closes the wrapper before invoking the upstream cancel, so future reports are
    /// ignored and queued reports that have not reached the downstream `report` yet become no-ops.
    ///
    /// Example:
    ///
    /// ```swift
    /// let job = Their.Job(work: Their.serialized { report in
    ///     someConcurrentSDK.subscribe { event in
    ///         report(.value(event))
    ///     }
    ///     return someConcurrentSDK.cancel
    /// })
    /// ```
    ///
    /// Cost: one short-lived `Task` per emitted event and a small `Lock` per active `Work`. For typical iOS sources
    /// — which are inherently serial — this overhead is unnecessary and should be avoided by **not** wrapping.
    ///
    /// Tested in `SerializedTests` (call-order FIFO through the chain, a queued report becoming a no-op after cancel via
    /// the DEBUG-only `SerializedWorkTestHooks` seam, a single upstream cancel) and stressed in `TheirCoreSharedStressTests`.
    public static func serialized<Value: Sendable, Failure: Swift.Error & Sendable>(
        _ work: @escaping Their.Work<Value, Failure>
    ) -> Their.Work<Value, Failure> {
        { report in
            let storage = SerializedWorkStorage<Value, Failure>()
            let serialReport: Their.WorkReport<Value, Failure> = { output in
                storage.append(output, report: report)
            }
            let cancel = work(serialReport)
            return {
                guard storage.cancel() else {
                    return
                }
                cancel()
            }
        }
    }
}

private struct SerializedWorkState: Sendable {

    var isCancelled = false
    var previousTask: Task<Void, Never>?
}

private final class SerializedWorkStorage<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    private let state = Their.Lock(SerializedWorkState())

    func append(
        _ output: Their.WorkOutput<Value, Failure>,
        report: @escaping Their.WorkReport<Value, Failure>
    ) {
        state.withLock { record in
            guard record.isCancelled == false else {
                return
            }
            let priorTask = record.previousTask
            // The `Task` is created while the lock is held so the chain link
            // (`priorTask` -> new task) is installed atomically with the state
            // update; the task body never runs synchronously on this thread,
            // so no user code executes under the lock.
            let task = Task { [self, priorTask] in
                await priorTask?.value
                guard isActive() else {
                    #if DEBUG
                    SerializedWorkTestHooks.didSkipQueuedReport?()
                    #endif
                    return
                }
                report(output)
            }
            record.previousTask = task
        }
    }

    /// Closes the wrapper exactly once. Returns whether this call performed
    /// the close — the caller then invokes the upstream cancel. Queued chain
    /// tasks are not cancelled: awaiting a `Task<Void, Never>.value` is not
    /// interruptible anyway, so each pending task simply observes
    /// `isActive() == false` and skips its `report`.
    func cancel() -> Bool {
        state.withLock { record in
            guard record.isCancelled == false else {
                return false
            }
            record.isCancelled = true
            record.previousTask = nil
            return true
        }
    }

    private func isActive() -> Bool {
        state.withLock { record in
            record.isCancelled == false
        }
    }
}

#if DEBUG
/// Task-scoped observation of a chained report that was skipped because the
/// wrapper was cancelled after that report had been queued. The binding is
/// inherited by the chain `Task` created in `append`, so a test can pin the
/// "queued report becomes a no-op" contract with a signal instead of polling.
enum SerializedWorkTestHooks {

    @TaskLocal
    static var didSkipQueuedReport: (@Sendable () -> Void)?
}
#endif
