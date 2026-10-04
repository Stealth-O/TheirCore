import Foundation

extension Their {

    /// Cancellation policy for awaiting one value from a job.
    public enum JobAwaitCancellation: Sendable {

        /// Keeps a bounded operation subscribed until a matching value or a
        /// terminal event, even if its waiting task is cancelled. Use only when
        /// abandoning an already submitted operation would lose its receipt.
        case awaitResult
        /// Cancels the subscription and throws `CancellationError` when the
        /// waiting task is cancelled before a result has been selected.
        case cancelSource
    }

    /// A job finished successfully without producing a matching value.
    public struct JobValueUnavailable: Error, Sendable {

        public init() {}
    }
}

public extension Their.Job {

    /// Awaits the first matching value and releases this single-use job's
    /// subscription. Source failures are thrown unchanged; successful finish
    /// before a match throws `Their.JobValueUnavailable`.
    ///
    /// A task already cancelled on entry never subscribes, for either policy.
    /// Otherwise `awaitResult` ignores later task cancellation and keeps the
    /// source pinned until a matching value, failure or finish. It is reserved
    /// for bounded work such as a submitted device command whose acknowledgement
    /// must be retained. It does not impose a timeout or clear Task.isCancelled;
    /// the source must provide its own bounded terminal outcome. `cancelSource`
    /// is the default for ordinary cancellable reads.
    ///
    /// The first result or cancellation to claim the waiter wins. Predicates
    /// execute on the reporting thread, outside the waiter's lock; an in-flight
    /// predicate may finish after cancellation. Cancellation, subscription
    /// teardown and continuation resumption all run outside that lock.
    ///
    /// Synchronous emission or cancellation before subscribe returns is safe:
    /// the late subscription handle is released exactly once. The job retains
    /// its normal single-subscriber/misuse contract; this is not a second stream
    /// or a way to reuse an already consumed job.
    func firstValue(
        cancellation: Their.JobAwaitCancellation = .cancelSource,
        where predicate: @escaping @Sendable (Value) -> Bool = { _ in true }
    ) async throws -> Value {
        try Task.checkCancellation()
        let owner = JobValueAwaiter<Value>()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                #if DEBUG
                JobFirstValueTestHooks.beforeRegister?()
                #endif
                guard owner.register(continuation) else { return }
                owner.resource.set(subscribe { event in
                    switch event {
                    case .failure(let failure):
                        owner.finish(.failure(failure))
                    case .finished:
                        owner.finish(.failure(Their.JobValueUnavailable()))
                    case .value(let value):
                        if predicate(value) { owner.finish(.success(value)) }
                    }
                })
            }
        } onCancel: {
            if case .cancelSource = cancellation { owner.finish(.failure(CancellationError())) }
        }
    }
}

private final class JobValueAwaiter<Value: Sendable>: Sendable {

    private let lock = Their.Lock(JobValueAwaitState<Value>())
    let resource = Their.Resource<Their.WorkCancel>(release: { $0() })

    func finish(_ result: Result<Value, any Error>) {
        let continuation = lock.withLock { state -> CheckedContinuation<Value, any Error>? in
            guard state.result == nil else { return nil }
            state.result = result
            let continuation = state.continuation
            state.continuation = nil
            return continuation
        }
        resource.cancel()
        continuation?.resume(with: result)
    }

    func register(_ continuation: CheckedContinuation<Value, any Error>) -> Bool {
        let result = lock.withLock { state -> Result<Value, any Error>? in
            if let result = state.result { return result }
            state.continuation = continuation
            return nil
        }
        if let result { continuation.resume(with: result); return false }
        return true
    }
}

private struct JobValueAwaitState<Value: Sendable>: Sendable {

    var continuation: CheckedContinuation<Value, any Error>?
    var result: Result<Value, any Error>?
}

#if DEBUG
/// Task-scoped seam for cancellation between the initial check and waiter
/// registration; tests drive the real cancellation handler without polling.
enum JobFirstValueTestHooks {

    @TaskLocal
    static var beforeRegister: (@Sendable () -> Void)?
}
#endif
