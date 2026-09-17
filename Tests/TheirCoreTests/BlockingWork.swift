import Dispatch
import TheirCore
import TheirCoreTesting

/// Runs one synchronous call on a Dispatch worker thread and exposes its
/// result to async test code, like `Task.value`.
///
/// Several scenarios hold a drainer inside a sink or transform with a
/// `DispatchSemaphore` so a later report can queue behind it. That hold must
/// not park a thread of Swift concurrency's cooperative pool: the pool is only
/// as wide as the CPU count, suites run in parallel, and a handful of held
/// drainers would starve the very continuation that releases them. Dispatch
/// worker threads live outside that pool.
final class BlockingWork<Output: Sendable>: Sendable {

    private let finished = Their.TestSignal()
    private let output = Their.Lock<Output?>(nil)
    var value: Output {
        get async throws {
            try await finished.wait()
            guard let output = output.withLock({ $0 }) else {
                preconditionFailure("BlockingWork finished without an output.")
            }
            return output
        }
    }

    init(_ body: @escaping @Sendable () -> Output) {
        DispatchQueue.global().async { [self] in
            let result = body()
            output.withLock { stored in
                stored = result
            }
            finished.signal()
        }
    }
}
