import Foundation
import TheirCore

extension Their {

    /// Default number of concurrent iterations used by `Their.stress { ... }`.
    ///
    /// Several tests reference this constant directly, for example to assert
    /// `count == Their.stressCountDefault`, so changing the value here changes the
    /// stress level of those tests. Documented in `Documentation/TheirCoreTesting.md`.
    public static let stressCountDefault: Int = 50
    /// Default per-iteration timeout used by `Their.stress { ... }`. The timeout
    /// wraps only the child task's user block after the barrier opens; task
    /// creation, initial executor delay before the operation-start transition
    /// and barrier-fill time are not counted. Suspension and rescheduling after
    /// the block starts are counted.
    public static let stressTimeoutDefault: Their.TestDuration = .milliseconds(299)

    /// Runs `block` `count` times concurrently: every child task is created,
    /// held at a barrier until all of them arrived, then released together.
    /// Each child's block runs under its own timeout. Results are returned in
    /// completion order.
    @concurrent
    @discardableResult
    public static func stress<T>(
        count: Int = Their.stressCountDefault,
        priority: @autoclosure @escaping @Sendable () -> TaskPriority = .medium,
        timeout: Their.TestDuration = Their.stressTimeoutDefault,
        _ block: @Sendable @escaping (Int) async throws -> T
    ) async throws -> [T] where T: Sendable {
        try await stress(
            count: count,
            priority: priority(),
            timeout: timeout,
            dependencies: .live,
            block
        )
    }

    @concurrent
    @discardableResult
    static func stress<T>(
        count: Int = Their.stressCountDefault,
        priority: @autoclosure @escaping @Sendable () -> TaskPriority = .medium,
        timeout: Their.TestDuration = Their.stressTimeoutDefault,
        dependencies: StressTimeoutDependencies,
        _ block: @Sendable @escaping (Int) async throws -> T
    ) async throws -> [T] where T: Sendable {
        assert(count >= 0)
        if count == 0 {
            return []
        }
        let queue = TestQueue(count: count)
        return try await withThrowingTaskGroup(of: T.self) { group in
            for iteration in 0 ..< count {
                group.addTask(priority: priority()) {
                    await queue.enter()
                    return try await withStressTimeout(
                        dependencies: dependencies,
                        iteration: iteration,
                        timeout: timeout
                    ) {
                        return try await block(iteration)
                    }
                }
            }
            await queue.full()
            queue.open()
            var result = [T]()
            for try await value in group {
                result.append(value)
            }
            assert(count == result.count)
            return result
        }
    }

    /// Index-free overload of `stress(count:priority:timeout:_:)`.
    @concurrent
    @discardableResult
    public static func stress<T>(
        count: Int = Their.stressCountDefault,
        priority: @autoclosure @escaping @Sendable () -> TaskPriority = .medium,
        timeout: Their.TestDuration = Their.stressTimeoutDefault,
        _ block: @Sendable @escaping () async throws -> T
    ) async throws -> [T] where T: Sendable {
        try await stress(
            count: count,
            priority: priority(),
            timeout: timeout,
            { _ in try await block() }
        )
    }
}

extension Their {

    /// Thrown by `Their.stress` when one iteration's block outlives its timeout.
    public struct StressTimeoutError: CustomStringConvertible, Error, Equatable, Sendable {

        public var description: String {
            "stress timed out at iteration \(iteration) after \(timeout.nanoseconds) ns"
        }
        public let iteration: Int
        public let timeout: Their.TestDuration

        public init(
            iteration: Int,
            timeout: Their.TestDuration
        ) {
            self.iteration = iteration
            self.timeout = timeout
        }
    }

    /// Readable timeout values without Swift's `Duration`, which needs iOS 16:
    /// `.milliseconds(250)`, `.seconds(1)`.
    public struct TestDuration: Equatable, Sendable {

        let nanoseconds: UInt64

        init(nanoseconds: UInt64) {
            self.nanoseconds = nanoseconds
        }

        public static func milliseconds(_ milliseconds: UInt64) -> Self {
            .init(nanoseconds: milliseconds * 1_000_000)
        }

        public static func seconds(_ seconds: UInt64) -> Self {
            .init(nanoseconds: seconds * 1_000_000_000)
        }
    }
}

struct StressTimeoutDependencies: Sendable {

    let afterOperationJoined: @Sendable () -> Void
    let beforeOperationStarts: @Sendable () async throws -> Void
    static let live = Self(
        afterOperationJoined: {},
        beforeOperationStarts: {},
        sleep: { nanoseconds in
            try await Task.sleep(nanoseconds: nanoseconds)
        }
    )
    let sleep: @Sendable (UInt64) async throws -> Void

    init(
        afterOperationJoined: @escaping @Sendable () -> Void = {},
        beforeOperationStarts: @escaping @Sendable () async throws -> Void = {},
        sleep: @escaping @Sendable (UInt64) async throws -> Void
    ) {
        self.afterOperationJoined = afterOperationJoined
        self.beforeOperationStarts = beforeOperationStarts
        self.sleep = sleep
    }
}

private func withStressTimeout<T: Sendable>(
    dependencies: StressTimeoutDependencies,
    iteration: Int,
    timeout: Their.TestDuration,
    _ operation: @Sendable @escaping () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        let operationStarted = Their.TestSignal()
        group.addTask {
            try await dependencies.beforeOperationStarts()
            operationStarted.signal()
            try Task.checkCancellation()
            return try await operation()
        }
        group.addTask {
            try await operationStarted.wait()
            try await dependencies.sleep(timeout.nanoseconds)
            throw Their.StressTimeoutError(
                iteration: iteration,
                timeout: timeout
            )
        }

        let firstResult: Result<T, Error>
        do {
            guard let value = try await group.next() else {
                preconditionFailure("Stress timeout race must contain two child tasks.")
            }
            firstResult = .success(value)
        } catch {
            firstResult = .failure(error)
        }

        group.cancelAll()
        while group.isEmpty == false {
            do {
                _ = try await group.next()
            } catch {}
        }
        dependencies.afterOperationJoined()

        if Task.isCancelled {
            throw CancellationError()
        }
        return try firstResult.get()
    }
}
