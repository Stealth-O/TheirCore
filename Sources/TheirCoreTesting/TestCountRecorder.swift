import Foundation
import TheirCore

extension Their {

    public final class TestCountRecorder: Sendable {

        public var count: Int {
            lock.withLock { record in
                record.count
            }
        }
        private let lock = Their.Lock(TestCountRecord())

        public init() {}

        @discardableResult
        public func get() -> Int {
            count
        }

        public func increment() -> Int {
            let result = lock.withLock { record in
                record.count += 1
                let count = record.count
                let output = Self.splitWaiters(
                    count: count,
                    waiters: record.waiters
                )
                record.waiters = output.pending
                return (count: count, waiters: output.ready)
            }
            result.waiters.forEach { $0.continuation.resume() }
            return result.count
        }

        private static func splitWaiters(
            count: Int,
            waiters: [TestCountRecorderWaiter]
        ) -> (
            pending: [TestCountRecorderWaiter],
            ready: [TestCountRecorderWaiter]
        ) {
            var pending = [TestCountRecorderWaiter]()
            var ready = [TestCountRecorderWaiter]()
            for waiter in waiters {
                if waiter.count <= count {
                    ready.append(waiter)
                } else {
                    pending.append(waiter)
                }
            }
            return (
                pending: pending,
                ready: ready
            )
        }

        public func waitForCount(_ count: Int) async throws {
            try await waitForCount(count, onSuspend: {})
        }

        private func waitForCount(
            _ count: Int,
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            let token = TestWaiterToken()
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    let output = lock.withLock { record in
                        if token.isCancelled {
                            return TestCountWaitOutput.cancelled
                        }
                        guard record.count < count else {
                            return .reached
                        }
                        record.waiters.append(.init(
                            continuation: continuation,
                            count: count,
                            token: token
                        ))
                        return .suspended
                    }
                    switch output {
                    case .cancelled:
                        continuation.resume(throwing: CancellationError())
                    case .reached:
                        continuation.resume()
                    case .suspended:
                        onSuspend()
                        break
                    }
                }
            } onCancel: {
                let continuation = lock.withLock { record in
                    token.cancel()
                    var cancelled: CheckedContinuation<Void, Error>?
                    var pending = [TestCountRecorderWaiter]()
                    for waiter in record.waiters {
                        if waiter.token === token {
                            cancelled = waiter.continuation
                        } else {
                            pending.append(waiter)
                        }
                    }
                    record.waiters = pending
                    return cancelled
                }
                continuation?.resume(throwing: CancellationError())
            }
        }

        #if DEBUG
        func waitForCountForTests(
            _ count: Int,
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            try await waitForCount(count, onSuspend: onSuspend)
        }
        #endif
    }
}

private struct TestCountRecord: Sendable {

    var count = 0
    var waiters = [TestCountRecorderWaiter]()
}

private struct TestCountRecorderWaiter: Sendable {

    let continuation: CheckedContinuation<Void, Error>
    let count: Int
    let token: TestWaiterToken
}

private enum TestCountWaitOutput {

    case cancelled
    case reached
    case suspended
}
