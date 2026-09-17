import Foundation
import TheirCore

extension Their {

    public final class TestEventRecorder<Event: Sendable>: Sendable {

        public var count: Int {
            lock.withLock { record in
                record.events.count
            }
        }
        public var events: [Event] {
            lock.withLock { record in
                record.events
            }
        }
        public var last: Event? {
            lock.withLock { record in
                record.events.last
            }
        }
        private let lock = Their.Lock(TestEventRecord<Event>())

        public init() {}

        public func append(_ event: Event) {
            let ready = lock.withLock { record in
                record.events.append(event)
                let waiters = Self.splitWaiters(
                    count: record.events.count,
                    waiters: record.waiters
                )
                record.waiters = waiters.pending
                return waiters.ready
            }
            ready.forEach { $0.continuation.resume() }
        }

        public func currentEvents() -> [Event] {
            events
        }

        public func event(at index: Int) -> Event? {
            lock.withLock { record in
                guard record.events.indices.contains(index) else {
                    return nil
                }
                return record.events[index]
            }
        }

        private static func splitWaiters(
            count: Int,
            waiters: [TestEventRecorderWaiter]
        ) -> (
            pending: [TestEventRecorderWaiter],
            ready: [TestEventRecorderWaiter]
        ) {
            var pending = [TestEventRecorderWaiter]()
            var ready = [TestEventRecorderWaiter]()
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

        public func waitForEvent(
            where predicate: @escaping @Sendable (Event) -> Bool
        ) async throws -> Event {
            var cursor = 0
            while true {
                try Task.checkCancellation()
                let snapshot = lock.withLock { record in
                    Array(record.events.dropFirst(cursor))
                }
                for event in snapshot {
                    cursor += 1
                    if predicate(event) {
                        return event
                    }
                }
                try await waitForEventCount(cursor + 1)
            }
        }

        public func waitForEventCount(_ count: Int) async throws {
            try await waitForEventCount(count, onSuspend: {})
        }

        private func waitForEventCount(
            _ count: Int,
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            let token = TestWaiterToken()
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    let output = lock.withLock { record in
                        if token.isCancelled {
                            return TestEventCountWaitOutput.cancelled
                        }
                        guard record.events.count < count else {
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
                    var pending = [TestEventRecorderWaiter]()
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
        func waitForEventCountForTests(
            _ count: Int,
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            try await waitForEventCount(count, onSuspend: onSuspend)
        }
        #endif
    }
}

private struct TestEventRecord<Event: Sendable>: Sendable {

    var events = [Event]()
    var waiters = [TestEventRecorderWaiter]()
}

private struct TestEventRecorderWaiter: Sendable {

    let continuation: CheckedContinuation<Void, Error>
    let count: Int
    let token: TestWaiterToken
}

private enum TestEventCountWaitOutput {

    case cancelled
    case reached
    case suspended
}
