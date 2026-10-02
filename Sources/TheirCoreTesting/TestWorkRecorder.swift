import TheirCore

extension Their {

    /// Records `Work` invocations, captures the upstream `WorkReport`, lets tests
    /// emit `WorkOutput<Value, Failure>` outcomes into the recorded report, and
    /// records cancellation calls. This is the canonical fake upstream `Work` for
    /// `Job` and `Hub` tests.
    public final class TestWorkRecorder<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

        public var cancelCallsCount: Int {
            lock.withLock { record in
                record.cancelCallsCount
            }
        }
        private let lock = Their.Lock(TestWorkRecord<Value, Failure>())
        private let onCancel: @Sendable () -> Void
        private let onStart: @Sendable () -> Void
        public var report: Their.WorkReport<Value, Failure>? {
            lock.withLock { record in
                record.report
            }
        }
        public var startCallsCount: Int {
            lock.withLock { record in
                record.startCallsCount
            }
        }

        public init(
            onCancel: @escaping @Sendable () -> Void = {},
            onStart: @escaping @Sendable () -> Void = {}
        ) {
            self.onCancel = onCancel
            self.onStart = onStart
        }

        public func emit(_ output: Their.WorkOutput<Value, Failure>) {
            report?(output)
        }

        #if DEBUG
        func isLockAvailableForTests() -> Bool {
            lock.withLockIfAvailable { _ in true } ?? false
        }
        #endif

        private static func splitWaiters(
            count: Int,
            kind: TestWorkRecorderWaiter.Kind,
            waiters: [TestWorkRecorderWaiter]
        ) -> (
            pending: [TestWorkRecorderWaiter],
            ready: [TestWorkRecorderWaiter]
        ) {
            var pending = [TestWorkRecorderWaiter]()
            var ready = [TestWorkRecorderWaiter]()
            for waiter in waiters {
                if waiter.kind == kind && waiter.count <= count {
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

        private func wait(
            for kind: TestWorkRecorderWaiter.Kind,
            count: Int,
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            let token = TestWaiterToken()
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    let output = lock.withLock { record in
                        if token.isCancelled {
                            return TestWorkWaitOutput.cancelled
                        }
                        switch kind {
                        case .cancel:
                            guard record.cancelCallsCount < count else {
                                return .reached
                            }
                        case .start:
                            guard record.startCallsCount < count else {
                                return .reached
                            }
                        }
                        record.waiters.append(.init(
                            continuation: continuation,
                            count: count,
                            kind: kind,
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
                    var pending = [TestWorkRecorderWaiter]()
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

        public func waitForCancelCallsCount(_ count: Int) async throws {
            try await wait(for: .cancel, count: count, onSuspend: {})
        }

        #if DEBUG
        func waitForCancelCallsCountForTests(
            _ count: Int,
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            try await wait(for: .cancel, count: count, onSuspend: onSuspend)
        }
        #endif

        public func waitForStartCallsCount(_ count: Int) async throws {
            try await wait(for: .start, count: count, onSuspend: {})
        }

        #if DEBUG
        func waitForStartCallsCountForTests(
            _ count: Int,
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            try await wait(for: .start, count: count, onSuspend: onSuspend)
        }
        #endif

        public func work(
            report: @escaping Their.WorkReport<Value, Failure>
        ) -> Their.WorkCancel {
            let output = lock.withLock { record in
                let oldReport = record.report
                record.report = report
                record.startCallsCount += 1
                let waiters = Self.splitWaiters(
                    count: record.startCallsCount,
                    kind: .start,
                    waiters: record.waiters
                )
                record.waiters = waiters.pending
                return (oldReport: oldReport, readyWaiters: waiters.ready)
            }
            // Report captures may reenter this recorder from their destructors.
            // Keep the replaced report alive until its state mutation has unlocked.
            withExtendedLifetime(output.oldReport) {
                output.readyWaiters.forEach { $0.continuation.resume() }
                onStart()
            }
            return { [self] in
                let readyWaiters = lock.withLock { record in
                    record.cancelCallsCount += 1
                    let output = Self.splitWaiters(
                        count: record.cancelCallsCount,
                        kind: .cancel,
                        waiters: record.waiters
                    )
                    record.waiters = output.pending
                    return output.ready
                }
                readyWaiters.forEach { $0.continuation.resume() }
                onCancel()
            }
        }
    }
}

private struct TestWorkRecord<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    var cancelCallsCount = 0
    var report: Their.WorkReport<Value, Failure>?
    var startCallsCount = 0
    var waiters = [TestWorkRecorderWaiter]()
}

private struct TestWorkRecorderWaiter: Sendable {

    enum Kind: Sendable {

        case cancel
        case start
    }

    let continuation: CheckedContinuation<Void, Error>
    let count: Int
    let kind: Kind
    let token: TestWaiterToken
}

private enum TestWorkWaitOutput {

    case cancelled
    case reached
    case suspended
}
