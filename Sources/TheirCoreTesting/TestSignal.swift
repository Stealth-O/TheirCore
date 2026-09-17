import TheirCore

extension Their {

    /// One-shot gate with a synchronous `signal()` and a cancellation-aware
    /// asynchronous `wait()`.
    ///
    /// `signal()` is a plain synchronous method, so it can be fired from any
    /// context — including inside a synchronous `Lock.withLock` body, a `WorkCancel`
    /// closure or a `DispatchQueue` block — without an `await` or a `Task { }`
    /// bridge. Backing the gate with a `Their.Lock` (rather than an `actor`) is what
    /// makes that possible.
    ///
    /// Semantics: `signal()` resumes every waiter currently suspended in `wait()`,
    /// later `wait()` calls return immediately, and repeated `signal()` calls are
    /// no-ops. Cancelling a waiting task removes its waiter and throws
    /// `CancellationError`. Waiter continuations are resumed outside the lock so
    /// the gate never runs user code while holding it.
    public final class TestSignal: Sendable {

        private let lock = Their.Lock(TestSignalState())

        public init() {}

        public func signal() {
            let waiters = lock.withLock { state in
                state.isSignaled = true
                let waiters = state.waiters
                state.waiters = []
                return waiters
            }
            waiters.forEach { $0.continuation.resume() }
        }

        public func wait() async throws {
            try await wait(onSuspend: {})
        }

        private func wait(
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            let token = TestWaiterToken()
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    let output = lock.withLock { state in
                        if token.isCancelled {
                            return TestSignalWaitOutput.cancelled
                        }
                        guard state.isSignaled == false else {
                            return .signaled
                        }
                        state.waiters.append(.init(
                            continuation: continuation,
                            token: token
                        ))
                        return .suspended
                    }
                    switch output {
                    case .cancelled:
                        continuation.resume(throwing: CancellationError())
                    case .signaled:
                        continuation.resume()
                    case .suspended:
                        onSuspend()
                        break
                    }
                }
            } onCancel: {
                let continuation = lock.withLock { state in
                    token.cancel()
                    var cancelled: CheckedContinuation<Void, Error>?
                    var pending = [TestSignalWaiter]()
                    for waiter in state.waiters {
                        if waiter.token === token {
                            cancelled = waiter.continuation
                        } else {
                            pending.append(waiter)
                        }
                    }
                    state.waiters = pending
                    return cancelled
                }
                continuation?.resume(throwing: CancellationError())
            }
        }

        #if DEBUG
        func waitForTests(
            onSuspend: @escaping @Sendable () -> Void
        ) async throws {
            try await wait(onSuspend: onSuspend)
        }
        #endif
    }
}

private struct TestSignalState: Sendable {

    var isSignaled = false
    var waiters = [TestSignalWaiter]()
}

private struct TestSignalWaiter: Sendable {

    let continuation: CheckedContinuation<Void, Error>
    let token: TestWaiterToken
}

private enum TestSignalWaitOutput {

    case cancelled
    case signaled
    case suspended
}

/// Registration handshake shared by cancellation-aware TheirCoreTesting waiters.
///
/// Cancellation can run before a waiter's continuation has been registered in
/// its owner. The token remembers that early cancellation so registration can
/// fail immediately instead of leaving an unreachable continuation behind.
final class TestWaiterToken: Sendable {

    var isCancelled: Bool {
        lock.withLock { isCancelled in
            isCancelled
        }
    }
    private let lock = Their.Lock(false)

    init() {}

    func cancel() {
        lock.withLock { isCancelled in
            isCancelled = true
        }
    }
}
