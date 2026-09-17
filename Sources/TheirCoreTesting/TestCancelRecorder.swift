import TheirCore

extension Their {

    /// Records `WorkCancel` invocations. Use it when a test needs to assert
    /// "cancel was called N times" without building a one-off counter, or when a
    /// fake upstream needs to produce a `WorkCancel` value that increments a
    /// counter on invocation.
    ///
    /// Construction:
    ///
    /// ```swift
    /// let recorder = Their.TestCancelRecorder()
    /// let cancel = recorder.cancel()
    /// cancel()
    /// try await recorder.waitForCancelCallsCount(1)
    /// ```
    public final class TestCancelRecorder: Sendable {

        public var cancelCallsCount: Int {
            recorder.count
        }
        private let onCancel: @Sendable () -> Void
        private let recorder = Their.TestCountRecorder()

        public init(
            onCancel: @escaping @Sendable () -> Void = {}
        ) {
            self.onCancel = onCancel
        }

        /// Returns a `WorkCancel` closure that, when invoked, records the call and
        /// also calls the optional `onCancel` side effect supplied at init.
        public func cancel() -> Their.WorkCancel {
            {
                self.record()
            }
        }

        /// Equivalent to invoking the closure returned by `cancel()`; exposed so
        /// tests can record cancellations through a non-closure path when that
        /// reads more naturally.
        public func record() {
            _ = recorder.increment()
            onCancel()
        }

        /// Suspends until `cancelCallsCount` reaches `count`. Canonical waiter
        /// name; matches `TestWorkRecorder.waitForCancelCallsCount(_:)`.
        public func waitForCancelCallsCount(_ count: Int) async throws {
            try await recorder.waitForCount(count)
        }
    }
}
