import TheirCore

extension Their {

    public final class TestJobDriver<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

        public var cancelCallsCount: Int {
            work.cancelCallsCount
        }
        public let job: Their.Job<Value, Failure>
        public var report: Their.WorkReport<Value, Failure>? {
            work.report
        }
        public var startCallsCount: Int {
            work.startCallsCount
        }
        private let work: Their.TestWorkRecorder<Value, Failure>

        public init(
            fileID: String = #fileID,
            function: String = #function,
            line: UInt = #line,
            misuseHandler: @escaping Their.MisuseHandler = Their.MisuseHandlers.fatal,
            onCancel: @escaping @Sendable () -> Void = {},
            onStart: @escaping @Sendable () -> Void = {}
        ) {
            let work = Their.TestWorkRecorder<Value, Failure>(
                onCancel: onCancel,
                onStart: onStart
            )
            self.work = work
            job = Their.Job(
                fileID: fileID,
                function: function,
                line: line,
                misuseHandler: misuseHandler,
                work: work.work
            )
        }

        public func emit(_ output: Their.WorkOutput<Value, Failure>) {
            work.emit(output)
        }

        public func emit(failure: Failure) {
            emit(.failure(failure))
        }

        public func emit(value: Value) {
            emit(.value(value))
        }

        public func emitFinished() {
            emit(.finished)
        }

        public func waitForCancelCallsCount(_ count: Int) async throws {
            try await work.waitForCancelCallsCount(count)
        }

        public func waitForStartCallsCount(_ count: Int) async throws {
            try await work.waitForStartCallsCount(count)
        }
    }
}
