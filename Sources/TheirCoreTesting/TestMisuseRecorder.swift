import TheirCore

extension Their {

    public final class TestMisuseRecorder: Sendable {

        public var misuses: [Their.Misuse] {
            recorder.events
        }
        private let recorder = Their.TestEventRecorder<Their.Misuse>()

        public init() {}

        public func append(_ misuse: Their.Misuse) {
            recorder.append(misuse)
        }

        public func currentMisuses() -> [Their.Misuse] {
            misuses
        }

        public func handler(_ misuse: Their.Misuse) {
            append(misuse)
        }

        public func waitForCount(_ count: Int) async throws {
            try await recorder.waitForEventCount(count)
        }
    }
}
