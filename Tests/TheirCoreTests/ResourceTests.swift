import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct ResourceTests {

    @Test func cancelBeforeSetReleasesSetValueImmediately() async throws {
        try await Their.stress {
            let recorder = ResourceReleaseRecorder<String>()
            let resource = Their.Resource<String>(
                release: recorder.append(_:)
            )
            resource.cancel()
            resource.set("listener")
            try await recorder.waitForEventCount(1)
            #expect(recorder.events == ["listener"])
        }
    }

    @Test func cancelReleasesStoredValueOnce() async throws {
        try await Their.stress {
            let recorder = ResourceReleaseRecorder<String>()
            let resource = Their.Resource<String>(
                release: recorder.append(_:)
            )
            resource.set("listener")
            #expect(recorder.events.isEmpty)
            resource.cancel()
            resource.cancel()
            try await recorder.waitForEventCount(1)
            #expect(recorder.events == ["listener"])
        }
    }

    @Test func concurrentSetReleasesEachValueOnceAfterCancel() async throws {
        let recorder = ResourceReleaseRecorder<Int>()
        let resource = Their.Resource<Int>(
            release: recorder.append(_:)
        )
        try await Their.stress { iteration in
            resource.set(iteration)
        }
        resource.cancel()
        try await recorder.waitForEventCount(Their.stressCountDefault)
        #expect(recorder.events.sorted() == Array(0 ..< Their.stressCountDefault))
    }

    @Test func deinitReleasesStoredValue() async throws {
        try await Their.stress {
            let recorder = ResourceReleaseRecorder<String>()
            var resource: Their.Resource<String>? = Their.Resource<String>(
                release: recorder.append(_:)
            )
            resource?.set("listener")
            resource = nil
            try await recorder.waitForEventCount(1)
            #expect(recorder.events == ["listener"])
        }
    }

    @Test func repeatedSetReleasesReplacementValueImmediately() async throws {
        try await Their.stress {
            let recorder = ResourceReleaseRecorder<String>()
            let resource = Their.Resource<String>(
                release: recorder.append(_:)
            )
            resource.set("first")
            resource.set("second")
            try await recorder.waitForEventCount(1)
            #expect(recorder.events == ["second"])
            resource.cancel()
            try await recorder.waitForEventCount(2)
            #expect(recorder.events == ["second", "first"])
        }
    }

    @Test func setBeforeCancelDoesNotReleaseUntilCancel() async throws {
        try await Their.stress {
            let recorder = ResourceReleaseRecorder<String>()
            let resource = Their.Resource<String>(
                release: recorder.append(_:)
            )
            resource.set("listener")
            #expect(recorder.events.isEmpty)
            resource.cancel()
            try await recorder.waitForEventCount(1)
            #expect(recorder.events == ["listener"])
        }
    }
}

private typealias ResourceReleaseRecorder<Value: Sendable> = Their.TestEventRecorder<Value>
