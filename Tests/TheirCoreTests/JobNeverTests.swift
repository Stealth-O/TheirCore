import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobNeverTests {

    @Test func cancelDoesNotEmit() async throws {
        try await Their.stress {
            let eventRecorder = JobNeverEventRecorder()
            let job: Their.Job<Int, Never> = .never()
            let cancel = job.subscribe(eventRecorder.append(_:))
            cancel()
            cancel()
            #expect(eventRecorder.events.isEmpty == true)
        }
    }

    @Test func secondSubscribeWhileRunningReportsMisuse() async throws {
        try await Their.stress {
            let eventRecorder = JobNeverEventRecorder()
            let misuseRecorder = JobNeverMisuseRecorder()
            let job: Their.Job<Int, Never> = .never(misuseHandler: misuseRecorder.handler)
            let firstCancel = job.subscribe(eventRecorder.append(_:))
            let secondCancel = job.subscribe { _ in }
            try await misuseRecorder.waitForCount(1)
            secondCancel()
            firstCancel()
            #expect(eventRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.count == 1)
        }
    }

    @Test func subscribeDoesNotEmit() async throws {
        try await Their.stress {
            let eventRecorder = JobNeverEventRecorder()
            let job: Their.Job<Int, Never> = .never()
            let cancel = job.subscribe(eventRecorder.append(_:))
            #expect(eventRecorder.events.isEmpty == true)
            cancel()
        }
    }
}

private typealias JobNeverEventRecorder = Their.TestEventRecorder<Their.JobEvent<Int, Never>>
private typealias JobNeverMisuseRecorder = Their.TestMisuseRecorder
