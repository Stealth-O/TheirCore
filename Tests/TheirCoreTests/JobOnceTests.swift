import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobOnceTests {

    @Test func cancelAfterCompletionKeepsDeliveredOutcome() async throws {
        try await Their.stress {
            let eventRecorder = JobOnceEventRecorder()
            let job: Their.Job<Int, Never> = Their.Job.once { 7 }
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(2)
            cancel()
            #expect(eventRecorder.events == [.value(7), .finished])
        }
    }

    @Test func cancelBeforeCompletionCancelsTaskAndSuppressesOutcome() async throws {
        try await Their.stress {
            let cancelledSignal = JobOnceTestSignal()
            let eventRecorder = JobOnceEventRecorder()
            let releaseGate = JobOnceTestSignal()
            let job: Their.Job<Int, Never> = Their.Job.once {
                await withTaskCancellationHandler {
                    _ = try? await releaseGate.wait()
                    return 7
                } onCancel: {
                    cancelledSignal.signal()
                }
            }
            let cancel = job.subscribe(eventRecorder.append(_:))
            cancel()
            try await cancelledSignal.wait()
            #expect(eventRecorder.events.isEmpty == true)
            releaseGate.signal()
        }
    }

    @Test func deinitBeforeCompletionCancelsTask() async throws {
        try await Their.stress {
            let cancelledSignal = JobOnceTestSignal()
            let eventRecorder = JobOnceEventRecorder()
            let releaseGate = JobOnceTestSignal()
            var job: Their.Job<Int, Never>? = Their.Job.once {
                await withTaskCancellationHandler {
                    _ = try? await releaseGate.wait()
                    return 7
                } onCancel: {
                    cancelledSignal.signal()
                }
            }
            _ = job?.subscribe(eventRecorder.append(_:))
            job = nil
            try await cancelledSignal.wait()
            #expect(eventRecorder.events.isEmpty == true)
            releaseGate.signal()
        }
    }

    @Test func failureDeliversMappedTerminalFailure() async throws {
        try await Their.stress {
            let eventRecorder = JobOnceFailingEventRecorder()
            let job: Their.Job<Int, JobOnceTestsError> = Their.Job.once(
                failure: { error in
                    (error as? JobOnceTestsError) ?? .unexpected
                }
            ) {
                throw JobOnceTestsError.sample
            }
            _ = job.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(1)
            #expect(eventRecorder.events == [.failure(.sample)])
        }
    }

    @Test func secondSubscribeWhileRunningReportsMisuse() async throws {
        try await Their.stress {
            let eventRecorder = JobOnceEventRecorder()
            let misuseRecorder = JobOnceMisuseRecorder()
            let releaseGate = JobOnceTestSignal()
            let job: Their.Job<Int, Never> = Their.Job.once(misuseHandler: misuseRecorder.handler) {
                _ = try? await releaseGate.wait()
                return 7
            }
            let cancel = job.subscribe(eventRecorder.append(_:))
            _ = job.subscribe { _ in }
            try await misuseRecorder.waitForCount(1)
            #expect(misuseRecorder.misuses.count == 1)
            releaseGate.signal()
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value(7), .finished])
            cancel()
        }
    }

    @Test func streamEndsNaturallyAfterSuccess() async throws {
        try await Their.stress {
            let eventRecorder = JobOnceEventRecorder()
            let finishedSignal = JobOnceTestSignal()
            let job: Their.Job<Int, Never> = Their.Job.once { 7 }
            let task = Task {
                for await event in job.stream() {
                    eventRecorder.append(event)
                }
                finishedSignal.signal()
            }
            try await finishedSignal.wait()
            #expect(eventRecorder.events == [.value(7), .finished])
            task.cancel()
        }
    }

    @Test func successDeliversValueThenEnd() async throws {
        try await Their.stress {
            let eventRecorder = JobOnceEventRecorder()
            let job: Their.Job<Int, Never> = Their.Job.once { 7 }
            _ = job.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value(7), .finished])
        }
    }

    /// Throwing overload under cancel: the operation observes cooperative
    /// cancellation by throwing `CancellationError`, `failure` maps it, and the
    /// mapped terminal failure is dropped by the already-terminated engine, so
    /// nothing reaches the cancelled subscriber.
    @Test func throwingOnceCancelBeforeCompletionMapsCancellationAndDeliversNothing() async throws {
        try await Their.stress {
            let eventRecorder = JobOnceFailingEventRecorder()
            let mappedErrors = JobOnceMappedErrorRecorder()
            let releaseGate = JobOnceTestSignal()
            let job: Their.Job<Int, JobOnceTestsError> = Their.Job.once(
                failure: { error in
                    mappedErrors.append(error is CancellationError)
                    return .unexpected
                }
            ) {
                try await releaseGate.wait()
                return 7
            }
            let cancel = job.subscribe(eventRecorder.append(_:))
            cancel()
            try await mappedErrors.waitForEventCount(1)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(mappedErrors.events == [true])
        }
    }

    @Test func throwingOnceSuccessDeliversValueThenEnd() async throws {
        try await Their.stress {
            let eventRecorder = JobOnceFailingEventRecorder()
            let job: Their.Job<Int, JobOnceTestsError> = Their.Job.once(failure: { _ in .unexpected }) {
                7
            }
            _ = job.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value(7), .finished])
        }
    }
}

private enum JobOnceTestsError: Swift.Error, Sendable {

    case sample
    case unexpected
}

private typealias JobOnceEventRecorder = Their.TestEventRecorder<Their.JobEvent<Int, Never>>
private typealias JobOnceFailingEventRecorder = Their.TestEventRecorder<Their.JobEvent<Int, JobOnceTestsError>>
private typealias JobOnceMappedErrorRecorder = Their.TestEventRecorder<Bool>
private typealias JobOnceMisuseRecorder = Their.TestMisuseRecorder
private typealias JobOnceTestSignal = Their.TestSignal
