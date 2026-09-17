import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobTests {

    @Test func cancelRepeatedlyStopsOnlyOnce() async throws {
        try await Their.stress {
            let cancelSignal = JobTestSignal()
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let job = Their.Job<Int, JobTestsError>(
                work: startRecorder.work
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            cancel()
            cancel()
            try await cancelSignal.wait()
            #expect(eventRecorder.events.isEmpty == true)
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func cancelStopsJobAndSuppressesLateOutput() async throws {
        try await Their.stress {
            let cancelSignal = JobTestSignal()
            let eventRecorder = JobEventRecorder()
            let misuseRecorder = JobMisuseRecorder()
            let startRecorder = JobStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let job = Their.Job<Int, JobTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: startRecorder.work
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            cancel()
            try await cancelSignal.wait()
            startRecorder.report?(.value(10))
            _ = job.subscribe { _ in }
            try await misuseRecorder.waitForCount(1)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.count == 1)
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func concurrentSubscribeAttemptsStartOnlyOneJobAndCancelOnlyOnce() async throws {
        let cancelSignal = JobTestSignal()
        let eventRecorder = JobEventRecorder()
        let misuseRecorder = JobMisuseRecorder()
        let startRecorder = JobStartRecorder(
            onCancel: {
                cancelSignal.signal()
            }
        )
        let job = Their.Job<Int, JobTestsError>(
            misuseHandler: misuseRecorder.append(_:),
            work: startRecorder.work
        )
        let cancels = try await Their.stress {
            job.subscribe(eventRecorder.append(_:))
        }
        try await misuseRecorder.waitForCount(Their.stressCountDefault - 1)
        cancels.forEach { cancel in
            cancel()
        }
        try await cancelSignal.wait()
        startRecorder.report?(.value(10))
        #expect(eventRecorder.events.isEmpty == true)
        #expect(misuseRecorder.misuses.count == Their.stressCountDefault - 1)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func deinitCancelsActiveJob() async throws {
        try await Their.stress {
            let cancelSignal = JobTestSignal()
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            var job: Their.Job<Int, JobTestsError>? = .init(
                work: startRecorder.work
            )
            _ = job?.subscribe(eventRecorder.append(_:))
            job = nil
            try await cancelSignal.wait()
            #expect(eventRecorder.events.isEmpty == true)
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func retainedCancelKeepsJobAliveUntilCanceled() async throws {
        try await Their.stress {
            let cancelSignal = JobTestSignal()
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            var job: Their.Job<Int, JobTestsError>? = .init(
                work: startRecorder.work
            )
            weak var weakJob = job
            let retainedCancel = job?.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            job = nil
            startRecorder.report?(.value(10))
            try await eventRecorder.waitForEventCount(1)
            #expect(eventRecorder.events == [.value(10)])
            #expect(startRecorder.cancelCallsCount == 0)
            #expect(startRecorder.startCallsCount == 1)
            #expect(weakJob != nil)

            retainedCancel?()
            try await cancelSignal.wait()
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(weakJob == nil)
        }
    }

    @Test func secondSubscribeWhileActiveReceivesMisuseWithoutStartingNewJob() async throws {
        try await Their.stress {
            let cancelSignal = JobTestSignal()
            let firstEventRecorder = JobEventRecorder()
            let misuseRecorder = JobMisuseRecorder()
            let startRecorder = JobStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let job = Their.Job<Int, JobTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: startRecorder.work
            )
            let firstCancel = job.subscribe(firstEventRecorder.append(_:))
            let secondCancel = job.subscribe { _ in }
            firstCancel()
            secondCancel()
            try await misuseRecorder.waitForCount(1)
            try await cancelSignal.wait()
            #expect(firstEventRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.count == 1)
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func subscribeStartsJobAndYieldsOutputs() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                work: startRecorder.work
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.value(10))
            try await eventRecorder.waitForEventCount(1)
            startRecorder.report?(.value(20))
            try await eventRecorder.waitForEventCount(2)
            cancel()
            #expect(eventRecorder.events == [.value(10), .value(20)])
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func subscribeYieldsRapidReportsInOrder() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                work: startRecorder.work
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            for value in 0..<Their.stressCountDefault {
                startRecorder.report?(.value(value))
            }
            try await eventRecorder.waitForEventCount(Their.stressCountDefault)
            cancel()
            #expect(eventRecorder.events == (0..<Their.stressCountDefault).map { .value($0) })
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalEndClearsSubscriberAndRejectsNewSubscription() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let misuseRecorder = JobMisuseRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: startRecorder.work
            )
            _ = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.finished)
            try await eventRecorder.waitForEventCount(1)
            _ = job.subscribe { _ in }
            try await misuseRecorder.waitForCount(1)
            #expect(eventRecorder.events == [.finished])
            #expect(misuseRecorder.misuses.count == 1)
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalEndIsOrderedBehindEarlierValues() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                work: startRecorder.work
            )
            _ = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.value(10))
            startRecorder.report?(.value(20))
            startRecorder.report?(.finished)
            try await eventRecorder.waitForEventCount(3)
            #expect(eventRecorder.events == [.value(10), .value(20), .finished])
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalEndSuppressesLateOutput() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let misuseRecorder = JobMisuseRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: startRecorder.work
            )
            _ = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.finished)
            try await eventRecorder.waitForEventCount(1)
            startRecorder.report?(.value(10))
            startRecorder.report?(.finished)
            startRecorder.report?(.failure(.sample))
            _ = job.subscribe { _ in }
            try await misuseRecorder.waitForCount(1)
            #expect(eventRecorder.events == [.finished])
            #expect(misuseRecorder.misuses.count == 1)
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalEndThenCancelDoesNotCancelTwice() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                work: startRecorder.work
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.finished)
            try await eventRecorder.waitForEventCount(1)
            cancel()
            #expect(eventRecorder.events == [.finished])
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalEndYieldsEndAndCancels() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                work: startRecorder.work
            )
            _ = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.finished)
            try await eventRecorder.waitForEventCount(1)
            #expect(eventRecorder.events == [.finished])
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalFailureClearsSubscriberAndRejectsNewSubscription() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let misuseRecorder = JobMisuseRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: startRecorder.work
            )
            _ = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(1)
            _ = job.subscribe { _ in }
            try await misuseRecorder.waitForCount(1)
            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(misuseRecorder.misuses.count == 1)
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalFailureSuppressesLateOutput() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let misuseRecorder = JobMisuseRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: startRecorder.work
            )
            _ = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(1)
            startRecorder.report?(.value(10))
            _ = job.subscribe { _ in }
            try await misuseRecorder.waitForCount(1)
            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(misuseRecorder.misuses.count == 1)
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalFailureThenCancelDoesNotCancelTwice() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                work: startRecorder.work
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(1)
            cancel()
            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalFailureYieldsErrorAndCancels() async throws {
        try await Their.stress {
            let eventRecorder = JobEventRecorder()
            let startRecorder = JobStartRecorder()
            let job = Their.Job<Int, JobTestsError>(
                work: startRecorder.work
            )
            _ = job.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(1)
            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }
}

private enum JobTestsError: Swift.Error, Sendable {

    case sample
}

private typealias JobEventRecorder = Their.TestEventRecorder<Their.JobEvent<Int, JobTestsError>>
private typealias JobMisuseRecorder = Their.TestMisuseRecorder
private typealias JobStartRecorder = Their.TestWorkRecorder<Int, JobTestsError>
private typealias JobTestSignal = Their.TestSignal
