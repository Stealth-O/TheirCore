import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobTryMapTests {

    @Test func tryMapCancelStopsUpstreamAndSuppressesLateValue() async throws {
        try await Their.stress {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            let cancel = mapped.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            cancel()
            try await driver.waitForCancelCallsCount(1)
            driver.emit(value: 5)
            #expect(recorder.events.isEmpty == true)
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapDeinitCancelsUpstream() async throws {
        try await Their.stress {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            var mapped: Their.Job<String, JobTryMapTestsError>? = driver.job.tryMap { value in
                String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped?.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            mapped = nil
            try await driver.waitForCancelCallsCount(1)
            driver.emit(value: 5)
            #expect(recorder.events.isEmpty == true)
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapEmitsTransformedValuesWithoutTerminating() async throws {
        try await Their.stress {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: 2)
            driver.emit(value: 3)
            try await recorder.waitForEventCount(2)
            #expect(recorder.events == [.value("2"), .value("3")])
            #expect(driver.cancelCallsCount == 0)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapForwardsUpstreamEndBypassingTransform() async throws {
        try await Their.stress {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            let transformRecorder = Their.TestCountRecorder()
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                transformRecorder.increment()
                return String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: 2)
            try await recorder.waitForEventCount(1)
            driver.emitFinished()
            try await recorder.waitForEventCount(2)
            driver.emit(value: 7)
            driver.emitFinished()
            #expect(recorder.events == [.value("2"), .finished])
            #expect(transformRecorder.count == 1)
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapForwardsUpstreamFailureUnchanged() async throws {
        try await Their.stress {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(failure: .upstream)
            try await recorder.waitForEventCount(1)
            driver.emit(value: 3)
            #expect(recorder.events == [.failure(.upstream)])
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapRejectsResubscribeAfterCancel() async throws {
        try await Their.stress {
            let firstRecorder = JobTryMapEventRecorder()
            let misuseRecorder = JobTryMapMisuseRecorder()
            let secondRecorder = JobTryMapEventRecorder()
            let driver = JobTryMapDriver(misuseHandler: misuseRecorder.handler)
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            let firstCancel = mapped.subscribe(firstRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            firstCancel()
            try await driver.waitForCancelCallsCount(1)
            let secondCancel = mapped.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            secondCancel()
            driver.emit(value: 5)
            #expect(firstRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Evolved Job supports only one subscriber per lifecycle."
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapRejectsResubscribeAfterThrowTerminal() async throws {
        try await Their.stress {
            let firstRecorder = JobTryMapEventRecorder()
            let misuseRecorder = JobTryMapMisuseRecorder()
            let secondRecorder = JobTryMapEventRecorder()
            let driver = JobTryMapDriver(misuseHandler: misuseRecorder.handler)
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                guard value >= 0 else {
                    throw JobTryMapTestsError.decode
                }
                return String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped.subscribe(firstRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: -1)
            try await firstRecorder.waitForEventCount(1)
            let secondCancel = mapped.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            secondCancel()
            #expect(firstRecorder.events == [.failure(.decode)])
            #expect(misuseRecorder.misuses.count == 1)
            #expect(secondRecorder.events.isEmpty == true)
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapRejectsSecondSubscriberWithCreationLocationTrace() async throws {
        try await Their.stress {
            let firstRecorder = JobTryMapEventRecorder()
            let misuseRecorder = JobTryMapMisuseRecorder()
            let secondRecorder = JobTryMapEventRecorder()
            let driver = JobTryMapDriver(misuseHandler: misuseRecorder.handler)
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap(
                fileID: "CoreTests/TryMapValueTrace.swift",
                function: "makeTracedTryMapValue()",
                line: 654
            ) { value in
                String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            let firstCancel = mapped.subscribe(firstRecorder.append(_:))
            let secondCancel = mapped.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            try await driver.waitForStartCallsCount(1)
            secondCancel()
            firstCancel()
            #expect(misuseRecorder.misuses.first?.trace == [
                Their.MisuseLocation(
                    fileID: "CoreTests/TryMapValueTrace.swift",
                    function: "makeTracedTryMapValue()",
                    line: 654
                )
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapThrowEmitsMappedFailureAndCancelsLiveUpstream() async throws {
        try await Their.stress {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                guard value >= 0 else {
                    throw JobTryMapTestsError.decode
                }
                return String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: -1)
            try await recorder.waitForEventCount(1)
            try await driver.waitForCancelCallsCount(1)
            driver.emit(value: 5)
            #expect(recorder.events == [.failure(.decode)])
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapThrowMapsUnknownErrorThroughFallback() async throws {
        try await Their.stress {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { _ in
                throw JobTryMapForeignError()
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: 7)
            try await recorder.waitForEventCount(1)
            #expect(recorder.events == [.failure(.fallback)])
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    /// Pins FIFO ordering of a value-driven terminal behind a blocked transform:
    /// the throwing value is processed by the same single drainer, so it cannot
    /// overtake the earlier value being transformed — the first value is emitted,
    /// then the queued throwing value terminates the lifecycle and cancels the
    /// live upstream exactly once.
    @Test func tryMapThrowQueuedBehindBlockedTransformDoesNotOvertakeValue() async throws {
        try await Their.stress(count: 1) {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            let releaseTransform = DispatchSemaphore(value: 0)
            let transformEntered = JobTryMapSignal()
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                if value == 1 {
                    transformEntered.signal()
                    releaseTransform.wait()
                }
                guard value >= 0 else {
                    throw JobTryMapTestsError.decode
                }
                return String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            let emitTask = BlockingWork {
                driver.emit(value: 1)
            }
            try await transformEntered.wait()
            driver.emit(value: -1)
            releaseTransform.signal()
            try await emitTask.value
            try await recorder.waitForEventCount(2)
            #expect(recorder.events == [.value("1"), .failure(.decode)])
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func tryMapThrowSuppressesAllLaterEvents() async throws {
        try await Their.stress {
            let driver = JobTryMapDriver()
            let recorder = JobTryMapEventRecorder()
            let mapped: Their.Job<String, JobTryMapTestsError> = driver.job.tryMap { value in
                guard value >= 0 else {
                    throw JobTryMapTestsError.decode
                }
                return String(value)
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            _ = mapped.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: -1)
            try await recorder.waitForEventCount(1)
            driver.emit(value: 2)
            driver.emitFinished()
            driver.emit(failure: .upstream)
            #expect(recorder.events == [.failure(.decode)])
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }
}

private struct JobTryMapForeignError: Swift.Error {}

private enum JobTryMapTestsError: Equatable, Swift.Error, Sendable {

    case decode
    case fallback
    case upstream
}

private typealias JobTryMapDriver = Their.TestJobDriver<Int, JobTryMapTestsError>
private typealias JobTryMapEventRecorder = Their.TestEventRecorder<Their.JobEvent<String, JobTryMapTestsError>>
private typealias JobTryMapMisuseRecorder = Their.TestMisuseRecorder
private typealias JobTryMapSignal = Their.TestSignal
