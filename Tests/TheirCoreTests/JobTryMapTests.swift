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

    /// The throwing report completes on another worker while work still owns
    /// its unreturned cancel. Terminal delivery must not wait for that handle;
    /// returning work later cleans it up exactly once.
    @Test func tryMapConcurrentThrowBeforeWorkReturnsDeliversFailureThenCancels() async throws {
        try await Their.stress(count: 1) {
            let onThrowCalls = Their.TestCountRecorder()
            let recorder = JobTryMapEventRecorder()
            let reportWorkSlot = Their.Lock<BlockingWork<Void>?>(nil)
            let trace = Their.TestEventRecorder<String>()
            let transformCalls = Their.TestCountRecorder()
            let workEntered = JobTryMapSignal()
            let workRelease = DispatchSemaphore(value: 0)
            let workRecorder = Their.TestWorkRecorder<Int, JobTryMapTestsError>(onCancel: {
                trace.append("cancel")
            })
            defer { workRelease.signal() }
            let upstream = Their.Job { report in
                let cancel = workRecorder.work(report: report)
                workEntered.signal()
                workRelease.wait()
                return cancel
            }
            let mapped: Their.Job<String, JobTryMapTestsError> = upstream.tryMap { _ in
                _ = transformCalls.increment()
                throw JobTryMapTestsError.decode
            } onThrow: { error in
                _ = onThrowCalls.increment()
                return (error as? JobTryMapTestsError) ?? .fallback
            }
            let subscribeWork = BlockingWork {
                mapped.subscribe { event in
                    recorder.append(event)
                    if case .failure = event {
                        trace.append("fail")
                    }
                }
            }
            do {
                try await workEntered.wait()
                let reportWork = BlockingWork {
                    workRecorder.emit(.value(-1))
                }
                reportWorkSlot.withLock { $0 = reportWork }
                try await reportWork.value
                let cancelsBeforeWorkReturns = workRecorder.cancelCallsCount
                let eventsBeforeWorkReturns = recorder.events
                let traceBeforeWorkReturns = trace.events

                workRelease.signal()
                let cancel = try await subscribeWork.value
                defer { cancel() }
                let cancelsAfterSubscribe = workRecorder.cancelCallsCount
                workRecorder.emit(.value(2))
                workRecorder.emit(.finished)
                workRecorder.emit(.failure(.upstream))
                cancel()
                cancel()

                #expect(cancelsBeforeWorkReturns == 0)
                #expect(cancelsAfterSubscribe == 1)
                #expect(eventsBeforeWorkReturns == [.failure(.decode)])
                #expect(traceBeforeWorkReturns == ["fail"])
                #expect(trace.events == ["fail", "cancel"])
                #expect(recorder.events == [.failure(.decode)])
                #expect(transformCalls.count == 1)
                #expect(onThrowCalls.count == 1)
                #expect(workRecorder.startCallsCount == 1)
                #expect(workRecorder.cancelCallsCount == 1)
            } catch {
                workRelease.signal()
                // Join both Dispatch workers from an uncancelled task even
                // when the scenario's timeout has cancelled its async waits.
                let cleanup = Task {
                    let cancel = try await subscribeWork.value
                    cancel()
                    if let reportWork = reportWorkSlot.withLock({ $0 }) {
                        try await reportWork.value
                    }
                }
                _ = await cleanup.result
                throw error
            }
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

    /// onThrow runs before the value outcome is committed. Cancelling here
    /// must discard the mapped failure as well as every later upstream event.
    @Test func tryMapOnThrowReentrantCancelSuppressesTerminalCallback() async throws {
        try await Their.stress {
            let cancelBox = Their.Lock<Their.WorkCancel?>(nil)
            let onThrowCalls = Their.TestCountRecorder()
            let recorder = JobTryMapEventRecorder()
            let reentrantCancelReturns = Their.TestCountRecorder()
            let trace = Their.TestEventRecorder<String>()
            let transformCalls = Their.TestCountRecorder()
            let workRecorder = Their.TestWorkRecorder<Int, JobTryMapTestsError>(onCancel: {
                trace.append("cancel")
            })
            let upstream = Their.Job(work: workRecorder.work)
            let mapped: Their.Job<String, JobTryMapTestsError> = upstream.tryMap { _ in
                _ = transformCalls.increment()
                throw JobTryMapTestsError.decode
            } onThrow: { error in
                _ = onThrowCalls.increment()
                trace.append("onThrow")
                guard let cancel = cancelBox.withLock({ $0 }) else {
                    Issue.record("The derived cancel must be stored before the throwing report.")
                    return .fallback
                }
                cancel()
                _ = reentrantCancelReturns.increment()
                trace.append("cancel-return")
                return (error as? JobTryMapTestsError) ?? .fallback
            }
            let cancel = mapped.subscribe(recorder.append(_:))
            cancelBox.withLock { $0 = cancel }
            defer {
                let detached = cancelBox.withLock { stored in
                    let detached = stored
                    stored = nil
                    return detached
                }
                withExtendedLifetime(detached) {}
                cancel()
            }

            workRecorder.emit(.value(-1))
            workRecorder.emit(.value(2))
            workRecorder.emit(.finished)
            workRecorder.emit(.failure(.upstream))
            cancel()
            cancel()

            #expect(trace.events == ["onThrow", "cancel", "cancel-return"])
            #expect(recorder.events.isEmpty == true)
            #expect(transformCalls.count == 1)
            #expect(onThrowCalls.count == 1)
            #expect(reentrantCancelReturns.count == 1)
            #expect(workRecorder.startCallsCount == 1)
            #expect(workRecorder.cancelCallsCount == 1)
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

    /// A synchronous value may throw before subscribe can store the upstream
    /// cancel. The failure arrives first; the late handle is then cancelled
    /// exactly once before the derived subscribe returns.
    @Test func tryMapSynchronousThrowBeforeWorkReturnsDeliversFailureThenCancels() async throws {
        try await Their.stress {
            let onThrowCalls = Their.TestCountRecorder()
            let recorder = JobTryMapEventRecorder()
            let trace = Their.TestEventRecorder<String>()
            let transformCalls = Their.TestCountRecorder()
            let workRecorder = Their.TestWorkRecorder<Int, JobTryMapTestsError>(onCancel: {
                trace.append("cancel")
            })
            let upstream = Their.Job { report in
                let cancel = workRecorder.work(report: report)
                report(.value(-1))
                // Observe the boundary inside work itself: a deferred terminal
                // after this return must not satisfy the final ordering alone.
                #expect(recorder.events == [.failure(.decode)])
                #expect(trace.events == ["fail"])
                #expect(workRecorder.cancelCallsCount == 0)
                return cancel
            }
            let mapped: Their.Job<String, JobTryMapTestsError> = upstream.tryMap { _ in
                _ = transformCalls.increment()
                throw JobTryMapTestsError.decode
            } onThrow: { error in
                _ = onThrowCalls.increment()
                return (error as? JobTryMapTestsError) ?? .fallback
            }
            let cancel = mapped.subscribe { event in
                recorder.append(event)
                if case .failure = event {
                    trace.append("fail")
                }
            }
            defer { cancel() }
            let cancelsAfterSubscribe = workRecorder.cancelCallsCount
            workRecorder.emit(.value(2))
            workRecorder.emit(.finished)
            workRecorder.emit(.failure(.upstream))
            cancel()
            cancel()

            #expect(trace.events == ["fail", "cancel"])
            #expect(cancelsAfterSubscribe == 1)
            #expect(recorder.events == [.failure(.decode)])
            #expect(transformCalls.count == 1)
            #expect(onThrowCalls.count == 1)
            #expect(workRecorder.startCallsCount == 1)
            #expect(workRecorder.cancelCallsCount == 1)
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

    /// A decoding failure tears down the still-live upstream before delivering
    /// its terminal callback. That teardown may cancel the derived subscription
    /// synchronously; a callback that has not started must then be suppressed.
    @Test func tryMapUpstreamTeardownReentrantCancelSuppressesTerminalCallback() async throws {
        try await Their.stress {
            let cancelBox = Their.Lock<Their.WorkCancel?>(nil)
            let reentrantCancelReturns = Their.TestCountRecorder()
            let transformCalls = Their.TestCountRecorder()
            let work = Their.TestWorkRecorder<Int, JobTryMapTestsError>(onCancel: {
                cancelBox.withLock { $0 }?()
                _ = reentrantCancelReturns.increment()
            })
            let upstream = Their.Job(work: work.work)
            let recorder = JobTryMapEventRecorder()
            let mapped: Their.Job<String, JobTryMapTestsError> = upstream.tryMap { _ in
                _ = transformCalls.increment()
                throw JobTryMapTestsError.decode
            } onThrow: { error in
                (error as? JobTryMapTestsError) ?? .fallback
            }
            let cancel = mapped.subscribe(recorder.append(_:))
            cancelBox.withLock { $0 = cancel }
            defer {
                let detached = cancelBox.withLock { stored in
                    let detached = stored
                    stored = nil
                    return detached
                }
                withExtendedLifetime(detached) {}
                cancel()
            }

            work.emit(.value(-1))
            work.emit(.value(2))
            cancel()

            #expect(reentrantCancelReturns.count == 1)
            #expect(transformCalls.count == 1)
            #expect(recorder.events.isEmpty == true)
            #expect(work.cancelCallsCount == 1)
            #expect(work.startCallsCount == 1)
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
