import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobEngineTests {

    @Test func cancelCanReentrantlyReportDuringStopAndLateValueIsIgnored() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let reportStore = Their.Lock<Their.WorkReport<Int, JobEngineTestsError>?>(nil)
            let cancelRecorder = Their.TestCancelRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: { report in
                    reportStore.withLock { currentReport in
                        currentReport = report
                    }
                    return {
                        cancelRecorder.record()
                        report(.value(2))
                    }
                }
            )
            #expect(engine.start() == true)
            let report = reportStore.withLock { currentReport in
                currentReport
            }
            report?(.value(1))

            engine.stop()

            #expect(eventRecorder.events == [
                .value(1),
                .message(.emitOutputIgnoredBecauseStateIsNotActive),
                .message(.stopStopped)
            ])
            #expect(cancelRecorder.cancelCallsCount == 1)
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func cancelReentrantlyStoppingDuringStopIsIgnoredAndCancelsOnce() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let engineStore = Their.Lock<JobEngine<Int, JobEngineTestsError>?>(nil)
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: { _ in
                    {
                        cancelRecorder.record()
                        engineStore.withLock { currentEngine in
                            currentEngine
                        }?.stop()
                    }
                }
            )
            engineStore.withLock { currentEngine in
                currentEngine = engine
            }
            #expect(engine.start() == true)

            engine.stop()

            #expect(eventRecorder.events == [
                .message(.stopIgnoredBecauseStateIsNotActive),
                .message(.stopStopped)
            ])
            #expect(cancelRecorder.cancelCallsCount == 1)
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func concurrentReportsRacingWithFailureCancelOnceAndNeverValueAfterFailure() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            let report = startRecorder.report
            #expect(report != nil)

            await withTaskGroup(of: Void.self) { group in
                group.addTask {
                    report?(.failure(.sample))
                }
                for value in 0 ..< Their.stressCountDefault {
                    group.addTask {
                        report?(.value(value))
                    }
                }
            }
            try await eventRecorder.waitForEventCount(Their.stressCountDefault + 1)

            let events = eventRecorder.events
            let failIndices = events.indices.filter { events[$0].isFailureError }
            #expect(failIndices.count == 1)
            if let failIndex = failIndices.first {
                #expect(events.suffix(from: events.index(after: failIndex)).containsValue == false)
            }
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test(arguments: [true, false])
    func controlSequenceKeepsDistinctDiagnosticsAndCancelsOnce(_ stopFirst: Bool) async throws {
        try await Their.stress {
            let observations = Their.TestEventRecorder<JobEngineControlObservation>()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { observations.append(.event($0)) },
                work: { _ in
                    { observations.append(.cancel) }
                }
            )
            #expect(engine.start())

            if stopFirst {
                engine.stop()
                #expect(engine.getState().isTerminated)
                engine.terminate()
                #expect(observations.events == [
                    .cancel,
                    .event(.message(.stopStopped)),
                    .event(.message(.terminateIgnoredBecauseStateIsNotActive))
                ])
            } else {
                engine.terminate()
                #expect(engine.getState().isTerminated)
                engine.stop()
                #expect(observations.events == [
                    .cancel,
                    .event(.message(.terminateStopped)),
                    .event(.message(.stopIgnoredBecauseStateIsNotActive))
                ])
            }
            #expect(engine.getState().isTerminated)
        }
    }

    @Test func deinitCancelsWhenActive() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            var engine: JobEngine<Int, JobEngineTestsError>? = .init(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine?.start()
            #expect(engine?.getState().isActive == true)
            #expect(startRecorder.cancelCallsCount == 0)
            engine = nil
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(eventRecorder.events.isEmpty)
        }
    }

    @Test func deinitDoesNotCancelWhenIdle() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            var engine: JobEngine<Int, JobEngineTestsError>? = .init(
                sink: eventRecorder.append(_:),
                state: .idle
            )
            #expect(engine?.getState().isIdle == true)
            engine = nil
            #expect(eventRecorder.events == [.message(.deinitIgnoredBecauseStateIsNotActive)])
        }
    }

    @Test func deinitDoesNotCancelWhenTerminated() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            var engine: JobEngine<Int, JobEngineTestsError>? = .init(
                sink: eventRecorder.append(_:),
                state: .terminated
            )
            #expect(engine?.getState().isTerminated == true)
            engine = nil
            #expect(eventRecorder.events == [.message(.deinitIgnoredBecauseStateIsNotActive)])
        }
    }

    @Test func deinitStopsWhenStarting() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            var engine: JobEngine<Int, JobEngineTestsError>? = .init(
                sink: eventRecorder.append(_:),
                state: .starting
            )
            #expect(engine?.getState().isActive == true)
            engine = nil
            #expect(eventRecorder.events == [.message(.deinitStoppedWhileStarting)])
        }
    }

    @Test func emitFailureIgnoresLateOutputAfterFailure() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.emit(failure: .sample)
            engine.emit(value: 123)
            #expect(eventRecorder.events == [
                .failure(.sample),
                .message(.emitOutputIgnoredBecauseStateIsNotActive)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func emitFailureReportsBreakWhenIdle() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .idle
            )
            engine.emit(failure: .sample)
            #expect(eventRecorder.events == [.message(.emitFailureIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isIdle == true)
        }
    }

    @Test func emitFailureReportsBreakWhenTerminated() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .terminated
            )
            engine.emit(failure: .sample)
            #expect(eventRecorder.events == [.message(.emitFailureIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func emitFailureTerminatesAndCancelsBeforeYieldsError() async throws {
        try await Their.stress {
            let orderRecorder = Their.TestEventRecorder<String>()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.isFailureError {
                        orderRecorder.append("error")
                    }
                },
                state: .active(
                    cancel: {
                        orderRecorder.append("cancel")
                    }
                )
            )
            engine.emit(failure: .sample)
            #expect(orderRecorder.events == ["cancel", "error"])
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func emitFailureYieldsErrorCancelsAndTerminates() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .active(cancel: {})
            )
            engine.emit(failure: .sample)
            #expect(eventRecorder.last?.isFailureError == true)
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func emitFinishedIgnoresLateReportsAfterFinished() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.emitFinished()
            engine.emit(value: 123)
            engine.emit(failure: .sample)
            engine.emitFinished()
            #expect(eventRecorder.events == [
                .finished,
                .message(.emitOutputIgnoredBecauseStateIsNotActive),
                .message(.emitFailureIgnoredBecauseStateIsNotActive),
                .message(.emitFinishedIgnoredBecauseStateIsNotActive)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func emitFinishedReportsBreakWhenIdle() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .idle
            )
            engine.emitFinished()
            #expect(eventRecorder.events == [.message(.emitFinishedIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isIdle == true)
        }
    }

    @Test func emitFinishedReportsBreakWhenTerminated() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .terminated
            )
            engine.emitFinished()
            #expect(eventRecorder.events == [.message(.emitFinishedIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func emitFinishedTerminatesAndCancelsBeforeYieldsFinished() async throws {
        try await Their.stress {
            let orderRecorder = Their.TestEventRecorder<String>()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if case .finished = event {
                        orderRecorder.append("end")
                    }
                },
                state: .active(
                    cancel: {
                        orderRecorder.append("cancel")
                    }
                )
            )
            engine.emitFinished()
            #expect(orderRecorder.events == ["cancel", "end"])
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func emitFinishedYieldsFinishedCancelsAndTerminates() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .active(cancel: {})
            )
            engine.emitFinished()
            #expect(eventRecorder.events == [.finished])
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func emitOutputReportsBreakWhenIdle() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .idle
            )
            engine.emit(value: 123)
            #expect(eventRecorder.events == [.message(.emitOutputIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isIdle == true)
        }
    }

    @Test func emitOutputReportsBreakWhenTerminated() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .terminated
            )
            engine.emit(value: 123)
            #expect(eventRecorder.events == [.message(.emitOutputIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func emitOutputYieldsOutput() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .active(cancel: {})
            )
            engine.emit(value: 123)
            #expect(eventRecorder.last?.value == 123)
            #expect(engine.getState().isActive == true)
        }
    }

    @Test func reportEndQueuedBehindInFlightValueDoesNotOvertakeValue() async throws {
        try await Their.stress(count: 1) {
            let endReturned = JobEngineSignal()
            let eventRecorder = JobEngineEventRecorder()
            let releaseValue = DispatchSemaphore(value: 0)
            let startRecorder = WorkRecorder()
            let valueEntered = JobEngineSignal()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        valueEntered.signal()
                        releaseValue.wait()
                    }
                    eventRecorder.append(event)
                },
                work: startRecorder.work
            )
            #expect(engine.start() == true)
            let report = startRecorder.report
            #expect(report != nil)

            let valueTask = BlockingWork {
                report?(.value(1))
            }
            try await valueEntered.wait()
            let endTask = Task {
                report?(.finished)
                endReturned.signal()
            }
            try await endReturned.wait()
            #expect(eventRecorder.events.isEmpty == true)

            releaseValue.signal()
            try await valueTask.value
            await endTask.value
            try await eventRecorder.waitForEventCount(2)

            #expect(eventRecorder.events == [
                .value(1),
                .finished
            ])
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func reportFailureQueuedBehindInFlightValueDoesNotOvertakeValue() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobEngineEventRecorder()
            let failureReturned = JobEngineSignal()
            let releaseValue = DispatchSemaphore(value: 0)
            let startRecorder = WorkRecorder()
            let valueEntered = JobEngineSignal()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        valueEntered.signal()
                        releaseValue.wait()
                    }
                    eventRecorder.append(event)
                },
                work: startRecorder.work
            )
            #expect(engine.start() == true)
            let report = startRecorder.report
            #expect(report != nil)

            let valueTask = BlockingWork {
                report?(.value(1))
            }
            try await valueEntered.wait()
            let failureTask = Task {
                report?(.failure(.sample))
                failureReturned.signal()
            }
            try await failureReturned.wait()
            #expect(eventRecorder.events.isEmpty == true)

            releaseValue.signal()
            try await valueTask.value
            await failureTask.value
            try await eventRecorder.waitForEventCount(2)

            #expect(eventRecorder.events == [
                .value(1),
                .failure(.sample)
            ])
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func reportValueQueuedBehindInFlightValueDoesNotOvertakeValue() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobEngineEventRecorder()
            let releaseValue = DispatchSemaphore(value: 0)
            let startRecorder = WorkRecorder()
            let valueEntered = JobEngineSignal()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        valueEntered.signal()
                        releaseValue.wait()
                    }
                    eventRecorder.append(event)
                },
                work: startRecorder.work
            )
            #expect(engine.start() == true)
            let report = startRecorder.report
            #expect(report != nil)

            let valueTask = BlockingWork {
                report?(.value(1))
            }
            try await valueEntered.wait()
            report?(.value(2))
            #expect(eventRecorder.events.isEmpty == true)

            releaseValue.signal()
            try await valueTask.value
            try await eventRecorder.waitForEventCount(2)

            #expect(eventRecorder.events == [
                .value(1),
                .value(2)
            ])
        }
    }

    @Test func sinkCanReentrantlyReportValueAndDeliverItAfterCurrentSinkReturns() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let orderRecorder = Their.TestEventRecorder<String>()
            let reportStore = Their.Lock<Their.WorkReport<Int, JobEngineTestsError>?>(nil)
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        orderRecorder.append("value1-enter")
                        reportStore.withLock { currentReport in
                            currentReport
                        }?(.value(2))
                        orderRecorder.append("value1-after-report")
                    }
                    if event.value == 2 {
                        orderRecorder.append("value2")
                    }
                    eventRecorder.append(event)
                },
                work: { report in
                    reportStore.withLock { currentReport in
                        currentReport = report
                    }
                    return {}
                }
            )
            #expect(engine.start() == true)
            let report = reportStore.withLock { currentReport in
                currentReport
            }
            report?(.value(1))

            #expect(orderRecorder.events == [
                "value1-enter",
                "value1-after-report",
                "value2"
            ])
            #expect(eventRecorder.events == [
                .value(1),
                .value(2)
            ])
            #expect(engine.getState().isActive == true)
        }
    }

    @Test func sinkCanReentrantlyStopAndDropQueuedValue() async throws {
        try await Their.stress {
            let engineStore = Their.Lock<JobEngine<Int, JobEngineTestsError>?>(nil)
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = JobEngineEventRecorder()
            let reportStore = Their.Lock<Their.WorkReport<Int, JobEngineTestsError>?>(nil)
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    eventRecorder.append(event)
                    if event.value == 1 {
                        reportStore.withLock { currentReport in
                            currentReport
                        }?(.value(2))
                        engineStore.withLock { currentEngine in
                            currentEngine
                        }?.stop()
                    }
                },
                work: { report in
                    reportStore.withLock { currentReport in
                        currentReport = report
                    }
                    return cancelRecorder.cancel()
                }
            )
            engineStore.withLock { currentEngine in
                currentEngine = engine
            }
            #expect(engine.start() == true)
            let report = reportStore.withLock { currentReport in
                currentReport
            }
            report?(.value(1))

            #expect(eventRecorder.events == [
                .value(1),
                .message(.stopStopped)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(cancelRecorder.cancelCallsCount == 1)
        }
    }

    @Test func sinkCanReentrantlyTerminateAndDropQueuedValue() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let engineStore = Their.Lock<JobEngine<Int, JobEngineTestsError>?>(nil)
            let eventRecorder = JobEngineEventRecorder()
            let reportStore = Their.Lock<Their.WorkReport<Int, JobEngineTestsError>?>(nil)
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    eventRecorder.append(event)
                    if event.value == 1 {
                        reportStore.withLock { currentReport in
                            currentReport
                        }?(.value(2))
                        engineStore.withLock { currentEngine in
                            currentEngine
                        }?.terminate()
                    }
                },
                work: { report in
                    reportStore.withLock { currentReport in
                        currentReport = report
                    }
                    return cancelRecorder.cancel()
                }
            )
            engineStore.withLock { currentEngine in
                currentEngine = engine
            }
            #expect(engine.start() == true)
            let report = reportStore.withLock { currentReport in
                currentReport
            }
            report?(.value(1))

            #expect(eventRecorder.events == [
                .value(1),
                .message(.terminateStopped)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(cancelRecorder.cancelCallsCount == 1)
        }
    }

    @Test func sinkReentrantlyFailingOnValueTerminatesAndCancelsAfterValue() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = JobEngineEventRecorder()
            let reportStore = Their.Lock<Their.WorkReport<Int, JobEngineTestsError>?>(nil)
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    eventRecorder.append(event)
                    if event.value == 1 {
                        reportStore.withLock { currentReport in
                            currentReport
                        }?(.failure(.sample))
                    }
                },
                work: { report in
                    reportStore.withLock { currentReport in
                        currentReport = report
                    }
                    return cancelRecorder.cancel()
                }
            )
            #expect(engine.start() == true)
            let report = reportStore.withLock { currentReport in
                currentReport
            }
            report?(.value(1))

            #expect(eventRecorder.events == [
                .value(1),
                .failure(.sample)
            ])
            #expect(cancelRecorder.cancelCallsCount == 1)
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func sinkSelfFeedingReportLoopDrainsIterativelyAndPreservesOrder() async throws {
        try await Their.stress {
            let bound = 100
            let depth = Their.Lock(JobEngineDepthState())
            let eventRecorder = JobEngineEventRecorder()
            let reportStore = Their.Lock<Their.WorkReport<Int, JobEngineTestsError>?>(nil)
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    guard let value = event.value else {
                        return
                    }
                    depth.withLock { state in
                        state.current += 1
                        state.maximum = max(state.maximum, state.current)
                    }
                    eventRecorder.append(event)
                    if value < bound {
                        reportStore.withLock { currentReport in
                            currentReport
                        }?(.value(value + 1))
                    }
                    depth.withLock { state in
                        state.current -= 1
                    }
                },
                work: { report in
                    reportStore.withLock { currentReport in
                        currentReport = report
                    }
                    return {}
                }
            )
            #expect(engine.start() == true)
            let report = reportStore.withLock { currentReport in
                currentReport
            }
            report?(.value(1))

            let recorded = eventRecorder.events
            #expect(depth.withLock { state in state.maximum } == 1)
            #expect(recorded.count == bound)
            #expect(recorded.enumerated().allSatisfy { $0.element.value == $0.offset + 1 })
            #expect(engine.getState().isActive == true)
        }
    }

    @Test func startDeliversReentrantValueReportedDuringStarting() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let orderRecorder = Their.TestEventRecorder<String>()
            let reportStore = Their.Lock<Their.WorkReport<Int, JobEngineTestsError>?>(nil)
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        orderRecorder.append("value1-enter")
                        reportStore.withLock { currentReport in
                            currentReport
                        }?(.value(2))
                        orderRecorder.append("value1-after-report")
                    }
                    if event.value == 2 {
                        orderRecorder.append("value2")
                    }
                    eventRecorder.append(event)
                },
                work: { report in
                    reportStore.withLock { currentReport in
                        currentReport = report
                    }
                    report(.value(1))
                    return {}
                }
            )
            #expect(engine.start() == true)

            #expect(orderRecorder.events == [
                "value1-enter",
                "value1-after-report",
                "value2"
            ])
            #expect(eventRecorder.events == [
                .value(1),
                .value(2)
            ])
            #expect(engine.getState().isActive == true)
        }
    }

    @Test func startDeliversValueReportedDuringStarting() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: { report in
                    report(.value(7))
                    return {}
                }
            )
            #expect(engine.start() == true)
            #expect(eventRecorder.events == [.value(7)])
            #expect(engine.getState().isActive == true)
        }
    }

    @Test func startReturnsMisuseForRepeatedStart() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let misuseRecorder = JobMisuseRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                misuseHandler: misuseRecorder.handler,
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            #expect(engine.getState().isActive == true)
            #expect(startRecorder.startCallsCount == 1)
            _ = engine.start()
            try await misuseRecorder.waitForCount(1)
            #expect(misuseRecorder.misuses.count == 1)
            #expect(engine.getState().isActive == true)
        }
    }

    @Test func startReturnsMisuseWhenTerminated() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let misuseRecorder = JobMisuseRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                misuseHandler: misuseRecorder.handler,
                sink: eventRecorder.append(_:),
                state: .terminated
            )
            _ = engine.start()
            try await misuseRecorder.waitForCount(1)
            #expect(misuseRecorder.misuses.count == 1)
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func startStartsJob() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            #expect(engine.getState().isIdle == true)
            #expect(startRecorder.startCallsCount == 0)
            _ = engine.start()
            #expect(engine.getState().isActive == true)
            #expect(startRecorder.startCallsCount == 1)
            #expect(startRecorder.cancelCallsCount == 0)
        }
    }

    @Test func startStopBeforeWorkReturnsCancelInvokesReturnedCancelOnce() async throws {
        try await Their.stress(count: 1) {
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = JobEngineEventRecorder()
            let releaseWork = DispatchSemaphore(value: 0)
            let workEntered = JobEngineSignal()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: { _ in
                    workEntered.signal()
                    releaseWork.wait()
                    return cancelRecorder.cancel()
                }
            )

            let startTask = BlockingWork {
                engine.start()
            }
            try await workEntered.wait()
            engine.stop()
            #expect(eventRecorder.events == [.message(.stopStopped)])
            #expect(cancelRecorder.cancelCallsCount == 0)

            releaseWork.signal()
            #expect(try await startTask.value == true)
            try await cancelRecorder.waitForCancelCallsCount(1)
            #expect(cancelRecorder.cancelCallsCount == 1)
        }
    }

    @Test func startSynchronousEndCancelsReturnedWorkAfterEnd() async throws {
        try await Their.stress {
            let orderRecorder = Their.TestEventRecorder<String>()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if case .finished = event {
                        orderRecorder.append("end")
                    }
                },
                work: { report in
                    report(.finished)
                    return {
                        orderRecorder.append("cancel")
                    }
                }
            )
            #expect(engine.start() == true)
            #expect(engine.getState().isTerminated == true)
            #expect(orderRecorder.events == ["end", "cancel"])
        }
    }

    @Test func startSynchronousFailureCancelsReturnedWorkAfterFailure() async throws {
        try await Their.stress {
            let orderRecorder = Their.TestEventRecorder<String>()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.isFailureError {
                        orderRecorder.append("fail")
                    }
                },
                work: { report in
                    report(.failure(.sample))
                    return {
                        orderRecorder.append("cancel")
                    }
                }
            )
            #expect(engine.start() == true)
            #expect(engine.getState().isTerminated == true)
            #expect(orderRecorder.events == ["fail", "cancel"])
        }
    }

    @Test func startTerminateBeforeWorkReturnsCancelInvokesReturnedCancelOnce() async throws {
        try await Their.stress(count: 1) {
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = JobEngineEventRecorder()
            let releaseWork = DispatchSemaphore(value: 0)
            let workEntered = JobEngineSignal()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: { _ in
                    workEntered.signal()
                    releaseWork.wait()
                    return cancelRecorder.cancel()
                }
            )

            let startTask = BlockingWork {
                engine.start()
            }
            try await workEntered.wait()
            engine.terminate()
            #expect(eventRecorder.events == [.message(.terminateStopped)])
            #expect(cancelRecorder.cancelCallsCount == 0)

            releaseWork.signal()
            #expect(try await startTask.value == true)
            try await cancelRecorder.waitForCancelCallsCount(1)
            #expect(cancelRecorder.cancelCallsCount == 1)
        }
    }

    @Test func startYieldsOutputsThenFailureCancelsAndTerminates() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            startRecorder.report?(.value(10))
            try await eventRecorder.waitForEventCount(1)
            #expect(eventRecorder.last?.value == 10)
            #expect(engine.getState().isActive == true)
            startRecorder.report?(.value(20))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.last?.value == 20)
            #expect(engine.getState().isActive == true)
            startRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(3)
            #expect(eventRecorder.last?.isFailureError == true)
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func stopCancelsAndTerminatesWhenActive() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.stop()
            #expect(eventRecorder.events == [.message(.stopStopped)])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func stopIgnoresLateUpstreamOutputAfterStop() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.stop()
            startRecorder.report?(.value(42))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [
                .message(.stopStopped),
                .message(.emitOutputIgnoredBecauseStateIsNotActive)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func stopImmediatelyCancelsAndInvalidatesQueuedValueBehindInFlightValue() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobEngineEventRecorder()
            let releaseValue = DispatchSemaphore(value: 0)
            let startRecorder = WorkRecorder()
            let valueEntered = JobEngineSignal()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        valueEntered.signal()
                        releaseValue.wait()
                    }
                    eventRecorder.append(event)
                },
                work: startRecorder.work
            )
            #expect(engine.start() == true)
            let report = startRecorder.report
            #expect(report != nil)

            let valueTask = BlockingWork {
                report?(.value(1))
            }
            try await valueEntered.wait()
            report?(.value(2))
            engine.stop()
            #expect(eventRecorder.events == [.message(.stopStopped)])
            #expect(startRecorder.cancelCallsCount == 1)

            releaseValue.signal()
            try await valueTask.value
            try await eventRecorder.waitForEventCount(2)

            #expect(eventRecorder.events == [
                .message(.stopStopped),
                .value(1)
            ])
        }
    }

    @Test func stopRepeatedlyCancelsOnlyOnce() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.stop()
            engine.stop()
            #expect(eventRecorder.events == [
                .message(.stopStopped),
                .message(.stopIgnoredBecauseStateIsNotActive)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func stopReportsBreakWhenIdle() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .idle
            )
            engine.stop()
            #expect(eventRecorder.events == [.message(.stopIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isIdle == true)
        }
    }

    @Test func stopReportsBreakWhenTerminated() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .terminated
            )
            engine.stop()
            #expect(eventRecorder.events == [.message(.stopIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func stopWhileStartReturnedQueuedBehindInFlightReportInvokesReturnedCancelOnce() async throws {
        try await Their.stress(count: 1) {
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = JobEngineEventRecorder()
            let releaseValue = DispatchSemaphore(value: 0)
            let valueEntered = JobEngineSignal()
            let valueEnteredForWork = DispatchSemaphore(value: 0)
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        valueEntered.signal()
                        valueEnteredForWork.signal()
                        releaseValue.wait()
                    }
                    eventRecorder.append(event)
                },
                work: { report in
                    DispatchQueue.global().async {
                        report(.value(1))
                    }
                    valueEnteredForWork.wait()
                    return cancelRecorder.cancel()
                }
            )

            let startTask = BlockingWork {
                engine.start()
            }
            try await valueEntered.wait()
            #expect(try await startTask.value == true)

            engine.stop()
            #expect(eventRecorder.events == [.message(.stopStopped)])
            #expect(cancelRecorder.cancelCallsCount == 1)

            releaseValue.signal()
            try await eventRecorder.waitForEventCount(2)
        }
    }

    @Test func terminateCancelsAndTerminatesWhenActive() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.terminate()
            #expect(eventRecorder.events == [.message(.terminateStopped)])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func terminateIgnoresLateUpstreamFailureAfterTerminate() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.terminate()
            startRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [
                .message(.terminateStopped),
                .message(.emitFailureIgnoredBecauseStateIsNotActive)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func terminateIgnoresLateUpstreamOutputAfterTerminate() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.terminate()
            startRecorder.report?(.value(42))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [
                .message(.terminateStopped),
                .message(.emitOutputIgnoredBecauseStateIsNotActive)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func terminateImmediatelyCancelsAndInvalidatesQueuedValueBehindInFlightValue() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobEngineEventRecorder()
            let releaseValue = DispatchSemaphore(value: 0)
            let startRecorder = WorkRecorder()
            let valueEntered = JobEngineSignal()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        valueEntered.signal()
                        releaseValue.wait()
                    }
                    eventRecorder.append(event)
                },
                work: startRecorder.work
            )
            #expect(engine.start() == true)
            let report = startRecorder.report
            #expect(report != nil)

            let valueTask = BlockingWork {
                report?(.value(1))
            }
            try await valueEntered.wait()
            report?(.value(2))
            engine.terminate()
            #expect(eventRecorder.events == [.message(.terminateStopped)])
            #expect(startRecorder.cancelCallsCount == 1)

            releaseValue.signal()
            try await valueTask.value
            try await eventRecorder.waitForEventCount(2)

            #expect(eventRecorder.events == [
                .message(.terminateStopped),
                .value(1)
            ])
        }
    }

    @Test func terminateRepeatedlyCancelsOnlyOnce() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let startRecorder = WorkRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                work: startRecorder.work
            )
            _ = engine.start()
            engine.terminate()
            engine.terminate()
            #expect(eventRecorder.events == [
                .message(.terminateStopped),
                .message(.terminateIgnoredBecauseStateIsNotActive)
            ])
            #expect(engine.getState().isTerminated == true)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func terminateReportsBreakWhenIdle() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .idle
            )
            engine.terminate()
            #expect(eventRecorder.events == [.message(.terminateIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isIdle == true)
        }
    }

    @Test func terminateReportsBreakWhenTerminated() async throws {
        try await Their.stress {
            let eventRecorder = JobEngineEventRecorder()
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: eventRecorder.append(_:),
                state: .terminated
            )
            engine.terminate()
            #expect(eventRecorder.events == [.message(.terminateIgnoredBecauseStateIsNotActive)])
            #expect(engine.getState().isTerminated == true)
        }
    }

    @Test func terminateWhileStartReturnedQueuedBehindInFlightReportInvokesReturnedCancelOnce() async throws {
        try await Their.stress(count: 1) {
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = JobEngineEventRecorder()
            let releaseValue = DispatchSemaphore(value: 0)
            let valueEntered = JobEngineSignal()
            let valueEnteredForWork = DispatchSemaphore(value: 0)
            let engine = JobEngine<Int, JobEngineTestsError>(
                sink: { event in
                    if event.value == 1 {
                        valueEntered.signal()
                        valueEnteredForWork.signal()
                        releaseValue.wait()
                    }
                    eventRecorder.append(event)
                },
                work: { report in
                    DispatchQueue.global().async {
                        report(.value(1))
                    }
                    valueEnteredForWork.wait()
                    return cancelRecorder.cancel()
                }
            )

            let startTask = BlockingWork {
                engine.start()
            }
            try await valueEntered.wait()
            #expect(try await startTask.value == true)

            engine.terminate()
            #expect(eventRecorder.events == [.message(.terminateStopped)])
            #expect(cancelRecorder.cancelCallsCount == 1)

            releaseValue.signal()
            try await eventRecorder.waitForEventCount(2)
        }
    }
}

private enum JobEngineControlObservation: Equatable, Sendable {

    case cancel
    case event(JobEngineEvent<Int, JobEngineTestsError>)
}

private enum JobEngineTestsError: Swift.Error, Sendable {

    case sample
}

private typealias JobEngineSignal = Their.TestSignal

private struct JobEngineDepthState: Sendable {

    var current = 0
    var maximum = 0
}

private typealias JobEngineEventRecorder = Their.TestEventRecorder<JobEngineEvent<Int, JobEngineTestsError>>
private typealias JobMisuseRecorder = Their.TestMisuseRecorder
private typealias WorkRecorder = Their.TestWorkRecorder<Int, JobEngineTestsError>

private extension JobEngineEvent where Value == Int, Failure == JobEngineTestsError {

    var isFailureError: Bool {
        switch self {
        case .failure(.sample):
            return true
        case .finished, .message, .failure, .value:
            return false
        }
    }

    var value: Int? {
        switch self {
        case .value(let value):
            return value
        case .finished, .message, .failure:
            return nil
        }
    }
}

private extension ArraySlice where Element == JobEngineEvent<Int, JobEngineTestsError> {

    var containsValue: Bool {
        contains { event in
            switch event {
            case .value:
                return true
            case .finished, .failure, .message:
                return false
            }
        }
    }
}
