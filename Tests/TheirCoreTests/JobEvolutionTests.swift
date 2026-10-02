import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobEvolutionTests {

    /// The mapper holds the drainer on a Dispatch worker. Cancellation finishes
    /// before it returns, so its mapped failure must not start a sink callback
    /// after cancellation. No executor timing or sleeps decide the ordering.
    @Test func evolveCancelDuringFailureMapperSuppressesTerminalCallback() async throws {
        try await Their.stress(count: 1) {
            let mapperCalls = Their.TestCountRecorder()
            let mapperEntered = JobEvolutionTestSignal()
            let releaseMapper = DispatchSemaphore(value: 0)
            let eventRecorder = JobEvolutionOtherEventRecorder<Int>()
            let upstream = JobEvolutionJobDriver()
            let evolved = upstream.job.mapError { failure in
                _ = mapperCalls.increment()
                mapperEntered.signal()
                releaseMapper.wait()
                return JobEvolutionOtherTestsError.wrapped(failure)
            }
            let cancel = evolved.subscribe(eventRecorder.append(_:))
            defer {
                releaseMapper.signal()
                cancel()
            }
            let emitWork = BlockingWork {
                upstream.emit(failure: .sample)
            }
            do {
                try await mapperEntered.wait()

                cancel()
                #expect(eventRecorder.events.isEmpty == true)
                releaseMapper.signal()
                try await emitWork.value
            } catch {
                releaseMapper.signal()
                // An unstructured task does not inherit this body's cancelled
                // status, so the Dispatch worker is joined even on timeout.
                let cleanup = Task {
                    try await emitWork.value
                }
                _ = await cleanup.result
                throw error
            }

            #expect(mapperCalls.count == 1)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(upstream.cancelCallsCount == 1)
        }
    }

    @Test func evolveCancelsUpstreamAndSuppressesLateFailureWhenSubscriptionCancels() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let upstream = JobEvolutionJobDriver()
            let evolved: Their.Job<Int, JobEvolutionTestsError> = upstream.job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            let cancel = evolved.subscribe(eventRecorder.append(_:))
            cancel()
            try await upstream.waitForCancelCallsCount(1)
            upstream.emit(failure: .sample)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func evolveCancelsUpstreamAndSuppressesLateOutputWhenSubscriptionCancels() async throws {
        try await Their.stress {
            let cancelSignal = JobEvolutionTestSignal()
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            let cancel = evolved.subscribe(eventRecorder.append(_:))
            cancel()
            try await cancelSignal.wait()
            workRecorder.report?(.value(10))
            #expect(eventRecorder.events.isEmpty == true)
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveCancelsUpstreamImmediatelyWhenTerminalEndArrivesBeforeCancelIsStored() async throws {
        try await Their.stress {
            let cancelRecorder = JobEvolutionCancelRecorder()
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let upstream = Their.Job<Int, JobEvolutionTestsError>(
                misuseHandler: { _ in },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    sink(.finished)
                    return cancelRecorder.cancel()
                }
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = upstream.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(1)
            try await cancelRecorder.waitForCancelCallsCount(1)
            #expect(cancelRecorder.cancelCallsCount == 1)
            #expect(eventRecorder.events == [.finished])
        }
    }

    @Test func evolveCancelsUpstreamImmediatelyWhenTerminalEventArrivesBeforeCancelIsStored() async throws {
        try await Their.stress {
            let cancelRecorder = JobEvolutionCancelRecorder()
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let upstream = Their.Job<Int, JobEvolutionTestsError>(
                misuseHandler: { _ in },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    sink(.failure(.sample))
                    return cancelRecorder.cancel()
                }
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = upstream.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(1)
            try await cancelRecorder.waitForCancelCallsCount(1)
            #expect(cancelRecorder.cancelCallsCount == 1)
            #expect(eventRecorder.events == [.failure(.sample)])
        }
    }

    @Test func evolveDeinitCancelsUpstreamSubscription() async throws {
        try await Their.stress {
            let cancelSignal = JobEvolutionTestSignal()
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            var evolved: Their.Job<Int, JobEvolutionTestsError>? = job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            _ = evolved?.subscribe(eventRecorder.append(_:))
            evolved = nil
            try await cancelSignal.wait()
            #expect(eventRecorder.events.isEmpty == true)
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveDeinitSuppressesLateFailure() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let upstream = JobEvolutionJobDriver()
            var evolved: Their.Job<Int, JobEvolutionTestsError>? = upstream.job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            _ = evolved?.subscribe(eventRecorder.append(_:))
            evolved = nil
            try await upstream.waitForCancelCallsCount(1)
            upstream.emit(failure: .sample)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    /// Pins the FIFO ordering of terminal end behind a blocked transform:
    /// the end is processed by the same single drainer, so it cannot overtake
    /// or interrupt the value being transformed — the value is committed and
    /// delivered first, then the queued end terminates the lifecycle and
    /// cancels the upstream exactly once.
    @Test func evolveEndQueuedBehindBlockedTransformDoesNotOvertakeValue() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let releaseTransform = DispatchSemaphore(value: 0)
            let transformEntered = JobEvolutionTestSignal()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                if output == 1 {
                    transformEntered.signal()
                    releaseTransform.wait()
                }
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            let emitTask = BlockingWork {
                workRecorder.report?(.value(1))
            }
            try await transformEntered.wait()
            workRecorder.report?(.finished)
            releaseTransform.signal()
            try await emitTask.value
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value(1), .finished])
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }


    /// A failure mapper is user code, like the value transform. Cancelling the
    /// subscription inside it must suppress the terminal callback that has not
    /// started yet, even though the upstream itself is already terminated.
    @Test func evolveFailureMapperReentrantCancelSuppressesTerminalCallback() async throws {
        try await Their.stress {
            let cancelBox = Their.Lock<Their.WorkCancel?>(nil)
            let mapperCalls = Their.TestCountRecorder()
            let eventRecorder = JobEvolutionOtherEventRecorder<Int>()
            let upstream = JobEvolutionJobDriver()
            let evolved = upstream.job.mapError { failure in
                _ = mapperCalls.increment()
                cancelBox.withLock { $0 }?()
                return JobEvolutionOtherTestsError.wrapped(failure)
            }
            let cancel = evolved.subscribe(eventRecorder.append(_:))
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

            upstream.emit(failure: .sample)

            #expect(mapperCalls.count == 1)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(upstream.cancelCallsCount == 1)
        }
    }

    /// Pins the FIFO ordering of terminal failure behind a blocked transform:
    /// the failure is processed by the same single drainer, so it cannot
    /// overtake or interrupt the value being transformed — the value is
    /// committed and delivered first, then the queued failure terminates the
    /// lifecycle and cancels the upstream exactly once.
    @Test func evolveFailureQueuedBehindBlockedTransformDoesNotOvertakeValue() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let releaseTransform = DispatchSemaphore(value: 0)
            let transformEntered = JobEvolutionTestSignal()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                if output == 1 {
                    transformEntered.signal()
                    releaseTransform.wait()
                }
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            let emitTask = BlockingWork {
                workRecorder.report?(.value(1))
            }
            try await transformEntered.wait()
            workRecorder.report?(.failure(.sample))
            releaseTransform.signal()
            try await emitTask.value
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value(1), .failure(.sample)])
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    /// The upstream cancel returns while the failure mapper still holds the
    /// terminating lifecycle. It must be invoked immediately, without clearing
    /// the pending terminal or leaving an owned cancel behind after delivery.
    @Test func evolveLateReturnedCancelDuringFailureMapperIsCancelledOnce() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobEvolutionOtherEventRecorder<Int>()
            let mapperCalls = Their.TestCountRecorder()
            let mapperEntered = DispatchSemaphore(value: 0)
            let mapperRelease = DispatchSemaphore(value: 0)
            let reportWorkSlot = Their.Lock<BlockingWork<Void>?>(nil)
            let workRecorder = JobEvolutionWorkRecorder()
            defer {
                mapperEntered.signal()
                mapperRelease.signal()
            }
            let upstream = Their.Job { report in
                let cancel = workRecorder.work(report: report)
                let reportWork = BlockingWork {
                    report(.failure(.sample))
                }
                reportWorkSlot.withLock { $0 = reportWork }
                mapperEntered.wait()
                return cancel
            }
            let evolved = upstream.mapError { failure in
                _ = mapperCalls.increment()
                mapperEntered.signal()
                mapperRelease.wait()
                return JobEvolutionOtherTestsError.wrapped(failure)
            }
            let subscribeWork = BlockingWork {
                evolved.subscribe(eventRecorder.append(_:))
            }
            do {
                let cancel = try await subscribeWork.value
                defer { cancel() }
                let reportWork = try #require(reportWorkSlot.withLock { $0 })
                let cancelsBeforeMapperReturns = workRecorder.cancelCallsCount
                let eventsBeforeMapperReturns = eventRecorder.events

                mapperRelease.signal()
                try await reportWork.value
                cancel()
                cancel()

                #expect(cancelsBeforeMapperReturns == 1)
                #expect(eventsBeforeMapperReturns.isEmpty == true)
                #expect(mapperCalls.count == 1)
                #expect(eventRecorder.events == [.failure(.wrapped(.sample))])
                #expect(workRecorder.startCallsCount == 1)
                #expect(workRecorder.cancelCallsCount == 1)
            } catch {
                mapperEntered.signal()
                mapperRelease.signal()
                // Join both Dispatch workers from a task that does not inherit
                // cancellation of the timed-out scenario.
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

    @Test func evolvePropagatesTerminalEndBypassingTransformAndSuppressesLateOutput() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let transformRecorder = Their.TestCountRecorder()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                transformRecorder.increment()
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.value(10))
            try await eventRecorder.waitForEventCount(1)
            workRecorder.report?(.finished)
            try await eventRecorder.waitForEventCount(2)
            workRecorder.report?(.value(20))
            workRecorder.report?(.finished)
            #expect(eventRecorder.events == [.value(10), .finished])
            #expect(transformRecorder.count == 1)
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolvePropagatesTerminalEndWithMappedFailureType() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionOtherEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved = job.evolve(
                failure: JobEvolutionOtherTestsError.wrapped(_:),
                initial: 0
            ) { state, output in
                state += output
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.finished)
            try await eventRecorder.waitForEventCount(1)
            workRecorder.report?(.value(10))
            #expect(eventRecorder.events == [.finished])
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolvePropagatesTerminalFailureAndSuppressesLateOutput() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(1)
            workRecorder.report?(.value(10))
            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolvePropagatesTerminalMappedFailureAndSuppressesLateOutput() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionOtherEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved = job.evolve(
                failure: JobEvolutionOtherTestsError.wrapped(_:),
                initial: 0
            ) { state, output in
                state += output
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(1)
            workRecorder.report?(.value(10))
            #expect(eventRecorder.events == [.failure(.wrapped(.sample))])
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolvePropagatesUpstreamMisuseWhenUpstreamIsAlreadySubscribed() async throws {
        try await Their.stress {
            let cancelSignal = JobEvolutionTestSignal()
            let directRecorder = JobEvolutionEventRecorder<Int>()
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let misuseRecorder = JobEvolutionMisuseRecorder()
            let workRecorder = JobEvolutionWorkRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let job = Their.Job<Int, JobEvolutionTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            let directCancel = job.subscribe(directRecorder.append(_:))
            let evolvedCancel = evolved.subscribe(eventRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            evolvedCancel()
            directCancel()
            try await cancelSignal.wait()
            #expect(eventRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.count == 1)
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveRejectsResubscribeAfterCancelWithoutStartingSecondUpstreamSubscription() async throws {
        try await Their.stress {
            let firstRecorder = JobEvolutionEventRecorder<Int>()
            let misuseRecorder = JobEvolutionMisuseRecorder()
            let secondRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            let firstCancel = evolved.subscribe(firstRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            firstCancel()
            try await workRecorder.waitForCancelCallsCount(1)
            let secondCancel = evolved.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            secondCancel()
            workRecorder.report?(.value(5))
            #expect(firstRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Evolved Job supports only one subscriber per lifecycle."
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveRejectsResubscribeAfterTerminalEndWithoutStartingSecondUpstreamSubscription() async throws {
        try await Their.stress {
            let firstRecorder = JobEvolutionEventRecorder<Int>()
            let misuseRecorder = JobEvolutionMisuseRecorder()
            let secondRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            _ = evolved.subscribe(firstRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.finished)
            try await firstRecorder.waitForEventCount(1)
            let secondCancel = evolved.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            secondCancel()
            #expect(firstRecorder.events == [.finished])
            #expect(misuseRecorder.misuses.count == 1)
            #expect(secondRecorder.events.isEmpty == true)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveRejectsResubscribeAfterTerminalFailureWithoutStartingSecondUpstreamSubscription() async throws {
        try await Their.stress {
            let firstRecorder = JobEvolutionEventRecorder<Int>()
            let misuseRecorder = JobEvolutionMisuseRecorder()
            let secondRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            _ = evolved.subscribe(firstRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.failure(.sample))
            try await firstRecorder.waitForEventCount(1)
            let secondCancel = evolved.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            secondCancel()
            #expect(firstRecorder.events == [.failure(.sample)])
            #expect(misuseRecorder.misuses.count == 1)
            #expect(secondRecorder.events.isEmpty == true)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveRejectsSecondSubscriberWithCreationLocationTrace() async throws {
        try await Their.stress {
            let firstRecorder = JobEvolutionEventRecorder<Int>()
            let misuseRecorder = JobEvolutionMisuseRecorder()
            let secondRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(
                fileID: "CoreTests/EvolveTrace.swift",
                function: "makeTracedEvolve()",
                initial: 0,
                line: 321
            ) { state, output in
                state += output
                return state
            }
            let firstCancel = evolved.subscribe(firstRecorder.append(_:))
            let secondCancel = evolved.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            try await workRecorder.waitForStartCallsCount(1)
            secondCancel()
            firstCancel()
            #expect(misuseRecorder.misuses.first?.trace == [
                Their.MisuseLocation(
                    fileID: "CoreTests/EvolveTrace.swift",
                    function: "makeTracedEvolve()",
                    line: 321
                )
            ])
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveRejectsSecondSubscriberWithoutStartingSecondUpstreamSubscription() async throws {
        try await Their.stress {
            let firstRecorder = JobEvolutionEventRecorder<Int>()
            let misuseRecorder = JobEvolutionMisuseRecorder()
            let secondRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                misuseHandler: misuseRecorder.handler,
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                return state
            }
            let firstCancel = evolved.subscribe(firstRecorder.append(_:))
            let secondCancel = evolved.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            try await workRecorder.waitForStartCallsCount(1)
            secondCancel()
            firstCancel()
            #expect(firstRecorder.events.isEmpty == true)
            #expect(secondRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.count == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    /// Pins the drainer execution model: the transform runs outside the
    /// internal lock, so it may synchronously cancel its own derived
    /// subscription without deadlocking. The cancel lands while the transform
    /// is in flight, so the emission produced by that very transform is
    /// dropped and the upstream is cancelled exactly once.
    @Test func evolveTransformReentrantCancelTerminatesWithoutDeadlockOrEmission() async throws {
        try await Their.stress {
            let cancelBox = Their.Lock<Their.WorkCancel?>(nil)
            let cancelSignal = JobEvolutionTestSignal()
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                if output == 1 {
                    cancelBox.withLock { cancel in cancel }?()
                }
                return state
            }
            let cancel = evolved.subscribe(eventRecorder.append(_:))
            cancelBox.withLock { stored in
                stored = cancel
            }
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.value(1))
            try await cancelSignal.wait()
            workRecorder.report?(.value(2))
            #expect(eventRecorder.events.isEmpty == true)
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveUpdatesStateSequentiallyAndSuppressesNilOutputs() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let evolved: Their.Job<Int, JobEvolutionTestsError> = job.evolve(initial: 0) { state, output in
                state += output
                guard state.isMultiple(of: 2) else {
                    return nil
                }
                return state
            }
            let cancel = evolved.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.value(1))
            workRecorder.report?(.value(1))
            try await eventRecorder.waitForEventCount(1)
            workRecorder.report?(.value(2))
            try await eventRecorder.waitForEventCount(2)
            cancel()
            #expect(eventRecorder.events == [.value(2), .value(4)])
            #expect(workRecorder.startCallsCount == 1)
        }
    }


    @Test func mapErrorTransformsFailureAndPropagatesValue() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionOtherEventRecorder<Int>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let mapped = job.mapError { failure in
                JobEvolutionOtherTestsError.wrapped(failure)
            }
            _ = mapped.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.value(30))
            try await eventRecorder.waitForEventCount(1)
            workRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value(30), .failure(.wrapped(.sample))])
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func mapTransformsOutputAndPropagatesFailure() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionEventRecorder<String>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let mapped = job.map { output in
                "value-\(output)"
            }
            _ = mapped.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.value(10))
            try await eventRecorder.waitForEventCount(1)
            workRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value("value-10"), .failure(.sample)])
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func mapTransformsOutputAndTransformsFailure() async throws {
        try await Their.stress {
            let eventRecorder = JobEvolutionOtherEventRecorder<String>()
            let workRecorder = JobEvolutionWorkRecorder()
            let job = Their.Job<Int, JobEvolutionTestsError>(
                work: workRecorder.work(report:)
            )
            let mapped = job.map(
                failure: JobEvolutionOtherTestsError.wrapped(_:)
            ) { output in
                "value-\(output)"
            }
            _ = mapped.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            workRecorder.report?(.value(40))
            try await eventRecorder.waitForEventCount(1)
            workRecorder.report?(.failure(.sample))
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value("value-40"), .failure(.wrapped(.sample))])
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

}

private enum JobEvolutionTestsError: Equatable, Swift.Error, Sendable {

    case sample
}

private enum JobEvolutionOtherTestsError: Equatable, Swift.Error, Sendable {

    case wrapped(JobEvolutionTestsError)
}

private typealias JobEvolutionCancelRecorder = Their.TestCancelRecorder
private typealias JobEvolutionEventRecorder<Value: Sendable> = Their.TestEventRecorder<Their.JobEvent<Value, JobEvolutionTestsError>>
private typealias JobEvolutionJobDriver = Their.TestJobDriver<Int, JobEvolutionTestsError>
private typealias JobEvolutionMisuseRecorder = Their.TestMisuseRecorder
private typealias JobEvolutionOtherEventRecorder<Value: Sendable> = Their.TestEventRecorder<Their.JobEvent<Value, JobEvolutionOtherTestsError>>
private typealias JobEvolutionTestSignal = Their.TestSignal
private typealias JobEvolutionWorkRecorder = Their.TestWorkRecorder<Int, JobEvolutionTestsError>
