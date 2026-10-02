import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobMergeTests {

    @Test func mergeCancelRepeatedlyCancelsUpstreamsOnlyOnce() async throws {
        try await Their.stress {
            let drivers = (0 ..< 2).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            let cancel = job.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            cancel()
            cancel()
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }

            #expect(eventRecorder.events.isEmpty == true)
            #expect(drivers.map(\.cancelCallsCount) == [1, 1])
            #expect(drivers.map(\.startCallsCount) == [1, 1])
        }
    }

    @Test func mergeCancelsAllUpstreamsAndSuppressesLateValues() async throws {
        try await Their.stress {
            let drivers = (0 ..< 3).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            let cancel = job.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            cancel()
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }
            drivers[0].emit(value: 10)
            drivers[1].emit(value: 20)
            drivers[2].emit(value: 30)

            #expect(eventRecorder.events.isEmpty == true)
            #expect(drivers.map(\.cancelCallsCount) == [1, 1, 1])
            #expect(drivers.map(\.startCallsCount) == [1, 1, 1])
        }
    }

    @Test func mergeCancelsCurrentUpstreamWhenSynchronousEndOfLastLiveUpstreamArrivesBeforeCancelIsStored() async throws {
        try await Their.stress {
            let cancelRecorder = JobMergeCancelRecorder()
            let eventRecorder = JobMergeEventRecorder()
            let synchronousEndingJob = Their.Job<Int, JobMergeTestsError>(
                misuseHandler: { _ in },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    sink(.finished)
                    return cancelRecorder.cancel()
                }
            )
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge([synchronousEndingJob])

            _ = job.subscribe(eventRecorder.append(_:))

            try await eventRecorder.waitForEventCount(1)
            try await cancelRecorder.waitForCancelCallsCount(1)
            #expect(eventRecorder.events == [.finished])
            #expect(cancelRecorder.cancelCallsCount == 1)
        }
    }

    @Test func mergeCancelsCurrentUpstreamWhenSynchronousFailureArrivesBeforeCancelIsStored() async throws {
        try await Their.stress {
            let cancelRecorder = JobMergeCancelRecorder()
            let eventRecorder = JobMergeEventRecorder()
            let otherDriver = JobMergeJobDriver()
            let synchronousFailingJob = Their.Job<Int, JobMergeTestsError>(
                misuseHandler: { _ in },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    sink(.failure(.sample))
                    return cancelRecorder.cancel()
                }
            )
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(
                synchronousFailingJob,
                otherDriver.job
            )

            _ = job.subscribe(eventRecorder.append(_:))

            try await eventRecorder.waitForEventCount(1)
            try await cancelRecorder.waitForCancelCallsCount(1)
            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(cancelRecorder.cancelCallsCount == 1)
            #expect(otherDriver.startCallsCount == 0)
        }
    }

    @Test func mergeConcurrentEndsTerminateOnceWithSingleEnd() async throws {
        try await Their.stress(count: 1, timeout: .seconds(2)) {
            let drivers = (0 ..< Their.stressCountDefault).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            _ = job.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            try await Their.stress(count: drivers.count) { index in
                drivers[index].emitFinished()
            }
            try await eventRecorder.waitForEventCount(1)
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }

            #expect(eventRecorder.events == [.finished])
            #expect(drivers.map(\.cancelCallsCount) == Array(repeating: 1, count: drivers.count))
            #expect(drivers.map(\.startCallsCount) == Array(repeating: 1, count: drivers.count))
        }
    }

    @Test func mergeConcurrentFailureAndValuesTerminatesOnce() async throws {
        try await Their.stress(count: 1, timeout: .seconds(2)) {
            let drivers = (0 ..< Their.stressCountDefault).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            _ = job.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            try await Their.stress(count: drivers.count) { index in
                if index == 0 {
                    drivers[index].emit(failure: .sample)
                } else {
                    drivers[index].emit(value: index)
                }
            }

            _ = try await eventRecorder.waitForEvent { event in
                switch event {
                case .failure:
                    return true
                case .finished, .value:
                    return false
                }
            }
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }

            #expect(eventRecorder.events.containsValueAfterFirstFailure == false)
            #expect(eventRecorder.events.failureCount == 1)
            #expect(drivers.map(\.cancelCallsCount) == Array(repeating: 1, count: Their.stressCountDefault))
            #expect(drivers.map(\.startCallsCount) == Array(repeating: 1, count: Their.stressCountDefault))
        }
    }

    @Test func mergeConcurrentValuesFromDynamicUpstreamsDeliversEveryValueOnce() async throws {
        try await Their.stress(count: 1, timeout: .seconds(2)) {
            let drivers = (0 ..< Their.stressCountDefault).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            let cancel = job.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            try await Their.stress(count: drivers.count) { index in
                drivers[index].emit(value: index)
            }
            try await eventRecorder.waitForEventCount(Their.stressCountDefault)
            cancel()
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }

            #expect(eventRecorder.events.values.sorted() == Array(0 ..< Their.stressCountDefault))
            #expect(drivers.map(\.cancelCallsCount) == Array(repeating: 1, count: Their.stressCountDefault))
            #expect(drivers.map(\.startCallsCount) == Array(repeating: 1, count: Their.stressCountDefault))
        }
    }

    @Test func mergeDeinitCancelsUpstreams() async throws {
        try await Their.stress {
            let drivers = (0 ..< 3).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            var job: Their.Job<Int, JobMergeTestsError>? = Their.Job.merge(drivers.map(\.job))
            _ = job?.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            job = nil
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }

            #expect(eventRecorder.events.isEmpty == true)
            #expect(drivers.map(\.cancelCallsCount) == [1, 1, 1])
            #expect(drivers.map(\.startCallsCount) == [1, 1, 1])
        }
    }

    @Test func mergeDeinitWithoutSubscribeDoesNotCancelUpstreams() async throws {
        try await Their.stress {
            let drivers = (0 ..< 2).map { _ in JobMergeJobDriver() }
            var job: Their.Job<Int, JobMergeTestsError>? = Their.Job.merge(drivers.map(\.job))

            job = nil

            #expect(drivers.map(\.cancelCallsCount) == [0, 0])
            #expect(drivers.map(\.startCallsCount) == [0, 0])
        }
    }

    /// Pins the degraded-input contract: a duplicate (or otherwise
    /// already-claimed) upstream cannot be subscribed twice — its own
    /// `MisuseHandler` records the violation, the duplicate contributes no
    /// events, and the merged job continues with the remaining live
    /// subscription.
    @Test func mergeDuplicateUpstreamReportsMisuseAndContinuesWithRemaining() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let misuseRecorder = JobMergeMisuseRecorder()
            let upstream = JobMergeJobDriver(misuseHandler: misuseRecorder.handler)
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(upstream.job, upstream.job)
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            try await misuseRecorder.waitForCount(1)

            upstream.emit(value: 7)
            try await eventRecorder.waitForEventCount(1)
            cancel()
            try await upstream.waitForCancelCallsCount(1)

            #expect(eventRecorder.events == [.value(7)])
            #expect(misuseRecorder.misuses.count == 1)
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func mergeEmptyArrayReportsMisuse() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let misuseRecorder = JobMergeMisuseRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(
                misuseHandler: misuseRecorder.handler,
                []
            )

            let cancel = job.subscribe(eventRecorder.append(_:))
            cancel()

            try await misuseRecorder.waitForCount(1)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Merged Job requires at least one upstream."
            ])
        }
    }

    /// Pins the intended composition: several independent jobs become one
    /// event stream through `merge`, and a single `evolve` reducer owns the
    /// derived state across all upstreams.
    @Test func mergeEndOfAllUpstreamsDeliversEndOnce() async throws {
        try await Their.stress {
            let drivers = (0 ..< 3).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            _ = job.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            drivers[0].emit(value: 10)
            try await eventRecorder.waitForEventCount(1)
            drivers[0].emitFinished()
            drivers[1].emitFinished()
            drivers[2].emit(value: 30)
            try await eventRecorder.waitForEventCount(2)
            drivers[2].emitFinished()
            try await eventRecorder.waitForEventCount(3)
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }

            #expect(eventRecorder.events == [
                .value(10),
                .value(30),
                .finished
            ])
            #expect(drivers.map(\.cancelCallsCount) == [1, 1, 1])
            #expect(drivers.map(\.startCallsCount) == [1, 1, 1])
        }
    }

    @Test func mergeEndOfSingleUpstreamIsSilentAndKeepsOthersAlive() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let endingDriver = JobMergeJobDriver()
            let livingDriver = JobMergeJobDriver()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(
                endingDriver.job,
                livingDriver.job
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await endingDriver.waitForStartCallsCount(1)
            try await livingDriver.waitForStartCallsCount(1)

            endingDriver.emitFinished()
            try await endingDriver.waitForCancelCallsCount(1)
            livingDriver.emit(value: 7)
            try await eventRecorder.waitForEventCount(1)
            cancel()
            try await livingDriver.waitForCancelCallsCount(1)

            #expect(endingDriver.cancelCallsCount == 1)
            #expect(eventRecorder.events == [.value(7)])
            #expect(livingDriver.cancelCallsCount == 1)
        }
    }

    @Test func mergeEndQueuedBehindBlockedSinkDoesNotOvertakeQueuedValues() async throws {
        try await Their.stress(count: 1) {
            let drivers = (0 ..< 3).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let releaseSink = DispatchSemaphore(value: 0)
            let sinkEntered = JobMergeTestSignal()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            _ = job.subscribe { event in
                if case .value(10) = event {
                    sinkEntered.signal()
                    releaseSink.wait()
                }
                eventRecorder.append(event)
            }
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            let emitTask = BlockingWork {
                drivers[0].emit(value: 10)
            }
            try await sinkEntered.wait()
            drivers[1].emit(value: 20)
            drivers[0].emitFinished()
            drivers[1].emitFinished()
            drivers[2].emitFinished()
            releaseSink.signal()
            try await emitTask.value
            try await eventRecorder.waitForEventCount(3)
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }

            #expect(eventRecorder.events == [
                .value(10),
                .value(20),
                .finished
            ])
            #expect(drivers.map(\.cancelCallsCount) == [1, 1, 1])
            #expect(drivers.map(\.startCallsCount) == [1, 1, 1])
        }
    }

    @Test func mergeEvolveDerivesStateFromAllUpstreams() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let firstDriver = JobMergeJobDriver()
            let secondDriver = JobMergeJobDriver()
            let evolved: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(
                firstDriver.job,
                secondDriver.job
            )
            .evolve(initial: 0) { state, value in
                state += value
                return state
            }
            let cancel = evolved.subscribe(eventRecorder.append(_:))
            try await firstDriver.waitForStartCallsCount(1)
            try await secondDriver.waitForStartCallsCount(1)

            firstDriver.emit(value: 1)
            secondDriver.emit(value: 10)
            firstDriver.emit(value: 2)
            try await eventRecorder.waitForEventCount(3)
            cancel()
            try await firstDriver.waitForCancelCallsCount(1)
            try await secondDriver.waitForCancelCallsCount(1)

            #expect(eventRecorder.events == [
                .value(1),
                .value(11),
                .value(13)
            ])
            #expect(firstDriver.startCallsCount == 1)
            #expect(secondDriver.startCallsCount == 1)
        }
    }

    @Test func mergeFailureAfterSomeUpstreamsEndedStillTerminatesWithFailure() async throws {
        try await Their.stress {
            let drivers = (0 ..< 3).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            _ = job.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            drivers[0].emitFinished()
            try await drivers[0].waitForCancelCallsCount(1)
            drivers[1].emit(failure: .sample)
            try await eventRecorder.waitForEventCount(1)
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }
            drivers[2].emit(value: 30)

            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(drivers.map(\.cancelCallsCount) == [1, 1, 1])
            #expect(drivers.map(\.startCallsCount) == [1, 1, 1])
        }
    }

    @Test func mergeFailureCancelsAllUpstreamsAndSuppressesLateValues() async throws {
        try await Their.stress {
            let drivers = (0 ..< 3).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            let cancel = job.subscribe(eventRecorder.append(_:))
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            drivers[1].emit(failure: .sample)
            try await eventRecorder.waitForEventCount(1)
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }
            drivers[0].emit(value: 10)
            drivers[2].emit(value: 30)
            cancel()

            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(drivers.map(\.cancelCallsCount) == [1, 1, 1])
            #expect(drivers.map(\.startCallsCount) == [1, 1, 1])
        }
    }

    /// Pins the FIFO ordering of terminal failure behind values already
    /// queued: the failure is a queued input processed by the same single
    /// drainer, so values enqueued before it are still delivered in order and
    /// the failure never overtakes them.
    @Test func mergeFailureQueuedBehindBlockedSinkDoesNotOvertakeQueuedValues() async throws {
        try await Their.stress(count: 1) {
            let drivers = (0 ..< 3).map { _ in JobMergeJobDriver() }
            let eventRecorder = JobMergeEventRecorder()
            let releaseSink = DispatchSemaphore(value: 0)
            let sinkEntered = JobMergeTestSignal()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(drivers.map(\.job))
            _ = job.subscribe { event in
                if case .value(10) = event {
                    sinkEntered.signal()
                    releaseSink.wait()
                }
                eventRecorder.append(event)
            }
            for driver in drivers {
                try await driver.waitForStartCallsCount(1)
            }

            let emitTask = BlockingWork {
                drivers[0].emit(value: 10)
            }
            try await sinkEntered.wait()
            drivers[1].emit(value: 20)
            drivers[2].emit(failure: .sample)
            releaseSink.signal()
            try await emitTask.value
            try await eventRecorder.waitForEventCount(3)
            for driver in drivers {
                try await driver.waitForCancelCallsCount(1)
            }

            #expect(eventRecorder.events == [
                .value(10),
                .value(20),
                .failure(.sample)
            ])
            #expect(drivers.map(\.cancelCallsCount) == [1, 1, 1])
            #expect(drivers.map(\.startCallsCount) == [1, 1, 1])
        }
    }

    @Test func mergePreservesPerUpstreamValueOrderAcrossInterleavedEmissions() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let firstDriver = JobMergeJobDriver()
            let secondDriver = JobMergeJobDriver()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(
                firstDriver.job,
                secondDriver.job
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await firstDriver.waitForStartCallsCount(1)
            try await secondDriver.waitForStartCallsCount(1)

            firstDriver.emit(value: 1)
            secondDriver.emit(value: 10)
            firstDriver.emit(value: 2)
            secondDriver.emit(value: 20)
            firstDriver.emit(value: 3)
            try await eventRecorder.waitForEventCount(5)
            cancel()

            #expect(eventRecorder.events == [
                .value(1),
                .value(10),
                .value(2),
                .value(20),
                .value(3)
            ])
        }
    }

    /// An already-started source fails while a later, still-live source is
    /// inside `subscribe` and another callback holds the drainer. The queued
    /// failure closes the loop, so the live source's late-returned cancel must
    /// run immediately instead of waiting for failure teardown, and teardown
    /// must not cancel it again. Both semaphore waits run on Dispatch workers.
    @Test func mergeQueuedFailureCancelsLiveUpstreamReturningFromSubscribeImmediately() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobMergeEventRecorder()
            let failingWork = JobMergeWorkRecorder()
            let holdingWork = JobMergeWorkRecorder()
            let lastWork = JobMergeWorkRecorder()
            let reportWorkSlot = Their.Lock<BlockingWork<Void>?>(nil)
            let sinkEntered = DispatchSemaphore(value: 0)
            let sinkRelease = DispatchSemaphore(value: 0)
            // Also release the workers if an async wait throws or the
            // enclosing Their.stress timeout cancels this body.
            defer {
                sinkEntered.signal()
                sinkRelease.signal()
            }
            let holdingJob = Their.Job { report in
                let cancel = holdingWork.work(report: report)
                let reportWork = BlockingWork {
                    report(.value(1))
                }
                reportWorkSlot.withLock { $0 = reportWork }
                sinkEntered.wait()
                // The already-started first source fails on this thread; its
                // failure queues behind the drainer parked in the value sink.
                failingWork.emit(.failure(.sample))
                return cancel
            }
            let merged = Their.Job.merge(
                Their.Job(work: failingWork.work),
                holdingJob,
                Their.Job(work: lastWork.work)
            )
            let subscribeWork = BlockingWork {
                merged.subscribe { event in
                    if case .value(1) = event {
                        sinkEntered.signal()
                        sinkRelease.wait()
                    }
                    eventRecorder.append(event)
                }
            }
            do {
                let cancel = try await subscribeWork.value
                defer { cancel() }
                let reportWork = try #require(reportWorkSlot.withLock { $0 })
                let eventsBeforeRelease = eventRecorder.events
                let holdingCancelsBeforeRelease = holdingWork.cancelCallsCount
                let startsBeforeRelease = [
                    failingWork.startCallsCount,
                    holdingWork.startCallsCount,
                    lastWork.startCallsCount
                ]

                sinkRelease.signal()
                try await reportWork.value
                cancel()

                #expect(eventsBeforeRelease.isEmpty == true)
                #expect(holdingCancelsBeforeRelease == 1)
                #expect(startsBeforeRelease == [1, 1, 0])
                #expect(eventRecorder.events == [.value(1), .failure(.sample)])
                #expect(failingWork.cancelCallsCount == 1)
                #expect(holdingWork.cancelCallsCount == 1)
                #expect(lastWork.cancelCallsCount == 0)
            } catch {
                sinkEntered.signal()
                sinkRelease.signal()
                // An unstructured task does not inherit this body's cancelled
                // status, so both Dispatch workers are joined even on timeout.
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

    @Test func mergeRejectsResubscribeAfterCancelWithoutRestartingUpstreams() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let misuseRecorder = JobMergeMisuseRecorder()
            let secondRecorder = JobMergeEventRecorder()
            let upstream = JobMergeJobDriver(misuseHandler: misuseRecorder.handler)
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge([upstream.job])

            let firstCancel = job.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            firstCancel()
            try await upstream.waitForCancelCallsCount(1)
            let secondCancel = job.subscribe(secondRecorder.append(_:))
            secondCancel()

            try await misuseRecorder.waitForCount(1)
            upstream.emit(value: 5)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Merged Job supports only one subscriber per lifecycle."
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func mergeRejectsResubscribeAfterTerminalEndWithoutRestartingUpstreams() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let misuseRecorder = JobMergeMisuseRecorder()
            let secondRecorder = JobMergeEventRecorder()
            let upstream = JobMergeJobDriver(misuseHandler: misuseRecorder.handler)
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge([upstream.job])

            _ = job.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emitFinished()
            try await eventRecorder.waitForEventCount(1)
            let secondCancel = job.subscribe(secondRecorder.append(_:))
            secondCancel()

            try await misuseRecorder.waitForCount(1)
            #expect(eventRecorder.events == [.finished])
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Merged Job supports only one subscriber per lifecycle."
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func mergeRejectsResubscribeAfterTerminalFailureWithoutRestartingUpstreams() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let misuseRecorder = JobMergeMisuseRecorder()
            let secondRecorder = JobMergeEventRecorder()
            let upstream = JobMergeJobDriver(misuseHandler: misuseRecorder.handler)
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge([upstream.job])

            _ = job.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(failure: .sample)
            try await eventRecorder.waitForEventCount(1)
            let secondCancel = job.subscribe(secondRecorder.append(_:))
            secondCancel()

            try await misuseRecorder.waitForCount(1)
            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Merged Job supports only one subscriber per lifecycle."
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func mergeRejectsSecondSubscriberWithCreationLocationTrace() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let misuseRecorder = JobMergeMisuseRecorder()
            let secondRecorder = JobMergeEventRecorder()
            let upstream = JobMergeJobDriver(misuseHandler: misuseRecorder.handler)
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(
                fileID: "CoreTests/MergeTrace.swift",
                function: "makeTracedMerge()",
                line: 321,
                [upstream.job]
            )

            let firstCancel = job.subscribe(eventRecorder.append(_:))
            let secondCancel = job.subscribe(secondRecorder.append(_:))
            secondCancel()
            firstCancel()

            try await misuseRecorder.waitForCount(1)
            try await upstream.waitForStartCallsCount(1)
            try await upstream.waitForCancelCallsCount(1)
            #expect(misuseRecorder.misuses.first?.trace == [
                Their.MisuseLocation(
                    fileID: "CoreTests/MergeTrace.swift",
                    function: "makeTracedMerge()",
                    line: 321
                )
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func mergeRetainedCancelKeepsRvalueChainAliveUntilCanceled() async throws {
        try await Their.stress {
            let cancelSignal = JobMergeTestSignal()
            let eventRecorder = JobMergeEventRecorder()
            let workRecorder = JobMergeWorkRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            var job: Their.Job<Int, JobMergeTestsError>? = Their.Job.merge(
                Their.Job(work: workRecorder.work)
            )
            weak var weakJob = job
            let retainedCancel = job?.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            job = nil
            workRecorder.report?(.value(10))
            try await eventRecorder.waitForEventCount(1)
            #expect(eventRecorder.events == [.value(10)])
            #expect(weakJob != nil)
            #expect(workRecorder.cancelCallsCount == 0)

            retainedCancel?()
            try await cancelSignal.wait()
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(weakJob == nil)
        }
    }

    @Test func mergeSynchronousEndOfFirstUpstreamStillStartsRemainingUpstreams() async throws {
        try await Their.stress {
            let cancelRecorder = JobMergeCancelRecorder()
            let eventRecorder = JobMergeEventRecorder()
            let otherDriver = JobMergeJobDriver()
            let synchronousEndingJob = Their.Job<Int, JobMergeTestsError>(
                misuseHandler: { _ in },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    sink(.finished)
                    return cancelRecorder.cancel()
                }
            )
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(
                synchronousEndingJob,
                otherDriver.job
            )

            _ = job.subscribe(eventRecorder.append(_:))
            try await otherDriver.waitForStartCallsCount(1)
            try await cancelRecorder.waitForCancelCallsCount(1)

            otherDriver.emit(value: 5)
            try await eventRecorder.waitForEventCount(1)
            otherDriver.emitFinished()
            try await eventRecorder.waitForEventCount(2)
            try await otherDriver.waitForCancelCallsCount(1)

            #expect(cancelRecorder.cancelCallsCount == 1)
            #expect(eventRecorder.events == [.value(5), .finished])
            #expect(otherDriver.cancelCallsCount == 1)
            #expect(otherDriver.startCallsCount == 1)
        }
    }

    /// A source can queue values before its synchronous failure while another
    /// source holds the drainer. Closing further starts must preserve those
    /// earlier values, including when the source cancel returns before drain.
    @Test func mergeSynchronousFailurePreservesValuesQueuedBeforeFailureDuringSubscribe() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobMergeEventRecorder()
            let firstWork = JobMergeWorkRecorder()
            let reportWorkSlot = Their.Lock<BlockingWork<Void>?>(nil)
            let secondWork = JobMergeWorkRecorder()
            let sinkEntered = DispatchSemaphore(value: 0)
            let sinkRelease = DispatchSemaphore(value: 0)
            let thirdWork = JobMergeWorkRecorder()
            defer {
                sinkEntered.signal()
                sinkRelease.signal()
            }
            let firstJob = Their.Job { report in
                let cancel = firstWork.work(report: report)
                let reportWork = BlockingWork {
                    report(.value(1))
                }
                reportWorkSlot.withLock { $0 = reportWork }
                sinkEntered.wait()
                return cancel
            }
            let secondJob = Their.Job { report in
                let cancel = secondWork.work(report: report)
                report(.value(2))
                report(.failure(.sample))
                return cancel
            }
            let merged = Their.Job.merge(firstJob, secondJob, Their.Job(work: thirdWork.work))
            let subscribeWork = BlockingWork {
                merged.subscribe { event in
                    if case .value(1) = event {
                        sinkEntered.signal()
                        sinkRelease.wait()
                    }
                    eventRecorder.append(event)
                }
            }
            do {
                let cancel = try await subscribeWork.value
                defer { cancel() }
                let reportWork = try #require(reportWorkSlot.withLock { $0 })
                let eventsBeforeRelease = eventRecorder.events
                let startsBeforeRelease = [
                    firstWork.startCallsCount,
                    secondWork.startCallsCount,
                    thirdWork.startCallsCount
                ]

                sinkRelease.signal()
                try await reportWork.value
                cancel()

                #expect(eventsBeforeRelease.isEmpty == true)
                #expect(startsBeforeRelease == [1, 1, 0])
                #expect(eventRecorder.events == [.value(1), .value(2), .failure(.sample)])
                #expect(firstWork.cancelCallsCount == 1)
                #expect(secondWork.cancelCallsCount == 1)
                #expect(thirdWork.cancelCallsCount == 0)
            } catch {
                sinkEntered.signal()
                sinkRelease.signal()
                // Join both workers before a cancelled scenario can leave.
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

    /// A synchronous failure must stop the subscription loop even when an
    /// earlier source owns the drainer. Both semaphore waits run on Dispatch
    /// workers; releasing the first sink then joins its entire report drain.
    @Test func mergeSynchronousFailureQueuedBehindActiveDrainerStopsSubscriptionLoop() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = JobMergeEventRecorder()
            let firstWork = JobMergeWorkRecorder()
            let reportWorkSlot = Their.Lock<BlockingWork<Void>?>(nil)
            let secondWork = JobMergeWorkRecorder()
            let sinkEntered = DispatchSemaphore(value: 0)
            let sinkRelease = DispatchSemaphore(value: 0)
            let thirdWork = JobMergeWorkRecorder()
            // Also release the worker waiting inside subscribe if an async
            // wait throws or the enclosing Their.stress timeout cancels this body.
            defer {
                sinkEntered.signal()
                sinkRelease.signal()
            }
            let firstJob = Their.Job { report in
                let cancel = firstWork.work(report: report)
                let reportWork = BlockingWork {
                    report(.value(1))
                }
                reportWorkSlot.withLock { $0 = reportWork }
                sinkEntered.wait()
                return cancel
            }
            let secondJob = Their.Job { report in
                let cancel = secondWork.work(report: report)
                report(.failure(.sample))
                return cancel
            }
            let merged = Their.Job.merge(firstJob, secondJob, Their.Job(work: thirdWork.work))
            let subscribeWork = BlockingWork {
                merged.subscribe { event in
                    if case .value(1) = event {
                        sinkEntered.signal()
                        sinkRelease.wait()
                    }
                    eventRecorder.append(event)
                }
            }
            do {
                let cancel = try await subscribeWork.value
                defer { cancel() }
                let reportWork = try #require(reportWorkSlot.withLock { $0 })
                let eventsBeforeRelease = eventRecorder.events
                let startsBeforeRelease = [
                    firstWork.startCallsCount,
                    secondWork.startCallsCount,
                    thirdWork.startCallsCount
                ]

                sinkRelease.signal()
                try await reportWork.value
                cancel()

                #expect(eventsBeforeRelease.isEmpty == true)
                #expect(startsBeforeRelease == [1, 1, 0])
                #expect(eventRecorder.events == [.value(1), .failure(.sample)])
                #expect(firstWork.cancelCallsCount == 1)
                #expect(secondWork.cancelCallsCount == 1)
                #expect(thirdWork.cancelCallsCount == 0)
            } catch {
                sinkEntered.signal()
                sinkRelease.signal()
                // An unstructured task does not inherit this body's cancelled
                // status, so both Dispatch workers are joined even on timeout.
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

    /// Failure tears down every live source before starting the merged terminal
    /// callback. A source's WorkCancel may cancel the merged subscription; the
    /// detached sink must not deliver a failure after that cancellation returns.
    @Test func mergeUpstreamTeardownReentrantCancelSuppressesTerminalCallback() async throws {
        try await Their.stress {
            let cancelBox = Their.Lock<Their.WorkCancel?>(nil)
            let reentrantCancelReturns = Their.TestCountRecorder()
            let failingWork = Their.TestWorkRecorder<Int, JobMergeTestsError>()
            let liveWork = Their.TestWorkRecorder<Int, JobMergeTestsError>(onCancel: {
                cancelBox.withLock { $0 }?()
                _ = reentrantCancelReturns.increment()
            })
            let eventRecorder = JobMergeEventRecorder()
            let merged = Their.Job.merge(
                Their.Job(work: failingWork.work),
                Their.Job(work: liveWork.work)
            )
            let cancel = merged.subscribe(eventRecorder.append(_:))
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

            failingWork.emit(.failure(.sample))
            liveWork.emit(.value(2))
            cancel()

            #expect(reentrantCancelReturns.count == 1)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(failingWork.cancelCallsCount == 1)
            #expect(liveWork.cancelCallsCount == 1)
            #expect(failingWork.startCallsCount == 1)
            #expect(liveWork.startCallsCount == 1)
        }
    }

    @Test func mergeVariadicEmitsValuesInDeterministicArrivalOrder() async throws {
        try await Their.stress {
            let firstDriver = JobMergeJobDriver()
            let secondDriver = JobMergeJobDriver()
            let thirdDriver = JobMergeJobDriver()
            let eventRecorder = JobMergeEventRecorder()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge(
                firstDriver.job,
                secondDriver.job,
                thirdDriver.job
            )
            let cancel = job.subscribe(eventRecorder.append(_:))
            try await firstDriver.waitForStartCallsCount(1)
            try await secondDriver.waitForStartCallsCount(1)
            try await thirdDriver.waitForStartCallsCount(1)

            thirdDriver.emit(value: 30)
            firstDriver.emit(value: 10)
            secondDriver.emit(value: 20)
            try await eventRecorder.waitForEventCount(3)
            cancel()

            #expect(eventRecorder.events == [
                .value(30),
                .value(10),
                .value(20)
            ])
            #expect(firstDriver.cancelCallsCount == 1)
            #expect(secondDriver.cancelCallsCount == 1)
            #expect(thirdDriver.cancelCallsCount == 1)
        }
    }

    @Test func mergeWithOneJobForwardsValuesAndEnd() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let upstream = JobMergeJobDriver()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge([upstream.job])

            _ = job.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 1)
            try await eventRecorder.waitForEventCount(1)
            upstream.emitFinished()
            try await eventRecorder.waitForEventCount(2)
            try await upstream.waitForCancelCallsCount(1)

            #expect(eventRecorder.events == [.value(1), .finished])
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func mergeWithOneJobForwardsValuesAndFailure() async throws {
        try await Their.stress {
            let eventRecorder = JobMergeEventRecorder()
            let upstream = JobMergeJobDriver()
            let job: Their.Job<Int, JobMergeTestsError> = Their.Job.merge([upstream.job])
            _ = job.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)

            upstream.emit(value: 10)
            try await eventRecorder.waitForEventCount(1)
            upstream.emit(failure: .sample)
            try await eventRecorder.waitForEventCount(2)

            #expect(eventRecorder.events == [
                .value(10),
                .failure(.sample)
            ])
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }
}

private enum JobMergeTestsError: Equatable, Swift.Error, Sendable {

    case sample
}

private typealias JobMergeCancelRecorder = Their.TestCancelRecorder
private typealias JobMergeEventRecorder = Their.TestEventRecorder<Their.JobEvent<Int, JobMergeTestsError>>
private typealias JobMergeJobDriver = Their.TestJobDriver<Int, JobMergeTestsError>
private typealias JobMergeMisuseRecorder = Their.TestMisuseRecorder
private typealias JobMergeTestSignal = Their.TestSignal
private typealias JobMergeWorkRecorder = Their.TestWorkRecorder<Int, JobMergeTestsError>

private extension Array where Element == Their.JobEvent<Int, JobMergeTestsError> {

    var containsValueAfterFirstFailure: Bool {
        guard let failureIndex = firstIndex(where: { event in
            switch event {
            case .failure:
                return true
            case .finished, .value:
                return false
            }
        }) else {
            return false
        }
        return self[index(after: failureIndex)...].contains { event in
            switch event {
            case .finished, .failure:
                return false
            case .value:
                return true
            }
        }
    }
    var failureCount: Int {
        filter { event in
            switch event {
            case .failure:
                return true
            case .finished, .value:
                return false
            }
        }
        .count
    }
    var values: [Int] {
        compactMap { event in
            switch event {
            case .finished, .failure:
                return nil
            case .value(let value):
                return value
            }
        }
    }
}
