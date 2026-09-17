import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct TheirCoreSharedStressTests {

    @Test func hubConcurrentCancelAndTerminalFailureRaceCancelsUpstreamOnce() async throws {
        let eventRecorder = HubStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub: Their.Hub<Int, TheirCoreSharedStressTestsError> = Their.Hub(
            work: startRecorder.work
        )

        let cancels = try await Their.stress {
            hub.subscribe(eventRecorder.append(_:))
        }
        try await startRecorder.waitForStartCallsCount(1)
        try await Their.stress(count: cancels.count) { iteration in
            if iteration == 0 {
                startRecorder.emit(.failure(.sample))
            } else {
                cancels[iteration]()
            }
        }
        try await startRecorder.waitForCancelCallsCount(1)

        #expect(eventRecorder.events.failureCount <= Their.stressCountDefault)
        #expect(eventRecorder.events.values.isEmpty)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func hubConcurrentCancelStormStopsSharedLifecycleOnceAndRestarts() async throws {
        let eventRecorder = HubStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub: Their.Hub<Int, TheirCoreSharedStressTestsError> = Their.Hub(
            work: startRecorder.work
        )

        let cancels = try await Their.stress {
            hub.subscribe(eventRecorder.append(_:))
        }
        try await startRecorder.waitForStartCallsCount(1)
        startRecorder.emit(.value(7))
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        try await Their.stress(count: cancels.count) { iteration in
            cancels[iteration]()
        }
        try await startRecorder.waitForCancelCallsCount(1)

        let restartRecorder = HubStressEventRecorder()
        let restartCancel = hub.subscribe(restartRecorder.append(_:))
        try await startRecorder.waitForStartCallsCount(2)
        startRecorder.emit(.value(9))
        try await restartRecorder.waitForEventCount(1)
        restartCancel()

        #expect(eventRecorder.events == Array(repeating: .value(7), count: Their.stressCountDefault))
        #expect(restartRecorder.events == [.value(9)])
        #expect(startRecorder.cancelCallsCount == 2)
        #expect(startRecorder.startCallsCount == 2)
    }

    @Test func hubConcurrentTerminalFailureClearsSubscribersAndRestarts() async throws {
        let eventRecorder = HubStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub: Their.Hub<Int, TheirCoreSharedStressTestsError> = Their.Hub(
            work: startRecorder.work
        )

        let cancels = try await Their.stress {
            hub.subscribe(eventRecorder.append(_:))
        }
        try await startRecorder.waitForStartCallsCount(1)
        startRecorder.emit(.failure(.sample))
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        try await Their.stress(count: cancels.count) { iteration in
            cancels[iteration]()
        }

        let restartRecorder = HubStressEventRecorder()
        let restartCancel = hub.subscribe(restartRecorder.append(_:))
        try await startRecorder.waitForStartCallsCount(2)
        startRecorder.emit(.value(11))
        try await restartRecorder.waitForEventCount(1)
        restartCancel()

        #expect(eventRecorder.events == Array(repeating: .failure(.sample), count: Their.stressCountDefault))
        #expect(restartRecorder.events == [.value(11)])
        #expect(startRecorder.cancelCallsCount == 2)
        #expect(startRecorder.startCallsCount == 2)
    }

    @Test func hubEngineConcurrentCancelStormStopsSharedLifecycleOnceAndRestarts() async throws {
        let eventRecorder = HubEngineStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub = HubEngine<Int, TheirCoreSharedStressTestsError>(
            work: startRecorder.work
        )

        let cancels = try await Their.stress {
            hub.subscribe(eventRecorder.append(_:))
        }
        try await startRecorder.waitForStartCallsCount(1)
        #expect(hub.getState() == .init(isRunning: true, subscribersCount: Their.stressCountDefault))

        startRecorder.emit(.value(13))
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        try await Their.stress(count: cancels.count) { iteration in
            cancels[iteration]()
        }
        try await startRecorder.waitForCancelCallsCount(1)
        #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))

        let restartRecorder = HubEngineStressEventRecorder()
        let restartCancel = hub.subscribe(restartRecorder.append(_:))
        try await startRecorder.waitForStartCallsCount(2)
        startRecorder.emit(.value(15))
        try await restartRecorder.waitForEventCount(1)
        restartCancel()

        #expect(eventRecorder.events == Array(repeating: .value(13), count: Their.stressCountDefault))
        #expect(restartRecorder.events == [.value(15)])
        #expect(startRecorder.cancelCallsCount == 2)
        #expect(startRecorder.startCallsCount == 2)
    }

    @Test func hubEngineConcurrentTerminalFailureClearsSubscribersAndRestarts() async throws {
        let eventRecorder = HubEngineStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub = HubEngine<Int, TheirCoreSharedStressTestsError>(
            work: startRecorder.work
        )

        let cancels = try await Their.stress {
            hub.subscribe(eventRecorder.append(_:))
        }
        try await startRecorder.waitForStartCallsCount(1)
        startRecorder.emit(.failure(.sample))
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)
        #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))

        try await Their.stress(count: cancels.count) { iteration in
            cancels[iteration]()
        }

        let restartRecorder = HubEngineStressEventRecorder()
        let restartCancel = hub.subscribe(restartRecorder.append(_:))
        try await startRecorder.waitForStartCallsCount(2)
        startRecorder.emit(.value(17))
        try await restartRecorder.waitForEventCount(1)
        restartCancel()

        #expect(eventRecorder.events == Array(repeating: .failure(.sample), count: Their.stressCountDefault))
        #expect(restartRecorder.events == [.value(17)])
        #expect(startRecorder.cancelCallsCount == 2)
        #expect(startRecorder.startCallsCount == 2)
    }

    @Test func hubEngineSubscriptionConcurrentCancelSuppressesLaterEvents() async throws {
        let eventRecorder = HubEngineStressEventRecorder()
        let subscription = HubEngineSubscription<Int, TheirCoreSharedStressTestsError>(
            sink: eventRecorder.append(_:)
        )

        try await Their.stress { iteration in
            subscription.emit(.value(iteration))
        }
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        try await Their.stress {
            subscription.cancel()
        }
        try await Their.stress { iteration in
            subscription.emit(.value(iteration))
        }

        #expect(eventRecorder.events.values.sorted() == Array(0 ..< Their.stressCountDefault))
    }

    @Test func hubEvolveConcurrentSubscribersShareStateAndResetAfterCancel() async throws {
        let eventRecorder = HubStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub: Their.Hub<Int, TheirCoreSharedStressTestsError> = Their.Hub(
            work: startRecorder.work
        )
        .evolve(initial: 0) { state, value in
            state += value
            return state
        }

        let cancels = try await Their.stress {
            hub.subscribe(eventRecorder.append(_:))
        }
        try await startRecorder.waitForStartCallsCount(1)
        startRecorder.emit(.value(2))
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        try await Their.stress(count: cancels.count) { iteration in
            cancels[iteration]()
        }
        try await startRecorder.waitForCancelCallsCount(1)

        let restartRecorder = HubStressEventRecorder()
        let restartCancel = hub.subscribe(restartRecorder.append(_:))
        try await startRecorder.waitForStartCallsCount(2)
        startRecorder.emit(.value(3))
        try await restartRecorder.waitForEventCount(1)
        restartCancel()

        #expect(eventRecorder.events == Array(repeating: .value(2), count: Their.stressCountDefault))
        #expect(restartRecorder.events == [.value(3)])
        #expect(startRecorder.cancelCallsCount == 2)
        #expect(startRecorder.startCallsCount == 2)
    }

    @Test func hubEvolveConcurrentTerminalFailureClearsSubscribersAndRestarts() async throws {
        let eventRecorder = HubStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub: Their.Hub<Int, TheirCoreSharedStressTestsError> = Their.Hub(
            work: startRecorder.work
        )
        .evolve(initial: 0) { state, value in
            state += value
            return state
        }

        let cancels = try await Their.stress {
            hub.subscribe(eventRecorder.append(_:))
        }
        try await startRecorder.waitForStartCallsCount(1)
        startRecorder.emit(.failure(.sample))
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        try await Their.stress(count: cancels.count) { iteration in
            cancels[iteration]()
        }

        let restartRecorder = HubStressEventRecorder()
        let restartCancel = hub.subscribe(restartRecorder.append(_:))
        try await startRecorder.waitForStartCallsCount(2)
        startRecorder.emit(.value(5))
        try await restartRecorder.waitForEventCount(1)
        restartCancel()

        #expect(eventRecorder.events == Array(repeating: .failure(.sample), count: Their.stressCountDefault))
        #expect(restartRecorder.events == [.value(5)])
        #expect(startRecorder.cancelCallsCount == 2)
        #expect(startRecorder.startCallsCount == 2)
    }

    @Test func hubShareLatestConcurrentLateSubscribersReplayLatestWithoutRestart() async throws {
        let lateRecorder = HubStressEventRecorder()
        let seedRecorder = HubStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub: Their.Hub<Int, TheirCoreSharedStressTestsError> = Their.Hub(
            work: startRecorder.work
        )
        .shareLatest()

        let seedCancel = hub.subscribe(seedRecorder.append(_:))
        try await startRecorder.waitForStartCallsCount(1)
        startRecorder.emit(.value(23))
        try await seedRecorder.waitForEventCount(1)

        let lateCancels = try await Their.stress {
            hub.subscribe(lateRecorder.append(_:))
        }
        try await lateRecorder.waitForEventCount(Their.stressCountDefault)
        startRecorder.emit(.value(29))
        try await lateRecorder.waitForEventCount(Their.stressCountDefault * 2)
        try await seedRecorder.waitForEventCount(2)

        try await Their.stress(count: lateCancels.count) { iteration in
            lateCancels[iteration]()
        }
        seedCancel()
        try await startRecorder.waitForCancelCallsCount(1)

        #expect(lateRecorder.events.values.filter { $0 == 23 }.count == Their.stressCountDefault)
        #expect(lateRecorder.events.values.filter { $0 == 29 }.count == Their.stressCountDefault)
        #expect(seedRecorder.events == [.value(23), .value(29)])
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func hubStreamConcurrentConsumersShareHubWithSinkSubscriber() async throws {
        let sinkRecorder = HubStressEventRecorder()
        let startRecorder = HubStressWorkRecorder()
        let hub: Their.Hub<Int, TheirCoreSharedStressTestsError> = Their.Hub(
            work: startRecorder.work
        )
        let stream = hub.stream()
        try await startRecorder.waitForStartCallsCount(1)
        let sinkCancel = hub.subscribe(sinkRecorder.append(_:))
        let streamEventsTask = Task {
            try await Their.stress {
                var iterator = stream.makeAsyncIterator()
                return await iterator.next()
            }
        }
        for value in 0 ..< Their.stressCountDefault {
            startRecorder.emit(.value(value))
        }
        let streamEvents = try await streamEventsTask.value
        try await sinkRecorder.waitForEventCount(Their.stressCountDefault)
        startRecorder.emit(.failure(.sample))
        try await startRecorder.waitForCancelCallsCount(1)
        sinkCancel()

        #expect(streamEvents.compactMap { $0?.value }.sorted() == Array(0 ..< Their.stressCountDefault))
        #expect(sinkRecorder.events.values == Array(0 ..< Their.stressCountDefault))
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func jobConcurrentCancelAndTerminalFailureRaceCancelsUpstreamOnce() async throws {
        let eventRecorder = JobStressEventRecorder()
        let startRecorder = JobStressWorkRecorder()
        let job: Their.Job<Int, TheirCoreSharedStressTestsError> = Their.Job(
            work: startRecorder.work
        )
        let cancel = job.subscribe(eventRecorder.append(_:))

        try await startRecorder.waitForStartCallsCount(1)
        try await Their.stress { iteration in
            if iteration == 0 {
                startRecorder.emit(.failure(.sample))
            } else {
                cancel()
            }
        }
        try await startRecorder.waitForCancelCallsCount(1)

        #expect(eventRecorder.events.failureCount <= 1)
        #expect(eventRecorder.events.values.isEmpty)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func jobEngineConcurrentFailureRacingValuesTerminatesOnce() async throws {
        let eventRecorder = JobEngineStressEventRecorder()
        let startRecorder = JobStressWorkRecorder()
        let engine = JobEngine<Int, TheirCoreSharedStressTestsError>(
            sink: eventRecorder.append(_:),
            work: startRecorder.work
        )

        _ = engine.start()
        try await startRecorder.waitForStartCallsCount(1)
        let report = startRecorder.report
        #expect(report != nil)

        try await Their.stress { iteration in
            if iteration == 0 {
                report?(.failure(.sample))
            } else {
                report?(.value(iteration))
            }
        }
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        #expect(engine.getState().isTerminated == true)
        #expect(eventRecorder.events.containsValueAfterFirstFailure == false)
        #expect(eventRecorder.events.failureCount == 1)
        #expect(startRecorder.cancelCallsCount == 1)
    }

    @Test func jobEngineConcurrentStartAttemptsStartSingleLifecycle() async throws {
        let eventRecorder = JobEngineStressEventRecorder()
        let misuseRecorder = Their.TestMisuseRecorder()
        let startRecorder = JobStressWorkRecorder()
        let engine = JobEngine<Int, TheirCoreSharedStressTestsError>(
            misuseHandler: misuseRecorder.handler,
            sink: eventRecorder.append(_:),
            work: startRecorder.work
        )

        let results = try await Their.stress {
            engine.start()
        }
        try await misuseRecorder.waitForCount(Their.stressCountDefault - 1)
        engine.stop()

        #expect(results.filter { $0 }.count == 1)
        #expect(misuseRecorder.misuses.count == Their.stressCountDefault - 1)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func jobEngineConcurrentStopAttemptsCancelOnce() async throws {
        let eventRecorder = JobEngineStressEventRecorder()
        let startRecorder = JobStressWorkRecorder()
        let engine = JobEngine<Int, TheirCoreSharedStressTestsError>(
            sink: eventRecorder.append(_:),
            work: startRecorder.work
        )

        _ = engine.start()
        try await startRecorder.waitForStartCallsCount(1)
        try await Their.stress {
            engine.stop()
        }
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        #expect(engine.getState().isTerminated == true)
        #expect(eventRecorder.events.messageCount(.stopIgnoredBecauseStateIsNotActive) == Their.stressCountDefault - 1)
        #expect(eventRecorder.events.messageCount(.stopStopped) == 1)
        #expect(startRecorder.cancelCallsCount == 1)
    }

    @Test func jobEngineConcurrentTerminateAttemptsCancelOnce() async throws {
        let eventRecorder = JobEngineStressEventRecorder()
        let startRecorder = JobStressWorkRecorder()
        let engine = JobEngine<Int, TheirCoreSharedStressTestsError>(
            sink: eventRecorder.append(_:),
            work: startRecorder.work
        )

        _ = engine.start()
        try await startRecorder.waitForStartCallsCount(1)
        try await Their.stress {
            engine.terminate()
        }
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)

        #expect(engine.getState().isTerminated == true)
        #expect(eventRecorder.events.messageCount(.terminateIgnoredBecauseStateIsNotActive) == Their.stressCountDefault - 1)
        #expect(eventRecorder.events.messageCount(.terminateStopped) == 1)
        #expect(startRecorder.cancelCallsCount == 1)
    }

    @Test func jobEngineDeinitRacingStopTerminateAndReportsCancelsOnce() async throws {
        let eventRecorder = JobEngineStressEventRecorder()
        let startRecorder = JobStressWorkRecorder()
        var engine: JobEngine<Int, TheirCoreSharedStressTestsError>? = JobEngine(
            sink: eventRecorder.append(_:),
            work: startRecorder.work
        )
        let engineBox = Their.Lock<JobEngine<Int, TheirCoreSharedStressTestsError>?>(engine)

        _ = engine?.start()
        try await startRecorder.waitForStartCallsCount(1)
        engine = nil
        try await Their.stress { iteration in
            switch iteration % 5 {
            case 0:
                engineBox.withLock { engine in
                    engine = nil
                }
            case 1:
                engineBox.withLock { engine in
                    engine
                }?.stop()
            case 2:
                engineBox.withLock { engine in
                    engine
                }?.terminate()
            case 3:
                startRecorder.emit(.failure(.sample))
            default:
                startRecorder.emit(.value(iteration))
            }
        }
        engineBox.withLock { engine in
            engine = nil
        }

        #expect(eventRecorder.events.containsValueAfterFirstFailure == false)
        #expect(eventRecorder.events.failureCount <= 1)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func jobEvolveConcurrentReportsTerminateOnceAndDoNotEmitAfterFailure() async throws {
        let eventRecorder = JobStressEventRecorder()
        let startRecorder = JobStressWorkRecorder()
        let job: Their.Job<Int, TheirCoreSharedStressTestsError> = Their.Job(
            work: startRecorder.work
        )
        let evolved = job.evolve(initial: 0) { state, value in
            state += value
            return state
        }
        let cancel = evolved.subscribe(eventRecorder.append(_:))

        try await startRecorder.waitForStartCallsCount(1)
        let report = startRecorder.report
        #expect(report != nil)
        try await Their.stress { iteration in
            if iteration == 0 {
                report?(.failure(.sample))
            } else {
                report?(.value(iteration))
            }
        }
        cancel()

        #expect(eventRecorder.events.containsValueAfterFirstFailure == false)
        #expect(eventRecorder.events.failureCount == 1)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func jobEvolveConcurrentSubscribeAttemptsStartSingleLifecycle() async throws {
        let eventRecorder = JobStressEventRecorder()
        let misuseRecorder = Their.TestMisuseRecorder()
        let startRecorder = JobStressWorkRecorder()
        let job = Their.Job<Int, TheirCoreSharedStressTestsError>(
            misuseHandler: misuseRecorder.handler,
            work: startRecorder.work
        )
        let evolved = job.evolve(initial: 0) { state, value in
            state += value
            return state
        }

        let cancels = try await Their.stress {
            evolved.subscribe(eventRecorder.append(_:))
        }
        try await misuseRecorder.waitForCount(Their.stressCountDefault - 1)
        try await startRecorder.waitForStartCallsCount(1)
        startRecorder.emit(.value(31))
        try await eventRecorder.waitForEventCount(1)
        try await Their.stress(count: cancels.count) { iteration in
            cancels[iteration]()
        }
        try await startRecorder.waitForCancelCallsCount(1)

        #expect(eventRecorder.events == [.value(31)])
        #expect(misuseRecorder.misuses.count == Their.stressCountDefault - 1)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func jobSinkStateConcurrentSetAndTakeHaveSingleWinners() async throws {
        let eventRecorder = JobStressEventRecorder()
        let state = JobSinkState<Int, TheirCoreSharedStressTestsError>()

        let setResults = try await Their.stress { _ in
            state.setSinkIfEmpty { event in
                eventRecorder.append(event)
            }
        }
        state.getSink()?(.value(19))

        let takeResults = try await Their.stress { _ in
            guard let sink = state.takeSink() else {
                return false
            }
            sink(.value(21))
            return true
        }

        state.getSink()?(.value(23))

        #expect(eventRecorder.events == [.value(19), .value(21)])
        #expect(setResults.filter { $0 }.count == 1)
        #expect(takeResults.filter { $0 }.count == 1)
    }

    @Test func jobStreamSharedIteratorsDistributeConcurrentValues() async throws {
        let startRecorder = JobStressWorkRecorder()
        let job: Their.Job<Int, TheirCoreSharedStressTestsError> = Their.Job(
            work: startRecorder.work
        )
        let stream = job.stream()

        try await startRecorder.waitForStartCallsCount(1)
        let streamEventsTask = Task {
            try await Their.stress {
                var iterator = stream.makeAsyncIterator()
                return await iterator.next()
            }
        }
        for value in 0 ..< Their.stressCountDefault {
            startRecorder.emit(.value(value))
        }
        let streamEvents = try await streamEventsTask.value
        startRecorder.emit(.failure(.sample))
        try await startRecorder.waitForCancelCallsCount(1)

        #expect(streamEvents.compactMap { $0?.value }.sorted() == Array(0 ..< Their.stressCountDefault))
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func resourceConcurrentSetAndCancelReleasesEveryValueOnce() async throws {
        let recorder = Their.TestEventRecorder<Int>()
        let resource = Their.Resource<Int>(
            release: recorder.append(_:)
        )

        try await Their.stress { iteration in
            if iteration.isMultiple(of: 2) {
                resource.cancel()
            }
            resource.set(iteration)
            if iteration.isMultiple(of: 2) == false {
                resource.cancel()
            }
        }
        try await recorder.waitForEventCount(Their.stressCountDefault)

        #expect(recorder.events.sorted() == Array(0 ..< Their.stressCountDefault))
    }

    @Test func serializedWorkCancelSuppressesFutureReports() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCountRecorder()
            let eventRecorder = Their.TestEventRecorder<Int>()
            let reportStore = Their.Lock<Their.WorkReport<Int, TheirCoreSharedStressTestsError>?>(nil)
            let work: Their.Work<Int, TheirCoreSharedStressTestsError> = Their.serialized { report in
                reportStore.withLock { storedReport in
                    storedReport = report
                }
                return {
                    _ = cancelRecorder.increment()
                }
            }
            let cancel = work { output in
                if case .value(let value) = output {
                    eventRecorder.append(value)
                }
            }
            let report = reportStore.withLock { storedReport in
                storedReport
            }
            #expect(report != nil)

            cancel()
            try await cancelRecorder.waitForCount(1)
            report?(.value(1))

            #expect(cancelRecorder.count == 1)
            #expect(eventRecorder.events.isEmpty)
        }
    }

    @Test func serializedWorkConcurrentReportsDeliverEveryValueOnce() async throws {
        let eventRecorder = JobStressEventRecorder()
        let reportStore = Their.Lock<Their.WorkReport<Int, TheirCoreSharedStressTestsError>?>(nil)
        let job: Their.Job<Int, TheirCoreSharedStressTestsError> = Their.Job(
            work: Their.serialized { report in
                reportStore.withLock { storedReport in
                    storedReport = report
                }
                return {}
            }
        )

        let cancel = job.subscribe(eventRecorder.append(_:))
        let report = reportStore.withLock { storedReport in
            storedReport
        }
        #expect(report != nil)

        try await Their.stress { iteration in
            report?(.value(iteration))
        }
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)
        cancel()

        #expect(eventRecorder.events.values.sorted() == Array(0 ..< Their.stressCountDefault))
    }

    @Test func serializedWorkFailureBeforeConcurrentValuesSuppressesReportsAfterCancel() async throws {
        let cancelRecorder = Their.TestCountRecorder()
        let eventRecorder = JobEngineStressEventRecorder()
        let reportStore = Their.Lock<Their.WorkReport<Int, TheirCoreSharedStressTestsError>?>(nil)
        let engine = JobEngine<Int, TheirCoreSharedStressTestsError>(
            sink: eventRecorder.append(_:),
            work: Their.serialized { report in
                reportStore.withLock { storedReport in
                    storedReport = report
                }
                return {
                    _ = cancelRecorder.increment()
                }
            }
        )

        _ = engine.start()
        let report = reportStore.withLock { storedReport in
            storedReport
        }
        #expect(report != nil)
        report?(.failure(.sample))
        try await eventRecorder.waitForEventCount(1)
        try await cancelRecorder.waitForCount(1)
        try await Their.stress { iteration in
            report?(.value(iteration))
        }

        #expect(eventRecorder.events.failureCount == 1)
        #expect(eventRecorder.events.messageCount(.emitOutputIgnoredBecauseStateIsNotActive) == 0)
        #expect(eventRecorder.events.values.isEmpty)
        #expect(cancelRecorder.count == 1)
    }

    @Test func weakValueCacheConcurrentAccessCreatesOneValuePerLiveKey() async throws {
        let cache = Their.WeakValueCache<Int, TheirCoreSharedStressObject>()
        let createCallsByKey = Their.Lock([Int: Int]())
        let retainedObjectsByKey = Their.Lock([Int: TheirCoreSharedStressObject]())
        let keyCount = 5

        try await Their.stress { iteration in
            let key = iteration % keyCount
            let value = cache.value(forKey: key) {
                createCallsByKey.withLock { counts in
                    counts[key, default: 0] += 1
                }
                let object = TheirCoreSharedStressObject(key: key)
                retainedObjectsByKey.withLock { objects in
                    objects[key] = object
                }
                return object
            }
            let reused = cache.value(forKey: key) {
                Issue.record("Expected cached value to be reused for key \(key).")
                return TheirCoreSharedStressObject(key: key)
            }

            #expect(value === reused)
            #expect(value.key == key)
        }

        let expectedCounts = Dictionary(uniqueKeysWithValues: (0 ..< keyCount).map { key in
            (key, 1)
        })
        #expect(createCallsByKey.withLock { counts in counts } == expectedCounts)
        #expect(retainedObjectsByKey.withLock { objects in objects.count } == keyCount)
    }

    @Test func weakValueCacheJobForKeyConcurrentAccessCreatesOneHubAndManyJobs() async throws {
        let cache = Their.WeakValueCache<String, Their.Hub<Int, TheirCoreSharedStressTestsError>>()
        let createCallsCount = Their.TestCountRecorder()
        let eventRecorder = JobStressEventRecorder()
        let retainedHub = Their.Lock<Their.Hub<Int, TheirCoreSharedStressTestsError>?>(nil)
        let startRecorder = HubStressWorkRecorder()

        let cancels = try await Their.stress {
            let job = cache.job(forKey: "shared") {
                _ = createCallsCount.increment()
                let hub: Their.Hub<Int, TheirCoreSharedStressTestsError> = Their.Hub(
                    work: startRecorder.work
                )
                .shareLatest()
                retainedHub.withLock { retainedHub in
                    retainedHub = hub
                }
                return hub
            }
            return job.subscribe(eventRecorder.append(_:))
        }
        try await startRecorder.waitForStartCallsCount(1)
        startRecorder.emit(.value(37))
        try await eventRecorder.waitForEventCount(Their.stressCountDefault)
        try await Their.stress(count: cancels.count) { iteration in
            cancels[iteration]()
        }
        try await startRecorder.waitForCancelCallsCount(1)

        #expect(createCallsCount.count == 1)
        #expect(eventRecorder.events == Array(repeating: .value(37), count: Their.stressCountDefault))
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }
}

private enum TheirCoreSharedStressTestsError: Swift.Error, Equatable, Sendable {

    case sample
}

private final class TheirCoreSharedStressObject: Sendable {

    let key: Int

    init(key: Int) {
        self.key = key
    }
}

private typealias HubEngineStressEventRecorder =
Their.TestEventRecorder<Their.HubEvent<Int, TheirCoreSharedStressTestsError>>
private typealias HubStressEventRecorder =
Their.TestEventRecorder<Their.HubEvent<Int, TheirCoreSharedStressTestsError>>
private typealias HubStressWorkRecorder =
Their.TestWorkRecorder<Int, TheirCoreSharedStressTestsError>
private typealias JobEngineStressEventRecorder =
Their.TestEventRecorder<JobEngineEvent<Int, TheirCoreSharedStressTestsError>>
private typealias JobStressEventRecorder =
Their.TestEventRecorder<Their.JobEvent<Int, TheirCoreSharedStressTestsError>>
private typealias JobStressWorkRecorder =
Their.TestWorkRecorder<Int, TheirCoreSharedStressTestsError>

private extension Their.HubEvent where Failure == TheirCoreSharedStressTestsError, Value == Int {

    var value: Int? {
        switch self {
        case .finished, .failure:
            return nil
        case .value(let value):
            return value
        }
    }
}

private extension Their.JobEvent where Failure == TheirCoreSharedStressTestsError, Value == Int {

    var value: Int? {
        switch self {
        case .finished, .failure:
            return nil
        case .value(let value):
            return value
        }
    }
}

private extension Array where Element == JobEngineEvent<Int, TheirCoreSharedStressTestsError> {

    var containsValueAfterFirstFailure: Bool {
        guard let failureIndex = firstIndex(where: { event in
            switch event {
            case .failure:
                return true
            case .finished, .message, .value:
                return false
            }
        }) else {
            return false
        }
        return suffix(from: index(after: failureIndex)).contains { event in
            switch event {
            case .value:
                return true
            case .finished, .failure, .message:
                return false
            }
        }
    }

    var failureCount: Int {
        filter { event in
            switch event {
            case .failure:
                return true
            case .finished, .message, .value:
                return false
            }
        }.count
    }

    var values: [Int] {
        compactMap { event in
            switch event {
            case .value(let value):
                return value
            case .finished, .failure, .message:
                return nil
            }
        }
    }

    func messageCount(_ message: JobEngineMessage) -> Int {
        filter { event in
            switch event {
            case .message(let currentMessage):
                return currentMessage == message
            case .finished, .failure, .value:
                return false
            }
        }.count
    }
}

private extension Array where Element == Their.HubEvent<Int, TheirCoreSharedStressTestsError> {

    var failureCount: Int {
        filter { event in
            switch event {
            case .failure:
                return true
            case .finished, .value:
                return false
            }
        }.count
    }

    var values: [Int] {
        compactMap { event in
            switch event {
            case .value(let value):
                return value
            case .finished, .failure:
                return nil
            }
        }
    }
}

private extension Array where Element == Their.JobEvent<Int, TheirCoreSharedStressTestsError> {

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
        return suffix(from: index(after: failureIndex)).contains { event in
            switch event {
            case .value:
                return true
            case .finished, .failure:
                return false
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
        }.count
    }

    var values: [Int] {
        compactMap { event in
            switch event {
            case .value(let value):
                return value
            case .finished, .failure:
                return nil
            }
        }
    }
}
