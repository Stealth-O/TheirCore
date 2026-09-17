import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct HubEvolutionTests {

    @Test func baseHubDoesNotReplayLastOutputToLateSubscriber() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let hub = upstream.hub
            _ = hub.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 10)
            try await firstRecorder.waitForEventCount(1)
            _ = hub.subscribe(secondRecorder.append(_:))
            #expect(secondRecorder.events.isEmpty == true)
            upstream.emit(value: 20)
            try await firstRecorder.waitForEventCount(2)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.value(10), .value(20)])
            #expect(secondRecorder.events == [.value(20)])
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func derivedHubWithoutRootDebugRemainsNil() async throws {
        try await Their.stress {
            let upstream = HubEvolutionHubDriver()
            let base = upstream.hub
            let derived = base
                .shareLatest()
                .evolve(initial: 0) { state, value in
                    state += value
                    return state
                }
            #expect(base.logging == nil)
            #expect(derived.logging == nil)
        }
    }

    @Test func disabledLoggingMetadataSurvivesDerivedHubs() async throws {
        try await Their.stress {
            let logging = Their.LifecycleLogging(
                file: "RootHub.swift",
                line: 91,
                label: "disabled-discovery",
                options: []
            )
            let work = HubEvolutionWorkRecorder()
            let hub: Their.Hub<Int, HubEvolutionTestsError> = Their.Hub(
                logging: logging,
                work: work.work
            )
            let derived = hub
                .shareLatest()
                .map(String.init)
                .evolve(initial: [String]()) { state, value in
                    state.append(value)
                    return state
                }
            #expect(hub.logging == logging)
            #expect(derived.logging == logging)
        }
    }

    @Test func evolveCancelsUpstreamAndResetsStateAfterLastSubscriberUnsubscribes() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let evolved = upstream.hub.evolve(initial: 0) { state, value in
                state += value
                return state
            }
            let cancel = evolved.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 10)
            try await firstRecorder.waitForEventCount(1)
            cancel()
            try await upstream.waitForCancelCallsCount(1)
            _ = evolved.subscribe(secondRecorder.append(_:))
            try await upstream.waitForStartCallsCount(2)
            upstream.emit(value: 5)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.value(10)])
            #expect(secondRecorder.events == [.value(5)])
            #expect(upstream.startCallsCount == 2)
        }
    }

    @Test func evolveDeinitCancelsUpstreamSubscription() async throws {
        try await Their.stress {
            let cancelSignal = HubEvolutionTestSignal()
            let eventRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let workRecorder = HubEvolutionWorkRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let hub = Their.Hub(work: workRecorder.work)
            var evolved: Their.Hub<Int, HubEvolutionTestsError>? = hub.evolve(initial: 0) { state, value in
                state += value
                return state
            }
            _ = evolved?.subscribe(eventRecorder.append(_:))
            try await workRecorder.waitForStartCallsCount(1)
            evolved = nil
            try await cancelSignal.wait()

            #expect(eventRecorder.events.isEmpty == true)
            #expect(workRecorder.cancelCallsCount == 1)
            #expect(workRecorder.startCallsCount == 1)
        }
    }

    @Test func evolveDropsLateUpstreamCallbacksFromPreviousLifecycleAfterRestart() async throws {
        try await Their.stress {
            let driver = HubEvolutionManualHubDriver()
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let evolved = driver.hub.evolve(initial: 0) { state, value in
                state += value
                return state
            }
            let firstCancel = evolved.subscribe(firstRecorder.append(_:))
            driver.emit(value: 10, withSinkAt: 0)
            try await firstRecorder.waitForEventCount(1)
            firstCancel()
            try await driver.waitForCancelCallsCount(1)

            _ = evolved.subscribe(secondRecorder.append(_:))
            try await driver.waitForStartCallsCount(2)
            driver.emit(value: 100, withSinkAt: 0)
            driver.emit(value: 5, withSinkAt: 1)
            try await secondRecorder.waitForEventCount(1)

            #expect(firstRecorder.events == [.value(10)])
            #expect(secondRecorder.events == [.value(5)])
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 2)
        }
    }

    /// Pins the FIFO ordering of terminal failure behind a blocked transform:
    /// the failure is processed by the same single drainer, so it cannot
    /// overtake or interrupt the value being transformed — the value is
    /// committed and broadcast first, then the queued failure terminates the
    /// shared lifecycle and cancels the upstream exactly once.
    @Test func evolveFailureQueuedBehindBlockedTransformDoesNotOvertakeValue() async throws {
        try await Their.stress(count: 1) {
            let eventRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let releaseTransform = DispatchSemaphore(value: 0)
            let transformEntered = HubEvolutionTestSignal()
            let upstream = HubEvolutionHubDriver()
            let evolved = upstream.hub.evolve(initial: 0) { state, value in
                state += value
                if value == 1 {
                    transformEntered.signal()
                    releaseTransform.wait()
                }
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            let emitTask = BlockingWork {
                upstream.emit(value: 1)
            }
            try await transformEntered.wait()
            upstream.emit(failure: .sample)
            releaseTransform.signal()
            try await emitTask.value
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value(1), .failure(.sample)])
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func evolvePropagatesTerminalEndBypassingTransformAndSuppressesLateOutput() async throws {
        try await Their.stress {
            let eventRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let transformRecorder = Their.TestCountRecorder()
            let upstream = HubEvolutionHubDriver()
            let evolved = upstream.hub.evolve(initial: 0) { state, value in
                state += value
                _ = transformRecorder.increment()
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 10)
            try await eventRecorder.waitForEventCount(1)
            upstream.emitFinished()
            try await eventRecorder.waitForEventCount(2)
            upstream.emit(value: 20)
            #expect(eventRecorder.events == [.value(10), .finished])
            #expect(transformRecorder.count == 1)
        }
    }

    @Test func evolvePropagatesTerminalFailureAndSuppressesLateOutput() async throws {
        try await Their.stress {
            let eventRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let evolved = upstream.hub.evolve(initial: 0) { state, value in
                state += value
                return state
            }
            _ = evolved.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(failure: .sample)
            try await eventRecorder.waitForEventCount(1)
            upstream.emit(value: 10)
            #expect(eventRecorder.events == [.failure(.sample)])
        }
    }

    @Test func evolveRestartDuringTransformKeepsOneDrainerAndResetsState() async throws {
        try await Their.stress {
            let driver = HubEvolutionManualHubDriver()
            let firstEvents = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let nextCancel = Their.Lock<Their.HubCancel?>(nil)
            let nextEvents = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let restart = Their.Lock<(@Sendable () -> Void)?>(nil)
            let trace = Their.TestEventRecorder<String>()
            let evolved = driver.hub.evolve(initial: 0) { state, value in
                trace.append("begin \(value)")
                state += value
                if value == 1 {
                    let action = restart.withLock { stored in
                        let action = stored
                        stored = nil
                        return action
                    }
                    action?()
                }
                trace.append("end \(value)")
                return state
            }
            let firstCancel = evolved.subscribe(firstEvents.append(_:))
            restart.withLock { stored in
                stored = {
                    firstCancel()
                    let cancel = evolved.subscribe(nextEvents.append(_:))
                    nextCancel.withLock { $0 = cancel }
                    trace.append("restarted")
                    driver.emit(value: 100, withSinkAt: 0)
                    driver.emit(value: 10, withSinkAt: 1)
                    trace.append("queued 10")
                    #expect(nextEvents.events.isEmpty)
                }
            }

            driver.emit(value: 1, withSinkAt: 0)

            // The old transform must finish before the new generation drains.
            // Its result/state commit and its late callback are both stale.
            #expect(trace.events == ["begin 1", "restarted", "queued 10", "end 1", "begin 10", "end 10"])
            #expect(firstEvents.events.isEmpty)
            #expect(nextEvents.events == [.value(10)])
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 2)
            firstCancel()
            driver.emit(value: 2, withSinkAt: 1)
            #expect(nextEvents.events == [.value(10), .value(12)])
            nextCancel.withLock { $0 }?()
            #expect(driver.cancelCallsCount == 2)
        }
    }

    @Test func evolveSharesStateAcrossSubscribersAndSuppressesNilOutputs() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let evolved: Their.Hub<Int, HubEvolutionTestsError> = upstream.hub.evolve(initial: 0) { state, value in
                state += value
                guard state.isMultiple(of: 2) else {
                    return nil
                }
                return state
            }
            _ = evolved.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 1)
            _ = evolved.subscribe(secondRecorder.append(_:))
            upstream.emit(value: 1)
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)
            upstream.emit(value: 2)
            try await firstRecorder.waitForEventCount(2)
            try await secondRecorder.waitForEventCount(2)
            #expect(firstRecorder.events == [.value(2), .value(4)])
            #expect(secondRecorder.events == [.value(2), .value(4)])
            #expect(upstream.startCallsCount == 1)
        }
    }

    /// Pins the recipient-snapshot timing: the subscribers that receive a
    /// value are captured when its input is dequeued, before the transform
    /// runs. A subscriber that joins while the transform is blocked is not in
    /// that snapshot, so the in-flight value does not reach it — the base
    /// evolve stays live-only for mid-transform joiners.
    @Test func evolveSubscriberJoiningDuringTransformDoesNotReceiveInFlightValue() async throws {
        try await Their.stress(count: 1) {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let releaseTransform = DispatchSemaphore(value: 0)
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let transformEntered = HubEvolutionTestSignal()
            let upstream = HubEvolutionHubDriver()
            let evolved = upstream.hub.evolve(initial: 0) { state, value in
                state += value
                if value == 1 {
                    transformEntered.signal()
                    releaseTransform.wait()
                }
                return state
            }
            let firstCancel = evolved.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            let emitTask = BlockingWork {
                upstream.emit(value: 1)
            }
            try await transformEntered.wait()
            let secondCancel = evolved.subscribe(secondRecorder.append(_:))
            releaseTransform.signal()
            try await emitTask.value
            try await firstRecorder.waitForEventCount(1)
            upstream.emit(value: 2)
            try await firstRecorder.waitForEventCount(2)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.value(1), .value(3)])
            #expect(secondRecorder.events == [.value(3)])
            firstCancel()
            secondCancel()
        }
    }

    /// Pins the drainer execution model: the shared transform runs outside the
    /// internal lock, so it may synchronously cancel a derived subscription
    /// without deadlocking. The cancel tears down the single-subscriber
    /// lifecycle while the transform is in flight, so the emission produced by
    /// that very transform is dropped and the upstream is cancelled exactly
    /// once.
    @Test func evolveTransformReentrantCancelStopsSharedLifecycleWithoutDeadlock() async throws {
        try await Their.stress {
            let cancelBox = Their.Lock<Their.HubCancel?>(nil)
            let eventRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let evolved = upstream.hub.evolve(initial: 0) { state, value in
                state += value
                if value == 1 {
                    cancelBox.withLock { cancel in cancel }?()
                }
                return state
            }
            let cancel = evolved.subscribe(eventRecorder.append(_:))
            cancelBox.withLock { stored in
                stored = cancel
            }
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 1)
            try await upstream.waitForCancelCallsCount(1)
            upstream.emit(value: 2)
            #expect(eventRecorder.events.isEmpty == true)
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func mapErrorTransformsFailureAndPropagatesValue() async throws {
        try await Their.stress {
            let eventRecorder = HubEvolutionEventRecorder<Int, HubEvolutionOtherTestsError>()
            let upstream = HubEvolutionHubDriver()
            let mapped = upstream.hub.mapError(HubEvolutionOtherTestsError.wrapped(_:))
            _ = mapped.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 7)
            try await eventRecorder.waitForEventCount(1)
            upstream.emit(failure: .sample)
            try await eventRecorder.waitForEventCount(2)

            #expect(eventRecorder.events == [.value(7), .failure(.wrapped(.sample))])
            #expect(upstream.startCallsCount == 1)
        }
    }

    @Test func mapTransformsValueAndFailure() async throws {
        try await Their.stress {
            let eventRecorder = HubEvolutionEventRecorder<String, HubEvolutionOtherTestsError>()
            let upstream = HubEvolutionHubDriver()
            let mapped: Their.Hub<String, HubEvolutionOtherTestsError> = upstream.hub.map(
                failure: HubEvolutionOtherTestsError.wrapped(_:)
            ) { value in
                "value-\(value)"
            }
            _ = mapped.subscribe(eventRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 30)
            try await eventRecorder.waitForEventCount(1)
            upstream.emit(failure: .sample)
            try await eventRecorder.waitForEventCount(2)
            #expect(eventRecorder.events == [.value("value-30"), .failure(.wrapped(.sample))])
        }
    }

    @Test func shareLatestClearsValueAfterLastSubscriberUnsubscribes() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let hub = upstream.hub.shareLatest()
            let cancel = hub.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 10)
            try await firstRecorder.waitForEventCount(1)
            cancel()
            try await upstream.waitForCancelCallsCount(1)
            _ = hub.subscribe(secondRecorder.append(_:))
            try await upstream.waitForStartCallsCount(2)
            #expect(secondRecorder.events.isEmpty == true)
            upstream.emit(value: 20)
            try await secondRecorder.waitForEventCount(1)
            #expect(secondRecorder.events == [.value(20)])
            #expect(upstream.startCallsCount == 2)
        }
    }

    @Test func shareLatestClearsValueAfterTerminalEnd() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let hub = upstream.hub.shareLatest()
            _ = hub.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 10)
            try await firstRecorder.waitForEventCount(1)
            upstream.emitFinished()
            try await firstRecorder.waitForEventCount(2)
            _ = hub.subscribe(secondRecorder.append(_:))
            try await upstream.waitForStartCallsCount(2)
            #expect(secondRecorder.events.isEmpty == true)
            upstream.emit(value: 20)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.value(10), .finished])
            #expect(secondRecorder.events == [.value(20)])
            #expect(upstream.startCallsCount == 2)
        }
    }

    @Test func shareLatestClearsValueAfterTerminalFailure() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let hub = upstream.hub.shareLatest()
            _ = hub.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 10)
            try await firstRecorder.waitForEventCount(1)
            upstream.emit(failure: .sample)
            try await firstRecorder.waitForEventCount(2)
            _ = hub.subscribe(secondRecorder.append(_:))
            try await upstream.waitForStartCallsCount(2)
            #expect(secondRecorder.events.isEmpty == true)
            upstream.emit(value: 20)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.value(10), .failure(.sample)])
            #expect(secondRecorder.events == [.value(20)])
            #expect(upstream.startCallsCount == 2)
        }
    }

    /// Pins the exactly-once replay contract: a subscriber that joins while a
    /// broadcast is being delivered (its replay request is queued behind that
    /// in-flight broadcast) receives the latest value exactly once — the
    /// replay reads the value at processing time and skips subscribers that
    /// already received a live broadcast, so the joiner sees neither a stale
    /// older value nor a duplicate.
    @Test func shareLatestJoinerDuringBroadcastReceivesLatestExactlyOnce() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let hubBox = Their.Lock<Their.Hub<Int, HubEvolutionTestsError>?>(nil)
            let joined = Their.Lock(false)
            let secondCancelBox = Their.Lock<Their.HubCancel?>(nil)
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let hub = upstream.hub.shareLatest()
            hubBox.withLock { box in
                box = hub
            }
            let firstCancel = hub.subscribe { event in
                firstRecorder.append(event)
                guard case .value(2) = event else {
                    return
                }
                let shouldJoin = joined.withLock { didJoin in
                    guard didJoin == false else {
                        return false
                    }
                    didJoin = true
                    return true
                }
                guard shouldJoin, let hub = hubBox.withLock({ box in box }) else {
                    return
                }
                let cancel = hub.subscribe(secondRecorder.append(_:))
                secondCancelBox.withLock { stored in
                    stored = cancel
                }
            }
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 1)
            try await firstRecorder.waitForEventCount(1)
            upstream.emit(value: 2)
            try await firstRecorder.waitForEventCount(2)
            try await secondRecorder.waitForEventCount(1)
            upstream.emit(value: 3)
            try await firstRecorder.waitForEventCount(3)
            try await secondRecorder.waitForEventCount(2)
            #expect(firstRecorder.events == [.value(1), .value(2), .value(3)])
            #expect(secondRecorder.events == [.value(2), .value(3)])
            hubBox.withLock { box in
                box = nil
            }
            firstCancel()
            secondCancelBox.withLock { stored in
                stored?()
                stored = nil
            }
        }
    }

    /// Pins the `shareLatest` joiner-during-transform path: the joiner is not
    /// in the in-flight value's recipient snapshot, but its queued replay
    /// survives that broadcast (only recipients lose pending replays) — it is
    /// processed right after the commit and delivers the new latest value
    /// exactly once. Built on the internal `replayLatest` seam so the gate
    /// can sit inside the transform itself.
    @Test func shareLatestJoinerDuringTransformReceivesValueViaReplayExactlyOnce() async throws {
        try await Their.stress(count: 1) {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let releaseTransform = DispatchSemaphore(value: 0)
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let transformEntered = HubEvolutionTestSignal()
            let upstream = HubEvolutionHubDriver()
            let evolved: Their.Hub<Int, HubEvolutionTestsError> = upstream.hub.evolve(
                fileID: #fileID,
                failure: { $0 },
                function: #function,
                initial: 0,
                line: #line,
                replayLatest: true
            ) { state, value in
                state += value
                if value == 1 {
                    transformEntered.signal()
                    releaseTransform.wait()
                }
                return state
            }
            let firstCancel = evolved.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            let emitTask = BlockingWork {
                upstream.emit(value: 1)
            }
            try await transformEntered.wait()
            let secondCancel = evolved.subscribe(secondRecorder.append(_:))
            releaseTransform.signal()
            try await emitTask.value
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.value(1)])
            #expect(secondRecorder.events == [.value(1)])
            upstream.emit(value: 2)
            try await firstRecorder.waitForEventCount(2)
            try await secondRecorder.waitForEventCount(2)
            #expect(firstRecorder.events == [.value(1), .value(3)])
            #expect(secondRecorder.events == [.value(1), .value(3)])
            firstCancel()
            secondCancel()
        }
    }

    /// Pins the FIFO ordering of replay against later values: a replay request
    /// queued before the next upstream value is delivered before that value,
    /// so the joiner observes the cached value first and the newer broadcast
    /// second — never the reverse.
    @Test func shareLatestReplayEnqueuedBeforeNextValueArrivesInOrder() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let hubBox = Their.Lock<Their.Hub<Int, HubEvolutionTestsError>?>(nil)
            let joined = Their.Lock(false)
            let secondCancelBox = Their.Lock<Their.HubCancel?>(nil)
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let hub = upstream.hub.shareLatest()
            hubBox.withLock { box in
                box = hub
            }
            let firstCancel = hub.subscribe { [weak upstream] event in
                firstRecorder.append(event)
                guard case .value(1) = event else {
                    return
                }
                let shouldJoin = joined.withLock { didJoin in
                    guard didJoin == false else {
                        return false
                    }
                    didJoin = true
                    return true
                }
                guard shouldJoin, let hub = hubBox.withLock({ box in box }) else {
                    return
                }
                // Join while the drainer is busy delivering value 1: the replay
                // request is queued first, then the re-entrant upstream emit
                // queues value 2 behind it.
                let cancel = hub.subscribe(secondRecorder.append(_:))
                secondCancelBox.withLock { stored in
                    stored = cancel
                }
                upstream?.emit(value: 2)
            }
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 1)
            try await firstRecorder.waitForEventCount(2)
            try await secondRecorder.waitForEventCount(2)
            #expect(firstRecorder.events == [.value(1), .value(2)])
            #expect(secondRecorder.events == [.value(1), .value(2)])
            hubBox.withLock { box in
                box = nil
            }
            firstCancel()
            secondCancelBox.withLock { stored in
                stored?()
                stored = nil
            }
        }
    }

    @Test func shareLatestReplaysLastOutputToLateSubscriber() async throws {
        try await Their.stress {
            let firstRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let secondRecorder = HubEvolutionEventRecorder<Int, HubEvolutionTestsError>()
            let upstream = HubEvolutionHubDriver()
            let hub = upstream.hub.shareLatest()
            _ = hub.subscribe(firstRecorder.append(_:))
            try await upstream.waitForStartCallsCount(1)
            upstream.emit(value: 10)
            try await firstRecorder.waitForEventCount(1)
            _ = hub.subscribe(secondRecorder.append(_:))
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.value(10)])
            #expect(secondRecorder.events == [.value(10)])
            #expect(upstream.startCallsCount == 1)
        }
    }
}

private enum HubEvolutionOtherTestsError: Equatable, Swift.Error, Sendable {

    case wrapped(HubEvolutionTestsError)
}

private enum HubEvolutionTestsError: Equatable, Swift.Error, Sendable {

    case sample
}

private typealias HubEvolutionEventRecorder<Value: Sendable, Failure: Swift.Error & Sendable> = Their.TestEventRecorder<Their.HubEvent<Value, Failure>>
private typealias HubEvolutionHubDriver = Their.TestHubDriver<Int, HubEvolutionTestsError>
private typealias HubEvolutionTestSignal = Their.TestSignal
private typealias HubEvolutionWorkRecorder = Their.TestWorkRecorder<Int, HubEvolutionTestsError>

private final class HubEvolutionManualHubDriver: Sendable {

    var cancelCallsCount: Int {
        cancelRecorder.count
    }
    private let cancelRecorder = Their.TestCountRecorder()
    let hub: Their.Hub<Int, HubEvolutionTestsError>
    var startCallsCount: Int {
        startRecorder.count
    }
    private let startRecorder = Their.TestCountRecorder()
    private let storage = HubEvolutionManualHubStorage()

    init() {
        hub = Their.Hub(
            misuseHandler: Their.MisuseHandlers.fatal,
            misuseLocation: .init(),
            onSubscribe: { [cancelRecorder, startRecorder, storage] sink in
                storage.lock.withLock { record in
                    record.sinks.append(sink)
                }
                _ = startRecorder.increment()
                return {
                    _ = cancelRecorder.increment()
                }
            }
        )
    }

    func emit(value: Int, withSinkAt index: Int) {
        let sink = storage.lock.withLock { record in
            record.sinks[index]
        }
        sink(.value(value))
    }

    func waitForCancelCallsCount(_ count: Int) async throws {
        try await cancelRecorder.waitForCount(count)
    }

    func waitForStartCallsCount(_ count: Int) async throws {
        try await startRecorder.waitForCount(count)
    }
}

private struct HubEvolutionManualHubRecord: Sendable {

    var sinks = [Their.HubSink<Int, HubEvolutionTestsError>]()
}

private final class HubEvolutionManualHubStorage: Sendable {

    let lock = Their.Lock(HubEvolutionManualHubRecord())
}
