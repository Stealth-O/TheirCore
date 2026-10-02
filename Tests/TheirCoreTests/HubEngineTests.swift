import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct HubEngineTests {

    /// Replacing or clearing a diagnostic hook must release its old captures
    /// after unlocking. The nonblocking probe reports the boundary without
    /// attempting a recursive unfair-lock acquisition.
    @Test(arguments: [false, true])
    func afterSnapshotHookReleasesOldCaptureOutsideLock(replace: Bool) async throws {
        try await Their.stress {
            let deinits = Their.TestCountRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(work: { _ in {} })
            let observations = Their.TestEventRecorder<Bool>()
            let reentries = Their.TestCountRecorder()
            hub.setAfterSnapshotForTests(makeHubEngineHook(
                deinits: deinits,
                hub: hub,
                observations: observations,
                reentries: reentries
            ))
            #expect(deinits.count == 0)

            if replace {
                hub.setAfterSnapshotForTests({})
            } else {
                hub.setAfterSnapshotForTests(nil)
            }

            withExtendedLifetime(hub) {
                #expect(deinits.count == 1)
                #expect(observations.events == [true])
                #expect(hub.isLockAvailableForTests())
                #expect(reentries.count == 1)
            }
            hub.setAfterSnapshotForTests(nil)
            #expect(deinits.count == 1)
        }
    }

    @Test(arguments: [false, true])
    func beforeDetachedEngineStopHookReleasesOldCaptureOutsideLock(replace: Bool) async throws {
        try await Their.stress {
            let deinits = Their.TestCountRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(work: { _ in {} })
            let observations = Their.TestEventRecorder<Bool>()
            let reentries = Their.TestCountRecorder()
            hub.setBeforeDetachedEngineStopForTests(makeHubEngineHook(
                deinits: deinits,
                hub: hub,
                observations: observations,
                reentries: reentries
            ))
            #expect(deinits.count == 0)

            if replace {
                hub.setBeforeDetachedEngineStopForTests({})
            } else {
                hub.setBeforeDetachedEngineStopForTests(nil)
            }

            withExtendedLifetime(hub) {
                #expect(deinits.count == 1)
                #expect(observations.events == [true])
                #expect(hub.isLockAvailableForTests())
                #expect(reentries.count == 1)
            }
            hub.setBeforeDetachedEngineStopForTests(nil)
            #expect(deinits.count == 1)
        }
    }

    /// Cancellation should retire a registered diagnostic waiter so a failed
    /// state expectation cannot prevent TheirCoreTesting's cooperative timeout join.
    /// Reach the requested state even after a failed assertion or parent
    /// cancellation, then join the waiter before releasing its subscription.
    @Test func cancelledStateWaiterIsRemovedBeforeTargetTransition() async throws {
        try await Their.stress {
            let completions = Their.TestCountRecorder()
            let registered = Their.TestSignal()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(work: startRecorder.work)
            let waiter = Task {
                await hub.waitForStateForTests(
                    .init(isRunning: true, subscribersCount: 1),
                    onSuspend: registered.signal
                )
                _ = completions.increment()
            }

            do {
                try await registered.wait()
                #expect(hub.stateWaitersCountForTests() == 1)
                waiter.cancel()

                #expect(hub.stateWaitersCountForTests() == 0)
                #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
                #expect(startRecorder.startCallsCount == 0)
                try await completions.waitForCount(1)
                #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
                #expect(startRecorder.startCallsCount == 0)
            } catch {
                waiter.cancel()
                let cleanupCancel = hub.subscribe { _ in }
                await waiter.value
                cleanupCancel()
                throw error
            }

            let cleanupCancel = hub.subscribe { _ in }
            await waiter.value
            cleanupCancel()

            #expect(completions.count == 1)
            #expect(hub.stateWaitersCountForTests() == 0)
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func deinitCancelsActiveJob() async throws {
        try await Their.stress {
            let startRecorder = HubEngineStartRecorder()
            var hub: HubEngine<Int, HubEngineTestsError>? = .init(
                work: startRecorder.work
            )
            let cancel = hub?.subscribe { _ in }
            #expect(cancel != nil)
            try await startRecorder.waitForStartCallsCount(1)
            #expect((hub?.getState().isRunning) == true)
            #expect(startRecorder.cancelCallsCount == 0)
            hub = nil
            try await startRecorder.waitForCancelCallsCount(1)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    /// Pins the generation fencing for the last-unsubscribe window: after the
    /// registry empties, the engine is detached under the lock but its `stop()`
    /// runs outside it. The DEBUG hook fires inside that window, where a new
    /// subscriber starts a fresh lifecycle and the still-running detached
    /// engine reports a stale value and a stale failure — both must be dropped
    /// instead of reaching, or tearing down, the new lifecycle.
    @Test func detachedEngineAfterLastUnsubscribeCannotTouchNextLifecycle() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let hookFired = Their.Lock(false)
            let hubBox = Their.Lock<HubEngine<Int, HubEngineTestsError>?>(nil)
            let reports = Their.Lock<[Their.WorkReport<Int, HubEngineTestsError>]>([])
            let secondCancelBox = Their.Lock<Their.HubCancel?>(nil)
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = Their.TestCountRecorder()
            let work: Their.Work<Int, HubEngineTestsError> = { report in
                _ = startRecorder.increment()
                reports.withLock { stored in
                    stored.append(report)
                }
                return {}
            }
            let hub = HubEngine<Int, HubEngineTestsError>(work: work)
            hubBox.withLock { box in
                box = hub
            }
            hub.setBeforeDetachedEngineStopForTests {
                let shouldRun = hookFired.withLock { fired in
                    guard fired == false else {
                        return false
                    }
                    fired = true
                    return true
                }
                guard shouldRun, let hub = hubBox.withLock({ box in box }) else {
                    return
                }
                let cancel = hub.subscribe(secondRecorder.append(_:))
                secondCancelBox.withLock { stored in
                    stored = cancel
                }
                let staleReport = reports.withLock { stored in stored.first }
                staleReport?(.value(99))
                staleReport?(.finished)
                staleReport?(.failure(.sample))
            }

            let firstCancel = hub.subscribe(firstRecorder.append(_:))
            firstCancel()

            #expect(hub.getState() == .init(isRunning: true, subscribersCount: 1))
            #expect(secondRecorder.events.isEmpty == true)
            let freshReport = reports.withLock { stored in stored.last }
            freshReport?(.value(7))
            #expect(firstRecorder.events.isEmpty == true)
            #expect(secondRecorder.events == [.value(7)])
            #expect(startRecorder.count == 2)
            hub.setBeforeDetachedEngineStopForTests(nil)
            hubBox.withLock { box in
                box = nil
            }
            secondCancelBox.withLock { stored in
                stored?()
                stored = nil
            }
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
        }
    }

    /// Pins the empty-registry start commit: a synchronous start failure clears
    /// the registry, so Phase 3 does not install the engine and stops it after
    /// releasing the lock. The DEBUG hook fires inside that window, where a new
    /// subscriber starts a fresh lifecycle and stale reports from the detached
    /// engine are dropped — by the terminated engine itself and by the
    /// generation fence — while the pending `stop()` must not affect the new
    /// lifecycle either.
    @Test func detachedEngineFromEmptyRegistryStartCannotTouchNextLifecycle() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let hookFired = Their.Lock(false)
            let hubBox = Their.Lock<HubEngine<Int, HubEngineTestsError>?>(nil)
            let reports = Their.Lock<[Their.WorkReport<Int, HubEngineTestsError>]>([])
            let secondCancelBox = Their.Lock<Their.HubCancel?>(nil)
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = Their.TestCountRecorder()
            let work: Their.Work<Int, HubEngineTestsError> = { report in
                let startNumber = startRecorder.increment()
                reports.withLock { stored in
                    stored.append(report)
                }
                if startNumber == 1 {
                    report(.failure(.sample))
                }
                return {}
            }
            let hub = HubEngine<Int, HubEngineTestsError>(work: work)
            hubBox.withLock { box in
                box = hub
            }
            hub.setBeforeDetachedEngineStopForTests {
                let shouldRun = hookFired.withLock { fired in
                    guard fired == false else {
                        return false
                    }
                    fired = true
                    return true
                }
                guard shouldRun, let hub = hubBox.withLock({ box in box }) else {
                    return
                }
                let cancel = hub.subscribe(secondRecorder.append(_:))
                secondCancelBox.withLock { stored in
                    stored = cancel
                }
                let staleReport = reports.withLock { stored in stored.first }
                staleReport?(.value(99))
                staleReport?(.finished)
                staleReport?(.failure(.sample))
            }

            let firstCancel = hub.subscribe(firstRecorder.append(_:))

            #expect(firstRecorder.events == [.failure(.sample)])
            #expect(hub.getState() == .init(isRunning: true, subscribersCount: 1))
            #expect(secondRecorder.events.isEmpty == true)
            let freshReport = reports.withLock { stored in stored.last }
            freshReport?(.value(7))
            #expect(secondRecorder.events == [.value(7)])
            #expect(startRecorder.count == 2)
            hub.setBeforeDetachedEngineStopForTests(nil)
            hubBox.withLock { box in
                box = nil
            }
            firstCancel()
            secondCancelBox.withLock { stored in
                stored?()
                stored = nil
            }
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
        }
    }

    /// The task is already cancelled when it enters the Hub wait. No suspended
    /// waiter should be installed, and no state transition is needed to finish.
    @Test func stateWaiterCancelledBeforeRegistrationCompletesWhileIdle() async throws {
        try await Their.stress {
            let completions = Their.TestCountRecorder()
            let entered = Their.TestSignal()
            let gate = Their.TestSignal()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(work: startRecorder.work)
            let suspensions = Their.TestCountRecorder()
            let waiter = Task {
                entered.signal()
                try? await gate.wait()
                #expect(Task.isCancelled)
                await hub.waitForStateForTests(
                    .init(isRunning: true, subscribersCount: 1),
                    onSuspend: { _ = suspensions.increment() }
                )
                _ = completions.increment()
            }

            do {
                try await entered.wait()
                waiter.cancel()
                try await completions.waitForCount(1)
            } catch {
                waiter.cancel()
                let cleanupCancel = hub.subscribe { _ in }
                await waiter.value
                cleanupCancel()
                throw error
            }
            await waiter.value

            #expect(completions.count == 1)
            #expect(hub.stateWaitersCountForTests() == 0)
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
            #expect(startRecorder.cancelCallsCount == 0)
            #expect(startRecorder.startCallsCount == 0)
            #expect(suspensions.count == 0)
        }
    }

    @Test func stateWaiterTargetTransitionBeforeCancelCompletesOnlyOnce() async throws {
        try await Their.stress {
            let completions = Their.TestCountRecorder()
            let registered = Their.TestSignal()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(work: startRecorder.work)
            let waiter = Task {
                await hub.waitForStateForTests(
                    .init(isRunning: true, subscribersCount: 1),
                    onSuspend: registered.signal
                )
                _ = completions.increment()
            }

            do {
                try await registered.wait()
            } catch {
                waiter.cancel()
                let cleanupCancel = hub.subscribe { _ in }
                await waiter.value
                cleanupCancel()
                throw error
            }
            let cancel = hub.subscribe { _ in }
            // Subscribe notifies synchronously, so the target transition, not
            // the cancellations below, must already have settled the waiter.
            #expect(hub.stateWaitersCountForTests() == 0)
            waiter.cancel()
            waiter.cancel()
            await waiter.value
            cancel()

            #expect(completions.count == 1)
            #expect(hub.stateWaitersCountForTests() == 0)
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func subscribeRestartsJobAfterLastSubscriberUnsubscribes() async throws {
        try await Their.stress {
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            let firstCancel = hub.subscribe { _ in }
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 1))
            try await startRecorder.waitForStartCallsCount(1)
            #expect(startRecorder.startCallsCount == 1)
            firstCancel()
            await hub.waitForStateForTests(.init(isRunning: false, subscribersCount: 0))
            try await startRecorder.waitForCancelCallsCount(1)
            #expect(startRecorder.cancelCallsCount == 1)
            let secondCancel = hub.subscribe { _ in }
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 1))
            try await startRecorder.waitForStartCallsCount(2)
            #expect(startRecorder.startCallsCount == 2)
            secondCancel()
        }
    }

    @Test func subscribeStartsOnlyOneJobForManySubscribers() async throws {
        try await Their.stress {
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            let firstCancel = hub.subscribe { _ in }
            let secondCancel = hub.subscribe { _ in }
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 2))
            try await startRecorder.waitForStartCallsCount(1)
            #expect(startRecorder.startCallsCount == 1)
            #expect(startRecorder.cancelCallsCount == 0)
            firstCancel()
            secondCancel()
        }
    }

    // MARK: - HubEngineSubscription

    @Test func subscriptionCancelIsIdempotent() async throws {
        try await Their.stress {
            let eventRecorder = HubEngineEventRecorder()
            let subscription = HubEngineSubscription<Int, HubEngineTestsError>(
                sink: eventRecorder.append(_:)
            )

            subscription.cancel()
            subscription.cancel()
            subscription.emit(.value(2))

            #expect(eventRecorder.events.isEmpty == true)
        }
    }

    @Test func subscriptionCancelSuppressesFutureEvents() async throws {
        try await Their.stress {
            let eventRecorder = HubEngineEventRecorder()
            let subscription = HubEngineSubscription<Int, HubEngineTestsError>(
                sink: eventRecorder.append(_:)
            )

            subscription.cancel()
            subscription.emit(.value(1))
            subscription.emit(.failure(.sample))

            #expect(eventRecorder.events.isEmpty == true)
        }
    }

    @Test func subscriptionEndIsTerminal() async throws {
        try await Their.stress {
            let eventRecorder = HubEngineEventRecorder()
            let subscription = HubEngineSubscription<Int, HubEngineTestsError>(
                sink: eventRecorder.append(_:)
            )

            subscription.emit(.finished)
            subscription.emit(.value(1))

            #expect(eventRecorder.events == [.finished])
        }
    }

    @Test func subscriptionFailureIsTerminal() async throws {
        try await Their.stress {
            let eventRecorder = HubEngineEventRecorder()
            let subscription = HubEngineSubscription<Int, HubEngineTestsError>(
                sink: eventRecorder.append(_:)
            )

            subscription.emit(.failure(.sample))
            subscription.emit(.value(1))

            #expect(eventRecorder.events == [.failure(.sample)])
        }
    }

    @Test func synchronousStartEndRestartsForResubscriber() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let probe = HubEngineLateSubscriberProbe()
            let secondRecorder = HubEngineEventRecorder()
            let startCount = Their.Lock(0)
            let work: Their.Work<Int, HubEngineTestsError> = { report in
                startCount.withLock { count in
                    count += 1
                }
                report(.finished)
                return {}
            }
            let hub = HubEngine<Int, HubEngineTestsError>(work: work)
            let firstCancel = hub.subscribe { [weak hub] event in
                firstRecorder.append(event)
                guard case .finished = event, let hub, probe.shouldSubscribeOnce() else {
                    return
                }
                probe.store(cancel: hub.subscribe(secondRecorder.append(_:)))
            }
            let totalStarts = startCount.withLock { count in count }
            #expect(firstRecorder.events == [.finished])
            #expect(secondRecorder.events == [.finished])
            #expect(totalStarts == 2)
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
            firstCancel()
            probe.cancelSecond()
        }
    }

    @Test func synchronousStartFailureRestartsForResubscriber() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let probe = HubEngineLateSubscriberProbe()
            let secondRecorder = HubEngineEventRecorder()
            let startCount = Their.Lock(0)
            let work: Their.Work<Int, HubEngineTestsError> = { report in
                startCount.withLock { count in
                    count += 1
                }
                report(.failure(.sample))
                return {}
            }
            let hub = HubEngine<Int, HubEngineTestsError>(work: work)
            let firstCancel = hub.subscribe { [weak hub] event in
                firstRecorder.append(event)
                guard case .failure = event, let hub, probe.shouldSubscribeOnce() else {
                    return
                }
                probe.store(cancel: hub.subscribe(secondRecorder.append(_:)))
            }
            let totalStarts = startCount.withLock { count in count }
            #expect(firstRecorder.events == [.failure(.sample)])
            #expect(secondRecorder.events == [.failure(.sample)])
            #expect(totalStarts == 2)
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
            firstCancel()
            probe.cancelSecond()
        }
    }

    @Test func terminalEndAllowsNewJobToStart() async throws {
        try await Their.stress {
            let recorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            _ = hub.subscribe(recorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.finished)
            try await recorder.waitForEventCount(1)
            await hub.waitForStateForTests(.init(isRunning: false, subscribersCount: 0))
            _ = hub.subscribe { _ in }
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 1))
            try await startRecorder.waitForStartCallsCount(2)
            #expect(startRecorder.startCallsCount == 2)
        }
    }

    @Test func terminalEndDeliversToSubscriberAddedAfterSnapshot() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let probe = HubEngineLateSubscriberProbe()
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            hub.setAfterSnapshotForTests { [hub, probe, secondRecorder] in
                guard probe.shouldSubscribeOnce() else {
                    return
                }
                let cancel = hub.subscribe(secondRecorder.append(_:))
                probe.store(cancel: cancel)
            }

            _ = hub.subscribe(firstRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.finished)
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)
            hub.setAfterSnapshotForTests(nil)
            probe.cancelSecond()

            #expect(firstRecorder.events == [.finished])
            #expect(secondRecorder.events == [.finished])
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
        }
    }

    @Test func terminalEndFinishesJobAndClearsSubscribers() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            _ = hub.subscribe(firstRecorder.append(_:))
            _ = hub.subscribe(secondRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.finished)
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.finished])
            #expect(secondRecorder.events == [.finished])
            await hub.waitForStateForTests(.init(isRunning: false, subscribersCount: 0))
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func terminalFailureAllowsNewJobToStart() async throws {
        try await Their.stress {
            let recorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            _ = hub.subscribe(recorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.failure(.sample))
            try await recorder.waitForEventCount(1)
            await hub.waitForStateForTests(.init(isRunning: false, subscribersCount: 0))
            _ = hub.subscribe { _ in }
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 1))
            try await startRecorder.waitForStartCallsCount(2)
            #expect(startRecorder.startCallsCount == 2)
        }
    }

    @Test func terminalFailureDeliversToSubscriberAddedAfterSnapshot() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let probe = HubEngineLateSubscriberProbe()
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            hub.setAfterSnapshotForTests { [hub, probe, secondRecorder] in
                guard probe.shouldSubscribeOnce() else {
                    return
                }
                let cancel = hub.subscribe(secondRecorder.append(_:))
                probe.store(cancel: cancel)
            }

            _ = hub.subscribe(firstRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.failure(.sample))
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)
            hub.setAfterSnapshotForTests(nil)
            probe.cancelSecond()

            #expect(firstRecorder.events == [.failure(.sample)])
            #expect(secondRecorder.events == [.failure(.sample)])
            #expect(hub.getState() == .init(isRunning: false, subscribersCount: 0))
        }
    }

    @Test func terminalFailureFinishesJobAndClearsSubscribers() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            _ = hub.subscribe(firstRecorder.append(_:))
            _ = hub.subscribe(secondRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.failure(.sample))
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.failure(.sample)])
            #expect(secondRecorder.events == [.failure(.sample)])
            await hub.waitForStateForTests(.init(isRunning: false, subscribersCount: 0))
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func unsubscribeLastSubscriberStopsJob() async throws {
        try await Their.stress {
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            let cancel = hub.subscribe { _ in }
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 1))
            cancel()
            await hub.waitForStateForTests(.init(isRunning: false, subscribersCount: 0))
            try await startRecorder.waitForCancelCallsCount(1)
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func unsubscribeOneSubscriberKeepsJobRunningForAnotherSubscriber() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            let firstCancel = hub.subscribe(firstRecorder.append(_:))
            _ = hub.subscribe(secondRecorder.append(_:))
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 2))
            firstCancel()
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 1))
            #expect(startRecorder.cancelCallsCount == 0)
            startRecorder.report?(.value(42))
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events.isEmpty == true)
            #expect(secondRecorder.events == [.value(42)])
            #expect(hub.getState() == .init(isRunning: true, subscribersCount: 1))
        }
    }

    // MARK: - Existing tests

    @Test func upstreamOutputIsBroadcastToAllSubscribers() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            _ = hub.subscribe(firstRecorder.append(_:))
            _ = hub.subscribe(secondRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.value(10))
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)
            #expect(firstRecorder.events == [.value(10)])
            #expect(secondRecorder.events == [.value(10)])
            await hub.waitForStateForTests(.init(isRunning: true, subscribersCount: 2))
        }
    }

    @Test func upstreamValueSkipsSubscriberAddedAfterSnapshot() async throws {
        try await Their.stress {
            let firstRecorder = HubEngineEventRecorder()
            let probe = HubEngineLateSubscriberProbe()
            let secondRecorder = HubEngineEventRecorder()
            let startRecorder = HubEngineStartRecorder()
            let hub = HubEngine<Int, HubEngineTestsError>(
                work: startRecorder.work
            )
            hub.setAfterSnapshotForTests { [hub, probe, secondRecorder] in
                guard probe.shouldSubscribeOnce() else {
                    return
                }
                probe.store(cancel: hub.subscribe(secondRecorder.append(_:)))
            }

            _ = hub.subscribe(firstRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.report?(.value(10))
            try await firstRecorder.waitForEventCount(1)
            hub.setAfterSnapshotForTests(nil)

            #expect(firstRecorder.events == [.value(10)])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(hub.getState() == .init(isRunning: true, subscribersCount: 2))
            probe.cancelSecond()
        }
    }
}

private enum HubEngineTestsError: Swift.Error, Sendable {

    case sample
}

private typealias HubEngineEventRecorder = Their.TestEventRecorder<Their.HubEvent<Int, HubEngineTestsError>>
private typealias HubEngineStartRecorder = Their.TestWorkRecorder<Int, HubEngineTestsError>

private final class HubEngineHookCapture: Sendable {

    private let onDeinit: @Sendable () -> Void

    init(onDeinit: @escaping @Sendable () -> Void) {
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }
}

private final class HubEngineLateSubscriberProbe: Sendable {

    private let didSubscribe = Their.Lock(false)
    private let secondCancel = Their.Lock<Their.HubCancel?>(nil)

    func cancelSecond() {
        secondCancel.withLock { secondCancel in
            secondCancel?()
            secondCancel = nil
        }
    }

    func shouldSubscribeOnce() -> Bool {
        didSubscribe.withLock { didSubscribe in
            guard didSubscribe == false else {
                return false
            }
            didSubscribe = true
            return true
        }
    }

    func store(cancel: @escaping Their.HubCancel) {
        secondCancel.withLock { secondCancel in
            secondCancel = cancel
        }
    }
}

private func makeHubEngineHook(
    deinits: Their.TestCountRecorder,
    hub: HubEngine<Int, HubEngineTestsError>,
    observations: Their.TestEventRecorder<Bool>,
    reentries: Their.TestCountRecorder
) -> @Sendable () -> Void {
    let capture = HubEngineHookCapture { [weak hub] in
        _ = deinits.increment()
        let isAvailable = hub?.isLockAvailableForTests() ?? false
        observations.append(isAvailable)
        guard isAvailable, let hub else {
            return
        }
        _ = hub.getState()
        _ = reentries.increment()
    }
    return {
        withExtendedLifetime(capture) {}
    }
}
