import Foundation
import Testing
import TheirCore
import TheirCoreTesting

/// Contract tests for the public `Hub` facade. `HubEngineTests` covers the
/// internal engine; this suite covers the facade-plus-pin-plus-cancel-closure
/// surface that production callers actually touch.
@Suite
struct HubTests {

    @Test func cancelOfLastSubscriberStopsUpstream() async throws {
        try await Their.stress {
            let cancelSignal = HubTestSignal()
            let startRecorder = HubStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let hub = Their.Hub(work: startRecorder.work)

            let cancel = hub.subscribe { _ in }
            try await startRecorder.waitForStartCallsCount(1)
            cancel()
            try await cancelSignal.wait()

            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func cancelOfOneSubscriberKeepsUpstreamRunningForOthers() async throws {
        try await Their.stress {
            let firstRecorder = HubEventRecorder()
            let secondRecorder = HubEventRecorder()
            let startRecorder = HubStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)

            let firstCancel = hub.subscribe(firstRecorder.append(_:))
            let secondCancel = hub.subscribe(secondRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            firstCancel()
            // The second subscriber receiving an event proves the upstream
            // is still running after one subscriber cancelled. No polling.
            startRecorder.emit(.value(42))
            try await secondRecorder.waitForEventCount(1)

            #expect(firstRecorder.events.isEmpty == true)
            #expect(secondRecorder.events == [.value(42)])
            #expect(startRecorder.cancelCallsCount == 0)
            #expect(startRecorder.startCallsCount == 1)
            secondCancel()
        }
    }

    @Test func deinitOfActiveHubCancelsUpstream() async throws {
        try await Their.stress {
            let cancelSignal = HubTestSignal()
            let startRecorder = HubStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            var hub: Their.Hub<Int, HubTestsError>? = Their.Hub(
                work: startRecorder.work
            )
            // Subscribe so the upstream is running; drop the cancel handle
            // so deinit is the only path that can stop the hub.
            _ = hub?.subscribe { _ in }
            try await startRecorder.waitForStartCallsCount(1)
            hub = nil
            try await cancelSignal.wait()

            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func lateSubscriberDoesNotReceivePastValues() async throws {
        try await Their.stress {
            let firstRecorder = HubEventRecorder()
            let secondRecorder = HubEventRecorder()
            let startRecorder = HubStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)

            let firstCancel = hub.subscribe(firstRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(1))
            try await firstRecorder.waitForEventCount(1)
            let secondCancel = hub.subscribe(secondRecorder.append(_:))
            startRecorder.emit(.value(2))
            try await firstRecorder.waitForEventCount(2)
            try await secondRecorder.waitForEventCount(1)

            #expect(firstRecorder.events == [.value(1), .value(2)])
            #expect(secondRecorder.events == [.value(2)])
            #expect(startRecorder.startCallsCount == 1)
            firstCancel()
            secondCancel()
        }
    }

    @Test func repeatedCancelOnSameSubscriberInvokesUpstreamCancelOnce() async throws {
        try await Their.stress {
            let cancelSignal = HubTestSignal()
            let startRecorder = HubStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            let hub = Their.Hub(work: startRecorder.work)

            let cancel = hub.subscribe { _ in }
            try await startRecorder.waitForStartCallsCount(1)
            cancel()
            cancel()
            try await cancelSignal.wait()

            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func retainedCancelKeepsHubAliveUntilCanceled() async throws {
        try await Their.stress {
            let cancelSignal = HubTestSignal()
            let eventRecorder = HubEventRecorder()
            let startRecorder = HubStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                }
            )
            var hub: Their.Hub<Int, HubTestsError>? = Their.Hub(
                work: startRecorder.work
            )
            weak var weakHub = hub
            let retainedCancel = hub?.subscribe(eventRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            hub = nil
            startRecorder.emit(.value(10))
            try await eventRecorder.waitForEventCount(1)
            #expect(eventRecorder.events == [.value(10)])
            #expect(startRecorder.cancelCallsCount == 0)
            #expect(startRecorder.startCallsCount == 1)
            #expect(weakHub != nil)

            retainedCancel?()
            try await cancelSignal.wait()
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(weakHub == nil)
        }
    }

    /// Regression test for the `HubEngine.subscribe` deadlock that existed when `JobEngine.start()` was called
    /// inside the `HubEngine` state `Lock`. A `work` closure that synchronously reports a terminal failure used
    /// to hit `makeSink` → `snapshotSubscribers()`, which tried to re-acquire the same non-reentrant `Lock` and
    /// deadlocked. The fix moves `job.start()` to a Phase 2 outside the `HubEngine` lock; this test pins that
    /// contract by subscribing to a hub whose work emits `.failure` synchronously and asserting the subscriber
    /// receives it. Before the fix, this test would deadlock and trip the `Their.stress` per-iteration watchdog.
    @Test func subscribeDeliversSyncReportedFailureFromWork() async throws {
        try await Their.stress {
            let recorder = HubEventRecorder()
            let hub: Their.Hub<Int, HubTestsError> = Their.Hub { report in
                report(.failure(.sample))
                return {}
            }
            let cancel = hub.subscribe(recorder.append(_:))
            try await recorder.waitForEventCount(1)
            #expect(recorder.events == [.failure(.sample)])
            cancel()
        }
    }

    @Test func subscribeYieldsRapidUpstreamReportsInOrder() async throws {
        // Regression test for the sync `HubEngine.makeSink` path: rapid
        // serial upstream reports should broadcast in the same order the
        // inner `JobEngine` sees them.
        try await Their.stress {
            let recorder = HubEventRecorder()
            let startRecorder = HubStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)

            let cancel = hub.subscribe(recorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            for value in 0 ..< Their.stressCountDefault {
                startRecorder.emit(.value(value))
            }
            try await recorder.waitForEventCount(Their.stressCountDefault)
            cancel()

            #expect(recorder.events == (0 ..< Their.stressCountDefault).map { .value($0) })
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func syncReportedFailureAllowsReentrantSubscriberToStartFreshLifecycle() async throws {
        try await Their.stress {
            let firstRecorder = HubEventRecorder()
            let secondCancel = Their.Lock<Their.HubCancel?>(nil)
            let secondRecorder = HubEventRecorder()
            let startRecorder = Their.TestCountRecorder()
            let hub: Their.Hub<Int, HubTestsError> = Their.Hub { report in
                let startCount = startRecorder.increment()
                if startCount == 1 {
                    report(.failure(.sample))
                } else {
                    report(.value(42))
                }
                return {}
            }
            let firstCancel = hub.subscribe { event in
                firstRecorder.append(event)
                switch event {
                case .failure:
                    let cancel = hub.subscribe(secondRecorder.append(_:))
                    secondCancel.withLock { secondCancel in
                        secondCancel = cancel
                    }
                case .finished, .value:
                    break
                }
            }
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)
            try await startRecorder.waitForCount(2)
            #expect(firstRecorder.events == [.failure(.sample)])
            #expect(secondRecorder.events == [.value(42)])
            firstCancel()
            secondCancel.withLock { secondCancel in
                secondCancel?()
                secondCancel = nil
            }
        }
    }

    @Test func terminalEndClearsAllSubscribersAndStopsUpstream() async throws {
        try await Their.stress {
            let firstRecorder = HubEventRecorder()
            let secondRecorder = HubEventRecorder()
            let startRecorder = HubStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)

            _ = hub.subscribe(firstRecorder.append(_:))
            _ = hub.subscribe(secondRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.finished)
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)

            #expect(firstRecorder.events == [.finished])
            #expect(secondRecorder.events == [.finished])
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func terminalFailureClearsAllSubscribersAndStopsUpstream() async throws {
        try await Their.stress {
            let firstRecorder = HubEventRecorder()
            let secondRecorder = HubEventRecorder()
            let startRecorder = HubStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)

            _ = hub.subscribe(firstRecorder.append(_:))
            _ = hub.subscribe(secondRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.failure(.sample))
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)

            #expect(firstRecorder.events == [.failure(.sample)])
            #expect(secondRecorder.events == [.failure(.sample)])
            #expect(startRecorder.cancelCallsCount == 1)
            #expect(startRecorder.startCallsCount == 1)
        }
    }

    @Test func upstreamValueIsBroadcastToAllSubscribers() async throws {
        try await Their.stress {
            let firstRecorder = HubEventRecorder()
            let secondRecorder = HubEventRecorder()
            let startRecorder = HubStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)

            let firstCancel = hub.subscribe(firstRecorder.append(_:))
            let secondCancel = hub.subscribe(secondRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(10))
            try await firstRecorder.waitForEventCount(1)
            try await secondRecorder.waitForEventCount(1)

            #expect(firstRecorder.events == [.value(10)])
            #expect(secondRecorder.events == [.value(10)])
            #expect(startRecorder.startCallsCount == 1)
            firstCancel()
            secondCancel()
        }
    }
}

private enum HubTestsError: Swift.Error, Sendable {

    case sample
}

private typealias HubEventRecorder = Their.TestEventRecorder<Their.HubEvent<Int, HubTestsError>>
private typealias HubStartRecorder = Their.TestWorkRecorder<Int, HubTestsError>
private typealias HubTestSignal = Their.TestSignal
