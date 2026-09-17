import Foundation
import Testing
import TheirCore
import TheirCoreTesting

@Suite
struct HubJobTests {

    @Test func deinitOfDerivedJobCancelsHubSubscription() async throws {
        try await Their.stress {
            let work = Their.TestWorkRecorder<Int, HubJobTestsError>()
            let hub = Their.Hub(work: work.work)
            var job: Their.Job<Int, HubJobTestsError>? = hub.job()
            let recorder = HubJobEventRecorder()

            // Subscribe and drop the cancel handle so deinit is the only
            // thing that can stop the hub subscription.
            _ = job?.subscribe(recorder.append(_:))
            try await work.waitForStartCallsCount(1)
            job = nil

            try await work.waitForCancelCallsCount(1)
            #expect(work.cancelCallsCount == 1)
            #expect(recorder.events.isEmpty == true)
        }
    }

    @Test func jobCancelsHubSubscriptionWhenCanceled() async throws {
        try await Their.stress {
            let driver = Their.TestHubDriver<Int, HubJobTestsError>()
            let job = driver.hub.job()
            let recorder = HubJobEventRecorder()

            let cancel = job.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            cancel()
            try await driver.waitForCancelCallsCount(1)

            #expect(driver.cancelCallsCount == 1)
        }
    }

    @Test func jobForwardsValuesAndEndFromHub() async throws {
        try await Their.stress {
            let driver = Their.TestHubDriver<Int, HubJobTestsError>()
            let job = driver.hub.job()
            let recorder = HubJobEventRecorder()

            let cancel = job.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: 10)
            try await recorder.waitForEventCount(1)
            driver.emitFinished()
            try await recorder.waitForEventCount(2)

            #expect(recorder.events == [
                .value(10),
                .finished
            ])
            cancel()
        }
    }

    @Test func jobForwardsValuesAndFailureFromHub() async throws {
        try await Their.stress {
            let driver = Their.TestHubDriver<Int, HubJobTestsError>()
            let job = driver.hub.job()
            let recorder = HubJobEventRecorder()

            let cancel = job.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: 10)
            try await recorder.waitForEventCount(1)
            driver.emit(failure: .sample)
            try await recorder.waitForEventCount(2)

            #expect(recorder.events == [
                .value(10),
                .failure(.sample)
            ])
            cancel()
        }
    }

    @Test func jobRejectsResubscribeAfterCancelWithoutRejoiningHub() async throws {
        // `cancelSubscription` marks the bridge terminal, so the returned `Job`
        // keeps the one-lifecycle contract: a later subscribe reports misuse
        // through the hub's handler and never rejoins the hub.
        try await Their.stress {
            let firstRecorder = HubJobEventRecorder()
            let misuseRecorder = Their.TestMisuseRecorder()
            let secondRecorder = HubJobEventRecorder()
            let driver = Their.TestHubDriver<Int, HubJobTestsError>(
                misuseHandler: misuseRecorder.handler
            )
            let job = driver.hub.job()

            let firstCancel = job.subscribe(firstRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            firstCancel()
            try await driver.waitForCancelCallsCount(1)
            let secondCancel = job.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            secondCancel()

            #expect(firstRecorder.events.isEmpty == true)
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Job supports only one subscriber per lifecycle."
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func jobRejectsResubscribeAfterTerminalEndWithoutRejoiningHub() async throws {
        try await Their.stress {
            let firstRecorder = HubJobEventRecorder()
            let misuseRecorder = Their.TestMisuseRecorder()
            let secondRecorder = HubJobEventRecorder()
            let driver = Their.TestHubDriver<Int, HubJobTestsError>(
                misuseHandler: misuseRecorder.handler
            )
            let job = driver.hub.job()

            _ = job.subscribe(firstRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emitFinished()
            try await firstRecorder.waitForEventCount(1)
            let secondCancel = job.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            secondCancel()

            #expect(firstRecorder.events == [.finished])
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Job supports only one subscriber per lifecycle."
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func jobRejectsResubscribeAfterTerminalFailureWithoutRejoiningHub() async throws {
        try await Their.stress {
            let firstRecorder = HubJobEventRecorder()
            let misuseRecorder = Their.TestMisuseRecorder()
            let secondRecorder = HubJobEventRecorder()
            let driver = Their.TestHubDriver<Int, HubJobTestsError>(
                misuseHandler: misuseRecorder.handler
            )
            let job = driver.hub.job()

            _ = job.subscribe(firstRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(failure: .sample)
            try await firstRecorder.waitForEventCount(1)
            let secondCancel = job.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)
            secondCancel()

            #expect(firstRecorder.events == [.failure(.sample)])
            #expect(misuseRecorder.misuses.map(\.message) == [
                "Job supports only one subscriber per lifecycle."
            ])
            #expect(secondRecorder.events.isEmpty == true)
            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
        }
    }

    @Test func jobRetainsHubWhileJobIsAlive() async throws {
        try await Their.stress {
            let work = Their.TestWorkRecorder<Int, HubJobTestsError>()
            var hub: Their.Hub<Int, HubJobTestsError>? = Their.Hub(work: work.work)
            weak var weakHub = hub
            let job = hub?.job()
            hub = nil

            #expect(weakHub != nil)
            let recorder = HubJobEventRecorder()
            let cancel = job?.subscribe(recorder.append(_:))
            try await work.waitForStartCallsCount(1)
            work.emit(.value(20))
            try await recorder.waitForEventCount(1)

            #expect(recorder.events == [.value(20)])
            #expect(weakHub != nil)
            cancel?()
            try await work.waitForCancelCallsCount(1)
        }
    }

    @Test func lateValueAfterCancelIsSuppressed() async throws {
        // Verifies the report() suppression branch in HubJob.swift. The
        // direct hub subscriber acts as a sentinel: when it receives the
        // late value we know the emit was fully processed by the hub, so
        // asserting absence on the canceled job-recorder becomes
        // deterministic instead of polling.
        try await Their.stress {
            let driver = Their.TestHubDriver<Int, HubJobTestsError>()
            let job = driver.hub.job()
            let jobRecorder = HubJobEventRecorder()
            let sentinelRecorder = Their.TestEventRecorder<Their.HubEvent<Int, HubJobTestsError>>()

            let jobCancel = job.subscribe(jobRecorder.append(_:))
            _ = driver.hub.subscribe(sentinelRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)

            jobCancel()
            driver.emit(value: 99)
            try await sentinelRecorder.waitForEventCount(1)

            #expect(sentinelRecorder.events == [.value(99)])
            #expect(jobRecorder.events.isEmpty == true)
        }
    }

    @Test func lateValueAfterTerminalEndIsSuppressed() async throws {
        // After .finished, the job bridge sets isTerminal and any further hub
        // broadcast must not reach this sink. Mirrors the terminal-failure
        // variant below: a fresh hub subscriber added AFTER the end acts as
        // the sentinel — the hub restarts for it, and once it receives the
        // new value the late emit is proven processed.
        try await Their.stress {
            let driver = Their.TestHubDriver<Int, HubJobTestsError>()
            let job = driver.hub.job()
            let jobRecorder = HubJobEventRecorder()

            _ = job.subscribe(jobRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emitFinished()
            try await jobRecorder.waitForEventCount(1)

            let sentinelRecorder = Their.TestEventRecorder<Their.HubEvent<Int, HubJobTestsError>>()
            _ = driver.hub.subscribe(sentinelRecorder.append(_:))
            try await driver.waitForStartCallsCount(2)
            driver.emit(value: 42)
            try await sentinelRecorder.waitForEventCount(1)

            #expect(sentinelRecorder.events == [.value(42)])
            #expect(jobRecorder.events == [.finished])
        }
    }

    @Test func lateValueAfterTerminalFailureIsSuppressed() async throws {
        // After .failure, the job bridge sets isTerminal and any further hub
        // broadcast must not reach this sink. Use a fresh hub subscriber
        // added AFTER the failure as a sentinel — once the hub restarts
        // (terminal failure clears subscribers and the next subscribe
        // starts a new lifecycle) it will deliver the new value, proving
        // the emit was processed by the time we assert.
        try await Their.stress {
            let driver = Their.TestHubDriver<Int, HubJobTestsError>()
            let job = driver.hub.job()
            let jobRecorder = HubJobEventRecorder()

            _ = job.subscribe(jobRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(failure: .sample)
            try await jobRecorder.waitForEventCount(1)

            // After the terminal failure the hub has cleared subscribers
            // and the underlying JobEngine is gone. A new subscriber will
            // start a fresh upstream (startCallsCount goes to 2). Use that
            // as the sentinel for "the new emit has been processed".
            let sentinelRecorder = Their.TestEventRecorder<Their.HubEvent<Int, HubJobTestsError>>()
            _ = driver.hub.subscribe(sentinelRecorder.append(_:))
            try await driver.waitForStartCallsCount(2)
            driver.emit(value: 42)
            try await sentinelRecorder.waitForEventCount(1)

            #expect(sentinelRecorder.events == [.value(42)])
            #expect(jobRecorder.events == [.failure(.sample)])
        }
    }

    @Test func retainedCancelKeepsHubJobAliveUntilCanceled() async throws {
        try await Their.stress {
            let work = Their.TestWorkRecorder<Int, HubJobTestsError>()
            let hub = Their.Hub(work: work.work)
            var job: Their.Job<Int, HubJobTestsError>? = hub.job()
            weak var weakJob = job
            let recorder = HubJobEventRecorder()

            let cancel = job?.subscribe(recorder.append(_:))
            try await work.waitForStartCallsCount(1)
            job = nil
            work.emit(.value(30))
            try await recorder.waitForEventCount(1)

            #expect(recorder.events == [.value(30)])
            #expect(weakJob != nil)
            #expect(work.cancelCallsCount == 0)
            cancel?()
            try await work.waitForCancelCallsCount(1)
            #expect(weakJob == nil)
        }
    }

    @Test func secondSubscribeOnSameHubJobReceivesMisuseAndDoesNotResubscribe() async throws {
        // Registries commonly expose a shared hub through `hub.job()`; the misuse
        // handler defaults to `MisuseHandlers.fatal`, so a second subscriber
        // crashes the process in production. This test guards that path by
        // injecting a recorder as the hub's misuse handler.
        //
        // Note: each `hub.job()` call returns a fresh single-subscriber
        // facade, so this test re-subscribes to the same returned `Job`
        // value (which is the production misuse shape).
        try await Their.stress {
            let misuseRecorder = Their.TestMisuseRecorder()
            let driver = Their.TestHubDriver<Int, HubJobTestsError>(
                misuseHandler: misuseRecorder.handler
            )
            let job = driver.hub.job()
            let firstRecorder = HubJobEventRecorder()
            let secondRecorder = HubJobEventRecorder()

            let firstCancel = job.subscribe(firstRecorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            let secondCancel = job.subscribe(secondRecorder.append(_:))
            try await misuseRecorder.waitForCount(1)

            #expect(misuseRecorder.misuses.count == 1)
            #expect(
                misuseRecorder.misuses.first?.message
                    == "Job supports only one subscriber per lifecycle."
            )
            #expect(secondRecorder.events.isEmpty == true)
            // First subscription is still wired up; the hub started once.
            #expect(driver.startCallsCount == 1)

            firstCancel()
            secondCancel()
            try await driver.waitForCancelCallsCount(1)
        }
    }
}

private enum HubJobTestsError: Swift.Error, Sendable {

    case sample
}

private typealias HubJobEventRecorder = Their.TestEventRecorder<Their.JobEvent<Int, HubJobTestsError>>
