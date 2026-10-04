import Foundation
import Testing
import TheirCore
import TheirCoreTesting

/// Uses the package the way a consumer does, without `@testable import`.
/// A declaration that lost `public` on its way into `Their` breaks this target
/// at compile time, and the scenarios mirror the examples in `README.md`.
@Suite
struct PublicAPITests {

    @Test func boxOwnsStateAndBothSourceKindsFromOutsideThePackage() async throws {
        try await Their.stress {
            let box = Their.Box<[Int], Int>([]) { state, event in state.append(event) }
            let job = Their.TestJobDriver<Int, LoadError>()
            let hub = Their.TestHubDriver<Int, LoadError>()
            box.send(1)
            box.bind(job.job, id: "save") { event in
                if case .value(let value) = event { return value }
                return nil
            }
            box.bind(hub.hub, id: "live") { event in
                if case .value(let value) = event { return value }
                return nil
            }
            job.emit(value: 2)
            job.emitFinished()
            hub.emit(value: 3)
            box.unbind("live")
            #expect(box.current == [1, 2, 3])
            let events = Their.TestEventRecorder<Their.HubEvent<[Int], Never>>()
            let cancel = box.changes.subscribe(events.append)
            #expect(events.events == [.value([1, 2, 3])])
            cancel()
        }
    }

    @Test func consumerTypesKeepTheirOwnNames() async throws {
        try await Their.stress {
            let local = Job(title: "mine")
            let events = Their.TestEventRecorder<Their.JobEvent<String, LoadError>>()
            let job = Their.Job<String, LoadError> { report in
                report(.value(local.title))
                report(.finished)
                return {}
            }
            let cancel = job.subscribe(events.append(_:))
            try await events.waitForEventCount(2)
            #expect(events.events == [.value("mine"), .finished])
            cancel()
        }
    }

    @Test func firstValueAndItsPoliciesArePublicConsumerAPI() async throws {
        try await Their.stress {
            let cancellation: Their.JobAwaitCancellation = .awaitResult
            let job = Their.Job<Int, LoadError> { report in
                report(.value(1)); report(.value(2)); report(.finished)
                return {}
            }
            let value = try await job.firstValue(cancellation: cancellation, where: { $0 == 2 })
            #expect(value == 2)
            let empty = Their.Job<Int, Never> { report in report(.finished); return {} }
            do { _ = try await empty.firstValue(); Issue.record("Expected missing value") }
            catch { #expect(error is Their.JobValueUnavailable) }
            _ = Their.JobValueUnavailable()
        }
    }

    @Test func hubSharesOneLifecycleAndReplaysTheLatestValue() async throws {
        try await Their.stress {
            let upstream = Their.TestHubDriver<Int, LoadError>()
            let prices = upstream.hub.shareLatest()
            let early = Their.TestEventRecorder<Their.HubEvent<Int, LoadError>>()
            let late = Their.TestEventRecorder<Their.HubEvent<Int, LoadError>>()
            let earlyCancel = prices.subscribe(early.append(_:))
            upstream.emit(value: 1)
            try await early.waitForEventCount(1)
            let lateCancel = prices.subscribe(late.append(_:))
            try await late.waitForEventCount(1)
            upstream.emit(failure: .offline)
            try await early.waitForEventCount(2)
            try await late.waitForEventCount(2)
            #expect(early.events == [.value(1), .failure(.offline)])
            #expect(late.events == [.value(1), .failure(.offline)])
            #expect(upstream.startCallsCount == 1)
            earlyCancel()
            lateCancel()
        }
    }

    @Test func jobEvolvesStateFromValues() async throws {
        try await Their.stress {
            let upstream = Their.TestJobDriver<Int, LoadError>()
            let totals = Their.TestEventRecorder<Their.JobEvent<Int, LoadError>>()
            let cancel = upstream.job
                .evolve(initial: 0) { total, value in
                    total += value
                    return total
                }
                .subscribe(totals.append(_:))
            upstream.emit(value: 1)
            upstream.emit(value: 2)
            upstream.emitFinished()
            try await totals.waitForEventCount(3)
            #expect(totals.events == [.value(1), .value(3), .finished])
            #expect(upstream.cancelCallsCount == 1)
            cancel()
        }
    }

    @Test func onceJobsMergeIntoOneStream() async throws {
        try await Their.stress {
            let first = Their.Job<Int, LoadError>.once(failure: { _ in .offline }) { 1 }
            let second = Their.Job<Int, LoadError>.once(failure: { _ in .offline }) { 2 }
            var values = [Int]()
            var finished = false
            for await event in Their.Job.merge(first, second).stream() {
                switch event {
                case .failure:
                    Issue.record("Unexpected failure.")
                case .finished:
                    finished = true
                case .value(let value):
                    values.append(value)
                }
            }
            #expect(values.sorted() == [1, 2])
            #expect(finished == true)
        }
    }

    @Test func synchronizationAndOwnershipHelpersWorkFromOutside() async throws {
        try await Their.stress {
            let counter = Their.Lock(0)
            counter.withLock { value in
                value += 1
            }
            #expect(counter.withLock { value in value } == 1)

            let released = Their.TestEventRecorder<String>()
            let registration = Their.Resource<String> { value in
                released.append(value)
            }
            registration.set("listener")
            registration.cancel()
            try await released.waitForEventCount(1)
            #expect(released.events == ["listener"])

            let configured = Their.TestCountRecorder()
            let configure = Their.MainThreadOnce(
                isMainThread: { true },
                runOnMain: { work in work() },
                work: { _ = configured.increment() }
            )
            configure.run()
            configure.run()
            #expect(configured.count == 1)

            let cache = Their.WeakValueCache<String, Their.Hub<Int, LoadError>>()
            let hub = cache.value(forKey: "prices") {
                Their.Hub { _ in {} }
            }
            let cached = cache.value(forKey: "prices") {
                Their.Hub { _ in {} }
            }
            #expect(cached === hub)
        }
    }

    @Test func testingKitReportsMisuseCancellationAndTimeouts() async throws {
        #expect(Their.stressCountDefault == 50)
        #expect(Their.stressTimeoutDefault == .milliseconds(299))
        let iterations = try await Their.stress(count: 4, timeout: .seconds(1)) { iteration in
            let misuses = Their.TestMisuseRecorder()
            let work = Their.TestWorkRecorder<Int, LoadError>()
            let job = Their.Job(
                logging: .common(label: "public-api", options: []),
                misuseHandler: misuses.handler,
                work: Their.serialized(work.work)
            )
            let cancel = job.subscribe { _ in }
            _ = job.subscribe { _ in }
            try await misuses.waitForCount(1)
            #expect(misuses.misuses.first?.message == "Job supports only one subscriber per lifecycle.")
            cancel()
            try await work.waitForCancelCallsCount(1)

            let cancels = Their.TestCancelRecorder()
            let signal = Their.TestSignal()
            let workCancel: Their.WorkCancel = cancels.cancel()
            let hubCancel: Their.HubCancel = {
                workCancel()
                signal.signal()
            }
            hubCancel()
            try await signal.wait()
            try await cancels.waitForCancelCallsCount(1)
            return iteration
        }
        #expect(iterations.sorted() == [0, 1, 2, 3])
        let timeout = Their.StressTimeoutError(iteration: 3, timeout: .milliseconds(1))
        #expect(timeout.description.contains("iteration 3"))
        let fatal: Their.MisuseHandler = Their.MisuseHandlers.fatal
        let misuse = Their.Misuse(message: "example", origin: Their.MisuseLocation(), trace: [])
        #expect(misuse.message == "example")
        _ = fatal
    }
}

/// A consumer's own `Job`: the `Their` namespace leaves the short name free.
private struct Job {

    let title: String
}

private enum LoadError: Equatable, Error {

    case offline
}
