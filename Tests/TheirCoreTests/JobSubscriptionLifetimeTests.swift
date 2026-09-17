import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

/// Observes the actual public subscribe/cancel path. A nonblocking probe gates
/// real recursive cancellation so a future regression fails an assertion
/// instead of trapping the runner. No timing or thread scheduling is required.
@Suite
struct JobSubscriptionLifetimeTests {

    @Test func cancellingLastPinInvokesUpstreamCancelOutsidePinLock() async throws {
        try await Their.stress(count: 1) {
            let cancelStore = Their.Lock<Their.WorkCancel?>(nil)
            let observations = Their.TestEventRecorder<Bool?>()
            let probeStore = Their.Lock<(@Sendable () -> Bool?)?>(nil)
            let reentries = Their.TestCountRecorder()
            let upstream = Their.TestWorkRecorder<Int, Never>(onCancel: {
                let probe = probeStore.withLock { $0 }
                let isAvailable = probe?()
                observations.append(isAvailable)
                guard isAvailable == true else {
                    return
                }
                let cancel = cancelStore.withLock { $0 }
                cancel?()
                _ = reentries.increment()
            })
            var job: Their.Job<Int, Never>? = Their.Job(work: upstream.work)
            let isJobAlive: @Sendable () -> Bool = { [weak job] in
                job != nil
            }
            let cancel = try #require(JobSubscriptionTestHooks.$didCreate.withValue({ probe in
                probeStore.withLock { $0 = probe }
            }) {
                job?.subscribe { _ in }
            })
            let probe = try #require(probeStore.withLock { $0 })
            cancelStore.withLock { $0 = cancel }
            defer { cancelStore.withLock { $0 = nil } }
            job = nil

            #expect(isJobAlive())
            #expect(probe() == true)
            #expect(upstream.startCallsCount == 1)
            #expect(upstream.cancelCallsCount == 0)

            withExtendedLifetime(cancel) {
                cancel()

                #expect(isJobAlive() == false)
                #expect(upstream.cancelCallsCount == 1)
                #expect(observations.events == [true])
                #expect(probe() == true)
                #expect(reentries.count == 1)

                cancel()
                #expect(upstream.cancelCallsCount == 1)
                #expect(observations.count == 1)
            }
        }
    }

    @Test func retainedJobAllowsUpstreamCancelToReenterPublicCancel() async throws {
        try await Their.stress(count: 1) {
            let cancelStore = Their.Lock<Their.WorkCancel?>(nil)
            let observations = Their.TestEventRecorder<Bool?>()
            let probeStore = Their.Lock<(@Sendable () -> Bool?)?>(nil)
            let upstream = Their.TestWorkRecorder<Int, Never>(onCancel: {
                let probe = probeStore.withLock { $0 }
                observations.append(probe?())
                let cancel = cancelStore.withLock { $0 }
                cancel?()
            })
            let job = Their.Job(work: upstream.work)
            let cancel = JobSubscriptionTestHooks.$didCreate.withValue({ probe in
                probeStore.withLock { $0 = probe }
            }) {
                job.subscribe { _ in }
            }
            let probe = try #require(probeStore.withLock { $0 })
            cancelStore.withLock { $0 = cancel }
            defer {
                cancelStore.withLock { $0 = nil }
            }

            withExtendedLifetime(job) {
                #expect(probe() == true)
                cancel()
                #expect(observations.events == [true])
                #expect(probe() == true)
                #expect(upstream.cancelCallsCount == 1)
                #expect(upstream.startCallsCount == 1)
            }
        }
    }
}
