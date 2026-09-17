import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct HubSubscriptionLifetimeTests {

    /// Guard the actual recursive cancel with a nonblocking observation so
    /// restoring the original held-lock release reports a failure, not a trap.
    @Test func cancellingLastPinAllowsUpstreamCancelToReenterPublicCancel() async throws {
        try await Their.stress(count: 1) {
            let cancelStore = Their.Lock<Their.HubCancel?>(nil)
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
            var hub: Their.Hub<Int, Never>? = Their.Hub(work: upstream.work)
            let isHubAlive: @Sendable () -> Bool = { [weak hub] in
                hub != nil
            }
            let cancel = try #require(HubSubscriptionTestHooks.$didCreate.withValue({ probe in
                probeStore.withLock { $0 = probe }
            }) {
                hub?.subscribe { _ in }
            })
            let probe = try #require(probeStore.withLock { $0 })
            cancelStore.withLock { $0 = cancel }
            defer { cancelStore.withLock { $0 = nil } }
            hub = nil

            #expect(isHubAlive())
            #expect(probe() == true)
            #expect(upstream.startCallsCount == 1)
            #expect(upstream.cancelCallsCount == 0)

            withExtendedLifetime(cancel) {
                cancel()
                #expect(isHubAlive() == false)
                #expect(observations.events == [true])
                #expect(probe() == true)
                #expect(reentries.count == 1)
                #expect(upstream.cancelCallsCount == 1)

                cancel()
                #expect(observations.count == 1)
                #expect(upstream.cancelCallsCount == 1)
            }
        }
    }
}
