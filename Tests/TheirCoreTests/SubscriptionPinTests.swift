import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct SubscriptionPinTests {

    @Test func concurrentReleaseDropsOwnerExactlyOnce() async throws {
        let deinits = Their.TestCountRecorder()
        let pin = SubscriptionPin(SubscriptionPinOwner {
            _ = deinits.increment()
        })

        try await Their.stress { _ in
            pin.release()
        }

        withExtendedLifetime(pin) {
            #expect(deinits.count == 1)
        }
    }

    @Test func droppingPinKeepsAnExternallyOwnedObjectAlive() async throws {
        try await Their.stress {
            let deinits = Their.TestCountRecorder()
            let owner = SubscriptionPinOwner {
                _ = deinits.increment()
            }
            var pin: SubscriptionPin<SubscriptionPinOwner>? = SubscriptionPin(owner)
            withExtendedLifetime(pin) {
                #expect(deinits.count == 0)
            }

            pin = nil

            withExtendedLifetime(owner) {
                #expect(deinits.count == 0)
            }
        }
    }

    @Test func droppingPinReleasesItsLastOwner() async throws {
        try await Their.stress {
            let deinits = Their.TestCountRecorder()
            var pin: SubscriptionPin<SubscriptionPinOwner>? = SubscriptionPin(SubscriptionPinOwner {
                _ = deinits.increment()
            })
            withExtendedLifetime(pin) {
                #expect(deinits.count == 0)
            }

            pin = nil

            #expect(deinits.count == 1)
        }
    }

    /// Observe the lock before reentry so a held-lock regression reports an
    /// assertion failure instead of trapping in a recursive unfair-lock call.
    @Test func releaseAllowsOwnerDestructionToReenterThePin() async throws {
        try await Their.stress {
            let observations = Their.TestEventRecorder<Bool>()
            let pinStore = Their.Lock<SubscriptionPin<SubscriptionPinOwner>?>(nil)
            let reentries = Their.TestCountRecorder()
            let pin = SubscriptionPin(SubscriptionPinOwner {
                guard let pin = pinStore.withLock({ $0 }) else {
                    return
                }
                let isAvailable = pin.isLockAvailableForTests()
                observations.append(isAvailable)
                guard isAvailable else {
                    return
                }
                pin.release()
                _ = reentries.increment()
            })
            pinStore.withLock { $0 = pin }
            defer { pinStore.withLock { $0 = nil } }

            pin.release()
            pin.release()

            #expect(observations.events == [true])
            #expect(reentries.count == 1)
        }
    }

    @Test func releaseDropsOwnerWhileThePinRemainsAlive() async throws {
        try await Their.stress {
            let deinits = Their.TestCountRecorder()
            var owner: SubscriptionPinOwner? = SubscriptionPinOwner {
                _ = deinits.increment()
            }
            let isOwnerAlive: @Sendable () -> Bool = { [weak owner] in
                owner != nil
            }
            let pin = SubscriptionPin(try #require(owner))
            owner = nil
            #expect(isOwnerAlive())

            pin.release()

            withExtendedLifetime(pin) {
                #expect(isOwnerAlive() == false)
                #expect(deinits.count == 1)
                pin.release()
                #expect(deinits.count == 1)
            }
        }
    }
}

private final class SubscriptionPinOwner: Sendable {

    private let onDeinit: @Sendable () -> Void

    init(onDeinit: @escaping @Sendable () -> Void) {
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }
}
