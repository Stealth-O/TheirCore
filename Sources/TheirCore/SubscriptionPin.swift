import Foundation

/// Shared ownership pin for the cancel closures returned by `Job` and `Hub`.
/// Retains the originating facade until explicit release or the pin's own
/// destruction. It does not issue a cancellation command on deinit: the facade
/// owns its lifecycle, and other references may legitimately keep it alive.
/// Explicit release detaches the owner under the lock and lets its destructor
/// run after unlock, including teardown that re-enters the same cancellation.
final class SubscriptionPin<Owner: AnyObject & Sendable>: Sendable {

    private let owner: Their.Lock<Owner?>

    init(_ owner: Owner) {
        self.owner = Their.Lock(owner)
    }

    #if DEBUG
    func isLockAvailableForTests() -> Bool {
        owner.withLockIfAvailable { _ in true } ?? false
    }
    #endif

    func release() {
        let releasedOwner = owner.withLock { owner in
            let releasedOwner = owner
            owner = nil
            return releasedOwner
        }
        withExtendedLifetime(releasedOwner) {}
    }
}
