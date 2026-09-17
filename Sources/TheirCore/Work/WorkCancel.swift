import Foundation

extension Their {

    /// Cancels subscriber delivery immediately and stops the underlying lifecycle.
    ///
    /// Calling `cancel()` is fully synchronous: it clears the subscriber slot, transitions the engine state to
    /// `.terminated`, and invokes the upstream `WorkCancel` before returning.
    public typealias WorkCancel = @Sendable () -> Void
}
