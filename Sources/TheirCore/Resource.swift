import Foundation

extension Their {

    /// One-handle owner for an external registration, observer token or cancellable
    /// handle that must be released exactly once — use it inside `Work`/SDK boundary
    /// code instead of an ad hoc `Lock<Registration?>` or callback bag. It is not a
    /// lifecycle abstraction on its own; `Job`, `Hub` and `evolve` still own event
    /// delivery and state.
    ///
    /// Ownership: `set(_:)` stores the value once. If `set(_:)` runs after `cancel()`
    /// the incoming value is released immediately; if a value is already stored, the
    /// replacement is released immediately and the first value stays owned until
    /// cancellation. `cancel()` marks the owner cancelled and releases the stored
    /// value exactly once; `deinit` cancels. The release closure always runs outside
    /// the internal lock.
    public final class Resource<Value: Sendable>: Sendable {

        private let lock = Their.Lock(ResourceState<Value>())
        private let release: @Sendable (Value) -> Void

        public init(
            release: @escaping @Sendable (Value) -> Void
        ) {
            self.release = release
        }

        deinit {
            cancel()
        }

        public func cancel() {
            let value: Value? = lock.withLock { state in
                guard state.isCancelled == false else {
                    return nil
                }
                state.isCancelled = true
                let value = state.value
                state.value = nil
                return value
            }
            value.map(release)
        }

        public func set(_ value: Value) {
            let valueToRelease: Value? = lock.withLock { state in
                guard state.isCancelled == false, state.value == nil else {
                    return value
                }
                state.value = value
                return nil
            }
            valueToRelease.map(release)
        }
    }
}

private struct ResourceState<Value: Sendable>: Sendable {

    var isCancelled = false
    var value: Value?
}
