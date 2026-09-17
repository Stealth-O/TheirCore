import func os.os_unfair_lock_lock
import func os.os_unfair_lock_trylock
import func os.os_unfair_lock_unlock
import struct os.os_unfair_lock
import struct os.os_unfair_lock_t

extension Their {

    /// `Their.Lock<Value>` is the canonical synchronization primitive: prefer it
    /// over `NSLock`, ad hoc mutable boxes or local lock wrappers, and store named
    /// state under it so the protected data model stays visible.
    ///
    /// A small mutex backed by `os_unfair_lock`. All access to the protected
    /// value goes through `withLock`/`withLockIfAvailable`, which acquire the
    /// lock, hand back an `inout` reference and release on return, so reads and
    /// writes are serialized. It supports noncopyable values
    /// (`Value: ~Copyable`). The API mirrors `Synchronization.Mutex`, so once
    /// iOS 18 / macOS 15 is the deployment floor the storage can move onto the
    /// standard library `Mutex` without touching call sites.
    public struct Lock<Value: ~Copyable>: ~Copyable {

        let storage: LockStorage<Value>

        public init(_ initialValue: consuming sending Value) {
            storage = LockStorage(initialValue)
        }

        /// Acquires the lock, passes the protected value to `body` as `inout`, and
        /// releases the lock when `body` returns or throws. Mirrors
        /// `Synchronization.Mutex.withLock`.
        public borrowing func withLock<Result, E: Error>(
            _ body: (inout sending Value) throws(E) -> sending Result
        ) throws(E) -> sending Result {
            storage.lock()
            defer {
                storage.unlock()
            }
            return try body(&storage.value)
        }

        /// Acquires the lock only if it is not already held, without blocking.
        /// Returns `nil` immediately — without running `body` — when the lock is
        /// held by this or another thread; otherwise runs `body` under the lock and
        /// releases it when `body` returns or throws. Mirrors
        /// `Synchronization.Mutex.withLockIfAvailable`.
        public borrowing func withLockIfAvailable<Result, E: Error>(
            _ body: (inout sending Value) throws(E) -> sending Result
        ) throws(E) -> sending Result? {
            guard storage.tryLock() else {
                return nil
            }
            defer {
                storage.unlock()
            }
            return try body(&storage.value)
        }
    }
}

// `@unchecked Sendable`: `LockStorage` wraps an `os_unfair_lock` and every
// access to `value` is serialized through `withLock`/`withLockIfAvailable`, an
// invariant the compiler cannot infer for `~Copyable` values. See the
// `@unchecked Sendable` registry in `Documentation/TheirCore.md`.
extension Their.Lock: @unchecked Sendable where Value: ~Copyable {}

final class LockStorage<Value: ~Copyable> {

    private let unfairLock: os_unfair_lock_t
    var value: Value

    init(_ initialValue: consuming Value) {
        unfairLock = .allocate(capacity: 1)
        unfairLock.initialize(to: os_unfair_lock())
        value = initialValue
    }

    deinit {
        unfairLock.deinitialize(count: 1)
        unfairLock.deallocate()
    }

    func lock() {
        os_unfair_lock_lock(unfairLock)
    }

    func tryLock() -> Bool {
        os_unfair_lock_trylock(unfairLock)
    }

    func unlock() {
        os_unfair_lock_unlock(unfairLock)
    }
}
