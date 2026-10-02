import Foundation

extension Their {

    /// Canonical synchronized weak-value cache for module-owned registries that need
    /// one read-or-create operation. Values are held weakly, so the cache does not
    /// retain cached objects; it is a small primitive, not a registry policy — key
    /// choice and object meaning stay with the owning module.
    ///
    /// `value(forKey:orInsert:)` is the primary operation: under one lock it returns
    /// the live value for the key, or creates, stores and returns a new one.
    /// `orInsert` runs synchronously while the lock is held, so it must be fast and
    /// must not call back into the same cache (the underlying `Lock` is not
    /// reentrant). Dead entries are pruned lazily — only the insert path (a miss)
    /// walks the dictionary, read hits are O(1) — so the cache cannot grow
    /// unboundedly while keys are touched. Copies of the cache share one backing
    /// storage. Pruned keys and temporary strong reads of weak values are released
    /// after unlocking, so their destructors may safely look up another cache value.
    ///
    /// When `Value` is a `Hub`, `job(forKey:orInsert:)` reads or inserts the hub and
    /// then exposes it as a fresh one-subscriber `Job` via `Hub.job()`; it adds no
    /// replay policy, so return `Their.Hub(...).shareLatest()` from `orInsert` when
    /// late-subscriber replay is needed.
    public struct WeakValueCache<Key: Hashable & Sendable, Value: AnyObject & Sendable>: Sendable {

        private let storage: WeakValueCacheStorage<Key, Value>

        public init() {
            storage = WeakValueCacheStorage()
        }

        /// Returns a fresh `Job` facade for the live cached hub at `key`, or for a newly inserted hub.
        ///
        /// The hub is read or inserted under the cache lock, then `Hub.job()` is called after that synchronized cache
        /// operation has completed. Use `shareLatest()` explicitly inside `makeHub` when late subscribers should replay
        /// the most recent value; this helper does not add replay policy on its own.
        public func job<HubValue: Sendable, HubFailure: Swift.Error & Sendable>(
            forKey key: Key,
            fileID: String = #fileID,
            function: String = #function,
            line: UInt = #line,
            orInsert makeHub: () -> Their.Hub<HubValue, HubFailure>
        ) -> Their.Job<HubValue, HubFailure> where Value == Their.Hub<HubValue, HubFailure> {
            let hub = value(
                forKey: key,
                orInsert: makeHub
            )
            return hub.job(
                fileID: fileID,
                function: function,
                line: line
            )
        }

        #if DEBUG
        /// Non-owning probe so a cached key's destructor can observe the lock
        /// without retaining the storage or attempting a recursive acquisition.
        func lockAvailabilityProbeForTests() -> @Sendable () -> Bool {
            { [weak storage] in
                storage?.isLockAvailableForTests() ?? false
            }
        }
        #endif

        /// Returns the live cached value for `key`, or stores and returns the value produced by `makeValue` if no live entry exists.
        ///
        /// `makeValue` runs synchronously while the cache lock is held, so it must be fast and must not call back into the same cache (the underlying lock is not reentrant).
        public func value(
            forKey key: Key,
            orInsert makeValue: () -> Value
        ) -> Value {
            storage.value(
                forKey: key,
                orInsert: makeValue
            )
        }
    }
}

private final class WeakValueCacheStorage<Key: Hashable & Sendable, Value: AnyObject & Sendable>: Sendable {

    private let state = Their.Lock(WeakValueCacheState<Key, Value>())

    #if DEBUG
    func isLockAvailableForTests() -> Bool {
        state.withLockIfAvailable { _ in true } ?? false
    }
    #endif

    func value(
        forKey key: Key,
        orInsert makeValue: () -> Value
    ) -> Value {
        let result = state.withLock { state -> (
            value: Value,
            retiredEntries: [Key: WeakValueCacheBox<Value>],
            retainedValues: [Value]
        ) in
            if let value = state.values[key]?.value {
                return (value, [:], [])
            }
            // Cache miss: prune dead entries before inserting so the cache does not
            // accumulate dead keys over time. Read hits skip the walk entirely.
            let cleanup = state.prune()
            let value = makeValue()
            state.values[key] = WeakValueCacheBox(value)
            return (value, cleanup.retiredEntries, cleanup.retainedValues)
        }
        // A retired key or the last temporary owner of a live value may reenter
        // this cache from deinit. Keep both alive until the lock is released.
        return withExtendedLifetime(result) {
            result.value
        }
    }
}

private struct WeakValueCacheState<Key: Hashable & Sendable, Value: AnyObject & Sendable> {

    var values: [Key: WeakValueCacheBox<Value>] = [:]

    mutating func prune() -> (
        retiredEntries: [Key: WeakValueCacheBox<Value>],
        retainedValues: [Value]
    ) {
        let retiredEntries = values
        var retainedValues: [Value] = []
        values = values.filter { _, box in
            guard let value = box.value else {
                return false
            }
            retainedValues.append(value)
            return true
        }
        return (retiredEntries, retainedValues)
    }
}

/// Weak indirection box. Declared `@unchecked Sendable` because the weak
/// reference is only ever read or written while the enclosing
/// `WeakValueCacheStorage.state` lock is held — the compiler can't see that
/// invariant, so we assert it here.
private final class WeakValueCacheBox<Object: AnyObject>: @unchecked Sendable {

    weak var value: Object?

    init(_ value: Object? = nil) {
        self.value = value
    }
}
