import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct WeakValueCacheTests {

    @Test func copiedCacheSharesStorage() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<String, WeakValueCacheTestObject>()
            let copy = cache
            let firstObject = cache.value(forKey: "value") {
                WeakValueCacheTestObject()
            }
            let secondObject = copy.value(forKey: "value") {
                WeakValueCacheTestObject()
            }

            #expect(firstObject === secondObject)
        }
    }

    @Test func copyOfCopySharesStorage() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<String, WeakValueCacheTestObject>()
            let firstCopy = cache
            let secondCopy = firstCopy
            let original = cache.value(forKey: "value") {
                WeakValueCacheTestObject()
            }
            let viaFirstCopy = firstCopy.value(forKey: "value") {
                Issue.record("Expected first copy to reuse cached value.")
                return WeakValueCacheTestObject()
            }
            let viaSecondCopy = secondCopy.value(forKey: "value") {
                Issue.record("Expected second copy to reuse cached value.")
                return WeakValueCacheTestObject()
            }

            #expect(original === viaFirstCopy)
            #expect(original === viaSecondCopy)
        }
    }

    @Test func differentKeysStoreIndependentObjects() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<String, WeakValueCacheTestObject>()
            let first = cache.value(forKey: "first") {
                WeakValueCacheTestObject()
            }
            let second = cache.value(forKey: "second") {
                WeakValueCacheTestObject()
            }
            let firstAgain = cache.value(forKey: "first") {
                Issue.record("Expected first key to reuse cached value.")
                return WeakValueCacheTestObject()
            }
            let secondAgain = cache.value(forKey: "second") {
                Issue.record("Expected second key to reuse cached value.")
                return WeakValueCacheTestObject()
            }

            #expect(first !== second)
            #expect(first === firstAgain)
            #expect(second === secondAgain)
        }
    }

    @Test func jobForKeyRecreatesHubAfterCachedHubDeallocates() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<String, Their.Hub<Int, WeakValueCacheTestsError>>()
            let firstWork = Their.TestWorkRecorder<Int, WeakValueCacheTestsError>()
            let secondWork = Their.TestWorkRecorder<Int, WeakValueCacheTestsError>()
            var createCallsCount = 0
            var firstHub: Their.Hub<Int, WeakValueCacheTestsError>? = nil
            var job: Their.Job<Int, WeakValueCacheTestsError>? = cache.job(forKey: "value") {
                createCallsCount += 1
                let hub = Their.Hub(work: firstWork.work)
                firstHub = hub
                return hub
            }
            weak var weakFirstHub = firstHub

            firstHub = nil

            #expect(weakFirstHub != nil)
            job = nil
            #expect(weakFirstHub == nil)

            let secondJob = cache.job(forKey: "value") {
                createCallsCount += 1
                return Their.Hub(work: secondWork.work)
            }
            let recorder = Their.TestEventRecorder<Their.JobEvent<Int, WeakValueCacheTestsError>>()

            let cancel = secondJob.subscribe(recorder.append(_:))
            try await secondWork.waitForStartCallsCount(1)

            #expect(createCallsCount == 2)
            #expect(firstWork.startCallsCount == 0)

            cancel()
            try await secondWork.waitForCancelCallsCount(1)
        }
    }

    @Test func jobForKeyReturnsFreshJobFacadeForCachedHub() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<String, Their.Hub<Int, WeakValueCacheTestsError>>()
            let work = Their.TestWorkRecorder<Int, WeakValueCacheTestsError>()
            var createCallsCount = 0
            let firstJob = cache.job(forKey: "value") {
                createCallsCount += 1
                return Their.Hub(work: work.work).shareLatest()
            }
            let firstRecorder = Their.TestEventRecorder<Their.JobEvent<Int, WeakValueCacheTestsError>>()

            let firstCancel = firstJob.subscribe(firstRecorder.append(_:))
            try await work.waitForStartCallsCount(1)
            work.emit(.value(10))
            try await firstRecorder.waitForEventCount(1)

            let secondJob = cache.job(forKey: "value") {
                Issue.record("Expected cached hub to be reused.")
                return Their.Hub(work: work.work)
            }
            let secondRecorder = Their.TestEventRecorder<Their.JobEvent<Int, WeakValueCacheTestsError>>()

            let secondCancel = secondJob.subscribe(secondRecorder.append(_:))
            try await secondRecorder.waitForEventCount(1)

            #expect(createCallsCount == 1)
            #expect(firstRecorder.events == [.value(10)])
            #expect(secondRecorder.events == [.value(10)])
            #expect(work.startCallsCount == 1)

            firstCancel()
            secondCancel()
            try await work.waitForCancelCallsCount(1)
        }
    }

    /// A stale key's destructor performs a real lookup of the entry whose miss
    /// triggered pruning. The nonblocking probe prevents a regressed held lock
    /// from trapping the runner before its failed assertion can be reported.
    @Test func pruningAllowsDeadEntryKeyDestructorToReadInsertedValue() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<WeakValueCacheTestKey, WeakValueCacheTestObject>()
            let deinitializations = Their.TestCountRecorder()
            let factoryCalls = Their.TestCountRecorder()
            let isCacheLockAvailable = cache.lockAvailabilityProbeForTests()
            let lockObservations = Their.TestEventRecorder<Bool>()
            let lookupReturns = Their.TestCountRecorder()
            let lookedUpValues = Their.TestEventRecorder<WeakValueCacheTestObject>()
            let newKey = WeakValueCacheTestKey(id: 2)
            let insertDeadEntry: @Sendable () -> Void = {
                let key = WeakValueCacheTestKey(id: 1) {
                    _ = deinitializations.increment()
                    let isAvailable = isCacheLockAvailable()
                    lockObservations.append(isAvailable)
                    guard isAvailable else {
                        return
                    }
                    let value = cache.value(forKey: newKey) {
                        _ = factoryCalls.increment()
                        return WeakValueCacheTestObject()
                    }
                    lookedUpValues.append(value)
                    _ = lookupReturns.increment()
                }
                let value = cache.value(forKey: key) {
                    WeakValueCacheTestObject()
                }
                withExtendedLifetime(value) {}
            }

            insertDeadEntry()
            #expect(deinitializations.count == 0)

            let insertedValue = cache.value(forKey: newKey) {
                _ = factoryCalls.increment()
                return WeakValueCacheTestObject()
            }

            #expect(deinitializations.count == 1)
            #expect(lockObservations.events == [true])
            #expect(lookupReturns.count == 1)
            #expect(factoryCalls.count == 1)
            #expect(lookedUpValues.events.count == 1)
            #expect(lookedUpValues.events.first === insertedValue)
            #expect(isCacheLockAvailable())
        }
    }

    @Test func pruningReleasesDeadEntryKeyOutsideCacheLock() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<WeakValueCacheTestKey, WeakValueCacheTestObject>()
            let deinitializations = Their.TestCountRecorder()
            let isCacheLockAvailable = cache.lockAvailabilityProbeForTests()
            let lockObservations = Their.TestEventRecorder<Bool>()
            let insertDeadEntry: @Sendable () -> Void = {
                let key = WeakValueCacheTestKey(id: 1) {
                    lockObservations.append(isCacheLockAvailable())
                    _ = deinitializations.increment()
                }
                let value = cache.value(forKey: key) {
                    WeakValueCacheTestObject()
                }
                withExtendedLifetime(value) {}
            }

            // The helper returns with no external owner of its key or value.
            // Only the cache owns the stale key; its observer holds no cache pin.
            insertDeadEntry()
            #expect(deinitializations.count == 0)
            #expect(lockObservations.events.isEmpty)
            #expect(isCacheLockAvailable())

            let otherKey = WeakValueCacheTestKey(id: 2)
            let otherValue = cache.value(forKey: otherKey) {
                WeakValueCacheTestObject()
            }

            #expect(deinitializations.count == 1)
            #expect(lockObservations.events == [true])
            #expect(isCacheLockAvailable())
            withExtendedLifetime(otherValue) {}
        }
    }

    @Test func pruningRemovesDeadEntriesWhileKeepingLiveOnes() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<String, WeakValueCacheTestObject>()
            var createCallsCount = 0
            let survivor = cache.value(forKey: "survivor") {
                createCallsCount += 1
                return WeakValueCacheTestObject()
            }
            var transient: WeakValueCacheTestObject? = cache.value(forKey: "transient") {
                createCallsCount += 1
                return WeakValueCacheTestObject()
            }
            weak var weakTransient = transient

            transient = nil

            let survivorAgain = cache.value(forKey: "survivor") {
                Issue.record("Expected survivor entry to be reused.")
                return WeakValueCacheTestObject()
            }
            let transientRecreated = cache.value(forKey: "transient") {
                createCallsCount += 1
                return WeakValueCacheTestObject()
            }

            #expect(weakTransient == nil)
            #expect(survivor === survivorAgain)
            #expect(transientRecreated !== survivor)
            #expect(createCallsCount == 3)
        }
    }

    @Test func recreatesValueAfterPreviousObjectDeallocates() async throws {
        try await Their.stress {
            let cache = Their.WeakValueCache<String, WeakValueCacheTestObject>()
            var createCallsCount = 0
            var firstObject: WeakValueCacheTestObject? = cache.value(forKey: "value") {
                createCallsCount += 1
                return WeakValueCacheTestObject()
            }
            weak var weakFirstObject = firstObject

            firstObject = nil

            let secondObject = cache.value(forKey: "value") {
                createCallsCount += 1
                return WeakValueCacheTestObject()
            }

            #expect(weakFirstObject == nil)
            #expect(createCallsCount == 2)
            _ = secondObject
        }
    }

    @Test func separateCachesDoNotShareStorage() async throws {
        try await Their.stress {
            let firstCache = Their.WeakValueCache<String, WeakValueCacheTestObject>()
            let secondCache = Their.WeakValueCache<String, WeakValueCacheTestObject>()
            let firstObject = firstCache.value(forKey: "value") {
                WeakValueCacheTestObject()
            }
            var secondCacheCreateCallsCount = 0
            let secondObject = secondCache.value(forKey: "value") {
                secondCacheCreateCallsCount += 1
                return WeakValueCacheTestObject()
            }

            #expect(firstObject !== secondObject)
            #expect(secondCacheCreateCallsCount == 1)
        }
    }

    @Test func sharedCacheCreatesValueOnlyOnceForConcurrentAccess() async throws {
        let cache = Their.WeakValueCache<String, WeakValueCacheTestObject>()
        let createCallsCount = Their.Lock(0)
        // Retain the first object across all `Their.stress` iterations so the cached
        // weak entry always points at a live value, regardless of which
        // iteration created it.
        let retainedObject = Their.Lock<WeakValueCacheTestObject?>(nil)

        try await Their.stress {
            let value = cache.value(forKey: "value") {
                createCallsCount.withLock { count in
                    count += 1
                }
                let object = WeakValueCacheTestObject()
                retainedObject.withLock { retained in
                    retained = object
                }
                return object
            }
            let reused = cache.value(forKey: "value") {
                Issue.record("Expected cached value to be reused.")
                return WeakValueCacheTestObject()
            }

            #expect(value === reused)
        }

        #expect(createCallsCount.withLock { count in count } == 1)
    }
}

private enum WeakValueCacheTestsError: Equatable, Swift.Error, Sendable {}

private final class WeakValueCacheTestKey: Hashable, Sendable {

    private let id: Int
    private let onDeinit: @Sendable () -> Void

    init(
        id: Int,
        onDeinit: @escaping @Sendable () -> Void = {}
    ) {
        self.id = id
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }

    static func == (lhs: WeakValueCacheTestKey, rhs: WeakValueCacheTestKey) -> Bool {
        lhs.id == rhs.id
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

private final class WeakValueCacheTestObject: Sendable {}
