import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct LockTests {

    @Test func withLockIfAvailablePropagatesTypedErrorAndReleasesLock() async throws {
        try await Their.stress {
            let lock = Their.Lock(0)
            #expect(throws: LockTestError.boom) {
                try lock.withLockIfAvailable { (value: inout Int) -> Void in
                    throw LockTestError.boom
                }
            }
            lock.withLock { value in
                value = 5
            }
            #expect(lock.withLock { value in value } == 5)
        }
    }

    @Test func withLockIfAvailableReturnsNilWhileAnotherThreadHoldsLock() async throws {
        try await Their.stress(count: 1) {
            let box = LockBox()
            let done = LockTestSignal()
            let held = LockTestSignal()
            let release = DispatchSemaphore(value: 0)
            DispatchQueue.global().async {
                box.lock.withLock { value in
                    held.signal()
                    release.wait()
                }
                done.signal()
            }
            try await held.wait()

            let blocked: Int? = box.lock.withLockIfAvailable { value in
                value += 1
                return value
            }
            #expect(blocked == nil)

            release.signal()
            try await done.wait()

            let acquired: Int? = box.lock.withLockIfAvailable { value in
                value += 1
                return value
            }
            #expect(acquired == 1)
            #expect(box.lock.withLock { value in value } == 1)
        }
    }

    @Test func withLockIfAvailableRunsBodyAndReturnsResultWhenLockIsFree() async throws {
        try await Their.stress {
            let lock = Their.Lock(10)
            let result: Int? = lock.withLockIfAvailable { value in
                value += 5
                return value
            }
            #expect(result == 15)
            #expect(lock.withLock { value in value } == 15)
        }
    }

    @Test func withLockPropagatesTypedErrorAndReleasesLock() async throws {
        try await Their.stress {
            let lock = Their.Lock(0)
            #expect(throws: LockTestError.boom) {
                try lock.withLock { (value: inout Int) -> Void in
                    value = 3
                    throw LockTestError.boom
                }
            }
            #expect(lock.withLock { value in value } == 3)
            lock.withLock { value in
                value = 7
            }
            #expect(lock.withLock { value in value } == 7)
        }
    }

    @Test func withLockProvidesMutableInOutAccessThatPersists() async throws {
        try await Their.stress {
            let lock = Their.Lock([Int]())
            lock.withLock { values in
                values.append(1)
            }
            lock.withLock { values in
                values.append(2)
            }
            #expect(lock.withLock { values in values } == [1, 2])
        }
    }

    @Test func withLockReturnsBodyResult() async throws {
        try await Their.stress {
            let lock = Their.Lock(21)
            let doubled = lock.withLock { value in
                value * 2
            }
            #expect(doubled == 42)
            #expect(lock.withLock { value in value } == 21)
        }
    }

    @Test func withLockSerializesConcurrentMutation() async throws {
        let counter = LockCounter()
        try await Their.stress {
            counter.increment()
        }
        #expect(counter.value == Their.stressCountDefault)
    }
}

private final class LockBox: Sendable {

    let lock = Their.Lock(0)
}

private final class LockCounter: Sendable {

    private let lock = Their.Lock(0)
    var value: Int {
        lock.withLock { value in
            value
        }
    }

    func increment() {
        lock.withLock { value in
            value += 1
        }
    }
}

private enum LockTestError: Error, Equatable, Sendable {

    case boom
}

private typealias LockTestSignal = Their.TestSignal
