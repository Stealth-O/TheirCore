import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct DrainQueueTests {

    @Test func concurrentProducersUseOneDrainerAndDeliverEveryInput() async throws {
        let callbacks = Their.Lock(0)
        let queue = Their.Lock(DrainQueue<Int>())
        let received = Their.TestEventRecorder<Int>()

        try await Their.stress { value in
            let shouldDrain = queue.withLock { $0.append(value) }
            guard shouldDrain else {
                return
            }
            while let input = queue.withLock({ $0.popFirst() }) {
                let activeCallbacks = callbacks.withLock { count in
                    count += 1
                    return count
                }
                #expect(activeCallbacks == 1)
                received.append(input)
                callbacks.withLock { $0 -= 1 }
            }
        }

        #expect(callbacks.withLock { $0 } == 0)
        #expect(received.events.sorted() == Array(0 ..< Their.stressCountDefault))
    }

    @Test func detachedInputsReleaseOutsideOwnerLockAndMayReenterQueue() async throws {
        try await Their.stress {
            let observations = Their.TestEventRecorder<Bool>()
            let queue = Their.Lock(DrainQueue<DrainQueueLifetimeToken>())
            let claimed = queue.withLock { record in
                record.append(DrainQueueLifetimeToken {
                    let canReenter = queue.withLockIfAvailable { record in
                        record.popFirst() == nil
                    } ?? false
                    observations.append(canReenter)
                })
            }
            #expect(claimed)
            var detached: InputQueue<DrainQueueLifetimeToken>? = queue.withLock { $0.takePending() }
            #expect(detached?.pending.count == 1)
            #expect(observations.events.isEmpty)
            detached = nil
            #expect(observations.events == [true])
        }
    }

    @Test func detachingDuringCallbackPreservesDrainerAcrossRestart() async throws {
        try await Their.stress {
            var queue = DrainQueue<Int>()
            #expect(queue.append(1) == true)
            #expect(queue.popFirst() == 1)
            #expect(queue.append(2) == false)
            let retired = queue.takePending()
            #expect(retired.pending == [2])
            // The old callback still owns the drain while a fresh lifecycle
            // appends its first input; it must not create a nested drainer.
            #expect(queue.append(3) == false)
            #expect(queue.popFirst() == 3)
            #expect(queue.popFirst() == nil)
            #expect(queue.append(4) == true)
            #expect(queue.popFirst() == 4)
        }
    }

    @Test func optionalNilIsAnInputAndKeepsTheDrainClaimed() async throws {
        try await Their.stress {
            var queue = DrainQueue<Int?>()
            #expect(queue.append(nil) == true)
            #expect(queue.popFirst() == .some(nil))
            #expect(queue.append(7) == false)
            #expect(queue.popFirst() == .some(.some(7)))
            #expect(queue.popFirst() == nil)
            #expect(queue.append(nil) == true)
        }
    }

    @Test func shortActionSequencesMatchReferenceOwnershipModel() async throws {
        // Each iteration exhausts all 256 length-four traces. Four parallel
        // copies repeat the model check without multiplying it fifty times.
        try await Their.stress(count: 4) {
            for encodedTrace in 0 ..< 256 {
                var expectedInputs = [Int]()
                var hasOwner = false
                var queue = DrainQueue<Int>()
                var trace = encodedTrace
                for value in 0 ..< 4 {
                    switch trace % 4 {
                    case 0:
                        let expectedClaim = hasOwner == false
                        expectedInputs.append(value)
                        hasOwner = true
                        #expect(queue.append(value) == expectedClaim)
                    case 1:
                        let expected = expectedInputs.isEmpty ? nil : expectedInputs.removeFirst()
                        if expected == nil {
                            hasOwner = false
                        }
                        #expect(queue.popFirst() == expected)
                    case 2:
                        hasOwner = false
                        #expect(queue.popFirst(isActive: false) == nil)
                    default:
                        let expected = expectedInputs
                        expectedInputs = []
                        #expect(queue.takePending().pending == expected)
                    }
                    trace /= 4
                }
                #expect(queue.append(4) == (hasOwner == false))
                expectedInputs.append(4)
                for expected in expectedInputs {
                    #expect(queue.popFirst() == expected)
                }
                #expect(queue.popFirst() == nil)
            }
        }
    }
}

private final class DrainQueueLifetimeToken: Sendable {

    private let onDeinit: @Sendable () -> Void

    init(onDeinit: @escaping @Sendable () -> Void) {
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }
}
