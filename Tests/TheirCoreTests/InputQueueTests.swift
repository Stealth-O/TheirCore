import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct InputQueueTests {

    @Test func appendAfterDrainReusesQueue() async throws {
        try await Their.stress {
            var queue = InputQueue<Int>()
            queue.append(1)
            #expect(queue.popFirst() == 1)
            #expect(queue.popFirst() == nil)
            queue.append(2)
            queue.append(3)
            #expect(queue.popFirst() == 2)
            #expect(queue.popFirst() == 3)
            #expect(queue.popFirst() == nil)
        }
    }

    @Test func clearDropsPendingElements() async throws {
        try await Their.stress {
            var queue = InputQueue<Int>()
            queue.append(1)
            queue.append(2)
            queue.clear()
            #expect(queue.pending.isEmpty == true)
            #expect(queue.popFirst() == nil)
            queue.append(3)
            #expect(queue.popFirst() == 3)
        }
    }

    @Test func interleavedAppendAndPopPreserveOrder() async throws {
        try await Their.stress {
            var popped = [Int]()
            var queue = InputQueue<Int>()
            queue.append(1)
            queue.append(2)
            popped.append(queue.popFirst() ?? -1)
            queue.append(3)
            popped.append(queue.popFirst() ?? -1)
            popped.append(queue.popFirst() ?? -1)
            #expect(popped == [1, 2, 3])
            #expect(queue.popFirst() == nil)
        }
    }

    @Test func longRunPreservesOrderAcrossCompaction() async throws {
        try await Their.stress {
            let count = 1_000
            var popped = [Int]()
            var queue = InputQueue<Int>()
            // Keep a growing backlog while popping so the head index crosses the
            // internal compaction threshold several times mid-stream.
            for value in 0 ..< count {
                queue.append(value)
                if value.isMultiple(of: 2), let element = queue.popFirst() {
                    popped.append(element)
                }
            }
            while let element = queue.popFirst() {
                popped.append(element)
            }
            #expect(popped == Array(0 ..< count))
            #expect(queue.pending.isEmpty == true)
        }
    }

    @Test func pendingExposesUnpoppedElementsInOrder() async throws {
        try await Their.stress {
            var queue = InputQueue<Int>()
            queue.append(1)
            queue.append(2)
            queue.append(3)
            _ = queue.popFirst()
            #expect(Array(queue.pending) == [2, 3])
        }
    }

    @Test func popFirstDoesNotRetainConsumedElementsAcrossCompaction() async throws {
        try await Their.stress {
            let released = Their.TestCountRecorder()
            var queue = InputQueue<QueueLifetimeToken>()
            for _ in 0 ..< 130 {
                queue.append(QueueLifetimeToken {
                    _ = released.increment()
                })
            }
            for count in 1 ... 130 {
                var value = queue.popFirst()
                let isValueAlive: @Sendable () -> Bool = { [weak value] in
                    value != nil
                }
                #expect(isValueAlive())
                value = nil
                #expect(isValueAlive() == false)
                #expect(released.count == count)
            }
            #expect(queue.pending.isEmpty)
        }
    }

    @Test func popFirstOnEmptyQueueReturnsNil() async throws {
        try await Their.stress {
            var queue = InputQueue<Int>()
            #expect(queue.popFirst() == nil)
            #expect(queue.pending.isEmpty == true)
        }
    }

    @Test func popFirstPreservesOptionalNilElement() async throws {
        try await Their.stress {
            var queue = InputQueue<Int?>()
            queue.append(nil)
            queue.append(7)
            #expect(queue.pending == [nil, 7])
            #expect(queue.popFirst() == .some(nil))
            #expect(queue.popFirst() == .some(.some(7)))
            #expect(queue.popFirst() == nil)
        }
    }
}

private final class QueueLifetimeToken: Sendable {

    private let onDeinit: @Sendable () -> Void

    init(onDeinit: @escaping @Sendable () -> Void) {
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }
}
