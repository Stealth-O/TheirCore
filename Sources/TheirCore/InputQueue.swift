import Foundation

/// Internal FIFO storage used by `DrainQueue`, also returned by its
/// `takePending()` for post-lock cleanup.
///
/// `append` keeps arrival order; `popFirst` consumes from the front in
/// amortized O(1) by advancing a head index instead of `Array.removeFirst()`,
/// compacting the backing array once the consumed prefix grows past a
/// threshold. Popping clears the consumed slot while the returned element is
/// retained, so later compaction never destroys previously delivered values.
/// `pending` snapshots the unpopped elements for cleanup inspection. Owners
/// clearing pending inputs under a lock must retain the old queue until unlock.
///
/// This storage owns no synchronization or drainer state. `DrainQueue` combines
/// it with the single-drainer claim under the engine/operator's existing
/// `Their.Lock`. That owner still provides the drain loop, lifecycle checks and
/// dequeue-time snapshots, and executes effects after unlock. Detached storage
/// stays alive outside the lock until cleanup completes. Tested in
/// `InputQueueTests`; claim and detach behavior is covered by `DrainQueueTests`.
struct InputQueue<Element: Sendable>: Sendable {

    private var elements = [Element?]()
    private var head = 0
    /// Elements appended but not yet popped, in FIFO order.
    var pending: [Element] {
        elements[head...].compactMap { $0 }
    }

    mutating func append(_ element: Element) {
        elements.append(.some(element))
    }

    mutating func clear() {
        elements = []
        head = 0
    }

    mutating func popFirst() -> Element? {
        guard head < elements.count else {
            clear()
            return nil
        }
        let element = elements[head]
        elements[head] = nil
        head += 1
        if head == elements.count {
            clear()
        } else if head > 64, head * 2 >= elements.count {
            elements.removeFirst(head)
            head = 0
        }
        return element
    }
}
