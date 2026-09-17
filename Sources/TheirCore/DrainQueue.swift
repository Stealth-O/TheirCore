import Foundation

/// FIFO inputs and the right to drain them, stored under the owner's `Lock`.
///
/// `append` returns true only to the caller that claims an idle drain. That
/// caller repeatedly takes an input under the same lock and processes it after
/// unlocking. Dequeue and any lifecycle/state/subscriber snapshot must stay in
/// one owner critical section. This type owns no lock, callback or scheduler.
///
/// Taking the last input keeps the drain claimed while its callback runs. Only
/// a subsequent empty/inactive `popFirst` releases it, so reentrant appends join
/// the current drain. `takePending` detaches storage without releasing that
/// claim: cancellation/restart cannot start a second drainer while the first
/// callback is still in flight. Keep detached inputs alive until after unlock.
struct DrainQueue<Element: Sendable>: Sendable {

    private var inputs = InputQueue<Element>()
    private var isDraining = false

    mutating func append(_ input: Element) -> Bool {
        inputs.append(input)
        guard isDraining == false else {
            return false
        }
        isDraining = true
        return true
    }

    mutating func popFirst(isActive: Bool = true) -> Element? {
        guard isActive, let input = inputs.popFirst() else {
            isDraining = false
            return nil
        }
        return input
    }

    mutating func takePending() -> InputQueue<Element> {
        let detached = inputs
        inputs = InputQueue()
        return detached
    }
}
