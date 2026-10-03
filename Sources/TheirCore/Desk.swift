import Foundation

extension Their {

    /// A state owner whose lifetime is independent of UI subscriptions.
    ///
    /// `current` retains the latest snapshot and `changes` immediately replays it.
    /// An internal subscription keeps the existing `Hub.evolve` reducer running
    /// even when there are no external observers. Desk owns named Job/Hub bindings:
    /// replacing or cancelling a binding suppresses its queued and future reducers.
    /// A reducer already claimed before cancellation may finish.
    ///
    /// Reducers run serially, outside locks, on the source's calling thread. There
    /// is no actor hop: UI adapters must deliver on their own actor. An uncontended
    /// update drains inline; concurrent or reentrant updates join the existing FIFO
    /// and may return before being applied. Keep reducers pure and short; perform
    /// IO in Jobs or application services. State should be a value snapshot.
    ///
    /// Releasing Desk cancels its bindings and finishes existing `changes`
    /// subscribers. Retaining `changes` or a binding's cancel does not retain Desk.
    public final class Desk<State: Sendable>: Sendable {

        public let changes: Their.Hub<State, Never>
        public var current: State { snapshot.current }
        private let snapshot: DeskSnapshot<State>
        private let source: DeskSource<State>
        private let bindings: DeskBindings<State>
        private let anchor: Their.HubCancel

        public init(_ initial: State) {
            let snapshot = DeskSnapshot(initial)
            let source = DeskSource<State>()
            self.snapshot = snapshot
            self.source = source
            bindings = DeskBindings(source: source)
            changes = Their.Hub<DeskInput<State>, Never>(work: source.connect(_:))
                .evolve(initial: initial) { state, input -> State? in
                    switch input {
                    case .publish:
                        return state
                    case .update(let reduce):
                        return reduce(&state) ? state : nil
                    }
                }
                .map { value in
                    snapshot.set(value)
                    return value
                }
                .shareLatest()
            anchor = changes.subscribe { _ in }
        }

        deinit {
            bindings.close()
            source.finish()
            anchor()
        }

        /// Queues one pure state change and emits the resulting snapshot.
        public func update(_ reduce: @escaping @Sendable (inout State) -> Void) {
            source.send(.update { state in
                reduce(&state)
                return true
            })
        }

        /// Starts one fresh Job and owns its subscription until terminal delivery,
        /// cancellation, replacement under the same id, or Desk release. Dropping
        /// the returned cancel does not cancel the binding; an old cancel cannot
        /// affect its replacement. Every accepted event, including terminal events,
        /// passes through the reducer and emits a snapshot.
        @discardableResult
        public func bind<Value: Sendable, Failure: Swift.Error & Sendable>(
            _ job: Their.Job<Value, Failure>,
            id: String,
            _ reduce: @escaping @Sendable (inout State, Their.JobEvent<Value, Failure>) -> Void
        ) -> Their.WorkCancel {
            bindings.bind(id: id, subscribe: job.subscribe(_:), isTerminal: { event in
                if case .value = event { return false }
                return true
            }, reduce: reduce)
        }

        /// Owns one subscription to a shared Hub, with the same named binding
        /// and reducer contract as the Job overload. Terminal delivery retires this
        /// binding; it does not reset Desk or restart the source automatically.
        @discardableResult
        public func bind<Value: Sendable, Failure: Swift.Error & Sendable>(
            _ hub: Their.Hub<Value, Failure>,
            id: String,
            _ reduce: @escaping @Sendable (inout State, Their.HubEvent<Value, Failure>) -> Void
        ) -> Their.WorkCancel {
            bindings.bind(id: id, subscribe: hub.subscribe(_:), isTerminal: { event in
                if case .value = event { return false }
                return true
            }, reduce: reduce)
        }

        /// Cancels the current binding and invalidates any of its queued reducers,
        /// including reducers queued just before terminal delivery.
        public func unbind(_ id: String) {
            bindings.cancel(id: id)
        }
    }
}

private enum DeskInput<State: Sendable>: Sendable {
    case publish
    case update(@Sendable (inout State) -> Bool)
}

private final class DeskSnapshot<State: Sendable>: Sendable {
    private let lock: Their.Lock<State>
    var current: State { lock.withLock { $0 } }

    init(_ initial: State) { lock = Their.Lock(initial) }

    func set(_ value: State) {
        let retired = lock.withLock { current in
            let retired = current
            current = value
            return retired
        }
        withExtendedLifetime(retired) {}
    }
}

/// This private source has one lifecycle, pinned by Desk's anchor. Once closed,
/// later attempts to subscribe finish immediately instead of reviving initial state.
private final class DeskSource<State: Sendable>: Sendable {
    private struct Record: Sendable {
        var report: Their.WorkReport<DeskInput<State>, Never>?
        var isClosed = false
    }
    private let lock = Their.Lock(Record())

    func connect(_ report: @escaping Their.WorkReport<DeskInput<State>, Never>) -> Their.WorkCancel {
        let connected = lock.withLock { record in
            guard !record.isClosed else { return false }
            record.report = report
            return true
        }
        report(connected ? .value(.publish) : .finished)
        return { [weak self] in self?.disconnect() }
    }

    func send(_ input: DeskInput<State>) {
        let report = lock.withLock { $0.report }
        report?(.value(input))
    }

    func finish() {
        let report = lock.withLock { record in
            record.isClosed = true
            let report = record.report
            record.report = nil
            return report
        }
        report?(.finished)
    }

    private func disconnect() {
        let retired = lock.withLock { record in
            let retired = record.report
            record.report = nil
            return retired
        }
        withExtendedLifetime(retired) {}
    }
}

private final class DeskBindings<State: Sendable>: Sendable {
    private struct Slot: Sendable {
        let generation: UInt64
        let cancellation: Their.Resource<Their.WorkCancel>
    }
    private struct Record: Sendable {
        var nextGeneration: UInt64 = 0
        // Keep the latest token after terminal delivery so previously queued
        // terminal/value reducers remain valid. Stable feature ids are intended;
        // these small tombstones live until Desk release.
        var generations: [String: UInt64] = [:]
        var slots: [String: Slot] = [:]
        var isClosed = false
    }
    private let lock = Their.Lock(Record())
    private let source: DeskSource<State>

    init(source: DeskSource<State>) { self.source = source }

    func bind<Event: Sendable>(
        id: String,
        subscribe: @Sendable (@escaping @Sendable (Event) -> Void) -> Their.WorkCancel,
        isTerminal: @escaping @Sendable (Event) -> Bool,
        reduce: @escaping @Sendable (inout State, Event) -> Void
    ) -> Their.WorkCancel {
        let reservation = lock.withLock { record -> (slot: Slot, retired: Slot?)? in
            guard !record.isClosed else { return nil }
            record.nextGeneration += 1
            let slot = Slot(generation: record.nextGeneration, cancellation: Their.Resource { $0() })
            let retired = record.slots.updateValue(slot, forKey: id)
            record.generations[id] = slot.generation
            return (slot, retired)
        }
        guard let reservation else { return {} }
        reservation.retired?.cancellation.cancel()
        let generation = reservation.slot.generation
        if isCurrent(id: id, generation: generation) {
            let cancel = subscribe { [weak self] event in
                guard let self else { return }
                source.send(.update { [weak self] state in
                    // The fence is checked at reduction time, not callback receipt:
                    // cancellation also cuts off already queued old results.
                    guard self?.isCurrent(id: id, generation: generation) == true else { return false }
                    reduce(&state, event)
                    return true
                })
                if isTerminal(event) { complete(id: id, generation: generation) }
            }
            // Replacement can occur while subscribe is still starting. Resource
            // immediately cancels a handle installed after that replacement.
            reservation.slot.cancellation.set(cancel)
        } else {
            reservation.slot.cancellation.cancel()
        }
        return { [weak self] in self?.cancel(id: id, generation: generation) }
    }

    private func isCurrent(id: String, generation: UInt64) -> Bool {
        lock.withLock { !$0.isClosed && $0.generations[id] == generation }
    }

    private func complete(id: String, generation: UInt64) {
        let retired = lock.withLock { record -> Slot? in
            guard record.slots[id]?.generation == generation else { return nil }
            return record.slots.removeValue(forKey: id)
        }
        retired?.cancellation.cancel()
    }

    func cancel(id: String, generation: UInt64? = nil) {
        let retired = lock.withLock { record -> Slot? in
            guard !record.isClosed, let current = record.generations[id],
                  generation == nil || generation == current else { return nil }
            record.nextGeneration += 1
            record.generations[id] = record.nextGeneration
            return record.slots.removeValue(forKey: id)
        }
        retired?.cancellation.cancel()
    }

    func close() {
        let retired = lock.withLock { record in
            let retired = record
            record = Record(isClosed: true)
            return retired
        }
        for slot in retired.slots.values { slot.cancellation.cancel() }
    }
}
