import Foundation

extension Their {

    /// A state owner with one fixed reducer for typed events.
    ///
    /// `send` and named Job/Hub bindings feed the same FIFO reduction path.
    /// Bindings only map source events to `Event`; they never receive mutable
    /// state. `current` is read-only and `changes` immediately replays the latest
    /// snapshot. An internal subscription keeps the existing `Hub.evolve` running
    /// even when there are no external observers.
    ///
    /// Replacing or cancelling a binding suppresses its queued and future events.
    /// An event already claimed by the reducer before cancellation may finish.
    /// Reducers run serially, outside locks, on the current drainer's thread.
    /// There is no actor hop or scheduler. An uncontended send drains inline;
    /// concurrent or reentrant sends join the FIFO and may return before being
    /// applied. Keep the reducer and source mappings pure and short; perform IO
    /// in Jobs or application services. State must be a value snapshot: Sendable
    /// alone does not prevent shared reference storage from being mutated elsewhere.
    ///
    /// Releasing Desk cancels its bindings and finishes existing `changes`
    /// subscribers. Retaining `changes` or a binding's cancel does not retain Desk.
    public final class Desk<State: Sendable, Event: Sendable>: Sendable {

        private let anchor: Their.HubCancel
        private let bindings: DeskBindings<Event>
        public let changes: Their.Hub<State, Never>
        public var current: State { snapshot.current }
        private let snapshot: DeskSnapshot<State>
        private let source: DeskSource<Event>

        /// Fixes the only state reducer for the lifetime of this Desk.
        /// Initial snapshot publication does not invoke the reducer. Every accepted
        /// event invokes it once and emits the resulting snapshot, even if unchanged.
        public init(
            _ initial: State,
            reducer: @escaping @Sendable (inout State, Event) -> Void
        ) {
            let snapshot = DeskSnapshot(initial)
            let source = DeskSource<Event>()
            let bindings = DeskBindings(source: source)
            self.snapshot = snapshot
            self.source = source
            self.bindings = bindings
            changes = Their.Hub<DeskInput<Event>, Never>(work: source.connect(_:))
                .evolve(initial: initial) { [weak bindings] state, input -> State? in
                    switch input {
                    case .publish:
                        return state
                    case .event(let event, let binding):
                        // Check at reduction time: replacement also cuts off events
                        // already queued, or produced by an in-flight source mapping.
                        if let binding {
                            guard bindings?.isCurrent(binding) == true else { return nil }
                        }
                        reducer(&state, event)
                        return state
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

        /// Starts one fresh Job and owns its subscription until terminal delivery,
        /// cancellation, replacement under the same id, or Desk release. The mapping
        /// returns a typed event for the fixed reducer, or nil to ignore the input.
        /// A terminal input retires the binding even when its mapping returns nil.
        /// Dropping the returned cancel does not cancel the binding; an old cancel
        /// cannot affect its replacement.
        @discardableResult
        public func bind<Value: Sendable, Failure: Swift.Error & Sendable>(
            _ job: Their.Job<Value, Failure>,
            id: String,
            _ map: @escaping @Sendable (Their.JobEvent<Value, Failure>) -> Event?
        ) -> Their.WorkCancel {
            bindings.bind(id: id, subscribe: job.subscribe(_:), isTerminal: { event in
                if case .value = event { return false }
                return true
            }, map: map)
        }

        /// Owns one subscription to a shared Hub, with the same event-mapping
        /// contract as the Job overload. Terminal delivery retires this binding;
        /// it does not reset Desk or restart the source automatically.
        @discardableResult
        public func bind<Value: Sendable, Failure: Swift.Error & Sendable>(
            _ hub: Their.Hub<Value, Failure>,
            id: String,
            _ map: @escaping @Sendable (Their.HubEvent<Value, Failure>) -> Event?
        ) -> Their.WorkCancel {
            bindings.bind(id: id, subscribe: hub.subscribe(_:), isTerminal: { event in
                if case .value = event { return false }
                return true
            }, map: map)
        }

        /// Queues a typed event for the fixed reducer and publishes its snapshot.
        public func send(_ event: Event) {
            source.send(.event(event, binding: nil))
        }

        /// Cancels the current binding and invalidates its queued events, including
        /// events queued just before terminal delivery. Does not change state itself.
        public func unbind(_ id: String) {
            bindings.cancel(id: id)
        }
    }
}

private struct DeskBinding: Sendable {
    let generation: UInt64
    let id: String
}

private enum DeskInput<Event: Sendable>: Sendable {
    case event(Event, binding: DeskBinding?)
    case publish
}

private final class DeskSnapshot<State: Sendable>: Sendable {
    var current: State { lock.withLock { $0 } }
    private let lock: Their.Lock<State>

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
private final class DeskSource<Event: Sendable>: Sendable {
    private struct Record: Sendable {
        var isClosed = false
        var report: Their.WorkReport<DeskInput<Event>, Never>?
    }
    private let lock = Their.Lock(Record())

    func connect(_ report: @escaping Their.WorkReport<DeskInput<Event>, Never>) -> Their.WorkCancel {
        let connected = lock.withLock { record in
            guard !record.isClosed else { return false }
            record.report = report
            return true
        }
        report(connected ? .value(.publish) : .finished)
        return { [weak self] in self?.disconnect() }
    }

    private func disconnect() {
        let retired = lock.withLock { record in
            let retired = record.report
            record.report = nil
            return retired
        }
        withExtendedLifetime(retired) {}
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

    func send(_ input: DeskInput<Event>) {
        let report = lock.withLock { $0.report }
        report?(.value(input))
    }
}

private final class DeskBindings<Event: Sendable>: Sendable {
    private struct Slot: Sendable {
        let cancellation: Their.Resource<Their.WorkCancel>
        let generation: UInt64
    }
    private struct Record: Sendable {
        // Keep the latest token after terminal delivery so previously queued
        // terminal/value events remain valid. Stable feature ids are intended;
        // these small tombstones live until Desk release.
        var generations: [String: UInt64] = [:]
        var isClosed = false
        var nextGeneration: UInt64 = 0
        var slots: [String: Slot] = [:]
    }
    private let lock = Their.Lock(Record())
    private let source: DeskSource<Event>

    init(source: DeskSource<Event>) { self.source = source }

    func bind<SourceEvent: Sendable>(
        id: String,
        subscribe: @Sendable (@escaping @Sendable (SourceEvent) -> Void) -> Their.WorkCancel,
        isTerminal: @escaping @Sendable (SourceEvent) -> Bool,
        map: @escaping @Sendable (SourceEvent) -> Event?
    ) -> Their.WorkCancel {
        let reservation = lock.withLock { record -> (slot: Slot, retired: Slot?)? in
            guard !record.isClosed else { return nil }
            record.nextGeneration += 1
            let slot = Slot(cancellation: Their.Resource { $0() }, generation: record.nextGeneration)
            let retired = record.slots.updateValue(slot, forKey: id)
            record.generations[id] = slot.generation
            return (slot, retired)
        }
        guard let reservation else { return {} }
        reservation.retired?.cancellation.cancel()
        let generation = reservation.slot.generation
        let binding = DeskBinding(generation: generation, id: id)
        if isCurrent(binding) {
            let cancel = subscribe { [weak self] event in
                guard let self, isCurrent(binding) else { return }
                if let mapped = map(event) {
                    source.send(.event(mapped, binding: binding))
                }
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

    private func complete(id: String, generation: UInt64) {
        let retired = lock.withLock { record -> Slot? in
            guard record.slots[id]?.generation == generation else { return nil }
            return record.slots.removeValue(forKey: id)
        }
        retired?.cancellation.cancel()
    }

    func isCurrent(_ binding: DeskBinding) -> Bool {
        lock.withLock { !$0.isClosed && $0.generations[binding.id] == binding.generation }
    }
}
