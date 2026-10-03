import Dispatch
import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct DeskTests {

    @Test func bindingIdsAreIndependentAndCancellationIsIdempotent() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, DeskError>()
            let second = Their.TestJobDriver<Int, DeskError>()
            let desk = makeIntDesk(0)
            let cancel = desk.bind(first.job, id: "first", mapJobValue)
            desk.bind(second.job, id: "second", mapJobValue)
            cancel()
            cancel()
            desk.unbind("first")
            second.emit(value: 3)
            #expect(desk.current == 3)
            #expect(first.cancelCallsCount == 1)
            #expect(second.cancelCallsCount == 0)
        }
    }

    @Test func cancellationCannotInterruptAnAlreadyClaimedReducer() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let source = Their.TestJobDriver<Int, DeskError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let desk = Their.Desk<Int, DeskEvent>(0) { state, event in
                entered.signal()
                gate.wait()
                reduceInt(&state, event)
            }
            desk.bind(source.job, id: "load", mapJobValue)
            let blocked = BlockingWork { source.emit(value: 2) }
            try await entered.wait()
            desk.unbind("load")
            gate.signal()
            try await blocked.value
            #expect(desk.current == 2)
            #expect(source.cancelCallsCount == 1)
        }
    }

    @Test func concurrentEventsDoNotLoseState() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let desk = makeIntDesk(0)
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<100 { group.addTask { desk.send(.add(1)) } }
            }
            #expect(desk.current == 100)
        }
    }

    @Test func currentIsPublishedBeforeObserversAndReentrantEventsAreFIFO() async throws {
        try await Their.stress {
            let desk = makeIntDesk(0)
            let events = Their.TestEventRecorder<Int>()
            let cancel = desk.changes.subscribe { event in
                guard case .value(let value) = event else { return }
                #expect(desk.current == value)
                events.append(value)
                if value == 1 { desk.send(.add(1)) }
            }
            desk.send(.add(1))
            #expect(events.events == [0, 1, 2])
            cancel()
        }
    }

    @Test func directJobAndHubEventsUseOneReducerAndOutputSequence() async throws {
        try await Their.stress {
            let reduced = Their.TestEventRecorder<DeskEvent>()
            let desk = Their.Desk<Int, DeskEvent>(0) { state, event in
                reduced.append(event)
                reduceInt(&state, event)
            }
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = desk.changes.subscribe(snapshots.append)
            let job = Their.TestJobDriver<Int, DeskError>()
            let hub = Their.TestHubDriver<Int, DeskError>()
            desk.bind(job.job, id: "job") { event in
                switch event {
                case .value(let value): return .add(value)
                case .finished: return .add(10)
                case .failure: return .set(-1)
                }
            }
            desk.bind(hub.hub, id: "hub") { event in
                switch event {
                case .value(let value): return .add(value)
                case .finished: return nil
                case .failure: return .set(-1)
                }
            }
            #expect(reduced.events.isEmpty)
            desk.send(.add(1))
            job.emit(value: 2)
            hub.emit(value: 3)
            job.emitFinished()
            hub.emit(failure: .failed)
            desk.send(.add(0))
            #expect(reduced.events == [.add(1), .add(2), .add(3), .add(10), .set(-1), .add(0)])
            #expect(snapshots.events == [0, 1, 3, 6, 16, -1, -1].map { .value($0) })
            #expect(desk.current == -1)
            #expect(job.cancelCallsCount == 1)
            #expect(hub.cancelCallsCount == 1)
            cancel()
        }
    }

    @Test func discardedCancelDoesNotEndBindingAndTerminalDoesNotResetState() async throws {
        try await Their.stress {
            let source = Their.TestJobDriver<Int, DeskError>()
            let desk = makeIntDesk(1)
            desk.bind(source.job, id: "load", mapJobValue)
            source.emit(value: 4)
            source.emitFinished()
            #expect(desk.current == 5)
            #expect(source.cancelCallsCount == 1)
            desk.send(.add(1))
            #expect(desk.current == 6)
        }
    }

    @Test func failureIsAnOrdinaryBindingEventAndFreshBindingCanRetry() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, DeskError>()
            let next = Their.TestJobDriver<Int, DeskError>()
            let desk = makeIntDesk(0)
            desk.bind(first.job, id: "load", mapJobReplacement)
            first.emit(failure: .failed)
            #expect(desk.current == -1)
            desk.bind(next.job, id: "load", mapJobReplacement)
            next.emit(value: 7)
            #expect(desk.current == 7)
        }
    }

    @Test func hubBindingRunsWithoutUIAndUnbindRemovesOnlyItsSubscription() async throws {
        try await Their.stress {
            let source = Their.TestHubDriver<Int, DeskError>()
            let external = Their.TestEventRecorder<Their.HubEvent<Int, DeskError>>()
            let externalCancel = source.hub.subscribe(external.append)
            let desk = makeIntDesk(0)
            desk.bind(source.hub, id: "live", mapHubReplacement)
            source.emit(value: 4)
            desk.unbind("live")
            source.emit(value: 9)
            #expect(desk.current == 4)
            #expect(external.events == [.value(4), .value(9)])
            #expect(source.startCallsCount == 1)
            #expect(source.cancelCallsCount == 0)
            externalCancel()
            #expect(source.cancelCallsCount == 1)
        }
    }

    @Test func hubTerminalRetiresBindingAndNewHubCanUseTheSameId() async throws {
        try await Their.stress {
            let first = Their.TestHubDriver<Int, DeskError>()
            let next = Their.TestHubDriver<Int, DeskError>()
            let desk = makeIntDesk(0)
            desk.bind(first.hub, id: "live", mapHubReplacement)
            first.emit(failure: .failed)
            desk.bind(next.hub, id: "live", mapHubReplacement)
            next.emit(value: 8)
            #expect(desk.current == 8)
            #expect(first.cancelCallsCount == 1)
        }
    }

    @Test func ignoredInputsDoNotReduceOrPublishButStillRetireBindings() async throws {
        try await Their.stress {
            let reductions = Their.TestCountRecorder()
            let mappings = Their.TestCountRecorder()
            let desk = Their.Desk<Int, DeskEvent>(9) { state, event in
                _ = reductions.increment()
                reduceInt(&state, event)
            }
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = desk.changes.subscribe(snapshots.append)
            let job = Their.TestJobDriver<Int, DeskError>()
            let hub = Their.TestHubDriver<Int, DeskError>()
            desk.bind(job.job, id: "job") { _ in _ = mappings.increment(); return nil }
            desk.bind(hub.hub, id: "hub") { _ in _ = mappings.increment(); return nil }
            job.emit(value: 7)
            job.emit(failure: .failed)
            hub.emit(value: 8)
            hub.emitFinished()
            #expect(mappings.count == 4)
            #expect(reductions.count == 0)
            #expect(snapshots.events == [.value(9)])
            #expect(job.cancelCallsCount == 1)
            #expect(hub.cancelCallsCount == 1)
            desk.send(.add(1))
            #expect(reductions.count == 1)
            #expect(snapshots.events == [.value(9), .value(10)])
            cancel()
        }
    }

    @Test func mappingCanReenterSendAndUnbindWithoutDeadlock() async throws {
        try await Their.stress {
            let desk = makeIntDesk(0)
            let source = Their.TestJobDriver<Int, DeskError>()
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = desk.changes.subscribe(snapshots.append)
            desk.bind(source.job, id: "load") { [weak desk] event in
                guard case .value(let value) = event else { return nil }
                desk?.send(.add(1))
                desk?.unbind("load")
                return .add(value)
            }
            source.emit(value: 100)
            #expect(desk.current == 1)
            #expect(snapshots.events == [.value(0), .value(1)])
            #expect(source.cancelCallsCount == 1)
            cancel()
        }
    }

    @Test func mixedIngressQueuedBehindTheReducerPreservesFIFO() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let desk = makeBlockedDesk(entered, gate)
            let job = Their.TestJobDriver<Int, DeskError>()
            let hub = Their.TestHubDriver<Int, DeskError>()
            desk.bind(job.job, id: "job", mapJobValue)
            desk.bind(hub.hub, id: "hub") { event in
                if case .value(let value) = event { return .add(value) }
                return nil
            }
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = desk.changes.subscribe(snapshots.append)
            let blocked = BlockingWork { desk.send(.block(1)) }
            try await entered.wait()
            desk.send(.add(2))
            job.emit(value: 3)
            hub.emit(value: 4)
            desk.send(.add(5))
            gate.signal()
            try await blocked.value
            #expect(desk.current == 15)
            #expect(snapshots.events == [0, 1, 3, 6, 10, 15].map { .value($0) })
            cancel()
        }
    }

    @Test func queuedEventsDoNotRetainMappingCapturesAfterTerminal() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let desk = makeBlockedDesk(entered, gate)
            let source = Their.TestJobDriver<Int, DeskError>()
            let released = Their.TestCountRecorder()
            let isReleased: @Sendable () -> Bool
            do {
                let probe = DeskLifetimeProbe { _ = released.increment() }
                isReleased = { [weak probe] in probe == nil }
                desk.bind(source.job, id: "load") { [probe] event in
                    withExtendedLifetime(probe) { mapJobValue(event) }
                }
            }
            let blocked = BlockingWork { desk.send(.block(0)) }
            try await entered.wait()
            source.emit(value: 2)
            source.emitFinished()
            #expect(isReleased())
            #expect(released.count == 1)
            #expect(desk.current == 0)
            gate.signal()
            try await blocked.value
            #expect(desk.current == 2)
        }
    }

    @Test func releasingDeskCancelsSourcesEvenIfChangesAndCancelAreRetained() async throws {
        try await Their.stress {
            let source = Their.TestJobDriver<Int, DeskError>()
            var desk: Their.Desk<Int, DeskEvent>? = makeIntDesk(0)
            let isReleased: @Sendable () -> Bool = { [weak desk] in desk == nil }
            let changes = desk!.changes
            let bindingCancel = desk!.bind(source.job, id: "load", mapJobValue)
            let events = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let observerCancel = changes.subscribe(events.append)
            source.emit(value: 2)
            desk = nil
            #expect(isReleased())
            #expect(source.cancelCallsCount == 1)
            #expect(events.events == [.value(0), .value(2), .finished])
            bindingCancel()
            observerCancel()
            let late = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let lateCancel = changes.subscribe(late.append)
            #expect(late.events == [.finished])
            lateCancel()
        }
    }

    @Test func replacedSnapshotDestructorCanReadCurrentState() async throws {
        try await Their.stress {
            let desk = Their.Desk<DeskLifetimeProbe?, DeskLifetimeProbe?>(nil) { state, event in
                state = event
            }
            let released = Their.TestCountRecorder()
            desk.send(DeskLifetimeProbe { [weak desk] in
                #expect(desk?.current == nil)
                _ = released.increment()
            })
            desk.send(nil)
            #expect(released.count == 1)
        }
    }

    @Test func replacementCancelsOldSourceAndOldCancelCannotCancelReplacement() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, DeskError>()
            let next = Their.TestJobDriver<Int, DeskError>()
            let desk = makeIntDesk(0)
            let oldCancel = desk.bind(first.job, id: "load", mapJobValue)
            let oldReport = first.report
            desk.bind(next.job, id: "load", mapJobValue)
            oldCancel()
            oldReport?(.value(100))
            next.emit(value: 2)
            #expect(first.cancelCallsCount == 1)
            #expect(next.cancelCallsCount == 0)
            #expect(desk.current == 2)
            desk.unbind("load")
            #expect(next.cancelCallsCount == 1)
        }
    }

    @Test func replacementDuringMappingCannotEnterTheReducer() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let first = Their.TestHubDriver<Int, DeskError>()
            let next = Their.TestHubDriver<Int, DeskError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let desk = makeIntDesk(0)
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = desk.changes.subscribe(snapshots.append)
            desk.bind(first.hub, id: "load") { event in
                guard case .value(let value) = event else { return nil }
                entered.signal()
                gate.wait()
                return .add(value)
            }
            let blocked = BlockingWork { first.emit(value: 100) }
            try await entered.wait()
            desk.bind(next.hub, id: "load", mapHubReplacement)
            next.emit(value: 2)
            gate.signal()
            try await blocked.value
            #expect(desk.current == 2)
            #expect(snapshots.events == [.value(0), .value(2)])
            #expect(first.cancelCallsCount == 1)
            #expect(next.cancelCallsCount == 0)
            cancel()
        }
    }

    @Test func replacementDuringSourceStartCancelsTheLateHandleExactlyOnce() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let desk = makeIntDesk(0)
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            let cancels = Their.TestCountRecorder()
            let next = Their.TestJobDriver<Int, DeskError>()
            defer { gate.signal() }
            let job = Their.Job<Int, DeskError> { report in
                entered.signal()
                gate.wait()
                report(.value(100))
                return { _ = cancels.increment() }
            }
            let blocked = BlockingWork { desk.bind(job, id: "load", mapJobValue) }
            try await entered.wait()
            desk.bind(next.job, id: "load", mapJobValue)
            next.emit(value: 2)
            gate.signal()
            let oldCancel = try await blocked.value
            oldCancel()
            #expect(desk.current == 2)
            #expect(cancels.count == 1)
            #expect(next.cancelCallsCount == 0)
        }
    }

    @Test func replacementSuppressesAnOldResultAlreadyQueuedBehindAnotherEvent() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let first = Their.TestJobDriver<Int, DeskError>()
            let next = Their.TestJobDriver<Int, DeskError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let desk = makeBlockedDesk(entered, gate)
            desk.bind(first.job, id: "load", mapJobValue)
            let blocked = BlockingWork { desk.send(.block(1)) }
            try await entered.wait()
            first.emit(value: 100)
            desk.bind(next.job, id: "load", mapJobValue)
            next.emit(value: 2)
            gate.signal()
            try await blocked.value
            #expect(desk.current == 3)
        }
    }

    @Test func replaysInitialAndRetainsStateWithoutObservers() async throws {
        try await Their.stress {
            let desk = makeIntDesk(10)
            let first = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = desk.changes.subscribe(first.append)
            #expect(first.events == [.value(10)])
            desk.send(.add(2))
            cancel()
            desk.send(.add(3))
            let late = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let lateCancel = desk.changes.subscribe(late.append)
            #expect(desk.current == 15)
            #expect(first.events == [.value(10), .value(12)])
            #expect(late.events == [.value(15)])
            lateCancel()
        }
    }

    @Test func sourceCancellationCanReenterBindingsWithoutDeadlock() async throws {
        try await Their.stress {
            let desk = makeIntDesk(0)
            let third = Their.TestJobDriver<Int, DeskError>()
            let first = Their.TestJobDriver<Int, DeskError>(onCancel: {
                desk.bind(third.job, id: "load", mapJobValue)
            })
            let second = Their.TestJobDriver<Int, DeskError>()
            desk.bind(first.job, id: "load", mapJobValue)
            desk.bind(second.job, id: "load", mapJobValue)
            third.emit(value: 3)
            #expect(desk.current == 3)
            #expect(first.cancelCallsCount == 1)
            #expect(second.startCallsCount == 0)
            #expect(third.startCallsCount == 1)
            desk.unbind("load")
        }
    }

    @Test func synchronousValueAndTerminalBeforeSubscribeReturnsAreReduced() async throws {
        try await Their.stress {
            let cancels = Their.TestCountRecorder()
            let desk = Their.Desk<[String], String>([]) { state, event in state.append(event) }
            let job = Their.Job<String, DeskError> { report in
                report(.value("loaded"))
                report(.finished)
                return { _ = cancels.increment() }
            }
            desk.bind(job, id: "load") { event in
                switch event {
                case .value(let value): return value
                case .finished: return "finished"
                case .failure: return "failed"
                }
            }
            #expect(desk.current == ["loaded", "finished"])
            #expect(cancels.count == 1)
        }
    }

    @Test func terminalAndReplacementReleaseMappingCaptures() async throws {
        try await Their.stress {
            let desk = makeIntDesk(0)
            let first = Their.TestJobDriver<Int, DeskError>()
            let next = Their.TestJobDriver<Int, DeskError>()
            let released = Their.TestCountRecorder()
            let firstIsReleased: @Sendable () -> Bool
            let nextIsReleased: @Sendable () -> Bool
            do {
                let probe = DeskLifetimeProbe { _ = released.increment() }
                firstIsReleased = { [weak probe] in probe == nil }
                desk.bind(first.job, id: "load") { [probe] event in
                    withExtendedLifetime(probe) { mapJobValue(event) }
                }
            }
            do {
                let probe = DeskLifetimeProbe { _ = released.increment() }
                nextIsReleased = { [weak probe] in probe == nil }
                desk.bind(next.job, id: "load") { [probe] event in
                    withExtendedLifetime(probe) { mapJobValue(event) }
                }
            }
            #expect(firstIsReleased())
            #expect(!nextIsReleased())
            next.emitFinished()
            #expect(nextIsReleased())
            #expect(released.count == 2)
        }
    }

    @Test func terminalQueuedBehindAnotherEventKeepsAllAcceptedValues() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let source = Their.TestJobDriver<Int, DeskError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let desk = makeBlockedDesk(entered, gate)
            desk.bind(source.job, id: "load") { event in
                switch event {
                case .value(let value): return .add(value)
                case .finished: return .add(10)
                case .failure: return nil
                }
            }
            let blocked = BlockingWork { desk.send(.block(0)) }
            try await entered.wait()
            source.emit(value: 2)
            source.emitFinished()
            #expect(source.cancelCallsCount == 1)
            gate.signal()
            try await blocked.value
            #expect(desk.current == 12)
        }
    }

    @Test func unbindAlsoSuppressesQueuedEventsFromAnAlreadyFinishedBinding() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let source = Their.TestJobDriver<Int, DeskError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let desk = makeBlockedDesk(entered, gate)
            desk.bind(source.job, id: "load", mapJobValue)
            let blocked = BlockingWork { desk.send(.block(0)) }
            try await entered.wait()
            source.emit(value: 100)
            source.emitFinished()
            desk.unbind("load")
            gate.signal()
            try await blocked.value
            #expect(desk.current == 0)
        }
    }
}

private enum DeskError: Error, Equatable { case failed }
private enum DeskEvent: Sendable, Equatable {
    case add(Int)
    case block(Int)
    case set(Int)
}

private func reduceInt(_ state: inout Int, _ event: DeskEvent) {
    switch event {
    case .add(let value), .block(let value): state += value
    case .set(let value): state = value
    }
}

private func makeIntDesk(_ initial: Int) -> Their.Desk<Int, DeskEvent> {
    Their.Desk(initial, reducer: reduceInt)
}

private func makeBlockedDesk(
    _ entered: Their.TestSignal,
    _ gate: DispatchSemaphore
) -> Their.Desk<Int, DeskEvent> {
    Their.Desk(0) { state, event in
        if case .block = event { entered.signal(); gate.wait() }
        reduceInt(&state, event)
    }
}

private func mapJobValue(_ event: Their.JobEvent<Int, DeskError>) -> DeskEvent? {
    if case .value(let value) = event { return .add(value) }
    return nil
}

private func mapJobReplacement(_ event: Their.JobEvent<Int, DeskError>) -> DeskEvent? {
    switch event {
    case .value(let value): return .set(value)
    case .failure: return .set(-1)
    case .finished: return nil
    }
}

private func mapHubReplacement(_ event: Their.HubEvent<Int, DeskError>) -> DeskEvent? {
    switch event {
    case .value(let value): return .set(value)
    case .failure: return .set(-1)
    case .finished: return nil
    }
}

private final class DeskLifetimeProbe: Sendable {
    let onDeinit: @Sendable () -> Void
    init(_ onDeinit: @escaping @Sendable () -> Void) { self.onDeinit = onDeinit }
    deinit { onDeinit() }
}
