import Dispatch
import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct BoxTests {

    @Test func bindingIdsAreIndependentAndCancellationIsIdempotent() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, BoxError>()
            let second = Their.TestJobDriver<Int, BoxError>()
            let box = makeIntBox(0)
            let cancel = box.bind(first.job, id: "first", mapJobValue)
            box.bind(second.job, id: "second", mapJobValue)
            cancel()
            cancel()
            box.unbind("first")
            second.emit(value: 3)
            #expect(box.current == 3)
            #expect(first.cancelCallsCount == 1)
            #expect(second.cancelCallsCount == 0)
        }
    }

    @Test func cancellationCannotInterruptAnAlreadyClaimedReducer() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let source = Their.TestJobDriver<Int, BoxError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let box = Their.Box<Int, BoxEvent>(0) { state, event in
                entered.signal()
                gate.wait()
                reduceInt(&state, event)
            }
            box.bind(source.job, id: "load", mapJobValue)
            let blocked = BlockingWork { source.emit(value: 2) }
            try await entered.wait()
            box.unbind("load")
            gate.signal()
            try await blocked.value
            #expect(box.current == 2)
            #expect(source.cancelCallsCount == 1)
        }
    }

    @Test func concurrentEventsDoNotLoseState() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let box = makeIntBox(0)
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<100 { group.addTask { box.send(.add(1)) } }
            }
            #expect(box.current == 100)
        }
    }

    @Test func currentIsPublishedBeforeObserversAndReentrantEventsAreFIFO() async throws {
        try await Their.stress {
            let box = makeIntBox(0)
            let events = Their.TestEventRecorder<Int>()
            let cancel = box.changes.subscribe { event in
                guard case .value(let value) = event else { return }
                #expect(box.current == value)
                events.append(value)
                if value == 1 { box.send(.add(1)) }
            }
            box.send(.add(1))
            #expect(events.events == [0, 1, 2])
            cancel()
        }
    }

    @Test func directJobAndHubEventsUseOneReducerAndOutputSequence() async throws {
        try await Their.stress {
            let reduced = Their.TestEventRecorder<BoxEvent>()
            let box = Their.Box<Int, BoxEvent>(0) { state, event in
                reduced.append(event)
                reduceInt(&state, event)
            }
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = box.changes.subscribe(snapshots.append)
            let job = Their.TestJobDriver<Int, BoxError>()
            let hub = Their.TestHubDriver<Int, BoxError>()
            box.bind(job.job, id: "job") { event in
                switch event {
                case .value(let value): return .add(value)
                case .finished: return .add(10)
                case .failure: return .set(-1)
                }
            }
            box.bind(hub.hub, id: "hub") { event in
                switch event {
                case .value(let value): return .add(value)
                case .finished: return nil
                case .failure: return .set(-1)
                }
            }
            #expect(reduced.events.isEmpty)
            box.send(.add(1))
            job.emit(value: 2)
            hub.emit(value: 3)
            job.emitFinished()
            hub.emit(failure: .failed)
            box.send(.add(0))
            #expect(reduced.events == [.add(1), .add(2), .add(3), .add(10), .set(-1), .add(0)])
            #expect(snapshots.events == [0, 1, 3, 6, 16, -1, -1].map { .value($0) })
            #expect(box.current == -1)
            #expect(job.cancelCallsCount == 1)
            #expect(hub.cancelCallsCount == 1)
            cancel()
        }
    }

    @Test func discardedCancelDoesNotEndBindingAndTerminalDoesNotResetState() async throws {
        try await Their.stress {
            let source = Their.TestJobDriver<Int, BoxError>()
            let box = makeIntBox(1)
            box.bind(source.job, id: "load", mapJobValue)
            source.emit(value: 4)
            source.emitFinished()
            #expect(box.current == 5)
            #expect(source.cancelCallsCount == 1)
            box.send(.add(1))
            #expect(box.current == 6)
        }
    }

    @Test func failureIsAnOrdinaryBindingEventAndFreshBindingCanRetry() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, BoxError>()
            let next = Their.TestJobDriver<Int, BoxError>()
            let box = makeIntBox(0)
            box.bind(first.job, id: "load", mapJobReplacement)
            first.emit(failure: .failed)
            #expect(box.current == -1)
            box.bind(next.job, id: "load", mapJobReplacement)
            next.emit(value: 7)
            #expect(box.current == 7)
        }
    }

    @Test func hubBindingRunsWithoutUIAndUnbindRemovesOnlyItsSubscription() async throws {
        try await Their.stress {
            let source = Their.TestHubDriver<Int, BoxError>()
            let external = Their.TestEventRecorder<Their.HubEvent<Int, BoxError>>()
            let externalCancel = source.hub.subscribe(external.append)
            let box = makeIntBox(0)
            box.bind(source.hub, id: "live", mapHubReplacement)
            source.emit(value: 4)
            box.unbind("live")
            source.emit(value: 9)
            #expect(box.current == 4)
            #expect(external.events == [.value(4), .value(9)])
            #expect(source.startCallsCount == 1)
            #expect(source.cancelCallsCount == 0)
            externalCancel()
            #expect(source.cancelCallsCount == 1)
        }
    }

    @Test func hubTerminalRetiresBindingAndNewHubCanUseTheSameId() async throws {
        try await Their.stress {
            let first = Their.TestHubDriver<Int, BoxError>()
            let next = Their.TestHubDriver<Int, BoxError>()
            let box = makeIntBox(0)
            box.bind(first.hub, id: "live", mapHubReplacement)
            first.emit(failure: .failed)
            box.bind(next.hub, id: "live", mapHubReplacement)
            next.emit(value: 8)
            #expect(box.current == 8)
            #expect(first.cancelCallsCount == 1)
        }
    }

    @Test func ignoredInputsDoNotReduceOrPublishButStillRetireBindings() async throws {
        try await Their.stress {
            let reductions = Their.TestCountRecorder()
            let mappings = Their.TestCountRecorder()
            let box = Their.Box<Int, BoxEvent>(9) { state, event in
                _ = reductions.increment()
                reduceInt(&state, event)
            }
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = box.changes.subscribe(snapshots.append)
            let job = Their.TestJobDriver<Int, BoxError>()
            let hub = Their.TestHubDriver<Int, BoxError>()
            box.bind(job.job, id: "job") { _ in _ = mappings.increment(); return nil }
            box.bind(hub.hub, id: "hub") { _ in _ = mappings.increment(); return nil }
            job.emit(value: 7)
            job.emit(failure: .failed)
            hub.emit(value: 8)
            hub.emitFinished()
            #expect(mappings.count == 4)
            #expect(reductions.count == 0)
            #expect(snapshots.events == [.value(9)])
            #expect(job.cancelCallsCount == 1)
            #expect(hub.cancelCallsCount == 1)
            box.send(.add(1))
            #expect(reductions.count == 1)
            #expect(snapshots.events == [.value(9), .value(10)])
            cancel()
        }
    }

    @Test func mappingCanReenterSendAndUnbindWithoutDeadlock() async throws {
        try await Their.stress {
            let box = makeIntBox(0)
            let source = Their.TestJobDriver<Int, BoxError>()
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = box.changes.subscribe(snapshots.append)
            box.bind(source.job, id: "load") { [weak box] event in
                guard case .value(let value) = event else { return nil }
                box?.send(.add(1))
                box?.unbind("load")
                return .add(value)
            }
            source.emit(value: 100)
            #expect(box.current == 1)
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
            let box = makeBlockedBox(entered, gate)
            let job = Their.TestJobDriver<Int, BoxError>()
            let hub = Their.TestHubDriver<Int, BoxError>()
            box.bind(job.job, id: "job", mapJobValue)
            box.bind(hub.hub, id: "hub") { event in
                if case .value(let value) = event { return .add(value) }
                return nil
            }
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = box.changes.subscribe(snapshots.append)
            let blocked = BlockingWork { box.send(.block(1)) }
            try await entered.wait()
            box.send(.add(2))
            job.emit(value: 3)
            hub.emit(value: 4)
            box.send(.add(5))
            gate.signal()
            try await blocked.value
            #expect(box.current == 15)
            #expect(snapshots.events == [0, 1, 3, 6, 10, 15].map { .value($0) })
            cancel()
        }
    }

    @Test func queuedEventsDoNotRetainMappingCapturesAfterTerminal() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let box = makeBlockedBox(entered, gate)
            let source = Their.TestJobDriver<Int, BoxError>()
            let released = Their.TestCountRecorder()
            let isReleased: @Sendable () -> Bool
            do {
                let probe = BoxLifetimeProbe { _ = released.increment() }
                isReleased = { [weak probe] in probe == nil }
                box.bind(source.job, id: "load") { [probe] event in
                    withExtendedLifetime(probe) { mapJobValue(event) }
                }
            }
            let blocked = BlockingWork { box.send(.block(0)) }
            try await entered.wait()
            source.emit(value: 2)
            source.emitFinished()
            #expect(isReleased())
            #expect(released.count == 1)
            #expect(box.current == 0)
            gate.signal()
            try await blocked.value
            #expect(box.current == 2)
        }
    }

    @Test func releasingBoxCancelsSourcesEvenIfChangesAndCancelAreRetained() async throws {
        try await Their.stress {
            let source = Their.TestJobDriver<Int, BoxError>()
            var box: Their.Box<Int, BoxEvent>? = makeIntBox(0)
            let isReleased: @Sendable () -> Bool = { [weak box] in box == nil }
            let changes = box!.changes
            let bindingCancel = box!.bind(source.job, id: "load", mapJobValue)
            let events = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let observerCancel = changes.subscribe(events.append)
            source.emit(value: 2)
            box = nil
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
            let box = Their.Box<BoxLifetimeProbe?, BoxLifetimeProbe?>(nil) { state, event in
                state = event
            }
            let released = Their.TestCountRecorder()
            box.send(BoxLifetimeProbe { [weak box] in
                #expect(box?.current == nil)
                _ = released.increment()
            })
            box.send(nil)
            #expect(released.count == 1)
        }
    }

    @Test func replacementCancelsOldSourceAndOldCancelCannotCancelReplacement() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, BoxError>()
            let next = Their.TestJobDriver<Int, BoxError>()
            let box = makeIntBox(0)
            let oldCancel = box.bind(first.job, id: "load", mapJobValue)
            let oldReport = first.report
            box.bind(next.job, id: "load", mapJobValue)
            oldCancel()
            oldReport?(.value(100))
            next.emit(value: 2)
            #expect(first.cancelCallsCount == 1)
            #expect(next.cancelCallsCount == 0)
            #expect(box.current == 2)
            box.unbind("load")
            #expect(next.cancelCallsCount == 1)
        }
    }

    @Test func replacementDuringMappingCannotEnterTheReducer() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let first = Their.TestHubDriver<Int, BoxError>()
            let next = Their.TestHubDriver<Int, BoxError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let box = makeIntBox(0)
            let snapshots = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = box.changes.subscribe(snapshots.append)
            box.bind(first.hub, id: "load") { event in
                guard case .value(let value) = event else { return nil }
                entered.signal()
                gate.wait()
                return .add(value)
            }
            let blocked = BlockingWork { first.emit(value: 100) }
            try await entered.wait()
            box.bind(next.hub, id: "load", mapHubReplacement)
            next.emit(value: 2)
            gate.signal()
            try await blocked.value
            #expect(box.current == 2)
            #expect(snapshots.events == [.value(0), .value(2)])
            #expect(first.cancelCallsCount == 1)
            #expect(next.cancelCallsCount == 0)
            cancel()
        }
    }

    @Test func replacementDuringSourceStartCancelsTheLateHandleExactlyOnce() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let box = makeIntBox(0)
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            let cancels = Their.TestCountRecorder()
            let next = Their.TestJobDriver<Int, BoxError>()
            defer { gate.signal() }
            let job = Their.Job<Int, BoxError> { report in
                entered.signal()
                gate.wait()
                report(.value(100))
                return { _ = cancels.increment() }
            }
            let blocked = BlockingWork { box.bind(job, id: "load", mapJobValue) }
            try await entered.wait()
            box.bind(next.job, id: "load", mapJobValue)
            next.emit(value: 2)
            gate.signal()
            let oldCancel = try await blocked.value
            oldCancel()
            #expect(box.current == 2)
            #expect(cancels.count == 1)
            #expect(next.cancelCallsCount == 0)
        }
    }

    @Test func replacementSuppressesAnOldResultAlreadyQueuedBehindAnotherEvent() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let first = Their.TestJobDriver<Int, BoxError>()
            let next = Their.TestJobDriver<Int, BoxError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let box = makeBlockedBox(entered, gate)
            box.bind(first.job, id: "load", mapJobValue)
            let blocked = BlockingWork { box.send(.block(1)) }
            try await entered.wait()
            first.emit(value: 100)
            box.bind(next.job, id: "load", mapJobValue)
            next.emit(value: 2)
            gate.signal()
            try await blocked.value
            #expect(box.current == 3)
        }
    }

    @Test func replaysInitialAndRetainsStateWithoutObservers() async throws {
        try await Their.stress {
            let box = makeIntBox(10)
            let first = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = box.changes.subscribe(first.append)
            #expect(first.events == [.value(10)])
            box.send(.add(2))
            cancel()
            box.send(.add(3))
            let late = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let lateCancel = box.changes.subscribe(late.append)
            #expect(box.current == 15)
            #expect(first.events == [.value(10), .value(12)])
            #expect(late.events == [.value(15)])
            lateCancel()
        }
    }

    @Test func sourceCancellationCanReenterBindingsWithoutDeadlock() async throws {
        try await Their.stress {
            let box = makeIntBox(0)
            let third = Their.TestJobDriver<Int, BoxError>()
            let first = Their.TestJobDriver<Int, BoxError>(onCancel: {
                box.bind(third.job, id: "load", mapJobValue)
            })
            let second = Their.TestJobDriver<Int, BoxError>()
            box.bind(first.job, id: "load", mapJobValue)
            box.bind(second.job, id: "load", mapJobValue)
            third.emit(value: 3)
            #expect(box.current == 3)
            #expect(first.cancelCallsCount == 1)
            #expect(second.startCallsCount == 0)
            #expect(third.startCallsCount == 1)
            box.unbind("load")
        }
    }

    @Test func synchronousValueAndTerminalBeforeSubscribeReturnsAreReduced() async throws {
        try await Their.stress {
            let cancels = Their.TestCountRecorder()
            let box = Their.Box<[String], String>([]) { state, event in state.append(event) }
            let job = Their.Job<String, BoxError> { report in
                report(.value("loaded"))
                report(.finished)
                return { _ = cancels.increment() }
            }
            box.bind(job, id: "load") { event in
                switch event {
                case .value(let value): return value
                case .finished: return "finished"
                case .failure: return "failed"
                }
            }
            #expect(box.current == ["loaded", "finished"])
            #expect(cancels.count == 1)
        }
    }

    @Test func terminalAndReplacementReleaseMappingCaptures() async throws {
        try await Their.stress {
            let box = makeIntBox(0)
            let first = Their.TestJobDriver<Int, BoxError>()
            let next = Their.TestJobDriver<Int, BoxError>()
            let released = Their.TestCountRecorder()
            let firstIsReleased: @Sendable () -> Bool
            let nextIsReleased: @Sendable () -> Bool
            do {
                let probe = BoxLifetimeProbe { _ = released.increment() }
                firstIsReleased = { [weak probe] in probe == nil }
                box.bind(first.job, id: "load") { [probe] event in
                    withExtendedLifetime(probe) { mapJobValue(event) }
                }
            }
            do {
                let probe = BoxLifetimeProbe { _ = released.increment() }
                nextIsReleased = { [weak probe] in probe == nil }
                box.bind(next.job, id: "load") { [probe] event in
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
            let source = Their.TestJobDriver<Int, BoxError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let box = makeBlockedBox(entered, gate)
            box.bind(source.job, id: "load") { event in
                switch event {
                case .value(let value): return .add(value)
                case .finished: return .add(10)
                case .failure: return nil
                }
            }
            let blocked = BlockingWork { box.send(.block(0)) }
            try await entered.wait()
            source.emit(value: 2)
            source.emitFinished()
            #expect(source.cancelCallsCount == 1)
            gate.signal()
            try await blocked.value
            #expect(box.current == 12)
        }
    }

    @Test func unbindAlsoSuppressesQueuedEventsFromAnAlreadyFinishedBinding() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let source = Their.TestJobDriver<Int, BoxError>()
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let box = makeBlockedBox(entered, gate)
            box.bind(source.job, id: "load", mapJobValue)
            let blocked = BlockingWork { box.send(.block(0)) }
            try await entered.wait()
            source.emit(value: 100)
            source.emitFinished()
            box.unbind("load")
            gate.signal()
            try await blocked.value
            #expect(box.current == 0)
        }
    }
}

private enum BoxError: Error, Equatable { case failed }
private enum BoxEvent: Sendable, Equatable {
    case add(Int)
    case block(Int)
    case set(Int)
}

private func reduceInt(_ state: inout Int, _ event: BoxEvent) {
    switch event {
    case .add(let value), .block(let value): state += value
    case .set(let value): state = value
    }
}

private func makeIntBox(_ initial: Int) -> Their.Box<Int, BoxEvent> {
    Their.Box(initial, reducer: reduceInt)
}

private func makeBlockedBox(
    _ entered: Their.TestSignal,
    _ gate: DispatchSemaphore
) -> Their.Box<Int, BoxEvent> {
    Their.Box(0) { state, event in
        if case .block = event { entered.signal(); gate.wait() }
        reduceInt(&state, event)
    }
}

private func mapJobValue(_ event: Their.JobEvent<Int, BoxError>) -> BoxEvent? {
    if case .value(let value) = event { return .add(value) }
    return nil
}

private func mapJobReplacement(_ event: Their.JobEvent<Int, BoxError>) -> BoxEvent? {
    switch event {
    case .value(let value): return .set(value)
    case .failure: return .set(-1)
    case .finished: return nil
    }
}

private func mapHubReplacement(_ event: Their.HubEvent<Int, BoxError>) -> BoxEvent? {
    switch event {
    case .value(let value): return .set(value)
    case .failure: return .set(-1)
    case .finished: return nil
    }
}

private final class BoxLifetimeProbe: Sendable {
    let onDeinit: @Sendable () -> Void
    init(_ onDeinit: @escaping @Sendable () -> Void) { self.onDeinit = onDeinit }
    deinit { onDeinit() }
}
