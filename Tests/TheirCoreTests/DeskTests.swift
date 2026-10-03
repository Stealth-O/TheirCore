import Dispatch
import Foundation
import Testing
@testable import TheirCore
import TheirCoreTesting

@Suite
struct DeskTests {

    @Test func replaysInitialAndRetainsUpdatesWithoutObservers() async throws {
        try await Their.stress {
            let desk = Their.Desk(10)
            let first = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let cancel = desk.changes.subscribe(first.append)
            #expect(first.events == [.value(10)])
            desk.update { $0 += 2 }
            cancel()
            desk.update { $0 += 3 }
            let late = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let lateCancel = desk.changes.subscribe(late.append)
            #expect(desk.current == 15)
            #expect(first.events == [.value(10), .value(12)])
            #expect(late.events == [.value(15)])
            lateCancel()
        }
    }

    @Test func currentIsPublishedBeforeObserversAndReentrantUpdatesAreFIFO() async throws {
        try await Their.stress {
            let desk = Their.Desk(0)
            let events = Their.TestEventRecorder<Int>()
            let cancel = desk.changes.subscribe { event in
                guard case .value(let value) = event else { return }
                #expect(desk.current == value)
                events.append(value)
                if value == 1 { desk.update { $0 += 1 } }
            }
            desk.update { $0 += 1 }
            #expect(events.events == [0, 1, 2])
            cancel()
        }
    }

    @Test func concurrentUpdatesDoNotLoseState() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let desk = Their.Desk(0)
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<100 { group.addTask { desk.update { $0 += 1 } } }
            }
            #expect(desk.current == 100)
        }
    }

    @Test func discardedCancelDoesNotEndBindingAndTerminalDoesNotResetState() async throws {
        try await Their.stress {
            let source = Their.TestJobDriver<Int, DeskError>()
            let desk = Their.Desk(1)
            desk.bind(source.job, id: "load") { state, event in
                if case .value(let value) = event { state += value }
            }
            source.emit(value: 4)
            source.emitFinished()
            #expect(desk.current == 5)
            #expect(source.cancelCallsCount == 1)
            desk.update { $0 += 1 }
            #expect(desk.current == 6)
        }
    }

    @Test func synchronousValueAndTerminalBeforeSubscribeReturnsAreReduced() async throws {
        try await Their.stress {
            let cancels = Their.TestCountRecorder()
            let desk = Their.Desk([String]())
            let job = Their.Job<String, DeskError> { report in
                report(.value("loaded"))
                report(.finished)
                return { _ = cancels.increment() }
            }
            desk.bind(job, id: "load") { state, event in
                switch event {
                case .value(let value): state.append(value)
                case .finished: state.append("finished")
                case .failure: state.append("failed")
                }
            }
            #expect(desk.current == ["loaded", "finished"])
            #expect(cancels.count == 1)
        }
    }

    @Test func failureIsAnOrdinaryBindingEventAndFreshBindingCanRetry() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, DeskError>()
            let next = Their.TestJobDriver<Int, DeskError>()
            let desk = Their.Desk(0)
            desk.bind(first.job, id: "load") { state, event in
                if case .failure = event { state = -1 }
            }
            first.emit(failure: .failed)
            #expect(desk.current == -1)
            desk.bind(next.job, id: "load") { state, event in
                if case .value(let value) = event { state = value }
            }
            next.emit(value: 7)
            #expect(desk.current == 7)
        }
    }

    @Test func replacementCancelsOldSourceAndOldCancelCannotCancelReplacement() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, DeskError>()
            let next = Their.TestJobDriver<Int, DeskError>()
            let desk = Their.Desk(0)
            let oldCancel = desk.bind(first.job, id: "load", addJobValue)
            let oldReport = first.report
            desk.bind(next.job, id: "load", addJobValue)
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

    @Test func bindingIdsAreIndependentAndCancellationIsIdempotent() async throws {
        try await Their.stress {
            let first = Their.TestJobDriver<Int, DeskError>()
            let second = Their.TestJobDriver<Int, DeskError>()
            let desk = Their.Desk(0)
            let cancel = desk.bind(first.job, id: "first", addJobValue)
            desk.bind(second.job, id: "second", addJobValue)
            cancel()
            cancel()
            desk.unbind("first")
            second.emit(value: 3)
            #expect(desk.current == 3)
            #expect(first.cancelCallsCount == 1)
            #expect(second.cancelCallsCount == 0)
        }
    }

    @Test func hubBindingRunsWithoutUIAndUnbindRemovesOnlyItsSubscription() async throws {
        try await Their.stress {
            let source = Their.TestHubDriver<Int, DeskError>()
            let external = Their.TestEventRecorder<Their.HubEvent<Int, DeskError>>()
            let externalCancel = source.hub.subscribe(external.append)
            let desk = Their.Desk(0)
            desk.bind(source.hub, id: "live") { state, event in
                if case .value(let value) = event { state = value }
            }
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
            let desk = Their.Desk(0)
            desk.bind(first.hub, id: "live") { state, event in
                if case .failure = event { state = -1 }
            }
            first.emit(failure: .failed)
            desk.bind(next.hub, id: "live") { state, event in
                if case .value(let value) = event { state = value }
            }
            next.emit(value: 8)
            #expect(desk.current == 8)
            #expect(first.cancelCallsCount == 1)
        }
    }

    @Test func replacementSuppressesAnOldResultAlreadyQueuedBehindAnotherReducer() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let first = Their.TestJobDriver<Int, DeskError>()
            let next = Their.TestJobDriver<Int, DeskError>()
            let desk = Their.Desk(0)
            desk.bind(first.job, id: "load", addJobValue)
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let blocked = BlockingWork {
                desk.update { state in
                    entered.signal()
                    gate.wait()
                    state += 1
                }
            }
            try await entered.wait()
            first.emit(value: 100)
            desk.bind(next.job, id: "load", addJobValue)
            next.emit(value: 2)
            gate.signal()
            try await blocked.value
            #expect(desk.current == 3)
        }
    }

    @Test func terminalQueuedBehindAnotherReducerKeepsAllAcceptedValues() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let source = Their.TestJobDriver<Int, DeskError>()
            let desk = Their.Desk(0)
            desk.bind(source.job, id: "load") { state, event in
                switch event {
                case .value(let value): state += value
                case .finished: state += 10
                case .failure: break
                }
            }
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let blocked = BlockingWork { desk.update { _ in entered.signal(); gate.wait() } }
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
            let desk = Their.Desk(0)
            desk.bind(source.job, id: "load", addJobValue)
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            let blocked = BlockingWork { desk.update { _ in entered.signal(); gate.wait() } }
            try await entered.wait()
            source.emit(value: 100)
            source.emitFinished()
            desk.unbind("load")
            gate.signal()
            try await blocked.value
            #expect(desk.current == 0)
        }
    }

    @Test func cancellationCannotInterruptAnAlreadyClaimedReducer() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let source = Their.TestJobDriver<Int, DeskError>()
            let desk = Their.Desk(0)
            let entered = Their.TestSignal()
            let gate = DispatchSemaphore(value: 0)
            defer { gate.signal() }
            desk.bind(source.job, id: "load") { state, event in
                guard case .value(let value) = event else { return }
                entered.signal()
                gate.wait()
                state += value
            }
            let blocked = BlockingWork { source.emit(value: 2) }
            try await entered.wait()
            desk.unbind("load")
            gate.signal()
            try await blocked.value
            #expect(desk.current == 2)
            #expect(source.cancelCallsCount == 1)
        }
    }

    @Test func replacementDuringSourceStartCancelsTheLateHandleExactlyOnce() async throws {
        try await Their.stress(timeout: .seconds(2)) {
            let desk = Their.Desk(0)
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
            let blocked = BlockingWork { desk.bind(job, id: "load", addJobValue) }
            try await entered.wait()
            desk.bind(next.job, id: "load", addJobValue)
            next.emit(value: 2)
            gate.signal()
            let oldCancel = try await blocked.value
            oldCancel()
            #expect(desk.current == 2)
            #expect(cancels.count == 1)
            #expect(next.cancelCallsCount == 0)
        }
    }

    @Test func sourceCancellationCanReenterBindingsWithoutDeadlock() async throws {
        try await Their.stress {
            let desk = Their.Desk(0)
            let third = Their.TestJobDriver<Int, DeskError>()
            let first = Their.TestJobDriver<Int, DeskError>(onCancel: {
                desk.bind(third.job, id: "load", addJobValue)
            })
            let second = Their.TestJobDriver<Int, DeskError>()
            desk.bind(first.job, id: "load", addJobValue)
            desk.bind(second.job, id: "load", addJobValue)
            third.emit(value: 3)
            #expect(desk.current == 3)
            #expect(first.cancelCallsCount == 1)
            #expect(second.startCallsCount == 0)
            #expect(third.startCallsCount == 1)
            desk.unbind("load")
        }
    }

    @Test func releasingDeskCancelsSourcesEvenIfChangesAndCancelAreRetained() async throws {
        try await Their.stress {
            let source = Their.TestJobDriver<Int, DeskError>()
            var desk: Their.Desk<Int>? = Their.Desk(0)
            weak var weakDesk = desk
            let changes = desk!.changes
            let bindingCancel = desk!.bind(source.job, id: "load", addJobValue)
            let events = Their.TestEventRecorder<Their.HubEvent<Int, Never>>()
            let observerCancel = changes.subscribe(events.append)
            source.emit(value: 2)
            desk = nil
            #expect(weakDesk == nil)
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

    @Test func terminalAndReplacementReleaseReducerCaptures() async throws {
        try await Their.stress {
            let desk = Their.Desk(0)
            let first = Their.TestJobDriver<Int, DeskError>()
            let next = Their.TestJobDriver<Int, DeskError>()
            let released = Their.TestCountRecorder()
            weak var firstCapture: DeskLifetimeProbe?
            weak var nextCapture: DeskLifetimeProbe?
            do {
                let probe = DeskLifetimeProbe { _ = released.increment() }
                firstCapture = probe
                desk.bind(first.job, id: "load") { [probe] state, event in
                    withExtendedLifetime(probe) { addJobValue(&state, event) }
                }
            }
            do {
                let probe = DeskLifetimeProbe { _ = released.increment() }
                nextCapture = probe
                desk.bind(next.job, id: "load") { [probe] state, event in
                    withExtendedLifetime(probe) { addJobValue(&state, event) }
                }
            }
            #expect(firstCapture == nil)
            #expect(nextCapture != nil)
            next.emitFinished()
            #expect(nextCapture == nil)
            #expect(released.count == 2)
        }
    }

    @Test func replacedSnapshotDestructorCanReadCurrentState() async throws {
        try await Their.stress {
            let desk = Their.Desk<DeskLifetimeProbe?>(nil)
            let released = Their.TestCountRecorder()
            desk.update { [weak desk] state in
                state = DeskLifetimeProbe { [weak desk] in
                    #expect(desk?.current == nil)
                    _ = released.increment()
                }
            }
            desk.update { $0 = nil }
            #expect(released.count == 1)
        }
    }
}

private enum DeskError: Error, Equatable { case failed }

private func addJobValue(_ state: inout Int, _ event: Their.JobEvent<Int, DeskError>) {
    if case .value(let value) = event { state += value }
}

private final class DeskLifetimeProbe: Sendable {
    let onDeinit: @Sendable () -> Void
    init(_ onDeinit: @escaping @Sendable () -> Void) { self.onDeinit = onDeinit }
    deinit { onDeinit() }
}
