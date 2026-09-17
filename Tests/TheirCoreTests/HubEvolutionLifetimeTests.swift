import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct HubEvolutionLifetimeTests {

    @Test func deinitReleasesSharedStateOutsideLock() async throws {
        try await Their.stress {
            let destructor = HubEvolutionDestructorAction()
            let upstream = Their.TestHubDriver<Int, HubEvolutionLifetimeError>()
            var hub: Their.Hub<Int, HubEvolutionLifetimeError>?
            do {
                let evolved = makeEvolvedHubForTests(
                    initial: Optional<HubEvolutionLifetimeToken>.none,
                    upstream: upstream.hub
                ) { state, value -> Int? in
                    state = HubEvolutionLifetimeToken(id: value, onDeinit: destructor.run)
                    return value
                }
                destructor.configure(cancel: evolved.cancelState, probe: evolved.isLockAvailable)
                hub = evolved.hub
            }
            _ = hub?.subscribe { _ in }
            upstream.emit(value: 1)
            #expect(destructor.observations.events.isEmpty)

            hub = nil
            try await destructor.completed.wait()
            #expect(destructor.observations.events == [true])
            #expect(destructor.reentries.count == 1)
            #expect(upstream.cancelCallsCount == 1)
        }
    }

    @Test func nonlastUnsubscribeKeepsSharedStateUntilLastSubscriberCancels() async throws {
        try await Their.stress {
            let destructor = HubEvolutionDestructorAction()
            let upstream = Their.TestHubDriver<Int, HubEvolutionLifetimeError>()
            let evolved = makeEvolvedHubForTests(
                initial: Optional<HubEvolutionLifetimeToken>.none,
                upstream: upstream.hub
            ) { state, value -> Int? in
                if state == nil {
                    state = HubEvolutionLifetimeToken(id: value, onDeinit: destructor.run)
                }
                return value
            }
            let firstCancel = evolved.hub.subscribe { _ in }
            let secondCancel = evolved.hub.subscribe { _ in }
            destructor.configure(cancel: secondCancel, probe: evolved.isLockAvailable)
            defer { destructor.clear() }
            upstream.emit(value: 1)

            firstCancel()
            #expect(destructor.observations.events.isEmpty)
            #expect(upstream.cancelCallsCount == 0)
            secondCancel()
            try await destructor.completed.wait()
            #expect(destructor.observations.events == [true])
            #expect(destructor.reentries.count == 1)
            #expect(upstream.cancelCallsCount == 1)
            withExtendedLifetime(evolved.hub) {}
        }
    }

    @Test(arguments: HubEvolutionLifetimeEnd.allCases)
    private func queuedInputCleanupAllowsItsDestructorToCancel(_ ending: HubEvolutionLifetimeEnd) async throws {
        try await Their.stress {
            let cancelSlot = Their.Lock<Their.HubCancel?>(nil)
            let destructor = HubEvolutionDestructorAction()
            let events = Their.TestEventRecorder<Their.HubEvent<Int, HubEvolutionLifetimeError>>()
            let inputSink = Their.Lock<Their.HubSink<HubEvolutionLifetimeInput, HubEvolutionLifetimeError>?>(nil)
            let transformedTokens = Their.TestCountRecorder()
            let upstreamCancels = Their.TestCancelRecorder()
            let upstream = Their.Hub<HubEvolutionLifetimeInput, HubEvolutionLifetimeError>(
                misuseHandler: { _ in Issue.record("The source accepts this shared subscription.") },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    inputSink.withLock { $0 = sink }
                    return upstreamCancels.cancel()
                }
            )
            let send: @Sendable (Their.HubEvent<HubEvolutionLifetimeInput, HubEvolutionLifetimeError>) -> Void = { event in
                inputSink.withLock { $0 }?(event)
            }
            let queueToken: @Sendable () -> Void = {
                send(.value(.token(HubEvolutionLifetimeToken(id: 2, onDeinit: destructor.run))))
            }
            let evolved = makeEvolvedHubForTests(initial: (), upstream: upstream) { _, input -> Int? in
                switch input {
                case .start:
                    switch ending {
                    case .cancel:
                        queueToken()
                        cancelSlot.withLock { $0 }?()
                    case .finished:
                        send(.finished)
                        queueToken()
                    case .failure:
                        send(.failure(.sample))
                        queueToken()
                    }
                    return 1
                case .token:
                    _ = transformedTokens.increment()
                    return 2
                }
            }
            let cancel = evolved.hub.subscribe(events.append(_:))
            cancelSlot.withLock { $0 = cancel }
            destructor.configure(cancel: cancel, probe: evolved.isLockAvailable)
            defer {
                destructor.clear()
                cancelSlot.withLock { $0 = nil }
            }
            send(.value(.start))

            try await destructor.completed.wait()
            #expect(destructor.observations.events == [true])
            #expect(destructor.reentries.count == 1)
            #expect(transformedTokens.count == 0)
            let expected: [Their.HubEvent<Int, HubEvolutionLifetimeError>] = ending == .cancel ? [] : ending.expectedEvents
            let expectedCancels = ending == .cancel ? 1 : 0
            #expect(events.events == expected)
            #expect(upstreamCancels.cancelCallsCount == expectedCancels)
            cancel()
            send(.value(.start))
            #expect(events.events == expected)
            #expect(destructor.observations.events == [true])
            #expect(upstreamCancels.cancelCallsCount == expectedCancels)
            withExtendedLifetime(evolved.hub) {}
        }
    }

    @Test func replacingReplayValueAllowsItsDestructorToCancel() async throws {
        try await Their.stress {
            let destructor = HubEvolutionDestructorAction()
            let events = Their.TestEventRecorder<Int>()
            let secondDestructions = Their.TestCountRecorder()
            let upstream = Their.TestHubDriver<Int, HubEvolutionLifetimeError>()
            let evolved = makeEvolvedHubForTests(
                initial: (),
                replayLatest: true,
                upstream: upstream.hub
            ) { _, value -> HubEvolutionLifetimeToken? in
                HubEvolutionLifetimeToken(id: value) {
                    if value == 1 {
                        destructor.run()
                    } else {
                        _ = secondDestructions.increment()
                    }
                }
            }
            let cancel = evolved.hub.subscribe { event in
                if case .value(let value) = event {
                    events.append(value.id)
                }
            }
            destructor.configure(cancel: cancel, probe: evolved.isLockAvailable)
            defer { destructor.clear() }
            upstream.emit(value: 1)
            #expect(destructor.observations.events.isEmpty)
            upstream.emit(value: 2)

            try await destructor.completed.wait()
            #expect(destructor.observations.events == [true])
            #expect(destructor.reentries.count == 1)
            #expect(secondDestructions.count == 1)
            #expect(events.events == [1, 2])
            #expect(upstream.cancelCallsCount == 1)
            upstream.emit(value: 3)
            cancel()
            #expect(events.events == [1, 2])
            #expect(secondDestructions.count == 1)
            withExtendedLifetime(evolved.hub) {}
        }
    }

    @Test func replacingStateOnSuppressedOutputAllowsItsDestructorToCancel() async throws {
        try await Their.stress {
            let destructor = HubEvolutionDestructorAction()
            let events = Their.TestEventRecorder<Their.HubEvent<Int, HubEvolutionLifetimeError>>()
            let upstream = Their.TestHubDriver<Int, HubEvolutionLifetimeError>()
            let evolved = makeEvolvedHubForTests(
                initial: Optional<HubEvolutionLifetimeToken>.none,
                upstream: upstream.hub
            ) { state, value -> Int? in
                if value == 1 {
                    state = HubEvolutionLifetimeToken(id: value, onDeinit: destructor.run)
                    return value
                }
                state = nil
                return nil
            }
            let cancel = evolved.hub.subscribe(events.append(_:))
            destructor.configure(cancel: cancel, probe: evolved.isLockAvailable)
            defer { destructor.clear() }
            upstream.emit(value: 1)
            upstream.emit(value: 2)

            try await destructor.completed.wait()
            #expect(destructor.observations.events == [true])
            #expect(destructor.reentries.count == 1)
            #expect(upstream.cancelCallsCount == 1)
            #expect(events.events == [.value(1)])
            withExtendedLifetime(evolved.hub) {}
        }
    }

    @Test(arguments: HubEvolutionLifetimeEnd.allCases)
    private func replayResetReleasesLatestValueOutsideLock(_ ending: HubEvolutionLifetimeEnd) async throws {
        try await Their.stress {
            let destructor = HubEvolutionDestructorAction()
            let events = Their.TestEventRecorder<Int>()
            let upstream = Their.TestHubDriver<Int, HubEvolutionLifetimeError>()
            let evolved = makeEvolvedHubForTests(
                initial: (),
                replayLatest: true,
                upstream: upstream.hub
            ) { _, value -> HubEvolutionLifetimeToken? in
                HubEvolutionLifetimeToken(id: value, onDeinit: destructor.run)
            }
            let cancel = evolved.hub.subscribe { event in
                if case .value(let value) = event {
                    events.append(value.id)
                }
            }
            destructor.configure(cancel: cancel, probe: evolved.isLockAvailable)
            defer { destructor.clear() }
            upstream.emit(value: 1)
            #expect(events.events == [1])
            #expect(destructor.observations.events.isEmpty)

            ending.finish(cancel: cancel, upstream: upstream)
            try await destructor.completed.wait()
            #expect(destructor.observations.events == [true])
            #expect(destructor.reentries.count == 1)
            #expect(upstream.cancelCallsCount == 1)
            cancel()
            upstream.emit(value: 2)
            #expect(events.events == [1])
            #expect(destructor.observations.events == [true])
            withExtendedLifetime(evolved.hub) {}
        }
    }

    @Test(arguments: HubEvolutionLifetimeEnd.allCases)
    private func stateResetAllowsItsDestructorToCancel(_ ending: HubEvolutionLifetimeEnd) async throws {
        try await Their.stress {
            let destructor = HubEvolutionDestructorAction()
            let events = Their.TestEventRecorder<Their.HubEvent<Int, HubEvolutionLifetimeError>>()
            let upstream = Their.TestHubDriver<Int, HubEvolutionLifetimeError>()
            let evolved = makeEvolvedHubForTests(
                initial: Optional<HubEvolutionLifetimeToken>.none,
                upstream: upstream.hub
            ) { state, value -> Int? in
                state = HubEvolutionLifetimeToken(id: value, onDeinit: destructor.run)
                return value
            }
            let cancel = evolved.hub.subscribe(events.append(_:))
            destructor.configure(cancel: cancel, probe: evolved.isLockAvailable)
            defer { destructor.clear() }
            upstream.emit(value: 1)
            #expect(destructor.observations.events.isEmpty)

            ending.finish(cancel: cancel, upstream: upstream)
            try await destructor.completed.wait()
            #expect(destructor.observations.events == [true])
            #expect(destructor.reentries.count == 1)
            #expect(events.events == ending.expectedEvents)
            #expect(upstream.cancelCallsCount == 1)
            cancel()
            upstream.emit(value: 2)
            #expect(events.events == ending.expectedEvents)
            #expect(destructor.observations.events == [true])
            withExtendedLifetime(evolved.hub) {}
        }
    }

    @Test(arguments: [HubEvolutionLifetimeEnd.finished, .failure])
    private func terminalRestartSurvivesOldStateAndReplayDestructorCancellation(_ ending: HubEvolutionLifetimeEnd) async throws {
        try await Their.stress {
            let destructor = HubEvolutionDestructorAction()
            let firstValues = Their.TestEventRecorder<Int>()
            let nextCancel = Their.Lock<Their.HubCancel?>(nil)
            let nextValues = Their.TestEventRecorder<Int>()
            let trace = Their.TestEventRecorder<String>()
            let upstream = Their.TestHubDriver<Int, HubEvolutionLifetimeError>()
            let evolved = makeEvolvedHubForTests(
                initial: (token: Optional<HubEvolutionLifetimeToken>.none, total: 0),
                replayLatest: true,
                upstream: upstream.hub
            ) { state, value -> HubEvolutionLifetimeToken? in
                state.total += value
                let total = state.total
                let token = HubEvolutionLifetimeToken(id: total) {
                    if total == 1 {
                        trace.append("oldCleanup")
                        destructor.run()
                    }
                }
                state.token = token
                return token
            }
            let oldCancel = evolved.hub.subscribe { event in
                switch event {
                case .finished, .failure:
                    trace.append("terminal")
                    let cancel = evolved.hub.subscribe { event in
                        if case .value(let value) = event {
                            nextValues.append(value.id)
                        }
                    }
                    nextCancel.withLock { $0 = cancel }
                    #expect(nextValues.events.isEmpty)
                    #expect(upstream.startCallsCount == 2)
                    trace.append("restarted")
                case .value(let value):
                    firstValues.append(value.id)
                }
            }
            destructor.configure(cancel: oldCancel, probe: evolved.isLockAvailable)
            defer {
                nextCancel.withLock { $0 }?()
                destructor.clear()
            }
            upstream.emit(value: 1)
            #expect(firstValues.events == [1])
            #expect(destructor.observations.events.isEmpty)

            ending.finish(cancel: oldCancel, upstream: upstream)
            try await destructor.completed.wait()
            #expect(trace.events == ["terminal", "restarted", "oldCleanup"])
            #expect(destructor.observations.events == [true])
            #expect(destructor.reentries.count == 1)
            #expect(upstream.cancelCallsCount == 1)
            #expect(upstream.startCallsCount == 2)
            #expect(nextValues.events.isEmpty)

            // The old handle has already been reentered by cleanup. The new
            // generation still has fresh State and no inherited replay.
            upstream.emit(value: 2)
            #expect(nextValues.events == [2])
            let replayValues = Their.TestEventRecorder<Int>()
            let replayCancel = evolved.hub.subscribe { event in
                if case .value(let value) = event {
                    replayValues.append(value.id)
                }
            }
            #expect(replayValues.events == [2])
            oldCancel()
            upstream.emit(value: 3)
            #expect(firstValues.events == [1])
            #expect(nextValues.events == [2, 5])
            #expect(replayValues.events == [2, 5])
            #expect(upstream.startCallsCount == 2)
            #expect(upstream.cancelCallsCount == 1)
            replayCancel()
            nextCancel.withLock { $0 }?()
            #expect(upstream.cancelCallsCount == 2)
            withExtendedLifetime(evolved.hub) {}
        }
    }
}

/// If a regression releases a token under the evolution lock, record a normal
/// test failure instead of recursively trapping os_unfair_lock. Every passing
/// observation also executes the actual reentrant cancellation synchronously.
private final class HubEvolutionDestructorAction: Sendable {

    let completed = Their.TestSignal()
    let observations = Their.TestEventRecorder<Bool>()
    let reentries = Their.TestCountRecorder()
    private let settings = Their.Lock<(cancel: Their.HubCancel, probe: @Sendable () -> Bool)?>(nil)

    func clear() {
        let retired = settings.withLock { settings in
            let retired = settings
            settings = nil
            return retired
        }
        withExtendedLifetime(retired) {}
    }

    func configure(cancel: @escaping Their.HubCancel, probe: @escaping @Sendable () -> Bool) {
        settings.withLock { $0 = (cancel, probe) }
    }

    func run() {
        guard let settings = settings.withLock({ $0 }) else {
            Issue.record("Destructor action must be configured before token cleanup.")
            completed.signal()
            return
        }
        let available = settings.probe()
        observations.append(available)
        if available {
            settings.cancel()
            _ = reentries.increment()
        }
        completed.signal()
    }
}

private enum HubEvolutionLifetimeEnd: CaseIterable, Sendable {

    case cancel
    case failure
    case finished

    var expectedEvents: [Their.HubEvent<Int, HubEvolutionLifetimeError>] {
        switch self {
        case .cancel:
            [.value(1)]
        case .finished:
            [.value(1), .finished]
        case .failure:
            [.value(1), .failure(.sample)]
        }
    }

    func finish(cancel: Their.HubCancel, upstream: Their.TestHubDriver<Int, HubEvolutionLifetimeError>) {
        switch self {
        case .cancel:
            cancel()
        case .finished:
            upstream.emitFinished()
        case .failure:
            upstream.emit(failure: .sample)
        }
    }
}

private enum HubEvolutionLifetimeError: Equatable, Error, Sendable {

    case sample
}

private enum HubEvolutionLifetimeInput: Sendable {

    case start
    case token(HubEvolutionLifetimeToken)
}

private final class HubEvolutionLifetimeToken: Sendable {

    let id: Int
    private let onDeinit: @Sendable () -> Void

    init(id: Int, onDeinit: @escaping @Sendable () -> Void) {
        self.id = id
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }
}
