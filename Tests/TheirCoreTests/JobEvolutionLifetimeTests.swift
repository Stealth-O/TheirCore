import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobEvolutionLifetimeTests {

    /// Executes the previously hypothetical reentry through the public cancel
    /// handle, rather than substituting a nonblocking lock observation.
    @Test func cancelAllowsEvolvedStateDestructorToCancelSameSubscription() async throws {
        try await Their.stress {
            let cancelSlot = Their.Lock<Their.WorkCancel?>(nil)
            let events = Their.TestEventRecorder<Their.JobEvent<Int, Never>>()
            let reentries = Their.TestCountRecorder()
            let upstream = Their.TestJobDriver<Int, Never>()
            let evolved = upstream.job.evolve(initial: Optional<EvolutionLifetimeToken>.none) { state, value -> Int? in
                state = EvolutionLifetimeToken {
                    cancelSlot.withLock { $0 }?()
                    _ = reentries.increment()
                }
                return value
            }
            let cancel = evolved.subscribe(events.append(_:))
            cancelSlot.withLock { $0 = cancel }
            defer { cancelSlot.withLock { $0 = nil } }

            upstream.emit(value: 1)
            #expect(reentries.count == 0)
            #expect(events.events == [.value(1)])

            withExtendedLifetime(evolved) {
                cancel()
                #expect(reentries.count == 1)
                #expect(upstream.cancelCallsCount == 1)

                cancel()
                upstream.emit(value: 2)
                #expect(reentries.count == 1)
                #expect(upstream.cancelCallsCount == 1)
                #expect(events.events == [.value(1)])
            }
        }
    }

    /// Reproduces the destructor's actual lock context without attempting a
    /// recursive cancel, which would trap the entire test process. The token
    /// is created by a completed transform, so only the stored State owns it
    /// when cancellation begins. An available lock is the desired behavior.
    @Test func cancelReleasesEvolvedStateOutsideLock() async throws {
        try await Their.stress(count: 1) {
            let events = Their.TestEventRecorder<Their.JobEvent<Int, Never>>()
            let lockObservations = Their.TestEventRecorder<Bool>()
            let lockProbe = Their.Lock<(@Sendable () -> Bool)?>(nil)
            let upstream = Their.TestJobDriver<Int, Never>()
            let evolved = makeEvolvedJobForTests(
                initial: Optional<EvolutionLifetimeToken>.none,
                upstream: upstream.job
            ) { state, value -> Int? in
                state = EvolutionLifetimeToken {
                    guard let probe = lockProbe.withLock({ $0 }) else {
                        Issue.record("Lock probe must be installed before State is released.")
                        return
                    }
                    lockObservations.append(probe())
                }
                return value
            }
            lockProbe.withLock { $0 = evolved.isLockAvailable }
            // The probe itself retains the evolution state. Release that
            // observation link after the scenario to avoid a test-only cycle.
            defer { lockProbe.withLock { $0 = nil } }
            let cancel = evolved.job.subscribe(events.append(_:))

            #expect(evolved.isLockAvailable())
            #expect(upstream.startCallsCount == 1)
            upstream.emit(value: 1)
            #expect(events.events == [.value(1)])
            #expect(lockObservations.events.isEmpty)
            #expect(upstream.cancelCallsCount == 0)

            withExtendedLifetime(evolved.job) {
                cancel()
                #expect(lockObservations.events == [true])
                #expect(evolved.isLockAvailable())
                #expect(upstream.cancelCallsCount == 1)

                cancel()
                upstream.emit(value: 2)
                #expect(lockObservations.events.count == 1)
                #expect(upstream.cancelCallsCount == 1)
                #expect(events.events == [.value(1)])
            }
        }
    }

    /// Covers both JobSinkState and the evolution's own sink slot. The closure
    /// is the only token owner, and its destructor invokes the actual cancel.
    @Test(arguments: [false, true])
    func cancelReleasesSinkWhoseDestructorCancelsSameSubscription(_ usesEvolution: Bool) async throws {
        try await Their.stress {
            let cancelSlot = Their.Lock<Their.WorkCancel?>(nil)
            let events = Their.TestEventRecorder<Their.JobEvent<Int, Never>>()
            let reentries = Their.TestCountRecorder()
            let upstream = Their.TestJobDriver<Int, Never>()
            let job = usesEvolution ? upstream.job.map { $0 } : upstream.job
            let cancel = job.subscribe { [token = EvolutionLifetimeToken {
                cancelSlot.withLock { $0 }?()
                _ = reentries.increment()
            }] event in
                withExtendedLifetime(token) {
                    events.append(event)
                }
            }
            cancelSlot.withLock { $0 = cancel }
            defer { cancelSlot.withLock { $0 = nil } }

            upstream.emit(value: 1)
            #expect(reentries.count == 0)
            #expect(events.events == [.value(1)])

            withExtendedLifetime(job) {
                cancel()
                #expect(reentries.count == 1)
                #expect(upstream.cancelCallsCount == 1)

                cancel()
                upstream.emit(value: 2)
                #expect(reentries.count == 1)
                #expect(upstream.cancelCallsCount == 1)
                #expect(events.events == [.value(1)])
            }
        }
    }

    @Test(arguments: [EvolutionLifetimeTermination.finished, .failure])
    private func terminalAllowsEvolvedStateDestructorToCancelSameSubscription(_ termination: EvolutionLifetimeTermination) async throws {
        try await Their.stress {
            let cancelSlot = Their.Lock<Their.WorkCancel?>(nil)
            let events = Their.TestEventRecorder<Their.JobEvent<Int, EvolutionLifetimeError>>()
            let reentries = Their.TestCountRecorder()
            let upstream = Their.TestJobDriver<Int, EvolutionLifetimeError>()
            let evolved = upstream.job.evolve(initial: Optional<EvolutionLifetimeToken>.none) { state, value -> Int? in
                state = EvolutionLifetimeToken {
                    cancelSlot.withLock { $0 }?()
                    _ = reentries.increment()
                }
                return value
            }
            let cancel = evolved.subscribe(events.append(_:))
            cancelSlot.withLock { $0 = cancel }
            defer { cancelSlot.withLock { $0 = nil } }

            upstream.emit(value: 1)
            #expect(reentries.count == 0)
            #expect(events.events == [.value(1)])

            withExtendedLifetime(evolved) {
                if termination == .finished {
                    upstream.emitFinished()
                } else {
                    upstream.emit(failure: .sample)
                }
                #expect(reentries.count == 1)
                #expect(upstream.cancelCallsCount == 1)
                let expected: [Their.JobEvent<Int, EvolutionLifetimeError>] = termination == .finished
                    ? [.value(1), .finished]
                    : [.value(1), .failure(.sample)]
                #expect(events.events == expected)

                cancel()
                upstream.emit(value: 2)
                #expect(reentries.count == 1)
                #expect(upstream.cancelCallsCount == 1)
                #expect(events.events == expected)
            }
        }
    }

    /// Control for the same observation mechanism: terminal processing holds
    /// a dequeued State copy until it returns, so clearing the stored State
    /// need not destroy the token in the critical section.
    @Test func terminalEndReleasesEvolvedStateOutsideLock() async throws {
        try await Their.stress(count: 1) {
            let events = Their.TestEventRecorder<Their.JobEvent<Int, Never>>()
            let lockObservations = Their.TestEventRecorder<Bool>()
            let lockProbe = Their.Lock<(@Sendable () -> Bool)?>(nil)
            let upstream = Their.TestJobDriver<Int, Never>()
            let evolved = makeEvolvedJobForTests(
                initial: Optional<EvolutionLifetimeToken>.none,
                upstream: upstream.job
            ) { state, value -> Int? in
                state = EvolutionLifetimeToken {
                    guard let probe = lockProbe.withLock({ $0 }) else {
                        Issue.record("Lock probe must be installed before State is released.")
                        return
                    }
                    lockObservations.append(probe())
                }
                return value
            }
            lockProbe.withLock { $0 = evolved.isLockAvailable }
            defer { lockProbe.withLock { $0 = nil } }
            let cancel = evolved.job.subscribe(events.append(_:))

            #expect(evolved.isLockAvailable())
            #expect(upstream.startCallsCount == 1)
            upstream.emit(value: 1)
            #expect(events.events == [.value(1)])
            #expect(lockObservations.events.isEmpty)
            #expect(upstream.cancelCallsCount == 0)

            withExtendedLifetime(evolved.job) {
                upstream.emitFinished()
                #expect(lockObservations.events == [true])
                #expect(evolved.isLockAvailable())
                #expect(upstream.cancelCallsCount == 1)
                #expect(events.events == [.value(1), .finished])

                cancel()
                upstream.emit(value: 2)
                #expect(lockObservations.events.count == 1)
                #expect(upstream.cancelCallsCount == 1)
                #expect(events.events == [.value(1), .finished])
            }
        }
    }

    /// This direct test source delivers reentrantly to the evolution queue;
    /// a root JobEngine would serialize these inputs in its own queue first.
    /// The pending token must be dropped without running its transform, and
    /// its destructor must be able to cancel the derived public subscription.
    @Test(arguments: EvolutionLifetimeTermination.allCases)
    private func terminationReleasesQueuedInputWhoseDestructorCancelsSameSubscription(_ termination: EvolutionLifetimeTermination) async throws {
        try await Their.stress {
            let cancelSlot = Their.Lock<Their.WorkCancel?>(nil)
            let events = Their.TestEventRecorder<Their.JobEvent<Int, EvolutionLifetimeError>>()
            let inputSink = Their.Lock<Their.JobSink<EvolutionLifetimeInput, EvolutionLifetimeError>?>(nil)
            let reentries = Their.TestCountRecorder()
            let transformedTokens = Their.TestCountRecorder()
            let upstreamCancels = Their.TestCancelRecorder()
            let upstream = Their.Job<EvolutionLifetimeInput, EvolutionLifetimeError>(
                misuseHandler: { _ in Issue.record("Only one subscription is expected.") },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    inputSink.withLock { $0 = sink }
                    return upstreamCancels.cancel()
                }
            )
            let send: @Sendable (Their.JobEvent<EvolutionLifetimeInput, EvolutionLifetimeError>) -> Void = { event in
                inputSink.withLock { $0 }?(event)
            }
            let queueToken: @Sendable () -> Void = {
                send(.value(.token(EvolutionLifetimeToken {
                    cancelSlot.withLock { $0 }?()
                    _ = reentries.increment()
                })))
            }
            let mapped: Their.Job<Int, EvolutionLifetimeError> = upstream.tryMap { input in
                switch input {
                case .start:
                    switch termination {
                    case .cancel:
                        queueToken()
                        cancelSlot.withLock { $0 }?()
                    case .finished:
                        send(.finished)
                        queueToken()
                    case .failure:
                        send(.failure(.sample))
                        queueToken()
                    case .transformFailure:
                        queueToken()
                        throw EvolutionLifetimeError.sample
                    }
                    return 1
                case .token:
                    _ = transformedTokens.increment()
                    return 2
                }
            } onThrow: { _ in
                .sample
            }
            let cancel = mapped.subscribe(events.append(_:))
            cancelSlot.withLock { $0 = cancel }
            defer { cancelSlot.withLock { $0 = nil } }

            withExtendedLifetime(mapped) {
                send(.value(.start))
                #expect(reentries.count == 1)
                #expect(transformedTokens.count == 0)
                let expected: [Their.JobEvent<Int, EvolutionLifetimeError>]
                switch termination {
                case .cancel:
                    expected = []
                case .finished:
                    expected = [.value(1), .finished]
                case .failure:
                    expected = [.value(1), .failure(.sample)]
                case .transformFailure:
                    expected = [.failure(.sample)]
                }
                #expect(events.events == expected)
                // The direct source has already ended on .finished/.failure. Their
                // stored upstream cancel is dropped; only control cancel and
                // a transform failure must invoke the live upstream cancel.
                let expectedCancels = termination == .cancel || termination == .transformFailure ? 1 : 0
                #expect(upstreamCancels.cancelCallsCount == expectedCancels)

                cancel()
                send(.value(.start))
                #expect(reentries.count == 1)
                #expect(transformedTokens.count == 0)
                #expect(upstreamCancels.cancelCallsCount == expectedCancels)
                #expect(events.events == expected)
            }
        }
    }
}

private enum EvolutionLifetimeError: Equatable, Error, Sendable {

    case sample
}

private enum EvolutionLifetimeInput: Sendable {

    case start
    case token(EvolutionLifetimeToken)
}

private enum EvolutionLifetimeTermination: CaseIterable, Equatable, Sendable {

    case cancel
    case failure
    case finished
    case transformFailure
}

private final class EvolutionLifetimeToken: Sendable {

    private let onDeinit: @Sendable () -> Void

    init(onDeinit: @escaping @Sendable () -> Void) {
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }
}
