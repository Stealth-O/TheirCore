import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobMergeLifetimeTests {

    /// A direct source reaches merge's own queue without first queueing in a
    /// root JobEngine. Every token is owned only by the queued input when
    /// cancellation or a terminal outcome discards it.
    @Test(arguments: MergeLifetimeTermination.allCases)
    private func cancelOrTerminalReleasesDiscardedQueuedInputOutsideLock(_ termination: MergeLifetimeTermination) async throws {
        try await Their.stress {
            let cancelSlot = Their.Lock<Their.WorkCancel?>(nil)
            let events = Their.TestEventRecorder<String>()
            let inputSink = Their.Lock<Their.JobSink<MergeLifetimeInput, MergeLifetimeError>?>(nil)
            let observations = Their.TestEventRecorder<Bool>()
            let reentries = Their.TestCountRecorder()
            let tokenDeliveries = Their.TestCountRecorder()
            let upstreamCancels = Their.TestCancelRecorder()
            let upstream = Their.Job<MergeLifetimeInput, MergeLifetimeError>(
                misuseHandler: { _ in Issue.record("Only one subscription is expected.") },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    inputSink.withLock { $0 = sink }
                    return upstreamCancels.cancel()
                }
            )
            let merged = makeMergedJobForTests([upstream])
            let probe = merged.isLockAvailable
            let send: @Sendable (Their.JobEvent<MergeLifetimeInput, MergeLifetimeError>) -> Void = { event in
                inputSink.withLock { $0 }?(event)
            }
            let cancel = merged.job.subscribe { event in
                switch event {
                case .finished:
                    events.append("end")
                case .failure:
                    events.append("fail")
                case .value(.start):
                    events.append("start")
                    switch termination {
                    case .cancel:
                        break
                    case .finished:
                        send(.finished)
                    case .failure:
                        send(.failure(.sample))
                    }
                    send(.value(.token(MergeLifetimeToken {
                        let available = probe()
                        observations.append(available)
                        // A regressed held lock records a normal failure;
                        // only the safe branch performs actual public reentry.
                        guard available else { return }
                        cancelSlot.withLock { $0 }?()
                        _ = reentries.increment()
                    })))
                    if termination == .cancel {
                        cancelSlot.withLock { $0 }?()
                    }
                case .value(.token):
                    _ = tokenDeliveries.increment()
                }
            }
            cancelSlot.withLock { $0 = cancel }
            defer { cancelSlot.withLock { $0 = nil } }

            withExtendedLifetime(merged.job) {
                send(.value(.start))

                let expected: [String]
                switch termination {
                case .cancel:
                    expected = ["start"]
                case .finished:
                    expected = ["start", "end"]
                case .failure:
                    expected = ["start", "fail"]
                }
                #expect(events.events == expected)
                #expect(observations.events == [true])
                #expect(reentries.count == 1)
                #expect(tokenDeliveries.count == 0)
                // The last .finished releases its already-ended source handle;
                // cancel and failure must invoke the live source cancel.
                let expectedCancels = termination == .finished ? 0 : 1
                #expect(upstreamCancels.cancelCallsCount == expectedCancels)

                cancel()
                send(.value(.start))
                #expect(events.events == expected)
                #expect(observations.events == [true])
                #expect(reentries.count == 1)
                #expect(tokenDeliveries.count == 0)
                #expect(upstreamCancels.cancelCallsCount == expectedCancels)
            }
        }
    }

    @Test func cancelReleasesSinkOutsideLockBeforeReturning() async throws {
        try await Their.stress {
            let cancelSlot = Their.Lock<Their.WorkCancel?>(nil)
            let events = Their.TestEventRecorder<Their.JobEvent<Int, Never>>()
            let observations = Their.TestEventRecorder<Bool>()
            let reentries = Their.TestCountRecorder()
            let upstreams = [Their.TestJobDriver<Int, Never>(), Their.TestJobDriver<Int, Never>()]
            let merged = makeMergedJobForTests(upstreams.map(\.job))
            let probe = merged.isLockAvailable
            let onDeinit: @Sendable () -> Void = {
                let available = probe()
                observations.append(available)
                guard available else { return }
                cancelSlot.withLock { $0 }?()
                _ = reentries.increment()
            }
            let cancel = merged.job.subscribe { [token = MergeLifetimeToken(onDeinit: onDeinit)] event in
                withExtendedLifetime(token) {
                    events.append(event)
                }
            }
            cancelSlot.withLock { $0 = cancel }
            defer { cancelSlot.withLock { $0 = nil } }
            upstreams[0].emit(value: 1)
            #expect(events.events == [.value(1)])
            #expect(observations.events.isEmpty)

            withExtendedLifetime(merged.job) {
                cancel()

                #expect(events.events == [.value(1)])
                #expect(observations.events == [true])
                #expect(reentries.count == 1)
                #expect(upstreams.map(\.cancelCallsCount) == [1, 1])

                cancel()
                upstreams[1].emit(value: 2)
                #expect(events.events == [.value(1)])
                #expect(observations.events == [true])
                #expect(reentries.count == 1)
                #expect(upstreams.map(\.cancelCallsCount) == [1, 1])
            }
        }
    }

    /// The handle's capture is created during subscribe, so only merge's
    /// cancel slot owns it after subscribe returns. Test both an individual
    /// .finished and the final .finished, which must still be delivered exactly once.
    @Test(arguments: [false, true])
    func endedUpstreamReleasesCancelCaptureOutsideLock(_ isLast: Bool) async throws {
        try await Their.stress {
            let cancelSlot = Their.Lock<Their.WorkCancel?>(nil)
            let events = Their.TestEventRecorder<Their.JobEvent<Int, Never>>()
            let inputSink = Their.Lock<Their.JobSink<Int, Never>?>(nil)
            let observations = Their.TestEventRecorder<Bool>()
            let probeSlot = Their.Lock<(@Sendable () -> Bool)?>(nil)
            let reentries = Their.TestCountRecorder()
            let remainingUpstream = Their.TestJobDriver<Int, Never>()
            let upstreamCancels = Their.TestCountRecorder()
            let onDeinit: @Sendable () -> Void = {
                guard let probe = probeSlot.withLock({ $0 }) else {
                    Issue.record("Lock probe must be installed before cancel capture release.")
                    return
                }
                let available = probe()
                observations.append(available)
                guard available else { return }
                cancelSlot.withLock { $0 }?()
                _ = reentries.increment()
            }
            let upstream = Their.Job<Int, Never>(
                misuseHandler: { _ in Issue.record("Only one subscription is expected.") },
                misuseLocation: .init(),
                onSubscribe: { sink in
                    inputSink.withLock { $0 = sink }
                    return { [token = MergeLifetimeToken(onDeinit: onDeinit)] in
                        withExtendedLifetime(token) {
                            _ = upstreamCancels.increment()
                        }
                    }
                }
            )
            let merged = makeMergedJobForTests(isLast ? [upstream] : [upstream, remainingUpstream.job])
            probeSlot.withLock { $0 = merged.isLockAvailable }
            let cancel = merged.job.subscribe(events.append(_:))
            cancelSlot.withLock { $0 = cancel }
            defer {
                cancelSlot.withLock { $0 = nil }
                probeSlot.withLock { $0 = nil }
            }
            #expect(observations.events.isEmpty)

            withExtendedLifetime(merged.job) {
                inputSink.withLock { $0 }?(.finished)

                let expected: [Their.JobEvent<Int, Never>] = isLast ? [.finished] : []
                #expect(events.events == expected)
                #expect(observations.events == [true])
                #expect(reentries.count == 1)
                #expect(upstreamCancels.count == 0)
                #expect(remainingUpstream.cancelCallsCount == (isLast ? 0 : 1))

                cancel()
                remainingUpstream.emit(value: 2)
                #expect(events.events == expected)
                #expect(observations.events == [true])
                #expect(reentries.count == 1)
                #expect(upstreamCancels.count == 0)
                #expect(remainingUpstream.cancelCallsCount == (isLast ? 0 : 1))
            }
        }
    }
}

private enum MergeLifetimeError: Error, Sendable {

    case sample
}

private enum MergeLifetimeInput: Sendable {

    case start
    case token(MergeLifetimeToken)
}

private enum MergeLifetimeTermination: CaseIterable, Sendable {

    case cancel
    case failure
    case finished
}

private final class MergeLifetimeToken: Sendable {

    private let onDeinit: @Sendable () -> Void

    init(onDeinit: @escaping @Sendable () -> Void) {
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }
}
