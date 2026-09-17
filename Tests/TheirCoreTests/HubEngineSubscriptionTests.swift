import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct HubEngineSubscriptionTests {

    @Test func cancelDuringValueCallbackKeepsCaptureUntilCallbackReturns() async throws {
        try await Their.stress {
            let deinitializations = Their.TestCountRecorder()
            let events = Their.TestEventRecorder<SubscriptionEvent>()
            let subscriptionBox = Their.Lock<Subscription?>(nil)
            var probe: SubscriptionCaptureProbe? = SubscriptionCaptureProbe(
                deinitializations: deinitializations,
                events: events
            )
            let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                probe != nil
            }
            let subscription = Subscription { [probe] event in
                probe?.receive(event)
                let subscription = subscriptionBox.withLock { $0 }
                subscription?.cancel()
                subscription?.emit(.value(2))
                subscription?.emit(.finished)
                subscription?.emit(.failure(.sample))
                withExtendedLifetime(probe) {
                    #expect(deinitializations.count == 0)
                }
            }
            subscriptionBox.withLock { $0 = subscription }
            probe = nil

            subscription.emit(.value(1))

            #expect(events.events == [.value(1)])
            #expect(isProbeAlive() == false)
            #expect(deinitializations.count == 1)
            subscription.cancel()
            #expect(deinitializations.count == 1)
            withExtendedLifetime(subscription) {}
        }
    }

    @Test func cancelReleasesCaptureWhoseDeinitCancelsAgain() async throws {
        try await Their.stress {
            let deinitializations = Their.TestCountRecorder()
            let events = Their.TestEventRecorder<SubscriptionEvent>()
            let reentrantCancels = Their.TestCountRecorder()
            let subscriptionBox = Their.Lock<Subscription?>(nil)
            var probe: SubscriptionCaptureProbe? = SubscriptionCaptureProbe(
                deinitializations: deinitializations,
                events: events,
                onDeinit: {
                    let subscription = subscriptionBox.withLock { $0 }
                    subscription?.cancel()
                    _ = reentrantCancels.increment()
                }
            )
            let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                probe != nil
            }
            let subscription = Subscription { [probe] event in
                probe?.receive(event)
            }
            subscriptionBox.withLock { $0 = subscription }
            probe = nil

            subscription.emit(.value(1))
            subscription.emit(.value(2))
            #expect(isProbeAlive())
            #expect(deinitializations.count == 0)
            subscription.cancel()
            subscription.cancel()
            subscription.emit(.value(3))
            subscription.emit(.finished)
            subscription.emit(.failure(.sample))

            #expect(events.events == [.value(1), .value(2)])
            #expect(isProbeAlive() == false)
            #expect(deinitializations.count == 1)
            #expect(reentrantCancels.count == 1)
            withExtendedLifetime(subscription) {}
        }
    }

    @Test func deinitReleasesActiveSinkCapture() async throws {
        try await Their.stress {
            let deinitializations = Their.TestCountRecorder()
            let events = Their.TestEventRecorder<SubscriptionEvent>()
            var probe: SubscriptionCaptureProbe? = SubscriptionCaptureProbe(
                deinitializations: deinitializations,
                events: events
            )
            let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                probe != nil
            }
            var subscription: Subscription? = Subscription { [probe] event in
                probe?.receive(event)
            }
            probe = nil
            subscription?.emit(.value(1))
            #expect(isProbeAlive())
            #expect(deinitializations.count == 0)

            subscription = nil

            #expect(events.events == [.value(1)])
            #expect(isProbeAlive() == false)
            #expect(deinitializations.count == 1)
        }
    }

    @Test func terminalClearsSinkBeforeCallbackAndDropsReentrantEvents() async throws {
        try await Their.stress {
            for terminal in [SubscriptionEvent.finished, .failure(.sample)] {
                let deinitializations = Their.TestCountRecorder()
                let events = Their.TestEventRecorder<SubscriptionEvent>()
                let subscriptionBox = Their.Lock<Subscription?>(nil)
                var probe: SubscriptionCaptureProbe? = SubscriptionCaptureProbe(
                    deinitializations: deinitializations,
                    events: events
                )
                let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                    probe != nil
                }
                let subscription = Subscription { [probe] event in
                    probe?.receive(event)
                    let subscription = subscriptionBox.withLock { $0 }
                    // These must already be ignored before callback-driven cancel.
                    subscription?.emit(.value(2))
                    subscription?.emit(.finished)
                    subscription?.emit(.failure(.sample))
                    subscription?.cancel()
                    withExtendedLifetime(probe) {
                        #expect(deinitializations.count == 0)
                    }
                }
                subscriptionBox.withLock { $0 = subscription }
                probe = nil

                subscription.emit(terminal)
                subscription.emit(terminal)
                subscription.emit(.value(3))
                subscription.cancel()

                #expect(events.events == [terminal])
                #expect(isProbeAlive() == false)
                #expect(deinitializations.count == 1)
                withExtendedLifetime(subscription) {}
            }
        }
    }

    @Test func terminalReleasesCaptureWhoseDeinitCancelsAgain() async throws {
        try await Their.stress {
            for terminal in [SubscriptionEvent.finished, .failure(.sample)] {
                let deinitializations = Their.TestCountRecorder()
                let events = Their.TestEventRecorder<SubscriptionEvent>()
                let reentrantCancels = Their.TestCountRecorder()
                let subscriptionBox = Their.Lock<Subscription?>(nil)
                var probe: SubscriptionCaptureProbe? = SubscriptionCaptureProbe(
                    deinitializations: deinitializations,
                    events: events,
                    onDeinit: {
                        let subscription = subscriptionBox.withLock { $0 }
                        subscription?.cancel()
                        _ = reentrantCancels.increment()
                    }
                )
                let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                    probe != nil
                }
                let subscription = Subscription { [probe] event in
                    probe?.receive(event)
                }
                subscriptionBox.withLock { $0 = subscription }
                probe = nil
                subscription.emit(.value(1))

                subscription.emit(terminal)
                subscription.emit(.value(2))
                subscription.emit(.finished)
                subscription.emit(.failure(.sample))
                subscription.cancel()

                #expect(events.events == [.value(1), terminal])
                #expect(isProbeAlive() == false)
                #expect(deinitializations.count == 1)
                #expect(reentrantCancels.count == 1)
                withExtendedLifetime(subscription) {}
            }
        }
    }
}

private typealias Subscription = HubEngineSubscription<Int, SubscriptionError>
private typealias SubscriptionEvent = Their.HubEvent<Int, SubscriptionError>

private final class SubscriptionCaptureProbe: Sendable {

    private let deinitializations: Their.TestCountRecorder
    private let events: Their.TestEventRecorder<SubscriptionEvent>
    private let onDeinit: @Sendable () -> Void

    init(
        deinitializations: Their.TestCountRecorder,
        events: Their.TestEventRecorder<SubscriptionEvent>,
        onDeinit: @escaping @Sendable () -> Void = {}
    ) {
        self.deinitializations = deinitializations
        self.events = events
        self.onDeinit = onDeinit
    }

    deinit {
        _ = deinitializations.increment()
        onDeinit()
    }

    func receive(_ event: SubscriptionEvent) {
        events.append(event)
    }
}

private enum SubscriptionError: Swift.Error, Sendable {

    case sample
}
