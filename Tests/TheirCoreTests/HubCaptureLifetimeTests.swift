import Foundation
import Testing
import TheirCore
import TheirCoreTesting

/// Regressions for sink-capture cleanup through the public facades. Cancel and
/// terminal delivery release the subscription's handler ownership even when the
/// cancel handle remains alive; an in-flight callback owns its own captures
/// until it returns. The Job case controls the same weak-reference observation.
/// Each scenario is synchronous and runs once so each failure has one result.
@Suite
struct HubCaptureLifetimeTests {

    @Test func retainedHubCancelReleasesSinkCaptureAfterCancel() async throws {
        try await Their.stress(count: 1) {
            let deinitializations = Their.TestCountRecorder()
            let events = Their.TestEventRecorder<Their.HubEvent<Int, CaptureLifetimeError>>()
            let work = Their.TestWorkRecorder<Int, CaptureLifetimeError>()
            let hub = Their.Hub(work: work.work)
            var probe: SinkCaptureProbe<Their.HubEvent<Int, CaptureLifetimeError>>? = SinkCaptureProbe(
                deinitializations: deinitializations,
                events: events
            )
            let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                probe != nil
            }
            var cancel: Their.HubCancel? = hub.subscribe { [probe] event in
                probe?.receive(event)
            }
            probe = nil
            work.emit(.value(1))

            #expect(isProbeAlive())
            #expect(deinitializations.count == 0)
            #expect(work.startCallsCount == 1)
            cancel?()
            cancel?()
            work.emit(.value(2))

            #expect(events.events == [.value(1)])
            #expect(work.cancelCallsCount == 1)
            withExtendedLifetime(cancel) {
                #expect(isProbeAlive() == false)
            }

            // Dropping an already-cancelled handle must not release the probe
            // again or invoke upstream cleanup a second time.
            cancel = nil
            #expect(isProbeAlive() == false)
            #expect(deinitializations.count == 1)
            #expect(work.cancelCallsCount == 1)
            withExtendedLifetime(hub) {}
        }
    }

    @Test func retainedHubCancelReleasesSinkCaptureAfterEnd() async throws {
        try await Their.stress(count: 1) {
            let deinitializations = Their.TestCountRecorder()
            let events = Their.TestEventRecorder<Their.HubEvent<Int, CaptureLifetimeError>>()
            let work = Their.TestWorkRecorder<Int, CaptureLifetimeError>()
            let hub = Their.Hub(work: work.work)
            var probe: SinkCaptureProbe<Their.HubEvent<Int, CaptureLifetimeError>>? = SinkCaptureProbe(
                deinitializations: deinitializations,
                events: events
            )
            let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                probe != nil
            }
            var cancel: Their.HubCancel? = hub.subscribe { [probe] event in
                probe?.receive(event)
            }
            probe = nil
            work.emit(.value(1))

            #expect(isProbeAlive())
            #expect(deinitializations.count == 0)
            #expect(work.startCallsCount == 1)
            work.emit(.finished)
            work.emit(.value(2))

            #expect(events.events == [.value(1), .finished])
            #expect(work.cancelCallsCount == 1)
            withExtendedLifetime(cancel) {
                #expect(isProbeAlive() == false)
            }

            cancel?()
            cancel?()
            cancel = nil
            #expect(isProbeAlive() == false)
            #expect(deinitializations.count == 1)
            #expect(work.cancelCallsCount == 1)
            withExtendedLifetime(hub) {}
        }
    }

    @Test func retainedHubCancelReleasesSinkCaptureAfterFail() async throws {
        try await Their.stress(count: 1) {
            let deinitializations = Their.TestCountRecorder()
            let events = Their.TestEventRecorder<Their.HubEvent<Int, CaptureLifetimeError>>()
            let work = Their.TestWorkRecorder<Int, CaptureLifetimeError>()
            let hub = Their.Hub(work: work.work)
            var probe: SinkCaptureProbe<Their.HubEvent<Int, CaptureLifetimeError>>? = SinkCaptureProbe(
                deinitializations: deinitializations,
                events: events
            )
            let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                probe != nil
            }
            var cancel: Their.HubCancel? = hub.subscribe { [probe] event in
                probe?.receive(event)
            }
            probe = nil
            work.emit(.value(1))

            #expect(isProbeAlive())
            #expect(deinitializations.count == 0)
            #expect(work.startCallsCount == 1)
            work.emit(.failure(.sample))
            work.emit(.value(2))

            #expect(events.events == [.value(1), .failure(.sample)])
            #expect(work.cancelCallsCount == 1)
            withExtendedLifetime(cancel) {
                #expect(isProbeAlive() == false)
            }

            cancel?()
            cancel?()
            cancel = nil
            #expect(isProbeAlive() == false)
            #expect(deinitializations.count == 1)
            #expect(work.cancelCallsCount == 1)
            withExtendedLifetime(hub) {}
        }
    }

    @Test func retainedJobCancelReleasesSinkCaptureAfterCancel() async throws {
        try await Their.stress(count: 1) {
            let deinitializations = Their.TestCountRecorder()
            let events = Their.TestEventRecorder<Their.JobEvent<Int, CaptureLifetimeError>>()
            let work = Their.TestWorkRecorder<Int, CaptureLifetimeError>()
            let job = Their.Job(work: work.work)
            var probe: SinkCaptureProbe<Their.JobEvent<Int, CaptureLifetimeError>>? = SinkCaptureProbe(
                deinitializations: deinitializations,
                events: events
            )
            let isProbeAlive: @Sendable () -> Bool = { [weak probe] in
                probe != nil
            }
            var cancel: Their.WorkCancel? = job.subscribe { [probe] event in
                probe?.receive(event)
            }
            probe = nil
            work.emit(.value(1))

            #expect(isProbeAlive())
            #expect(deinitializations.count == 0)
            #expect(work.startCallsCount == 1)
            cancel?()
            cancel?()
            work.emit(.value(2))

            #expect(events.events == [.value(1)])
            #expect(work.cancelCallsCount == 1)
            withExtendedLifetime(cancel) {
                #expect(isProbeAlive() == false)
            }

            cancel = nil
            #expect(isProbeAlive() == false)
            #expect(deinitializations.count == 1)
            #expect(work.cancelCallsCount == 1)
            withExtendedLifetime(job) {}
        }
    }
}

private enum CaptureLifetimeError: Swift.Error, Sendable {

    case sample
}

/// Owns only recorders; deliberately has no reference to the cancel or facade.
private final class SinkCaptureProbe<Event: Sendable>: Sendable {

    private let deinitializations: Their.TestCountRecorder
    private let events: Their.TestEventRecorder<Event>

    init(
        deinitializations: Their.TestCountRecorder,
        events: Their.TestEventRecorder<Event>
    ) {
        self.deinitializations = deinitializations
        self.events = events
    }

    deinit {
        _ = deinitializations.increment()
    }

    func receive(_ event: Event) {
        events.append(event)
    }
}
