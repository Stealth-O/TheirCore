import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct SerializedTests {

    /// Pins the queued-report no-op contract: a report chained behind an
    /// in-flight delivery is dropped when the wrapper is cancelled before that
    /// delivery returns, and the upstream cancel still runs exactly once. The
    /// first downstream report blocks on a semaphore, so the scenario is
    /// serialized under `Their.stress(count: 1)` like the other blocked-drainer tests;
    /// the skip itself is observed through the DEBUG-only task-local seam
    /// inherited by the chain `Task`. Unlike the drainers moved onto
    /// `BlockingWork`, this hold runs on the wrapper's own chain `Task`, so it
    /// is the one scenario that parks a single cooperative thread briefly.
    @Test func cancelDropsReportQueuedBehindInFlightDelivery() async throws {
        try await Their.stress(count: 1) {
            let cancels = SerializedCancelRecorder()
            let entered = SerializedTestSignal()
            let eventRecorder = SerializedEventRecorder()
            let release = DispatchSemaphore(value: 0)
            let reportStore = Their.Lock<Their.WorkReport<Int, SerializedTestsError>?>(nil)
            let skipped = SerializedTestSignal()
            let work: Their.Work<Int, SerializedTestsError> = Their.serialized { report in
                reportStore.withLock { stored in
                    stored = report
                }
                return cancels.cancel()
            }
            try await SerializedWorkTestHooks.$didSkipQueuedReport.withValue({ skipped.signal() }) {
                let cancel = work { output in
                    if case .value(1) = output {
                        entered.signal()
                        release.wait()
                    }
                    eventRecorder.append(output)
                }
                let report = reportStore.withLock { stored in
                    stored
                }
                #expect(report != nil)

                report?(.value(1))
                try await entered.wait()
                report?(.value(2))
                cancel()
                release.signal()
                try await skipped.wait()
                try await cancels.waitForCancelCallsCount(1)
                report?(.value(3))

                #expect(cancels.cancelCallsCount == 1)
                #expect(eventRecorder.events == [.value(1)])
            }
        }
    }

    /// The wrapper closes once: a repeated cancel neither re-enters the
    /// upstream cancel nor reopens the chain for later reports.
    @Test func repeatedCancelInvokesUpstreamCancelOnce() async throws {
        try await Their.stress {
            let cancels = SerializedCancelRecorder()
            let eventRecorder = SerializedEventRecorder()
            let reportStore = Their.Lock<Their.WorkReport<Int, SerializedTestsError>?>(nil)
            let work: Their.Work<Int, SerializedTestsError> = Their.serialized { report in
                reportStore.withLock { stored in
                    stored = report
                }
                return cancels.cancel()
            }
            let cancel = work(eventRecorder.append(_:))
            let report = reportStore.withLock { stored in
                stored
            }
            #expect(report != nil)

            cancel()
            cancel()
            try await cancels.waitForCancelCallsCount(1)
            report?(.value(1))

            #expect(cancels.cancelCallsCount == 1)
            #expect(eventRecorder.events.isEmpty == true)
        }
    }

    /// Pins the FIFO promise of the chain: reports issued in sequence from one
    /// thread reach the downstream report in call order — values first, the
    /// terminal outcome last — even though every hop runs on its own `Task`.
    @Test func sequentialReportsReachDownstreamInCallOrder() async throws {
        try await Their.stress {
            let eventRecorder = SerializedEventRecorder()
            let reportStore = Their.Lock<Their.WorkReport<Int, SerializedTestsError>?>(nil)
            let work: Their.Work<Int, SerializedTestsError> = Their.serialized { report in
                reportStore.withLock { stored in
                    stored = report
                }
                return {}
            }
            let cancel = work(eventRecorder.append(_:))
            let report = reportStore.withLock { stored in
                stored
            }
            #expect(report != nil)

            report?(.value(1))
            report?(.value(2))
            report?(.value(3))
            report?(.finished)
            try await eventRecorder.waitForEventCount(4)
            cancel()

            #expect(eventRecorder.events == [.value(1), .value(2), .value(3), .finished])
        }
    }
}

private enum SerializedTestsError: Equatable, Swift.Error, Sendable {

    case sample
}

private typealias SerializedCancelRecorder = Their.TestCancelRecorder
private typealias SerializedEventRecorder = Their.TestEventRecorder<Their.WorkOutput<Int, SerializedTestsError>>
private typealias SerializedTestSignal = Their.TestSignal
