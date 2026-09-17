import Dispatch
import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

/// Benchmarks are intentionally not wrapped in `Their.stress { ... }`: each test
/// measures the serial throughput of one instance, and the concurrency
/// amplifier would distort the timing it reports. Correctness of the measured
/// primitives is pinned by the regular suites; these tests only assert basic
/// delivery counts around the measurement.
@Suite
struct TheirCoreBenchmarkTests {

    @Test func benchmarkHubBroadcastToManySubscribers() async throws {
        let delivered = Their.Lock(0)
        let iterations = 1_000
        let subscribersCount = 10
        let startRecorder = TheirCoreBenchmarkWorkRecorder()
        let hub: Their.Hub<Int, TheirCoreBenchmarkTestsError> = Their.Hub(
            work: startRecorder.work
        )
        let cancels = (0 ..< subscribersCount).map { _ in
            hub.subscribe { _ in
                delivered.withLock { count in
                    count += 1
                }
            }
        }
        try await startRecorder.waitForStartCallsCount(1)

        let metrics = TheirCoreBenchmark.measure(
            iterations: iterations * subscribersCount,
            label: "Hub broadcast to \(subscribersCount) subscribers"
        ) {
            for value in 0 ..< iterations {
                startRecorder.emit(.value(value))
            }
        }
        TheirCoreBenchmark.report(metrics)

        cancels.forEach { cancel in
            cancel()
        }
        try await startRecorder.waitForCancelCallsCount(1)

        #expect(delivered.withLock { count in count } == iterations * subscribersCount)
        #expect(metrics.elapsedNanoseconds > 0)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func benchmarkJobEngineQueuedReportBurst() async throws {
        let eventRecorder = TheirCoreBenchmarkJobEngineEventRecorder()
        let iterations = 1_000
        let releaseFirstValue = DispatchSemaphore(value: 0)
        let startRecorder = TheirCoreBenchmarkWorkRecorder()
        let valueEntered = Their.TestSignal()
        let engine = JobEngine<Int, TheirCoreBenchmarkTestsError>(
            sink: { event in
                if case .value(0) = event {
                    valueEntered.signal()
                    releaseFirstValue.wait()
                }
                eventRecorder.append(event)
            },
            work: startRecorder.work
        )
        _ = engine.start()
        try await startRecorder.waitForStartCallsCount(1)
        let report = startRecorder.report
        #expect(report != nil)

        let firstValueTask = BlockingWork {
            report?(.value(0))
        }
        try await valueEntered.wait()

        let metrics = TheirCoreBenchmark.measure(
            iterations: iterations - 1,
            label: "JobEngine queued report burst"
        ) {
            for value in 1 ..< iterations {
                report?(.value(value))
            }
        }
        TheirCoreBenchmark.report(metrics)

        releaseFirstValue.signal()
        try await firstValueTask.value
        try await eventRecorder.waitForEventCount(iterations)
        engine.stop()

        #expect(eventRecorder.events.values == Array(0 ..< iterations))
        #expect(metrics.elapsedNanoseconds > 0)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func benchmarkJobEngineSerialReports() async throws {
        let delivered = Their.Lock(0)
        let iterations = 10_000
        let startRecorder = TheirCoreBenchmarkWorkRecorder()
        let engine = JobEngine<Int, TheirCoreBenchmarkTestsError>(
            sink: { event in
                switch event {
                case .finished, .failure, .message:
                    break
                case .value:
                    delivered.withLock { count in
                        count += 1
                    }
                }
            },
            work: startRecorder.work
        )
        _ = engine.start()
        try await startRecorder.waitForStartCallsCount(1)

        let metrics = TheirCoreBenchmark.measure(
            iterations: iterations,
            label: "JobEngine serial reports"
        ) {
            for value in 0 ..< iterations {
                startRecorder.emit(.value(value))
            }
        }
        TheirCoreBenchmark.report(metrics)
        engine.stop()

        #expect(delivered.withLock { count in count } == iterations)
        #expect(metrics.elapsedNanoseconds > 0)
        #expect(startRecorder.cancelCallsCount == 1)
        #expect(startRecorder.startCallsCount == 1)
    }

    @Test func benchmarkLockWithLockMutation() {
        let iterations = 50_000
        let lock = Their.Lock(0)

        let metrics = TheirCoreBenchmark.measure(
            iterations: iterations,
            label: "Lock.withLock mutation"
        ) {
            for _ in 0 ..< iterations {
                lock.withLock { value in
                    value += 1
                }
            }
        }
        TheirCoreBenchmark.report(metrics)

        #expect(lock.withLock { value in value } == iterations)
        #expect(metrics.elapsedNanoseconds > 0)
    }

    @Test func benchmarkSerializedConcurrentReportBridge() async throws {
        let eventRecorder = TheirCoreBenchmarkJobEngineEventRecorder()
        let iterations = Their.stressCountDefault
        let reportStore = Their.Lock<Their.WorkReport<Int, TheirCoreBenchmarkTestsError>?>(nil)
        let engine = JobEngine<Int, TheirCoreBenchmarkTestsError>(
            sink: eventRecorder.append(_:),
            work: Their.serialized { report in
                reportStore.withLock { storedReport in
                    storedReport = report
                }
                return {}
            }
        )
        _ = engine.start()
        let report = reportStore.withLock { storedReport in
            storedReport
        }
        #expect(report != nil)

        let metrics = try await TheirCoreBenchmark.measureAsync(
            iterations: iterations,
            label: "serialized(_:) concurrent reports"
        ) {
            try await Their.stress {
                report?(.value($0))
            }
            try await eventRecorder.waitForEventCount(iterations)
        }
        TheirCoreBenchmark.report(metrics)
        engine.stop()

        #expect(eventRecorder.events.values.sorted() == Array(0 ..< iterations))
        #expect(metrics.elapsedNanoseconds > 0)
    }
}

private enum TheirCoreBenchmark {

    static func measure(
        iterations: Int,
        label: String,
        _ operation: () -> Void
    ) -> TheirCoreBenchmarkMetrics {
        let start = DispatchTime.now().uptimeNanoseconds
        operation()
        let end = DispatchTime.now().uptimeNanoseconds
        return TheirCoreBenchmarkMetrics(
            elapsedNanoseconds: end - start,
            iterations: iterations,
            label: label
        )
    }

    static func measureAsync(
        iterations: Int,
        label: String,
        _ operation: () async throws -> Void
    ) async throws -> TheirCoreBenchmarkMetrics {
        let start = DispatchTime.now().uptimeNanoseconds
        try await operation()
        let end = DispatchTime.now().uptimeNanoseconds
        return TheirCoreBenchmarkMetrics(
            elapsedNanoseconds: end - start,
            iterations: iterations,
            label: label
        )
    }

    static func report(_ metrics: TheirCoreBenchmarkMetrics) {
        print(
            "BENCHMARK \(metrics.label): \(metrics.iterations) iterations, "
                + "\(metrics.elapsedNanoseconds) ns total, "
                + "\(String(format: "%.0f", metrics.operationsPerSecond)) ops/s"
        )
    }
}

private struct TheirCoreBenchmarkMetrics: Sendable {

    let elapsedNanoseconds: UInt64
    let iterations: Int
    let label: String

    var operationsPerSecond: Double {
        guard elapsedNanoseconds > 0 else {
            return 0
        }
        return Double(iterations) * 1_000_000_000 / Double(elapsedNanoseconds)
    }
}

private enum TheirCoreBenchmarkTestsError: Equatable, Swift.Error, Sendable {}

private typealias TheirCoreBenchmarkJobEngineEventRecorder =
Their.TestEventRecorder<JobEngineEvent<Int, TheirCoreBenchmarkTestsError>>
private typealias TheirCoreBenchmarkWorkRecorder =
Their.TestWorkRecorder<Int, TheirCoreBenchmarkTestsError>

private extension Array where Element == JobEngineEvent<Int, TheirCoreBenchmarkTestsError> {

    var values: [Int] {
        compactMap { event in
            switch event {
            case .value(let value):
                return value
            case .finished, .failure, .message:
                return nil
            }
        }
    }
}
