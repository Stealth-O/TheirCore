import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite(.serialized)
struct LifecycleLoggingTests {

    @Test func activeLoggingMetadataDoesNotCrossDerivedHubChain() async throws {
        try await Their.stress(count: 1) {
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests { _ in }
            do {
                let logging = Their.LifecycleLogging(
                    file: "RootHub.swift",
                    line: 77,
                    label: "discovery"
                )
                let work = Their.TestWorkRecorder<Int, LifecycleLoggingTestsError>()
                let hub: Their.Hub<Int, LifecycleLoggingTestsError> = Their.Hub(
                    logging: logging,
                    work: work.work
                )
                let derived = hub
                    .shareLatest()
                    .map(String.init)
                    .evolve(initial: [String]()) { state, value in
                        state.append(value)
                        return state
                    }

                #expect(hub.logging == logging)
                #expect(derived.logging == nil)
            }
        }
    }

    @Test func activeLoggingMetadataDoesNotCrossEvolve() async throws {
        try await Their.stress(count: 1) {
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests { _ in }
            do {
                let logging = Their.LifecycleLogging(
                    file: "RootHub.swift",
                    line: 33,
                    label: "saved-discoveries"
                )
                let work = Their.TestWorkRecorder<Int, LifecycleLoggingTestsError>()
                let hub: Their.Hub<Int, LifecycleLoggingTestsError> = Their.Hub(
                    logging: logging,
                    work: work.work
                )
                let evolved = hub.evolve(initial: 0) { state, value in
                    state += value
                    return state
                }

                #expect(evolved.logging == nil)
            }
        }
    }

    @Test func activeLoggingMetadataDoesNotCrossShareLatest() async throws {
        try await Their.stress(count: 1) {
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests { _ in }
            do {
                let logging = Their.LifecycleLogging(
                    file: "RootHub.swift",
                    line: 52,
                    label: "active-discovery"
                )
                let work = Their.TestWorkRecorder<Int, LifecycleLoggingTestsError>()
                let hub: Their.Hub<Int, LifecycleLoggingTestsError> = Their.Hub(
                    logging: logging,
                    work: work.work
                ).shareLatest()

                #expect(hub.logging == nil)
            }
        }
    }

    @Test func concurrentRootJobsOutputFollowsLiveCountFIFO() async throws {
        let jobs = Their.Lock<[Their.Job<Int, LifecycleLoggingTestsError>?]>([])
        let logging = Their.LifecycleLogging(
            file: "Stress.swift",
            line: 404,
            label: "Stress"
        )
        let recorder = LifecycleLoggingOutputRecorder()
        Their.LifecycleLogging.resetForTests()
        defer {
            Their.LifecycleLogging.resetForTests()
        }
        Their.LifecycleLogging.setOutputForTests(recorder.append(_:))

        try await Their.stress {
            let job: Their.Job<Int, LifecycleLoggingTestsError> = Their.Job(
                logging: logging
            ) { _ in {} }
            jobs.withLock { jobs in
                jobs.append(job)
            }
        }
        #expect(recorder.liveCounts == Array(1 ... Their.stressCountDefault))

        try await Their.stress(count: Their.stressCountDefault) { iteration in
            jobs.withLock { jobs in
                jobs[iteration] = nil
            }
        }

        let expectedCounts = Array(1 ... Their.stressCountDefault)
            + Array((0 ..< Their.stressCountDefault).reversed())
        #expect(recorder.liveCounts == expectedCounts)
    }

    @Test func outputDrainsReentrantLogsInFIFOOrder() async throws {
        try await Their.stress(count: 1) {
            let logging = Their.LifecycleLogging(
                file: "Reentrant.swift",
                line: 1,
                label: "Reentrant"
            )
            let recorder = LifecycleLoggingOutputRecorder()
            let shouldReenter = Their.Lock(true)
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests { line in
                recorder.append("begin \(line)")
                let reenter = shouldReenter.withLock { shouldReenter in
                    let result = shouldReenter
                    shouldReenter = false
                    return result
                }
                if reenter {
                    logging.logLifecycle("job init")
                }
                recorder.append("end \(line)")
            }

            logging.logLifecycle("job init")

            #expect(recorder.lines == [
                "begin ~~| [Reentrant] (1)",
                "end ~~| [Reentrant] (1)",
                "begin ~~| [Reentrant] (2)",
                "end ~~| [Reentrant] (2)",
            ])
        }
    }

    @Test func resetDuringReentrantOutputPreservesQueueAndDrainer() async throws {
        try await Their.stress(count: 1) {
            let logging = Their.LifecycleLogging(
                file: "Reset.swift",
                line: 1,
                label: "Reset"
            )
            let recorder = LifecycleLoggingOutputRecorder()
            let shouldReset = Their.Lock(true)
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            let output: @Sendable (String) -> Void = { line in
                recorder.append("begin \(line)")
                let reset = shouldReset.withLock { shouldReset in
                    let result = shouldReset
                    shouldReset = false
                    return result
                }
                if reset {
                    logging.logLifecycle("job init")
                    Their.LifecycleLogging.resetForTests()
                    Their.LifecycleLogging.setOutputForTests(recorder.append(_:))
                    logging.logLifecycle("job init")
                }
                recorder.append("end \(line)")
            }
            Their.LifecycleLogging.setOutputForTests(output)

            logging.logLifecycle("job init")

            #expect(recorder.lines == [
                "begin ~~| [Reset] (1)",
                "end ~~| [Reset] (1)",
                "begin ~~| [Reset] (2)",
                "end ~~| [Reset] (2)",
                "~~| [Reset] (1)",
            ])
        }
    }

    @Test func topLevelLoggingCanBeDisabled() async throws {
        try await Their.stress(count: 1) {
            let recorder = LifecycleLoggingOutputRecorder()
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests(recorder.append(_:))
            var job: Their.Job<Int, LifecycleLoggingTestsError>? = Their.Job(
                logging: Their.LifecycleLogging(
                    file: "Disabled.swift",
                    line: 1,
                    label: "Disabled",
                    options: []
                )
            ) { _ in {} }
            job = nil

            #expect(recorder.lines.isEmpty == true)
        }
    }

    @Test func topLevelLoggingCountsDistinctOriginsIndependently() async throws {
        try await Their.stress(count: 1) {
            let first = Their.LifecycleLogging(
                file: "First.swift",
                line: 1,
                label: "First"
            )
            let second = Their.LifecycleLogging(
                file: "Second.swift",
                line: 2,
                label: "Second"
            )
            let recorder = LifecycleLoggingOutputRecorder()
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests(recorder.append(_:))
            var firstJob: Their.Job<Int, LifecycleLoggingTestsError>? = Their.Job(logging: first) { _ in {} }
            var secondJob: Their.Job<Int, LifecycleLoggingTestsError>? = Their.Job(logging: second) { _ in {} }
            // Distinct origins keep independent counters: the second root reports
            // (1) for its own key rather than incrementing a shared counter to (2).
            #expect(recorder.lines == [
                "~~| [First] (1)",
                "~~| [Second] (1)",
            ])

            firstJob = nil
            secondJob = nil
            #expect(recorder.lines == [
                "~~| [First] (1)",
                "~~| [Second] (1)",
                "~~| [First] (0)",
                "~~| [Second] (0)",
            ])
        }
    }

    @Test func topLevelLoggingCountsRootJobsFromSameOrigin() async throws {
        try await Their.stress(count: 1) {
            let logging = Their.LifecycleLogging(
                file: "Discovery.swift",
                line: 42,
                label: "Discovery"
            )
            let recorder = LifecycleLoggingOutputRecorder()
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests(recorder.append(_:))
            var jobs: [Their.Job<Int, LifecycleLoggingTestsError>?] = [
                Their.Job(logging: logging) { _ in {} },
                Their.Job(logging: logging) { _ in {} },
            ]

            #expect(recorder.lines == [
                "~~| [Discovery] (1)",
                "~~| [Discovery] (2)",
            ])

            jobs[0] = nil
            #expect(recorder.lines == [
                "~~| [Discovery] (1)",
                "~~| [Discovery] (2)",
                "~~| [Discovery] (1)",
            ])
            jobs[1] = nil
            #expect(recorder.lines == [
                "~~| [Discovery] (1)",
                "~~| [Discovery] (2)",
                "~~| [Discovery] (1)",
                "~~| [Discovery] (0)",
            ])
        }
    }

    @Test func topLevelLoggingIgnoresDerivedAndInternalEvents() async throws {
        try await Their.stress(count: 1) {
            let logging = Their.LifecycleLogging(
                file: "Discovery.swift",
                line: 77,
                label: "Discovery"
            )
            let recorder = LifecycleLoggingOutputRecorder()
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests(recorder.append(_:))
            logging.logLifecycle("engine init")
            logging.logLifecycle("engine subscribe")
            logging.logLifecycle("job engine init")
            var hub: Their.Hub<Int, LifecycleLoggingTestsError>? = Their.Hub(
                logging: logging
            ) { _ in {} }
                .shareLatest()
            #expect(hub?.logging == nil)

            #expect(recorder.lines == [
                "~~| [Discovery] (1)",
            ])

            hub = nil
            #expect(recorder.lines == [
                "~~| [Discovery] (1)",
                "~~| [Discovery] (0)",
            ])
        }
    }

    @Test func topLevelLoggingIncludesOriginWhenRequested() async throws {
        try await Their.stress(count: 1) {
            let recorder = LifecycleLoggingOutputRecorder()
            Their.LifecycleLogging.resetForTests()
            defer {
                Their.LifecycleLogging.resetForTests()
            }
            Their.LifecycleLogging.setOutputForTests(recorder.append(_:))
            var job: Their.Job<Int, LifecycleLoggingTestsError>? = Their.Job(
                logging: Their.LifecycleLogging(
                    file: "Origin.swift",
                    line: 9,
                    label: "Origin",
                    showOrigin: true
                )
            ) { _ in {} }
            job = nil

            #expect(recorder.lines == [
                "~~| [Origin] (1) origin=Origin.swift:9",
                "~~| [Origin] (0) origin=Origin.swift:9",
            ])
        }
    }
}

private enum LifecycleLoggingTestsError: Swift.Error, Sendable {}

private final class LifecycleLoggingOutputRecorder: Sendable {

    var lines: [String] {
        lock.withLock { lines in
            lines
        }
    }
    var liveCounts: [Int] {
        lines.compactMap(Self.liveCount(from:))
    }
    private let lock = Their.Lock([String]())

    func append(_ line: String) {
        lock.withLock { lines in
            lines.append(line)
        }
    }

    private static func liveCount(from line: String) -> Int? {
        guard let open = line.lastIndex(of: "("),
              let close = line.lastIndex(of: ")"),
              open < close
        else {
            return nil
        }
        return Int(line[line.index(after: open) ..< close])
    }
}
