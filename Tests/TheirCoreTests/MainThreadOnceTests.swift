import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct MainThreadOnceTests {

    @Test func concurrentCallsAfterConfiguredAreNoOps() async throws {
        let background = DispatchQueue(
            label: "MainThreadOnceTests.background",
            attributes: .concurrent
        )
        let completions = Their.TestCountRecorder()
        let fakeMain = FakeMain()
        let workCalls = Their.TestCountRecorder()
        let once = Their.MainThreadOnce(
            isMainThread: fakeMain.isMainThread,
            runOnMain: fakeMain.runOnMain,
            work: { _ = workCalls.increment() }
        )
        fakeMain.runSync { once.run() }
        #expect(workCalls.count == 1)

        try await Their.stress { iteration in
            if iteration.isMultiple(of: 2) {
                fakeMain.enqueue {
                    once.run()
                    _ = completions.increment()
                }
            } else {
                background.async {
                    once.run()
                    _ = completions.increment()
                }
            }
        }
        try await completions.waitForCount(Their.stressCountDefault)

        #expect(workCalls.count == 1)
    }

    @Test func concurrentMainAndMainConfigureOnce() async throws {
        try await Their.stress {
            let completions = Their.TestCountRecorder()
            let fakeMain = FakeMain()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: fakeMain.runOnMain,
                work: { _ = workCalls.increment() }
            )

            fakeMain.enqueue {
                once.run()
                _ = completions.increment()
            }
            fakeMain.enqueue {
                once.run()
                _ = completions.increment()
            }
            try await completions.waitForCount(2)

            #expect(workCalls.count == 1)
        }
    }

    @Test func concurrentMainAndNonMainConfigureOnce() async throws {
        try await Their.stress {
            let background = DispatchQueue(label: "MainThreadOnceTests.background")
            let completions = Their.TestCountRecorder()
            let fakeMain = FakeMain()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: fakeMain.runOnMain,
                work: { _ = workCalls.increment() }
            )

            fakeMain.enqueue {
                once.run()
                _ = completions.increment()
            }
            background.async {
                once.run()
                _ = completions.increment()
            }
            try await completions.waitForCount(2)

            #expect(workCalls.count == 1)
        }
    }

    @Test func concurrentMixedCallersConfigureWorkExactlyOnce() async throws {
        let background = DispatchQueue(
            label: "MainThreadOnceTests.background",
            attributes: .concurrent
        )
        let completions = Their.TestCountRecorder()
        let fakeMain = FakeMain()
        let workCalls = Their.TestCountRecorder()
        let once = Their.MainThreadOnce(
            isMainThread: fakeMain.isMainThread,
            runOnMain: fakeMain.runOnMain,
            work: { _ = workCalls.increment() }
        )

        try await Their.stress { iteration in
            if iteration.isMultiple(of: 2) {
                fakeMain.enqueue {
                    once.run()
                    _ = completions.increment()
                }
            } else {
                background.async {
                    once.run()
                    _ = completions.increment()
                }
            }
        }
        try await completions.waitForCount(Their.stressCountDefault)

        #expect(workCalls.count == 1)
    }

    @Test func concurrentNonMainAndMainConfigureOnce() async throws {
        try await Their.stress {
            let background = DispatchQueue(label: "MainThreadOnceTests.background")
            let completions = Their.TestCountRecorder()
            let fakeMain = FakeMain()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: fakeMain.runOnMain,
                work: { _ = workCalls.increment() }
            )

            background.async {
                once.run()
                _ = completions.increment()
            }
            fakeMain.enqueue {
                once.run()
                _ = completions.increment()
            }
            try await completions.waitForCount(2)

            #expect(workCalls.count == 1)
        }
    }

    @Test func concurrentNonMainAndNonMainConfigureOnce() async throws {
        try await Their.stress {
            let backgroundOne = DispatchQueue(label: "MainThreadOnceTests.background.one")
            let backgroundTwo = DispatchQueue(label: "MainThreadOnceTests.background.two")
            let completions = Their.TestCountRecorder()
            let fakeMain = FakeMain()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: fakeMain.runOnMain,
                work: { _ = workCalls.increment() }
            )

            backgroundOne.async {
                once.run()
                _ = completions.increment()
            }
            backgroundTwo.async {
                once.run()
                _ = completions.increment()
            }
            try await completions.waitForCount(2)

            #expect(workCalls.count == 1)
        }
    }

    @Test func configuredNonMainCallerReturnsWithoutScheduling() async throws {
        try await Their.stress {
            let background = DispatchQueue(label: "MainThreadOnceTests.background")
            let completions = Their.TestCountRecorder()
            let fakeMain = FakeMain()
            let scheduledWorks = Their.TestEventRecorder<MainThreadOnceMainWork>()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: { work in
                    scheduledWorks.append(MainThreadOnceMainWork(work: work))
                },
                work: { _ = workCalls.increment() }
            )

            fakeMain.runSync { once.run() }
            #expect(workCalls.count == 1)

            background.async {
                once.run()
                _ = completions.increment()
            }
            try await completions.waitForCount(1)

            #expect(workCalls.count == 1)
            #expect(scheduledWorks.count == 0)
        }
    }

    @Test func mainCallerConfiguresImmediatelyAndRepeatIsNoOp() async throws {
        try await Their.stress {
            let fakeMain = FakeMain()
            let scheduledWorks = Their.TestEventRecorder<MainThreadOnceMainWork>()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: { work in
                    scheduledWorks.append(MainThreadOnceMainWork(work: work))
                },
                work: { _ = workCalls.increment() }
            )

            fakeMain.runSync { once.run() }
            #expect(workCalls.count == 1)
            #expect(scheduledWorks.count == 0)

            fakeMain.runSync { once.run() }
            #expect(workCalls.count == 1)
            #expect(scheduledWorks.count == 0)
        }
    }

    @Test func nonMainCallerIsCompletedByMainCallerBeforeScheduledWorkRuns() async throws {
        try await Their.stress {
            let background = DispatchQueue(label: "MainThreadOnceTests.background")
            let completions = Their.TestCountRecorder()
            let fakeMain = FakeMain()
            let scheduledWorks = Their.TestEventRecorder<MainThreadOnceMainWork>()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: { work in
                    scheduledWorks.append(MainThreadOnceMainWork(work: work))
                },
                work: { _ = workCalls.increment() }
            )

            background.async {
                once.run()
                _ = completions.increment()
            }
            let scheduled = try await scheduledWorks.waitForEvent { _ in true }
            #expect(workCalls.count == 0)

            // A main caller configures and releases the waiting background caller
            // before the scheduled main hop ever runs.
            fakeMain.runSync { once.run() }
            try await completions.waitForCount(1)
            #expect(workCalls.count == 1)

            // The late scheduled hop now sees `configured` and is a no-op.
            fakeMain.runSync { scheduled.run() }
            #expect(workCalls.count == 1)
            #expect(scheduledWorks.count == 1)
        }
    }

    @Test func nonMainCallerSchedulesOnMainAndConfiguresWhenMainRuns() async throws {
        try await Their.stress {
            let background = DispatchQueue(label: "MainThreadOnceTests.background")
            let completions = Their.TestCountRecorder()
            let fakeMain = FakeMain()
            let scheduledWorks = Their.TestEventRecorder<MainThreadOnceMainWork>()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: { work in
                    scheduledWorks.append(MainThreadOnceMainWork(work: work))
                },
                work: { _ = workCalls.increment() }
            )

            background.async {
                once.run()
                _ = completions.increment()
            }
            let scheduled = try await scheduledWorks.waitForEvent { _ in true }
            #expect(workCalls.count == 0)
            #expect(completions.count == 0)

            fakeMain.runSync { scheduled.run() }
            try await completions.waitForCount(1)

            #expect(workCalls.count == 1)
            #expect(scheduledWorks.count == 1)
        }
    }

    @Test func reentrantRunInsideWorkDoesNotReconfigure() async throws {
        try await Their.stress {
            let fakeMain = FakeMain()
            let onceBox = Their.Lock<Their.MainThreadOnce?>(nil)
            let reentrantCalls = Their.TestCountRecorder()
            let scheduledWorks = Their.TestEventRecorder<MainThreadOnceMainWork>()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: { work in
                    scheduledWorks.append(MainThreadOnceMainWork(work: work))
                },
                work: {
                    _ = workCalls.increment()
                    // Re-enter exactly once from inside `work`, while the phase is
                    // `configuring`: it must return immediately without recursing,
                    // re-running `work` or scheduling another hop.
                    if reentrantCalls.increment() == 1 {
                        onceBox.withLock { box in box }?.run()
                    }
                }
            )
            onceBox.withLock { box in box = once }

            fakeMain.runSync { once.run() }

            #expect(workCalls.count == 1)
            #expect(reentrantCalls.count == 1)
            #expect(scheduledWorks.count == 0)
        }
    }

    @Test func secondNonMainCallerDoesNotScheduleAndIsReleasedByOneConfigure() async throws {
        try await Their.stress {
            let backgroundOne = DispatchQueue(label: "MainThreadOnceTests.background.one")
            let backgroundTwo = DispatchQueue(label: "MainThreadOnceTests.background.two")
            let completions = Their.TestCountRecorder()
            let fakeMain = FakeMain()
            let scheduledWorks = Their.TestEventRecorder<MainThreadOnceMainWork>()
            let workCalls = Their.TestCountRecorder()
            let once = Their.MainThreadOnce(
                isMainThread: fakeMain.isMainThread,
                runOnMain: { work in
                    scheduledWorks.append(MainThreadOnceMainWork(work: work))
                },
                work: { _ = workCalls.increment() }
            )

            backgroundOne.async {
                once.run()
                _ = completions.increment()
            }
            let scheduled = try await scheduledWorks.waitForEvent { _ in true }
            // First caller has scheduled the single main hop and is now waiting;
            // the phase stays pre-`configured` until the captured hop runs. The
            // second caller therefore takes either the `waitingForMain` wait or the
            // later `configured` no-op, and in both cases must not schedule again.
            backgroundTwo.async {
                once.run()
                _ = completions.increment()
            }

            fakeMain.runSync { scheduled.run() }
            try await completions.waitForCount(2)

            #expect(workCalls.count == 1)
            #expect(scheduledWorks.count == 1)
        }
    }
}

private final class FakeMain: Sendable {

    private let flagKey = "MainThreadOnceTests.fakeMain.flag"
    var isMainThread: @Sendable () -> Bool {
        let flagKey = flagKey
        return {
            Thread.current.threadDictionary[flagKey] as? Bool == true
        }
    }
    private let queue = DispatchQueue(label: "MainThreadOnceTests.fakeMain")
    var runOnMain: @Sendable (@escaping @Sendable () -> Void) -> Void {
        { [self] work in
            enqueue(work)
        }
    }

    func enqueue(_ body: @escaping @Sendable () -> Void) {
        let flagKey = flagKey
        queue.async {
            Thread.current.threadDictionary[flagKey] = true
            body()
            Thread.current.threadDictionary[flagKey] = false
        }
    }

    func runSync(_ body: () -> Void) {
        let flagKey = flagKey
        queue.sync {
            Thread.current.threadDictionary[flagKey] = true
            body()
            Thread.current.threadDictionary[flagKey] = false
        }
    }
}

private struct MainThreadOnceMainWork: Sendable {

    let work: @Sendable () -> Void

    func run() {
        work()
    }
}
