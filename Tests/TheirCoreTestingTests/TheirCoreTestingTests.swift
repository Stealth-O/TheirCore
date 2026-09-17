import Testing
import TheirCore
@testable
import TheirCoreTesting

@Suite
struct TheirCoreTestingTests {

    @Test func testCancelRecorderCancellationPropagatesFromDelegatedWaiter() async throws {
        try await Their.stress {
            let recorder = Their.TestCancelRecorder()
            let waiter = Task {
                do {
                    try await recorder.waitForCancelCallsCount(1)
                    return false
                } catch is CancellationError {
                    return true
                } catch {
                    return false
                }
            }

            waiter.cancel()

            #expect(await waiter.value)
            recorder.record()
            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func testCancelRecorderRecordsWorkCancelInvocations() async throws {
        try await Their.stress {
            let recorder = Their.TestCancelRecorder()
            let cancel = recorder.cancel()

            cancel()
            try await recorder.waitForCancelCallsCount(1)

            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func testCountRecorderCancellationRemovesRegisteredWaiter() async throws {
        try await Their.stress {
            let recorder = Their.TestCountRecorder()
            let suspended = Their.TestSignal()
            let waiter = Task {
                do {
                    try await recorder.waitForCountForTests(1) {
                        suspended.signal()
                    }
                    return false
                } catch is CancellationError {
                    return true
                } catch {
                    return false
                }
            }

            try await suspended.wait()
            waiter.cancel()

            #expect(await waiter.value)
            #expect(recorder.increment() == 1)
        }
    }

    @Test func testCountRecorderResumesManyWaitersAtTheirThresholds() async throws {
        try await Their.stress {
            let gate = TheirCoreTestingGate()
            let recorder = Their.TestCountRecorder()
            let releaseRecorder = TheirCoreTestingEventGate<Int>()
            let waiters = (1 ... 3).map { threshold in
                Task {
                    await gate.enter()
                    try await recorder.waitForCount(threshold)
                    await releaseRecorder.append(threshold)
                }
            }

            await gate.waitForCount(3)
            #expect(await releaseRecorder.events().isEmpty)
            #expect(recorder.increment() == 1)
            await releaseRecorder.waitForCount(1)
            #expect(await releaseRecorder.events() == [1])
            #expect(recorder.increment() == 2)
            await releaseRecorder.waitForCount(2)
            #expect(await releaseRecorder.events().sorted() == [1, 2])
            #expect(recorder.increment() == 3)
            await releaseRecorder.waitForCount(3)
            #expect(await releaseRecorder.events().sorted() == [1, 2, 3])

            for waiter in waiters {
                try await waiter.value
            }
        }
    }

    @Test func testCountRecorderWaitReturnsImmediatelyWhenThresholdAlreadyReached() async throws {
        try await Their.stress {
            let recorder = Their.TestCountRecorder()

            _ = recorder.increment()
            try await recorder.waitForCount(1)

            #expect(recorder.count == 1)
        }
    }

    @Test func testEventRecorderCountCancellationRemovesRegisteredWaiter() async throws {
        try await Their.stress {
            let recorder = Their.TestEventRecorder<Int>()
            let suspended = Their.TestSignal()
            let waiter = Task {
                do {
                    try await recorder.waitForEventCountForTests(1) {
                        suspended.signal()
                    }
                    return false
                } catch is CancellationError {
                    return true
                } catch {
                    return false
                }
            }

            try await suspended.wait()
            waiter.cancel()

            #expect(await waiter.value)
            recorder.append(1)
            #expect(recorder.events == [1])
        }
    }

    @Test func testEventRecorderCountWaitersResumeAtTheirThresholds() async throws {
        try await Their.stress {
            let gate = TheirCoreTestingGate()
            let recorder = Their.TestEventRecorder<String>()
            let releaseRecorder = TheirCoreTestingEventGate<Int>()
            let waiters = (1 ... 3).map { threshold in
                Task {
                    await gate.enter()
                    try await recorder.waitForEventCount(threshold)
                    await releaseRecorder.append(threshold)
                }
            }

            await gate.waitForCount(3)
            #expect(await releaseRecorder.events().isEmpty)
            recorder.append("a")
            await releaseRecorder.waitForCount(1)
            #expect(await releaseRecorder.events() == [1])
            recorder.append("b")
            await releaseRecorder.waitForCount(2)
            #expect(await releaseRecorder.events().sorted() == [1, 2])
            recorder.append("c")
            await releaseRecorder.waitForCount(3)
            #expect(await releaseRecorder.events().sorted() == [1, 2, 3])
            #expect(recorder.currentEvents() == ["a", "b", "c"])

            for waiter in waiters {
                try await waiter.value
            }
        }
    }

    @Test func testEventRecorderEventCancellationRaceResumesExactlyOnce() async throws {
        try await Their.stress {
            let queue = TestQueue(count: 2)
            let recorder = Their.TestEventRecorder<Int>()
            let suspended = Their.TestSignal()
            let waiter = Task {
                do {
                    try await recorder.waitForEventCountForTests(1) {
                        suspended.signal()
                    }
                    return "event"
                } catch is CancellationError {
                    return "cancellation"
                } catch {
                    return "unexpected"
                }
            }

            try await suspended.wait()
            let append = Task {
                await queue.enter()
                recorder.append(1)
            }
            let cancel = Task {
                await queue.enter()
                waiter.cancel()
            }
            await queue.full()
            queue.open()

            await append.value
            await cancel.value
            let result = await waiter.value

            #expect(result == "event" || result == "cancellation")
            #expect(recorder.events == [1])
        }
    }

    @Test func testEventRecorderPredicateCancellationThrowsAndLateEventIsSafe() async throws {
        try await Their.stress {
            let recorder = Their.TestEventRecorder<Int>()
            let waiter = Task {
                do {
                    _ = try await recorder.waitForEvent { $0 == 1 }
                    return false
                } catch is CancellationError {
                    return true
                } catch {
                    return false
                }
            }

            waiter.cancel()

            #expect(await waiter.value)
            recorder.append(1)
            #expect(recorder.events == [1])
        }
    }

    @Test func testEventRecorderPredicateRunsOutsideLockOncePerEvent() async throws {
        try await Their.stress {
            let calls = Their.Lock<[Int]>([])
            let recorder = Their.TestEventRecorder<Int>()

            recorder.append(1)
            recorder.append(2)

            let event = try await recorder.waitForEvent { event in
                calls.withLock { calls in
                    calls.append(event)
                }
                return recorder.count == 2 && event == 2
            }

            #expect(calls.withLock { $0 } == [1, 2])
            #expect(event == 2)
        }
    }

    @Test func testEventRecorderPredicateWaiterReturnsExistingEventImmediately() async throws {
        try await Their.stress {
            let recorder = Their.TestEventRecorder<String>()

            recorder.append("first")
            recorder.append("second")

            let event = try await recorder.waitForEvent { $0 == "second" }
            #expect(event == "second")
        }
    }

    @Test func testEventRecorderPredicateWaitersResumeOnlyForMatchingEvents() async throws {
        try await Their.stress {
            let gate = TheirCoreTestingGate()
            let recorder = Their.TestEventRecorder<String>()
            let releaseRecorder = TheirCoreTestingEventGate<String>()
            let first = Task {
                await gate.enter()
                let event = try await recorder.waitForEvent { $0 == "first" }
                await releaseRecorder.append(event)
            }
            let second = Task {
                await gate.enter()
                let event = try await recorder.waitForEvent { $0 == "second" }
                await releaseRecorder.append(event)
            }

            await gate.waitForCount(2)
            recorder.append("ignored")
            #expect(await releaseRecorder.events().isEmpty)
            recorder.append("second")
            await releaseRecorder.waitForCount(1)
            #expect(await releaseRecorder.events() == ["second"])
            recorder.append("first")
            await releaseRecorder.waitForCount(2)
            #expect(await releaseRecorder.events().sorted() == ["first", "second"])

            try await first.value
            try await second.value
        }
    }

    @Test func testHubDriverStartsEmitsAndCancelsHub() async throws {
        try await Their.stress {
            let driver = Their.TestHubDriver<Int, TheirCoreTestingTestsError>()
            let recorder = Their.TestEventRecorder<Their.HubEvent<Int, TheirCoreTestingTestsError>>()

            let cancel = driver.hub.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: 2)
            try await recorder.waitForEventCount(1)
            cancel()
            try await driver.waitForCancelCallsCount(1)

            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
            #expect(recorder.events == [.value(2)])
        }
    }

    @Test func testJobDriverStartsEmitsAndCancelsJob() async throws {
        try await Their.stress {
            let driver = Their.TestJobDriver<Int, TheirCoreTestingTestsError>()
            let recorder = Their.TestEventRecorder<Their.JobEvent<Int, TheirCoreTestingTestsError>>()

            let cancel = driver.job.subscribe(recorder.append(_:))
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: 1)
            try await recorder.waitForEventCount(1)
            cancel()
            try await driver.waitForCancelCallsCount(1)

            #expect(driver.cancelCallsCount == 1)
            #expect(driver.startCallsCount == 1)
            #expect(recorder.events == [.value(1)])
        }
    }

    @Test func testMisuseRecorderHandlerRecordsMisuse() async throws {
        try await Their.stress {
            let recorder = Their.TestMisuseRecorder()
            let misuse = Their.Misuse(message: "sample misuse")

            recorder.handler(misuse)
            try await recorder.waitForCount(1)

            #expect(recorder.misuses == [misuse])
        }
    }

    @Test func testQueueHoldsEntrantsUntilOpened() async throws {
        try await Their.stress {
            let queue = TestQueue(count: 2)
            let recorder = TheirCoreTestingEventGate<String>()
            let first = Task {
                await queue.enter()
                await recorder.append("first")
            }
            let second = Task {
                await queue.enter()
                await recorder.append("second")
            }

            await queue.full()
            #expect(await recorder.events().isEmpty == true)
            queue.open()
            await recorder.waitForCount(2)

            #expect(await recorder.events().sorted() == ["first", "second"])
            await first.value
            await second.value
        }
    }

    @Test func testSignalCancellationRemovesRegisteredWaiter() async throws {
        try await Their.stress {
            let signal = Their.TestSignal()
            let suspended = Their.TestSignal()
            let waiter = Task {
                do {
                    try await signal.waitForTests {
                        suspended.signal()
                    }
                    return false
                } catch is CancellationError {
                    return true
                } catch {
                    return false
                }
            }

            try await suspended.wait()
            waiter.cancel()

            #expect(await waiter.value)
            signal.signal()
        }
    }

    @Test func testSignalRepeatedSignalIsNoop() async throws {
        try await Their.stress {
            let signal = Their.TestSignal()

            signal.signal()
            signal.signal()
            try await signal.wait()
        }
    }

    @Test func testSignalResumesManyWaiters() async throws {
        try await Their.stress {
            let gate = TheirCoreTestingGate()
            let releaseRecorder = TheirCoreTestingEventGate<Int>()
            let signal = Their.TestSignal()
            let waiters = (0 ..< 5).map { index in
                Task {
                    await gate.enter()
                    try await signal.wait()
                    await releaseRecorder.append(index)
                }
            }

            await gate.waitForCount(5)
            #expect(await releaseRecorder.events().isEmpty)
            signal.signal()
            await releaseRecorder.waitForCount(5)
            #expect(await releaseRecorder.events().sorted() == [0, 1, 2, 3, 4])

            for waiter in waiters {
                try await waiter.value
            }
        }
    }

    @Test func testSignalSignalsFromSynchronousContext() async throws {
        try await Their.stress {
            let lock = Their.Lock(0)
            let signal = Their.TestSignal()
            let waiter = Task {
                try await signal.wait()
            }
            lock.withLock { value in
                value += 1
                signal.signal()
            }
            try await waiter.value
            #expect(lock.withLock { value in value } == 1)
        }
    }

    @Test func testSignalWaitReturnsImmediatelyAfterSignal() async throws {
        try await Their.stress {
            let signal = Their.TestSignal()

            signal.signal()
            try await signal.wait()
        }
    }

    // Not wrapped in `Their.stress { }`: this is the meta-test for `Their.stress` itself.
    @Test func testStressParentCancellationCancelsAndJoinsOperation() async throws {
        let cleanupGate = TheirCoreTestingUncancellableGate()
        let cleanupReleased = Their.Lock(false)
        let joinObservedCleanup = Their.Lock(false)
        let observedError = Their.Lock<String?>(nil)
        let operationCancelled = Their.TestSignal()
        let operationGate = Their.TestSignal()
        let operationStarted = Their.TestSignal()
        let timeoutClock = TheirCoreTestingManualTimeoutClock()
        let task = Task {
            do {
                try await Their.stress(
                    count: 1,
                    timeout: .milliseconds(99),
                    dependencies: .init(
                        afterOperationJoined: {
                            joinObservedCleanup.withLock { observed in
                                observed = cleanupReleased.withLock { $0 }
                            }
                        },
                        sleep: { nanoseconds in
                            try await timeoutClock.sleep(nanoseconds: nanoseconds)
                        }
                    )
                ) { _ in
                    operationStarted.signal()
                    do {
                        try await operationGate.wait()
                    } catch {
                        operationCancelled.signal()
                        await cleanupGate.wait()
                        throw error
                    }
                }
                Issue.record("Expected parent cancellation.")
            } catch is CancellationError {
                observedError.withLock { observedError in
                    observedError = "cancellation"
                }
            } catch {
                observedError.withLock { observedError in
                    observedError = String(describing: error)
                }
            }
        }

        try await operationStarted.wait()
        task.cancel()
        try await operationCancelled.wait()

        #expect(joinObservedCleanup.withLock { $0 } == false)
        cleanupReleased.withLock { $0 = true }
        await cleanupGate.open()
        await task.value

        #expect(joinObservedCleanup.withLock { $0 })
        #expect(observedError.withLock { $0 } == "cancellation")
    }

    // Not wrapped in `Their.stress { }`: this is the meta-test for `Their.stress` itself.
    @Test func testStressRunsDefaultFastIterationsUnderShortPerChildTimeout() async throws {
        let iterations = Their.stressCountDefault
        let recorder = Their.TestEventRecorder<Int>()

        try await Their.stress(
            count: iterations,
            timeout: .milliseconds(99)
        ) { iteration in
            recorder.append(iteration)
        }

        try await recorder.waitForEventCount(iterations)
        #expect(recorder.events.sorted() == Array(0 ..< iterations))
    }

    // Not wrapped in `Their.stress { }`: this is the meta-test for `Their.stress` itself.
    @Test func testStressRunsEveryIterationAfterBarrierOpens() async throws {
        let recorder = Their.TestEventRecorder<Int>()

        try await Their.stress { iteration in
            recorder.append(iteration)
        }

        try await recorder.waitForEventCount(Their.stressCountDefault)
        #expect(recorder.events.sorted() == Array(0 ..< Their.stressCountDefault))
    }

    // Not wrapped in `Their.stress { }`: this is the meta-test for `Their.stress` itself.
    @Test func testStressTimeoutClockArmsOnlyAfterOperationStarts() async throws {
        let beforeOperationEntered = Their.TestSignal()
        let beforeOperationRelease = Their.TestSignal()
        let operationRelease = Their.TestSignal()
        let timeoutClock = TheirCoreTestingManualTimeoutClock()
        let task = Task {
            try await Their.stress(
                count: 1,
                timeout: .milliseconds(99),
                dependencies: .init(
                    beforeOperationStarts: {
                        beforeOperationEntered.signal()
                        try await beforeOperationRelease.wait()
                    },
                    sleep: { nanoseconds in
                        try await timeoutClock.sleep(nanoseconds: nanoseconds)
                    }
                )
            ) { _ in
                try await operationRelease.wait()
                return 7
            }
        }

        try await beforeOperationEntered.wait()
        #expect(timeoutClock.sleepCallsCount == 0)
        beforeOperationRelease.signal()
        try await timeoutClock.waitForSleepCallsCount(1)
        operationRelease.signal()

        #expect(try await task.value == [7])
    }

    // Not wrapped in `Their.stress { }`: this is the meta-test for `Their.stress` itself.
    @Test func testStressTimeoutThrowsErrorInsteadOfStoppingProcess() async {
        do {
            try await Their.stress(
                count: 1,
                timeout: .milliseconds(1)
            ) {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
            Issue.record("Expected stress timeout.")
        } catch let error as Their.StressTimeoutError {
            #expect(error.iteration == 0)
            #expect(error.timeout == .milliseconds(1))
        } catch {
            Issue.record("Expected StressTimeoutError, got \(error).")
        }
    }

    // Not wrapped in `Their.stress { }`: this is the meta-test for `Their.stress` itself.
    @Test func testStressTimeoutWaitsForOperationCleanupBeforeThrowing() async throws {
        let cleanupGate = TheirCoreTestingUncancellableGate()
        let cleanupReleased = Their.Lock(false)
        let cleanupStarted = Their.TestSignal()
        let joinObservedCleanup = Their.Lock(false)
        let operationGate = Their.TestSignal()
        let timeoutClock = TheirCoreTestingManualTimeoutClock()
        let task = Task { () -> Their.StressTimeoutError? in
            do {
                try await Their.stress(
                    count: 1,
                    timeout: .milliseconds(99),
                    dependencies: .init(
                        afterOperationJoined: {
                            joinObservedCleanup.withLock { observed in
                                observed = cleanupReleased.withLock { $0 }
                            }
                        },
                        sleep: { nanoseconds in
                            try await timeoutClock.sleep(nanoseconds: nanoseconds)
                        }
                    )
                ) { _ in
                    do {
                        try await operationGate.wait()
                    } catch {
                        cleanupStarted.signal()
                        await cleanupGate.wait()
                        throw error
                    }
                }
                Issue.record("Expected stress timeout.")
                return nil
            } catch let error as Their.StressTimeoutError {
                return error
            } catch {
                Issue.record("Expected StressTimeoutError, got \(error).")
                return nil
            }
        }

        try await timeoutClock.waitForSleepCallsCount(1)
        timeoutClock.fire()
        try await cleanupStarted.wait()

        #expect(joinObservedCleanup.withLock { $0 } == false)
        cleanupReleased.withLock { $0 = true }
        await cleanupGate.open()
        let error = await task.value

        #expect(error?.iteration == 0)
        #expect(error?.timeout == .milliseconds(99))
        #expect(joinObservedCleanup.withLock { $0 })
    }

    // Not wrapped in `Their.stress { }`: this is the meta-test for `Their.stress` itself.
    @Test func testStressZeroCountReturnsEmptyResultWithoutRunningBlock() async throws {
        let recorder = Their.TestEventRecorder<Int>()

        let results = try await Their.stress(count: 0) { iteration in
            recorder.append(iteration)
            return iteration
        }

        #expect(results.isEmpty == true)
        #expect(recorder.events.isEmpty == true)
    }

    @Test func testWorkRecorderCancellationRemovesRegisteredWaiters() async throws {
        try await Their.stress {
            let cancelSuspended = Their.TestSignal()
            let recorder = Their.TestWorkRecorder<Int, TheirCoreTestingTestsError>()
            let startSuspended = Their.TestSignal()
            let cancelWaiter = Task {
                do {
                    try await recorder.waitForCancelCallsCountForTests(1) {
                        cancelSuspended.signal()
                    }
                    return false
                } catch is CancellationError {
                    return true
                } catch {
                    return false
                }
            }
            let startWaiter = Task {
                do {
                    try await recorder.waitForStartCallsCountForTests(1) {
                        startSuspended.signal()
                    }
                    return false
                } catch is CancellationError {
                    return true
                } catch {
                    return false
                }
            }

            try await cancelSuspended.wait()
            try await startSuspended.wait()
            cancelWaiter.cancel()
            startWaiter.cancel()

            #expect(await cancelWaiter.value)
            #expect(await startWaiter.value)
            let cancel = recorder.work { _ in }
            cancel()
            #expect(recorder.cancelCallsCount == 1)
            #expect(recorder.startCallsCount == 1)
        }
    }

    @Test func testWorkRecorderResumesStartAndCancelWaitersAtTheirThresholds() async throws {
        try await Their.stress {
            let cancelGate = TheirCoreTestingGate()
            let cancelReleaseRecorder = TheirCoreTestingEventGate<Int>()
            let recorder = Their.TestWorkRecorder<Int, TheirCoreTestingTestsError>()
            let startGate = TheirCoreTestingGate()
            let startReleaseRecorder = TheirCoreTestingEventGate<Int>()
            let cancelWaiters = (1 ... 2).map { threshold in
                Task {
                    await cancelGate.enter()
                    try await recorder.waitForCancelCallsCount(threshold)
                    await cancelReleaseRecorder.append(threshold)
                }
            }
            let startWaiters = (1 ... 2).map { threshold in
                Task {
                    await startGate.enter()
                    try await recorder.waitForStartCallsCount(threshold)
                    await startReleaseRecorder.append(threshold)
                }
            }

            await cancelGate.waitForCount(2)
            await startGate.waitForCount(2)
            let firstCancel = recorder.work { _ in }
            await startReleaseRecorder.waitForCount(1)
            #expect(await startReleaseRecorder.events() == [1])
            #expect(await cancelReleaseRecorder.events().isEmpty)
            let secondCancel = recorder.work { _ in }
            await startReleaseRecorder.waitForCount(2)
            #expect(await startReleaseRecorder.events().sorted() == [1, 2])
            firstCancel()
            await cancelReleaseRecorder.waitForCount(1)
            #expect(await cancelReleaseRecorder.events() == [1])
            secondCancel()
            await cancelReleaseRecorder.waitForCount(2)
            #expect(await cancelReleaseRecorder.events().sorted() == [1, 2])

            for waiter in cancelWaiters + startWaiters {
                try await waiter.value
            }
        }
    }
}

private actor TheirCoreTestingEventGate<Event: Sendable> {

    private var _events = [Event]()
    private var waiters = [TheirCoreTestingEventGateWaiter]()

    func append(_ event: Event) {
        _events.append(event)
        let output = splitWaiters(count: _events.count)
        waiters = output.pending
        output.ready.forEach { $0.continuation.resume() }
    }

    func events() -> [Event] {
        _events
    }

    private func splitWaiters(count: Int) -> (
        pending: [TheirCoreTestingEventGateWaiter],
        ready: [TheirCoreTestingEventGateWaiter]
    ) {
        var pending = [TheirCoreTestingEventGateWaiter]()
        var ready = [TheirCoreTestingEventGateWaiter]()
        for waiter in waiters {
            if waiter.count <= count {
                ready.append(waiter)
            } else {
                pending.append(waiter)
            }
        }
        return (
            pending: pending,
            ready: ready
        )
    }

    func waitForCount(_ count: Int) async {
        guard _events.count < count else {
            return
        }
        await withCheckedContinuation { continuation in
            guard _events.count < count else {
                continuation.resume()
                return
            }
            waiters.append(.init(
                continuation: continuation,
                count: count
            ))
        }
    }
}

private struct TheirCoreTestingEventGateWaiter: Sendable {

    let continuation: CheckedContinuation<Void, Never>
    let count: Int
}

private actor TheirCoreTestingGate {

    private var count = 0
    private var waiters = [TheirCoreTestingGateWaiter]()

    func enter() {
        count += 1
        let output = splitWaiters()
        waiters = output.pending
        output.ready.forEach { $0.continuation.resume() }
    }

    private func splitWaiters() -> (
        pending: [TheirCoreTestingGateWaiter],
        ready: [TheirCoreTestingGateWaiter]
    ) {
        var pending = [TheirCoreTestingGateWaiter]()
        var ready = [TheirCoreTestingGateWaiter]()
        for waiter in waiters {
            if waiter.count <= count {
                ready.append(waiter)
            } else {
                pending.append(waiter)
            }
        }
        return (
            pending: pending,
            ready: ready
        )
    }

    func waitForCount(_ count: Int) async {
        guard self.count < count else {
            return
        }
        await withCheckedContinuation { continuation in
            guard self.count < count else {
                continuation.resume()
                return
            }
            waiters.append(.init(
                continuation: continuation,
                count: count
            ))
        }
    }
}

private struct TheirCoreTestingGateWaiter: Sendable {

    let continuation: CheckedContinuation<Void, Never>
    let count: Int
}

private final class TheirCoreTestingManualTimeoutClock: Sendable {

    private let fireSignal = Their.TestSignal()
    private let sleepCalls = Their.TestCountRecorder()
    var sleepCallsCount: Int {
        sleepCalls.count
    }

    init() {}

    func fire() {
        fireSignal.signal()
    }

    func sleep(nanoseconds: UInt64) async throws {
        _ = nanoseconds
        _ = sleepCalls.increment()
        try await fireSignal.wait()
    }

    func waitForSleepCallsCount(_ count: Int) async throws {
        try await sleepCalls.waitForCount(count)
    }
}

/// Intentionally ignores task cancellation so timeout tests can hold a
/// cancelled operation in deterministic cleanup and prove that `Their.stress` joins
/// it before returning to the test method.
private actor TheirCoreTestingUncancellableGate {

    private var continuations = [CheckedContinuation<Void, Never>]()
    private var isOpen = false

    func open() {
        isOpen = true
        let continuations = continuations
        self.continuations = []
        continuations.forEach { $0.resume() }
    }

    func wait() async {
        guard isOpen == false else {
            return
        }
        await withCheckedContinuation { continuation in
            guard isOpen == false else {
                continuation.resume()
                return
            }
            continuations.append(continuation)
        }
    }
}

private enum TheirCoreTestingTestsError: Equatable, Error, Sendable {}
