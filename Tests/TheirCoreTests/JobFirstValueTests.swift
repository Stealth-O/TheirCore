import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobFirstValueTests {

    @Test(arguments: [Their.JobAwaitCancellation.cancelSource, .awaitResult])
    func alreadyCancelledTaskNeverStartsTheSource(cancellation: Their.JobAwaitCancellation) async throws {
        try await Their.stress {
            let driver = Their.TestJobDriver<Int, JobFirstValueFailure>()
            let waiter = Task {
                withUnsafeCurrentTask { $0?.cancel() }
                return try await driver.job.firstValue(cancellation: cancellation)
            }
            do { _ = try await waiter.value; Issue.record("Expected cancellation") }
            catch { #expect(error is CancellationError) }
            #expect(driver.startCallsCount == 0)
            #expect(driver.cancelCallsCount == 0)
        }
    }

    @Test func cancellationAfterASelectedValueDoesNotReplaceItsOutcome() async throws {
        try await Their.stress {
            let cancelled = Their.TestCountRecorder()
            let job = Their.Job<Int, Never> { report in
                report(.value(7))
                withUnsafeCurrentTask { $0?.cancel() }
                report(.value(8))
                return { _ = cancelled.increment() }
            }
            let waiter = Task { try await job.firstValue() }
            let value = try await waiter.value
            #expect(value == 7)
            #expect(cancelled.count == 1)
        }
    }

    @Test func cancellationBeforeWaiterRegistrationResumesWithoutStartingTheSource() async throws {
        try await Their.stress {
            let driver = Their.TestJobDriver<Int, JobFirstValueFailure>()
            let waiter = Task {
                try await JobFirstValueTestHooks.$beforeRegister.withValue({
                    withUnsafeCurrentTask { $0?.cancel() }
                }) {
                    try await driver.job.firstValue()
                }
            }
            do { _ = try await waiter.value; Issue.record("Expected cancellation") }
            catch { #expect(error is CancellationError) }
            #expect(driver.startCallsCount == 0)
            #expect(driver.cancelCallsCount == 0)
        }
    }

    @Test func cancellationDuringSynchronousStartReleasesTheLateHandle() async throws {
        try await Their.stress {
            let cancelled = Their.TestCountRecorder()
            let job = Their.Job<Int, Never> { report in
                withUnsafeCurrentTask { $0?.cancel() }
                report(.value(7))
                return { _ = cancelled.increment() }
            }
            let waiter = Task { try await job.firstValue() }
            do { _ = try await waiter.value; Issue.record("Expected cancellation") }
            catch { #expect(error is CancellationError) }
            #expect(cancelled.count == 1)
        }
    }

    @Test func cancellationStopsTheSourceAndIgnoresLateValues() async throws {
        try await Their.stress {
            let driver = Their.TestJobDriver<Int, JobFirstValueFailure>()
            let waiter = Task { try await driver.job.firstValue() }
            try await driver.waitForStartCallsCount(1)
            waiter.cancel()
            do { _ = try await waiter.value; Issue.record("Expected cancellation") }
            catch { #expect(error is CancellationError) }
            driver.emit(value: 42)
            driver.emit(failure: .sample)
            #expect(driver.cancelCallsCount == 1)
        }
    }

    @Test func cancelRacingAMatchingValueChoosesOneOutcomeAndOneTeardown() async throws {
        try await Their.stress {
            let driver = Their.TestJobDriver<Int, JobFirstValueFailure>()
            let waiter = Task { try await driver.job.firstValue() }
            try await driver.waitForStartCallsCount(1)
            await withTaskGroup(of: Void.self) { group in
                group.addTask { waiter.cancel() }
                group.addTask { driver.emit(value: 42) }
            }
            do { #expect(try await waiter.value == 42) }
            catch { #expect(error is CancellationError) }
            #expect(driver.cancelCallsCount == 1)
        }
    }

    @Test func committedOperationPreservesFailureAfterTaskCancellation() async throws {
        try await Their.stress {
            let driver = Their.TestJobDriver<Int, JobFirstValueFailure>()
            let waiter = Task { try await driver.job.firstValue(cancellation: .awaitResult) }
            try await driver.waitForStartCallsCount(1)
            waiter.cancel()
            #expect(driver.cancelCallsCount == 0)
            driver.emit(failure: .sample)
            do { _ = try await waiter.value; Issue.record("Expected source failure") }
            catch { #expect(error as? JobFirstValueFailure == .sample) }
            #expect(driver.cancelCallsCount == 1)
        }
    }

    @Test func committedOperationSurvivesCancellationUntilItsMatchingResult() async throws {
        try await Their.stress {
            let driver = Their.TestJobDriver<Int, JobFirstValueFailure>()
            let waiter = Task { try await driver.job.firstValue(cancellation: .awaitResult, where: { $0 == 42 }) }
            try await driver.waitForStartCallsCount(1)
            waiter.cancel()
            driver.emit(value: 1)
            #expect(driver.cancelCallsCount == 0)
            driver.emit(value: 42)
            #expect(try await waiter.value == 42)
            #expect(driver.cancelCallsCount == 1)
        }
    }

    @Test func committedSynchronousOperationSurvivesCancellationBeforeItsHandleReturns() async throws {
        try await Their.stress {
            let cancelled = Their.TestCountRecorder()
            let job = Their.Job<Int, Never> { report in
                withUnsafeCurrentTask { $0?.cancel() }
                report(.value(7))
                return { _ = cancelled.increment() }
            }
            let waiter = Task {
                let value = try await job.firstValue(cancellation: .awaitResult)
                #expect(Task.isCancelled)
                return value
            }
            let value = try await waiter.value
            #expect(value == 7)
            #expect(cancelled.count == 1)
        }
    }

    @Test func failureAndEmptyFinishResumeInsteadOfLeaking() async throws {
        try await Their.stress {
            let failing = Their.Job<Int, JobFirstValueFailure> { report in report(.failure(.sample)); return {} }
            do { _ = try await failing.firstValue(); Issue.record("Expected source failure") }
            catch { #expect(error as? JobFirstValueFailure == .sample) }
            let empty = Their.Job<Int, Never> { report in report(.value(1)); report(.finished); return {} }
            do { _ = try await empty.firstValue(where: { $0 == 2 }); Issue.record("Expected no matching value") }
            catch { #expect(error is Their.JobValueUnavailable) }
        }
    }

    @Test func reentrantTeardownCanCancelTheWaiterWithoutDeadlockOrChangingTheResult() async throws {
        try await Their.stress {
            let holder = Their.Lock<Task<Int, any Error>?>(nil)
            let gate = Their.TestSignal()
            let driver = Their.TestJobDriver<Int, JobFirstValueFailure>(onCancel: {
                holder.withLock { $0 }?.cancel()
            })
            let waiter = Task { try await gate.wait(); return try await driver.job.firstValue() }
            holder.withLock { $0 = waiter }
            gate.signal()
            try await driver.waitForStartCallsCount(1)
            driver.emit(value: 42)
            #expect(try await waiter.value == 42)
            #expect(driver.cancelCallsCount == 1)
            holder.withLock { $0 = nil }
        }
    }

    @Test func synchronousValuesAreFilteredAndTheLateHandleIsReleased() async throws {
        try await Their.stress {
            let stopped = Their.TestCountRecorder()
            let seen = Their.TestEventRecorder<Int>()
            let job = Their.Job<Int, Never> { report in
                report(.value(1)); report(.value(2)); report(.finished)
                return { _ = stopped.increment() }
            }
            let value = try await job.firstValue(where: { seen.append($0); return $0 == 2 })
            #expect(value == 2)
            #expect(seen.events == [1, 2])
            #expect(stopped.count == 1)
        }
    }
}

private enum JobFirstValueFailure: Error, Sendable {

    case sample
}
