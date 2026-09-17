import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobStreamTests {

    @Test func competingIteratorsOnLiveStreamDistributeValues() async throws {
        try await Their.stress {
            let startRecorder = JobStreamStartRecorder()
            let stream = Their.Job(work: startRecorder.work).stream()
            let firstTask = Task {
                var iterator = stream.makeAsyncIterator()
                return await iterator.next()
            }
            let secondTask = Task {
                var iterator = stream.makeAsyncIterator()
                return await iterator.next()
            }
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(1))
            startRecorder.emit(.value(2))

            let events = await [firstTask.value, secondTask.value]
            let values = events.compactMap { $0?.value }.sorted()
            #expect(values == [1, 2])
            #expect(startRecorder.cancelCallsCount == 0)
            #expect(startRecorder.startCallsCount == 1)
            startRecorder.emit(.failure(.sample))
            try await startRecorder.waitForCancelCallsCount(1)
        }
    }

    @Test func jobStreamCancelsJobWhenTerminated() async throws {
        try await Their.stress {
            let recorder = JobStreamStartRecorder()
            let job = Their.Job(work: recorder.work)
            let task = Task {
                for await _ in job.stream() {}
            }
            try await recorder.waitForStartCallsCount(1)
            task.cancel()
            try await recorder.waitForCancelCallsCount(1)
            await task.value

            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func jobStreamDropsSubscriptionWhenLastStreamCopyIsReleased() async throws {
        try await Their.stress {
            let recorder = JobStreamStartRecorder()
            var stream: AsyncStream<Their.JobEvent<Int, JobStreamTestsError>>? = Their.Job(
                work: recorder.work
            ).stream()
            var streamCopy = stream
            #expect(recorder.startCallsCount == 1)
            #expect(recorder.cancelCallsCount == 0)

            stream = nil
            withExtendedLifetime(streamCopy) {
                #expect(recorder.cancelCallsCount == 0)
            }
            streamCopy = nil

            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func jobStreamDropsSubscriptionWhenOnlyIteratorIsReleased() async throws {
        try await Their.stress {
            let recorder = JobStreamStartRecorder()
            var stream: AsyncStream<Their.JobEvent<Int, JobStreamTestsError>>? = Their.Job(
                work: recorder.work
            ).stream()
            var iterator = stream?.makeAsyncIterator()
            #expect(recorder.startCallsCount == 1)
            #expect(recorder.cancelCallsCount == 0)

            stream = nil
            withExtendedLifetime(iterator) {
                #expect(recorder.cancelCallsCount == 0)
            }
            iterator = nil

            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func jobStreamDropsSubscriptionWhenOnlyStreamIsReleased() async throws {
        try await Their.stress {
            let recorder = JobStreamStartRecorder()
            var stream: AsyncStream<Their.JobEvent<Int, JobStreamTestsError>>? = Their.Job(
                work: recorder.work
            ).stream()
            #expect(recorder.startCallsCount == 1)
            #expect(recorder.cancelCallsCount == 0)
            withExtendedLifetime(stream) {}

            stream = nil

            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func jobStreamRetainsRvalueChainUntilTermination() async throws {
        try await Their.stress {
            let startRecorder = JobStreamStartRecorder()
            let task = Task { () -> [Their.JobEvent<Int, JobStreamTestsError>] in
                var events = [Their.JobEvent<Int, JobStreamTestsError>]()
                for await event in Their.Job(work: startRecorder.work)
                    .map({ value in value * 2 })
                    .map({ value in value + 1 })
                    .stream() {
                    events.append(event)
                }
                return events
            }
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(5))
            startRecorder.emit(.failure(.sample))
            let events = await task.value
            try await startRecorder.waitForCancelCallsCount(1)

            #expect(events == [.value(11), .failure(.sample)])
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }

    @Test func jobStreamRvalueIterationCancelsAfterEarlyReturn() async throws {
        try await Their.stress {
            let recorder = JobStreamStartRecorder()
            let task = Task { () -> Their.JobEvent<Int, JobStreamTestsError>? in
                for await event in Their.Job(work: recorder.work).stream() {
                    return event
                }
                return nil
            }
            try await recorder.waitForStartCallsCount(1)
            recorder.emit(.value(10))

            #expect(await task.value == .value(10))
            try await recorder.waitForCancelCallsCount(1)
            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func jobStreamStartsBeforeStreamReturns() async throws {
        try await Their.stress {
            let recorder = JobStreamStartRecorder()
            let stream: AsyncStream<Their.JobEvent<Int, JobStreamTestsError>> = Their.Job(
                work: recorder.work
            ).stream()

            #expect(recorder.startCallsCount == 1)
            #expect(recorder.cancelCallsCount == 0)
            withExtendedLifetime(stream) {}
        }
    }

    @Test func jobStreamValueCopySharesTerminatedLifecycle() async throws {
        try await Their.stress {
            let cancelSignal = JobStreamTestSignal()
            let startSignal = JobStreamTestSignal()
            let recorder = JobStreamStartRecorder(
                onCancel: {
                    cancelSignal.signal()
                },
                onStart: {
                    startSignal.signal()
                }
            )
            let stream: AsyncStream<Their.JobEvent<Int, JobStreamTestsError>> = Their.Job(
                work: recorder.work
            ).stream()
            let streamCopy = stream
            var iterator = stream.makeAsyncIterator()
            try await startSignal.wait()
            recorder.emit(.value(10))
            #expect(await iterator.next() == .value(10))
            recorder.emit(.failure(.sample))
            #expect(await iterator.next() == .failure(.sample))
            #expect(await iterator.next() == nil)
            try await cancelSignal.wait()
            var copiedIterator = streamCopy.makeAsyncIterator()
            #expect(await copiedIterator.next() == nil)
            #expect(recorder.startCallsCount == 1)
            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func jobStreamYieldsValuesThenEndAndFinishes() async throws {
        try await Their.stress {
            let finishedSignal = JobStreamTestSignal()
            let recorder = JobStreamEventRecorder()
            let startRecorder = JobStreamStartRecorder()
            let job = Their.Job(work: startRecorder.work)
            let task = Task {
                for await event in job.stream() {
                    recorder.append(event)
                }
                finishedSignal.signal()
            }
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(10))
            startRecorder.emit(.finished)
            try await recorder.waitForEventCount(2)
            try await finishedSignal.wait()
            try await startRecorder.waitForCancelCallsCount(1)

            #expect(recorder.events == [
                .value(10),
                .finished
            ])
            #expect(startRecorder.cancelCallsCount == 1)
            task.cancel()
            await task.value
        }
    }

    @Test func jobStreamYieldsValuesThenFailureAndFinishes() async throws {
        try await Their.stress {
            let finishedSignal = JobStreamTestSignal()
            let recorder = JobStreamEventRecorder()
            let startRecorder = JobStreamStartRecorder()
            let job = Their.Job(work: startRecorder.work)
            let task = Task {
                for await event in job.stream() {
                    recorder.append(event)
                }
                finishedSignal.signal()
            }
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(10))
            startRecorder.emit(.failure(.sample))
            try await recorder.waitForEventCount(2)
            try await finishedSignal.wait()
            try await startRecorder.waitForCancelCallsCount(1)

            #expect(recorder.events == [
                .value(10),
                .failure(.sample)
            ])
            #expect(startRecorder.cancelCallsCount == 1)
            task.cancel()
            await task.value
        }
    }
}

private enum JobStreamTestsError: Swift.Error, Sendable {

    case sample
}

private typealias JobStreamEventRecorder = Their.TestEventRecorder<Their.JobEvent<Int, JobStreamTestsError>>
private typealias JobStreamStartRecorder = Their.TestWorkRecorder<Int, JobStreamTestsError>
private typealias JobStreamTestSignal = Their.TestSignal

private extension Their.JobEvent where Failure == JobStreamTestsError, Value == Int {

    var value: Int? {
        switch self {
        case .finished, .failure:
            return nil
        case .value(let value):
            return value
        }
    }
}
