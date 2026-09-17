import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct HubStreamTests {

    @Test func competingIteratorsOnLiveStreamDistributeValues() async throws {
        try await Their.stress {
            let startRecorder = HubStreamStartRecorder()
            let stream = Their.Hub(work: startRecorder.work).stream()
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

    @Test func streamDropsSubscriptionWhenLastStreamCopyIsReleased() async throws {
        try await Their.stress {
            let recorder = HubStreamStartRecorder()
            var stream: AsyncStream<Their.HubEvent<Int, HubStreamTestsError>>? = Their.Hub(
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

    @Test func streamDropsSubscriptionWhenOnlyIteratorIsReleased() async throws {
        try await Their.stress {
            let recorder = HubStreamStartRecorder()
            var stream: AsyncStream<Their.HubEvent<Int, HubStreamTestsError>>? = Their.Hub(
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

    @Test func streamDropsSubscriptionWhenOnlyStreamIsReleased() async throws {
        try await Their.stress {
            let recorder = HubStreamStartRecorder()
            var stream: AsyncStream<Their.HubEvent<Int, HubStreamTestsError>>? = Their.Hub(
                work: recorder.work
            ).stream()
            #expect(recorder.startCallsCount == 1)
            #expect(recorder.cancelCallsCount == 0)
            withExtendedLifetime(stream) {}

            stream = nil

            #expect(recorder.cancelCallsCount == 1)
        }
    }

    @Test func streamFinishesOnEnd() async throws {
        try await Their.stress {
            let finishedSignal = HubStreamSignal()
            let recorder = HubStreamEventRecorder()
            let startRecorder = HubStreamStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)
            let task = Task {
                for await event in hub.stream() {
                    recorder.append(event)
                }
                finishedSignal.signal()
            }
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(10))
            try await recorder.waitForEventCount(1)
            startRecorder.emit(.finished)
            try await recorder.waitForEventCount(2)
            try await finishedSignal.wait()
            try await startRecorder.waitForCancelCallsCount(1)
            #expect(recorder.events == [.value(10), .finished])
            #expect(startRecorder.cancelCallsCount == 1)
            task.cancel()
            await task.value
        }
    }

    @Test func streamFinishesOnError() async throws {
        try await Their.stress {
            let finishedSignal = HubStreamSignal()
            let recorder = HubStreamEventRecorder()
            let startRecorder = HubStreamStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)
            let task = Task {
                for await event in hub.stream() {
                    recorder.append(event)
                }
                finishedSignal.signal()
            }
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.failure(.sample))
            try await recorder.waitForEventCount(1)
            try await finishedSignal.wait()
            try await startRecorder.waitForCancelCallsCount(1)
            #expect(recorder.events == [.failure(.sample)])
            #expect(startRecorder.cancelCallsCount == 1)
            task.cancel()
            await task.value
        }
    }

    @Test func streamJoinsActiveHubBeforeReturningAndReceivesImmediateTerminal() async throws {
        try await Their.stress {
            let sinkRecorder = HubStreamEventRecorder()
            let startRecorder = HubStreamStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)
            let sinkCancel = hub.subscribe(sinkRecorder.append(_:))
            #expect(startRecorder.startCallsCount == 1)

            let stream = hub.stream()
            #expect(startRecorder.startCallsCount == 1)
            startRecorder.emit(.value(10))
            startRecorder.emit(.finished)

            var iterator = stream.makeAsyncIterator()
            #expect(await iterator.next() == .value(10))
            #expect(await iterator.next() == .finished)
            #expect(await iterator.next() == nil)
            #expect(sinkRecorder.events == [.value(10), .finished])
            #expect(startRecorder.startCallsCount == 1)
            #expect(startRecorder.cancelCallsCount == 1)
            sinkCancel()
            withExtendedLifetime(stream) {}
        }
    }

    @Test func streamReplaysLatestOutputFromShareLatestHub() async throws {
        try await Their.stress {
            let sinkRecorder = HubStreamEventRecorder()
            let streamRecorder = HubStreamEventRecorder()
            let startRecorder = HubStreamStartRecorder()
            let hub = Their.Hub(work: startRecorder.work).shareLatest()
            _ = hub.subscribe(sinkRecorder.append(_:))
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(10))
            try await sinkRecorder.waitForEventCount(1)
            let task = Task {
                for await event in hub.stream() {
                    streamRecorder.append(event)
                }
            }
            try await streamRecorder.waitForEventCount(1)
            #expect(sinkRecorder.events == [.value(10)])
            #expect(streamRecorder.events == [.value(10)])
            #expect(startRecorder.startCallsCount == 1)
            task.cancel()
            await task.value
        }
    }

    @Test func streamRetainsRvalueChainUntilTermination() async throws {
        try await Their.stress {
            let startRecorder = HubStreamStartRecorder()
            let task = Task { () -> [Their.HubEvent<Int, HubStreamTestsError>] in
                var events = [Their.HubEvent<Int, HubStreamTestsError>]()
                for await event in Their.Hub(work: startRecorder.work)
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

    @Test func streamRvalueIterationCancelsAfterEarlyReturn() async throws {
        try await Their.stress {
            let recorder = HubStreamStartRecorder()
            let task = Task { () -> Their.HubEvent<Int, HubStreamTestsError>? in
                for await event in Their.Hub(work: recorder.work).stream() {
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

    @Test func streamSharesHubWithSinkSubscribers() async throws {
        try await Their.stress {
            let sinkRecorder = HubStreamEventRecorder()
            let startRecorder = HubStreamStartRecorder()
            let streamRecorder = HubStreamEventRecorder()
            let hub = Their.Hub(work: startRecorder.work).shareLatest()
            _ = hub.subscribe(sinkRecorder.append(_:))
            let task = Task {
                for await event in hub.stream() {
                    streamRecorder.append(event)
                }
            }
            try await startRecorder.waitForStartCallsCount(1)
            startRecorder.emit(.value(20))
            try await sinkRecorder.waitForEventCount(1)
            try await streamRecorder.waitForEventCount(1)
            #expect(sinkRecorder.events == [.value(20)])
            #expect(streamRecorder.events == [.value(20)])
            #expect(startRecorder.startCallsCount == 1)
            task.cancel()
            await task.value
        }
    }

    @Test func streamStartsBeforeStreamReturns() async throws {
        try await Their.stress {
            let recorder = HubStreamStartRecorder()
            let stream: AsyncStream<Their.HubEvent<Int, HubStreamTestsError>> = Their.Hub(
                work: recorder.work
            ).stream()

            #expect(recorder.startCallsCount == 1)
            #expect(recorder.cancelCallsCount == 0)
            withExtendedLifetime(stream) {}
        }
    }

    @Test func streamTerminationUnsubscribesAndStopsLastSubscriber() async throws {
        try await Their.stress {
            let recorder = HubStreamEventRecorder()
            let startRecorder = HubStreamStartRecorder()
            let hub = Their.Hub(work: startRecorder.work)
            let task = Task {
                for await event in hub.stream() {
                    recorder.append(event)
                }
            }
            try await startRecorder.waitForStartCallsCount(1)
            task.cancel()
            try await startRecorder.waitForCancelCallsCount(1)
            await task.value
            #expect(startRecorder.cancelCallsCount == 1)
        }
    }
}

private enum HubStreamTestsError: Swift.Error, Sendable {

    case sample
}

private typealias HubStreamEventRecorder = Their.TestEventRecorder<Their.HubEvent<Int, HubStreamTestsError>>
private typealias HubStreamSignal = Their.TestSignal
private typealias HubStreamStartRecorder = Their.TestWorkRecorder<Int, HubStreamTestsError>

private extension Their.HubEvent where Failure == HubStreamTestsError, Value == Int {

    var value: Int? {
        switch self {
        case .finished, .failure:
            return nil
        case .value(let value):
            return value
        }
    }
}
