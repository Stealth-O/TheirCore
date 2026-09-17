import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct SyncSubscribeRegressionTests {

    @Test func hubEvolveSynchronousFailureCancelsUpstreamWhenCancelIsReturnedAfterSink() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = SyncSubscribeHubEventRecorder()
            let upstream = Their.Hub<Int, SyncSubscribeRegressionError>(
                misuseHandler: Their.MisuseHandlers.fatal,
                misuseLocation: Their.MisuseLocation(),
                onSubscribe: { sink in
                    sink(.failure(.sample))
                    return cancelRecorder.cancel()
                }
            )
            let evolved = upstream.evolve(initial: 0) { state, value in
                state += value
                return state
            }

            let cancel = evolved.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(1)
            try await cancelRecorder.waitForCancelCallsCount(1)
            cancel()

            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(cancelRecorder.cancelCallsCount == 1)
        }
    }

    @Test func hubJobSynchronousFailureCancelsHubSubscriptionWhenCancelIsReturnedAfterSink() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = SyncSubscribeJobEventRecorder()
            let hub = Their.Hub<Int, SyncSubscribeRegressionError>(
                misuseHandler: Their.MisuseHandlers.fatal,
                misuseLocation: Their.MisuseLocation(),
                onSubscribe: { sink in
                    sink(.failure(.sample))
                    return cancelRecorder.cancel()
                }
            )
            let job = hub.job()

            let cancel = job.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(1)
            try await cancelRecorder.waitForCancelCallsCount(1)
            cancel()

            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(cancelRecorder.cancelCallsCount == 1)
        }
    }

    @Test func hubStreamSynchronousEndCancelsHubSubscriptionWhenCancelIsReturnedAfterSink() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let hub = Their.Hub<Int, SyncSubscribeRegressionError>(
                misuseHandler: Their.MisuseHandlers.fatal,
                misuseLocation: Their.MisuseLocation(),
                onSubscribe: { sink in
                    sink(.value(1))
                    sink(.finished)
                    return cancelRecorder.cancel()
                }
            )
            let stream = hub.stream()

            #expect(cancelRecorder.cancelCallsCount == 1)
            var iterator = stream.makeAsyncIterator()
            #expect(await iterator.next() == .value(1))
            #expect(await iterator.next() == .finished)
            #expect(await iterator.next() == nil)
            withExtendedLifetime(stream) {
                #expect(cancelRecorder.cancelCallsCount == 1)
            }
        }
    }

    @Test func hubStreamSynchronousFailureCancelsHubSubscriptionWhenCancelIsReturnedAfterSink() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let hub = Their.Hub<Int, SyncSubscribeRegressionError>(
                misuseHandler: Their.MisuseHandlers.fatal,
                misuseLocation: Their.MisuseLocation(),
                onSubscribe: { sink in
                    sink(.value(1))
                    sink(.failure(.sample))
                    return cancelRecorder.cancel()
                }
            )
            let stream = hub.stream()

            #expect(cancelRecorder.cancelCallsCount == 1)
            var iterator = stream.makeAsyncIterator()
            #expect(await iterator.next() == .value(1))
            #expect(await iterator.next() == .failure(.sample))
            #expect(await iterator.next() == nil)
            withExtendedLifetime(stream) {
                #expect(cancelRecorder.cancelCallsCount == 1)
            }
        }
    }

    @Test func jobStreamSynchronousEndCancelsJobWhenCancelIsReturnedAfterSink() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let job = Their.Job<Int, SyncSubscribeRegressionError>(
                misuseHandler: Their.MisuseHandlers.fatal,
                misuseLocation: Their.MisuseLocation(),
                onSubscribe: { sink in
                    sink(.value(1))
                    sink(.finished)
                    return cancelRecorder.cancel()
                }
            )
            let stream = job.stream()

            #expect(cancelRecorder.cancelCallsCount == 1)
            var iterator = stream.makeAsyncIterator()
            #expect(await iterator.next() == .value(1))
            #expect(await iterator.next() == .finished)
            #expect(await iterator.next() == nil)
            withExtendedLifetime(stream) {
                #expect(cancelRecorder.cancelCallsCount == 1)
            }
        }
    }

    @Test func jobStreamSynchronousFailureCancelsJobWhenCancelIsReturnedAfterSink() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let job = Their.Job<Int, SyncSubscribeRegressionError>(
                misuseHandler: Their.MisuseHandlers.fatal,
                misuseLocation: Their.MisuseLocation(),
                onSubscribe: { sink in
                    sink(.value(1))
                    sink(.failure(.sample))
                    return cancelRecorder.cancel()
                }
            )
            let stream = job.stream()

            #expect(cancelRecorder.cancelCallsCount == 1)
            var iterator = stream.makeAsyncIterator()
            #expect(await iterator.next() == .value(1))
            #expect(await iterator.next() == .failure(.sample))
            #expect(await iterator.next() == nil)
            withExtendedLifetime(stream) {
                #expect(cancelRecorder.cancelCallsCount == 1)
            }
        }
    }

    @Test func jobSynchronousFailureClearsSubscriberAndCancelDoesNotCancelTwice() async throws {
        try await Their.stress {
            let cancelRecorder = Their.TestCancelRecorder()
            let eventRecorder = SyncSubscribeJobEventRecorder()
            let reportStore = Their.Lock<Their.WorkReport<Int, SyncSubscribeRegressionError>?>(nil)
            let job: Their.Job<Int, SyncSubscribeRegressionError> = Their.Job { report in
                reportStore.withLock { currentReport in
                    currentReport = report
                }
                report(.failure(.sample))
                return cancelRecorder.cancel()
            }

            let cancel = job.subscribe(eventRecorder.append(_:))
            try await eventRecorder.waitForEventCount(1)
            try await cancelRecorder.waitForCancelCallsCount(1)
            reportStore.withLock { currentReport in
                currentReport
            }?(.value(1))
            cancel()

            #expect(eventRecorder.events == [.failure(.sample)])
            #expect(cancelRecorder.cancelCallsCount == 1)
        }
    }
}

private enum SyncSubscribeRegressionError: Equatable, Swift.Error, Sendable {

    case sample
}

private typealias SyncSubscribeHubEventRecorder = Their.TestEventRecorder<Their.HubEvent<Int, SyncSubscribeRegressionError>>
private typealias SyncSubscribeJobEventRecorder = Their.TestEventRecorder<Their.JobEvent<Int, SyncSubscribeRegressionError>>
