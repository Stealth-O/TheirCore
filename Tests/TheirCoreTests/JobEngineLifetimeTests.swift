import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct JobEngineLifetimeTests {

    @Test(arguments: EngineLifetimeCleanup.allCases)
    private func queuedPayloadIsDestroyedOutsideEngineLock(_ cleanup: EngineLifetimeCleanup) async throws {
        try await Their.stress(count: 1) {
            let cancels = Their.TestCountRecorder()
            let engineStore = Their.Lock<JobEngine<EngineLifetimeToken, Never>?>(nil)
            let observations = Their.TestEventRecorder<Bool>()
            let reentries = Their.TestCountRecorder()
            let values = Their.TestEventRecorder<Int>()
            let engine = JobEngine<EngineLifetimeToken, Never>(sink: { event in
                guard case .value(let token) = event else {
                    return
                }
                values.append(token.id)
                guard token.id == 0, let engine = engineStore.withLock({ $0 }) else {
                    return
                }
                if cleanup == .lateAfterEnd {
                    // Both inputs join the active drainer. This payload is
                    // ignored only after the queued terminal has been reduced.
                    engine.emitFinished()
                }
                engine.emit(value: EngineLifetimeToken(id: 1) {
                    guard let engine = engineStore.withLock({ $0 }) else {
                        Issue.record("Engine must outlive the queued token.")
                        return
                    }
                    let isAvailable = engine.isLockAvailableForTests()
                    observations.append(isAvailable)
                    guard isAvailable else {
                        return
                    }
                    engine.stop()
                    _ = reentries.increment()
                })
                switch cleanup {
                case .lateAfterEnd:
                    break
                case .stop:
                    engine.stop()
                case .terminate:
                    engine.terminate()
                }
            }, work: { _ in
                { _ = cancels.increment() }
            })
            engineStore.withLock { $0 = engine }
            defer { engineStore.withLock { $0 = nil } }

            #expect(engine.start())
            #expect(engine.isLockAvailableForTests())
            engine.emit(value: EngineLifetimeToken(id: 0, onDeinit: {}))

            #expect(observations.events == [true])
            #expect(reentries.count == 1)
            #expect(values.events == [0])
            #expect(cancels.count == 1)
            #expect(engine.getState().isTerminated)
            #expect(engine.isLockAvailableForTests())
        }
    }
}

private enum EngineLifetimeCleanup: CaseIterable, Sendable {

    case lateAfterEnd
    case stop
    case terminate
}

private final class EngineLifetimeToken: Sendable {

    let id: Int
    private let onDeinit: @Sendable () -> Void

    init(id: Int, onDeinit: @escaping @Sendable () -> Void) {
        self.id = id
        self.onDeinit = onDeinit
    }

    deinit {
        onDeinit()
    }
}
