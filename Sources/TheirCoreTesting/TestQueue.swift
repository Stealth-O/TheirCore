import TheirCore

final class TestQueue: Sendable {

    private struct State {

        var enterContinuations: [CheckedContinuation<Void, Never>] = []
        var fullCalled = false
        var fullContinuation: CheckedContinuation<Void, Never>?
        var openCalled = false
    }

    private let count: Int
    private let state = Their.Lock(State())

    init(count: Int) {
        self.count = count
    }

    func enter() async {
        await withCheckedContinuation { continuation in
            var fullContinuation: CheckedContinuation<Void, Never>?
            state.withLock { state in
                assert(!state.openCalled)
                state.enterContinuations.append(continuation)
                assert(state.enterContinuations.count <= count)
                if state.enterContinuations.count == count {
                    fullContinuation = state.fullContinuation
                    state.fullContinuation = nil
                }
            }
            fullContinuation?.resume()
        }
    }

    func full() async {
        await withCheckedContinuation { continuation in
            var resume = false
            state.withLock { state in
                assert(!state.openCalled)
                assert(!state.fullCalled)
                state.fullCalled = true
                if state.enterContinuations.count >= count {
                    resume = true
                } else {
                    state.fullContinuation = continuation
                }
            }
            if resume {
                continuation.resume()
            }
        }
    }

    func open() {
        let enterContinuations = state.withLock { state in
            assert(!state.openCalled)
            state.openCalled = true
            let enterContinuations = state.enterContinuations
            state.enterContinuations.removeAll()
            return enterContinuations
        }
        enterContinuations.forEach { $0.resume() }
    }
}
