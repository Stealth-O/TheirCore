import Foundation

extension Their {

    /// Runs one `work` effect exactly once, always on the main thread, and blocks
    /// every caller until that work has finished — usable from any thread. Use it
    /// for synchronous "configure the SDK before first touch" gates (such as
    /// `FirebaseApp.configure()`) instead of a `static let` + `DispatchQueue.main.sync`
    /// pair, which can deadlock when a background caller holds the lazy-init token
    /// while the main thread waits on the same token.
    ///
    /// Ownership and threading. `work`, `isMainThread` and `runOnMain` are injected
    /// so the primitive stays SDK-agnostic and deterministically testable; the
    /// production defaults are `Thread.isMainThread` and `DispatchQueue.main.async`.
    /// `work` only ever runs on the main thread, decided by `isMainThread()` which is
    /// read once per `run()` outside the lock. All mutable state lives behind a
    /// `Their.Lock`; `work`, the scheduled re-entry and the waiter signals always run
    /// outside the lock, so user code never executes under the lock and the lock is
    /// never held across a blocking wait.
    ///
    /// State machine. The internal phase moves `idle -> (configuring | waitingForMain)
    /// -> configured` and never leaves `configured`:
    /// - `idle` + main: run `work` here, then release waiters (`configureHere`).
    /// - `idle` + non-main: flip to `waitingForMain`, schedule `run()` back onto main
    ///   and block this caller until main configures (`scheduleAndWait`). Only the
    ///   first non-main caller schedules; later callers just wait.
    /// - `waitingForMain` + main: run `work` here (`configureHere`); + non-main: wait.
    /// - `configuring` + main: return immediately — this is re-entrancy from inside
    ///   `work` itself, so it must not recurse or block (`returnNow`); + non-main: wait.
    /// - `configured`: return immediately for any caller (`returnNow`).
    ///
    /// The phase check and waiter enqueue happen atomically under one `withLock`, so a
    /// non-main caller that observes a pre-`configured` phase is guaranteed to be
    /// signaled by `finish()`; a caller that races past `finish()` observes
    /// `configured` and returns without waiting. There is no lost wakeup and `work`
    /// runs exactly once.
    public final class MainThreadOnce: Sendable {

        private let isMainThread: @Sendable () -> Bool
        private let runOnMain: @Sendable (@escaping @Sendable () -> Void) -> Void
        private let state = Their.Lock(MainThreadOnceState())
        private let work: @Sendable () -> Void

        public init(
            isMainThread: @escaping @Sendable () -> Bool = { Thread.isMainThread },
            runOnMain: @escaping @Sendable (@escaping @Sendable () -> Void) -> Void = { work in
                DispatchQueue.main.async(execute: work)
            },
            work: @escaping @Sendable () -> Void
        ) {
            self.isMainThread = isMainThread
            self.runOnMain = runOnMain
            self.work = work
        }

        private func finish() {
            let waiters = state.withLock { state in
                state.phase = .configured
                let waiters = state.waiters
                state.waiters.removeAll()
                return waiters
            }
            for waiter in waiters {
                waiter.signal()
            }
        }

        public func run() {
            let onMainThread = isMainThread()
            let action = state.withLock { state -> MainThreadOnceAction in
                switch state.phase {
                case .configured:
                    return .returnNow
                case .configuring:
                    guard onMainThread == false else {
                        return .returnNow
                    }
                    let waiter = DispatchSemaphore(value: 0)
                    state.waiters.append(waiter)
                    return .wait(waiter)
                case .idle:
                    guard onMainThread == false else {
                        state.phase = .configuring
                        return .configureHere
                    }
                    let waiter = DispatchSemaphore(value: 0)
                    state.phase = .waitingForMain
                    state.waiters.append(waiter)
                    return .scheduleAndWait(waiter)
                case .waitingForMain:
                    guard onMainThread == false else {
                        state.phase = .configuring
                        return .configureHere
                    }
                    let waiter = DispatchSemaphore(value: 0)
                    state.waiters.append(waiter)
                    return .wait(waiter)
                }
            }
            switch action {
            case .configureHere:
                work()
                finish()
            case .returnNow:
                return
            case .scheduleAndWait(let waiter):
                runOnMain { [self] in
                    run()
                }
                waiter.wait()
            case .wait(let waiter):
                waiter.wait()
            }
        }
    }
}

private enum MainThreadOnceAction: Sendable {

    case configureHere
    case returnNow
    case scheduleAndWait(DispatchSemaphore)
    case wait(DispatchSemaphore)
}

private enum MainThreadOncePhase: Sendable {

    case configured
    case configuring
    case idle
    case waitingForMain
}

private struct MainThreadOnceState: Sendable {

    var phase = MainThreadOncePhase.idle
    var waiters = [DispatchSemaphore]()
}
