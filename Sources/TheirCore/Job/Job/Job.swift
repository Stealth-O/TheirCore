import Foundation

extension Their {

    /// Public single-subscriber, single-lifecycle sink facade over one `JobEngine`.
    ///
    /// A `Job` represents one finite event-producing lifecycle with exactly one
    /// subscriber. `subscribe(_:)` installs that subscriber and starts the upstream
    /// `JobEngine`; the engine owns ordering, cancellation and terminal behavior
    /// (its model is documented in `JobEngine.swift`). This type adds the public
    /// sink surface, single-subscriber enforcement and the cancel/pin contract.
    ///
    /// Subscription:
    /// - `subscribe(_:)` is synchronous. It installs the subscriber sink and starts
    ///   the upstream lifecycle on the calling thread; by the time it returns,
    ///   `work(report:)` has already been invoked and the returned `WorkCancel` is
    ///   ready to use.
    /// - The lifecycle is single-use. A second `subscribe` while a subscriber is
    ///   still installed reports a `Misuse` ("Job supports only one subscriber per
    ///   lifecycle") and returns an inert cancel. A `subscribe` that finds the sink
    ///   free but the engine no longer idle (e.g. after cancel or terminal failure)
    ///   loses the start race: the engine reports its own `Misuse`, the freshly
    ///   installed sink is cleared, and an inert cancel is returned. Either way no
    ///   second lifecycle starts and `work` runs at most once.
    ///
    /// Events:
    /// - `.value` is delivered to the current subscriber.
    /// - `.finished` is terminal: it takes and clears the subscriber, delivers the
    ///   successful end once, and makes every later `subscribe` lose the start
    ///   race above.
    /// - `.failure` is terminal: it takes and clears the subscriber, delivers the
    ///   failure once, and makes every later `subscribe` lose the start race above.
    /// - Engine `.message` diagnostics are dropped at this facade.
    ///
    /// Cancel and pin: the `WorkCancel` returned by `subscribe(_:)` clears the
    /// subscriber and stops the upstream lifecycle before it returns, so no further
    /// event reaches the subscriber afterwards. It also pins the originating `Job`
    /// chain — the closure strongly retains the chain, so rvalue pipelines like
    /// `Their.Job(...).evolve(...).subscribe(...)` stay alive while the cancel is
    /// alive. Calling `cancel()` releases that pin synchronously; dropping the
    /// cancel without invoking it releases the pin via ARC. Explicit pin release
    /// runs facade destruction outside the pin lock, so upstream
    /// teardown may re-enter cancellation. Cancelling after a
    /// terminal failure is a no-op on the already-terminated engine, so the upstream
    /// resource is never cancelled twice. The inert cancel returned by a rejected
    /// `subscribe` performs no work and does not pin the `Job`.
    ///
    /// Lifetime: `deinit` clears the subscriber and stops the engine, so an active
    /// lifecycle is cancelled once the `Job` is released. Because a live returned
    /// cancel pins the `Job`, the object cannot deinit while that cancel is
    /// retained; the lifecycle then stops either when `cancel()` is called or when
    /// the last reference (including the pin) is dropped.
    ///
    /// Threading: every step runs synchronously on the caller's thread — the facade
    /// adds no `main` hop or queue dispatch of its own, and subscriber state lives
    /// behind a cheap `Their.Lock` with tiny critical sections. Clearing the sink on
    /// cancel or terminal failure stops *future* delivery; a subscriber callback
    /// already running concurrently on the engine drainer is not interrupted, so the
    /// sink must tolerate one in-flight value overlapping a cancel.
    public final class Job<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

        public let logging: Their.LifecycleLogging?
        let misuseHandler: Their.MisuseHandler
        private let misuseLocation: Their.MisuseLocation
        private let onDeinit: Their.WorkCancel?
        /// Installs the subscriber and starts the lifecycle, returning the cancel
        /// for the new subscription, or `nil` when the subscription was rejected —
        /// `subscribe(_:)` then hands out a non-pinning inert cancel.
        private let onSubscribe: @Sendable (@escaping Their.JobSink<Value, Failure>) -> Their.WorkCancel?

        public init(
            fileID: String = #fileID,
            function: String = #function,
            line: UInt = #line,
            logging: Their.LifecycleLogging? = nil,
            misuseHandler: @escaping Their.MisuseHandler = Their.MisuseHandlers.fatal,
            work: @escaping Their.Work<Value, Failure>
        ) {
            let state = JobSinkState<Value, Failure>()
            let misuseLocation = Their.MisuseLocation(
                fileID: fileID,
                function: function,
                line: line
            )
            let engine = JobEngine(
                misuseHandler: misuseHandler,
                misuseLocation: misuseLocation,
                sink: Self.makeSink(state: state),
                work: work
            )
            let cancel: Their.WorkCancel = { [engine, state] in
                state.clearSink()
                engine.stop()
            }
            self.logging = logging
            self.misuseHandler = misuseHandler
            self.misuseLocation = misuseLocation
            onDeinit = cancel
            onSubscribe = { sink in
                guard state.setSinkIfEmpty(sink) else {
                    misuseHandler(
                        .init(
                            message: "Job supports only one subscriber per lifecycle.",
                            origin: .init(),
                            trace: [misuseLocation]
                        )
                    )
                    return nil
                }
                guard engine.start() else {
                    state.clearSink()
                    return nil
                }
                return cancel
            }
            logging?.logLifecycle("job init")
        }

        init(
            logging: Their.LifecycleLogging? = nil,
            misuseHandler: @escaping Their.MisuseHandler,
            misuseLocation: Their.MisuseLocation,
            onDeinit: Their.WorkCancel? = nil,
            onSubscribe: @escaping @Sendable (@escaping Their.JobSink<Value, Failure>) -> Their.WorkCancel?
        ) {
            self.logging = logging
            self.misuseHandler = misuseHandler
            self.misuseLocation = misuseLocation
            self.onDeinit = onDeinit
            self.onSubscribe = onSubscribe
        }

        deinit {
            logging?.logLifecycle("job deinit")
            onDeinit?()
        }

        private static func makeSink(
            state: JobSinkState<Value, Failure>
        ) -> JobEngineSink<Value, Failure> {
            { event in
                switch event {
                case .finished:
                    state.takeSink()?(.finished)
                case .failure(let failure):
                    state.takeSink()?(.failure(failure))
                case .message:
                    break
                case .value(let value):
                    state.getSink()?(.value(value))
                }
            }
        }

        public func subscribe(
            _ sink: @escaping Their.JobSink<Value, Failure>
        ) -> Their.WorkCancel {
            guard let cancel = onSubscribe(sink) else {
                return {}
            }
            let subscription = SubscriptionPin(self)
            #if DEBUG
            JobSubscriptionTestHooks.didCreate? { [weak subscription] in
                subscription?.isLockAvailableForTests()
            }
            #endif
            return { [subscription] in
                subscription.release()
                cancel()
            }
        }
    }
}

#if DEBUG
/// Task-scoped observation of the real subscription pin. The weak probe adds
/// no owner and never blocks, so lifetime tests can observe a held pin lock
/// without performing the recursive acquisition that would trap the runner.
enum JobSubscriptionTestHooks {

    @TaskLocal
    static var didCreate: (@Sendable (@escaping @Sendable () -> Bool?) -> Void)?
}
#endif
