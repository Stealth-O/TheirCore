import Foundation

extension Their {

    /// Public shared, multi-subscriber sink facade over one `HubEngine`.
    ///
    /// A `Hub` exposes one shared event-producing lifecycle to any number of
    /// subscribers. The first `subscribe(_:)` starts the upstream `HubEngine`;
    /// later subscribers join the same live lifecycle instead of starting new
    /// work. The engine owns broadcast ordering, value snapshots and terminal
    /// re-snapshots, the three-phase start, restart after synchronous terminal
    /// outcomes (`.finished` / `.failure`) and last-subscriber teardown (documented in
    /// `HubEngine.swift`). This type adds the public sink surface and the
    /// cancel/pin contract; engine and facade use the same `HubEvent` / `HubSink`.
    ///
    /// Subscription:
    /// - `subscribe(_:)` is synchronous. For the first subscriber it installs the
    ///   sink and starts the shared lifecycle on the calling thread, so by the time
    ///   it returns `work(report:)` has already been invoked; later subscribers
    ///   attach to the running lifecycle without starting new work.
    /// - Many concurrent subscribers are allowed, so there is no single-subscriber
    ///   misuse at this facade (contrast `Job`). The `MisuseHandler` is forwarded to
    ///   the engine, which owns any start-race misuse.
    ///
    /// Events:
    /// - `.value` is broadcast to every subscriber attached when the upstream emits
    ///   it. The base `Hub` is live-only: a subscriber that joins after a value was
    ///   emitted does not receive that past value — use `shareLatest()` for
    ///   late-subscriber replay.
    /// - `.finished` is terminal: it is broadcast to all current subscribers, clears
    ///   them and stops the shared lifecycle. A later `subscribe` can start a fresh
    ///   lifecycle.
    /// - `.failure` is terminal: it is broadcast to all current subscribers, clears
    ///   them and stops the shared lifecycle. A later `subscribe` can start a fresh
    ///   lifecycle.
    /// - The engine delivers `HubEvent` directly; there is no diagnostic
    ///   `.message` at this layer.
    ///
    /// Cancel and pin: the `HubCancel` returned by `subscribe(_:)` has three
    /// effects. It synchronously suppresses future delivery to that one subscriber;
    /// it synchronously removes the subscriber from the engine registry, stopping
    /// the shared lifecycle when it was the last subscriber; and it pins the
    /// originating `Hub` chain — the closure strongly retains the chain, so rvalue
    /// pipelines like `Their.Hub(...).evolve(...).subscribe(...)` stay alive while the
    /// cancel is alive. Calling `cancel()` releases that pin synchronously; dropping
    /// the cancel without invoking it releases the pin via ARC. Cancelling a
    /// subscriber after the lifecycle already stopped is a no-op, so the upstream
    /// resource is never cancelled twice.
    /// The subscription drops its sink reference on cancel or terminal delivery;
    /// retaining the returned cancel afterwards does not retain the sink's captures.
    /// A callback already in flight may keep those captures until it returns.
    ///
    /// Last-subscriber teardown timing: when the shared lifecycle is already
    /// installed and running, removing the last subscriber stops it inline via the
    /// engine's `JobEngine.stop()` before `cancel()` returns. If the last subscriber
    /// instead leaves while the lifecycle is still starting — its first
    /// `work(report:)` has not yet returned, e.g. a reentrant subscriber cancelling
    /// from a synchronous report sink — the engine is not yet installed, so the
    /// in-progress starter stops it on commit rather than the `cancel()` call
    /// itself. Either way the upstream is stopped exactly once.
    ///
    /// Lifetime: the root `Hub` installs no explicit `onDeinit`. The cancel/pin
    /// invariant guarantees no live subscription outlives the `Hub` — a retained
    /// cancel pins the `Hub` — so releasing the `Hub` drops the last reference to
    /// the `HubEngine` it holds through `onSubscribe`, and the engine's own deinit
    /// cancels any active lifecycle. Derived hubs built through the secondary
    /// initializer (`evolve`/`map`/`stream`/`job`/`shareLatest`) instead supply an
    /// explicit `onDeinit` that cancels their evolved state and upstream
    /// subscription.
    ///
    /// Threading: every step runs synchronously on the caller's thread — the facade
    /// adds no `main` hop or queue dispatch of its own, and the engine state lives
    /// behind a cheap `Their.Lock`. Removing a subscriber on cancel or a terminal
    /// outcome stops *future* delivery; a subscriber callback already running
    /// concurrently on the engine drainer is not interrupted, so sinks must stay
    /// lightweight and hop out for expensive work.
    public final class Hub<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

        public let logging: Their.LifecycleLogging?
        let misuseHandler: Their.MisuseHandler
        private let misuseLocation: Their.MisuseLocation
        private let onDeinit: Their.HubCancel?
        /// Registers the subscriber on the shared lifecycle, returning the cancel
        /// for the new subscription, or `nil` when the subscription was rejected —
        /// `subscribe(_:)` then hands out a non-pinning inert cancel. No current
        /// facade rejects: the root hub and every derived hub (`evolve`, `map`,
        /// `shareLatest`) always return a cancel, so the inert path is unreachable
        /// today. The optional mirrors `Job.onSubscribe`, whose single-subscriber
        /// facades do reject; `Hub.job()` builds a `Job`, so its rejection lives
        /// there, not here.
        private let onSubscribe: @Sendable (@escaping Their.HubSink<Value, Failure>) -> Their.HubCancel?

        public init(
            fileID: String = #fileID,
            function: String = #function,
            line: UInt = #line,
            logging: Their.LifecycleLogging? = nil,
            misuseHandler: @escaping Their.MisuseHandler = Their.MisuseHandlers.fatal,
            work: @escaping Their.Work<Value, Failure>
        ) {
            let location = Their.MisuseLocation(
                fileID: fileID,
                function: function,
                line: line
            )
            let engine = HubEngine(
                misuseHandler: misuseHandler,
                misuseLocation: location,
                work: work
            )
            self.logging = logging
            self.misuseHandler = misuseHandler
            misuseLocation = location
            onDeinit = nil
            onSubscribe = { sink in
                engine.subscribe(sink)
            }
            logging?.logLifecycle("hub init")
        }

        init(
            logging: Their.LifecycleLogging? = nil,
            misuseHandler: @escaping Their.MisuseHandler,
            misuseLocation: Their.MisuseLocation,
            onDeinit: Their.HubCancel? = nil,
            onSubscribe: @escaping @Sendable (@escaping Their.HubSink<Value, Failure>) -> Their.HubCancel?
        ) {
            self.logging = logging
            self.misuseHandler = misuseHandler
            self.misuseLocation = misuseLocation
            self.onDeinit = onDeinit
            self.onSubscribe = onSubscribe
        }

        deinit {
            logging?.logLifecycle("hub deinit")
            onDeinit?()
        }

        public func subscribe(
            _ sink: @escaping Their.HubSink<Value, Failure>
        ) -> Their.HubCancel {
            guard let cancel = onSubscribe(sink) else {
                return {}
            }
            let subscription = SubscriptionPin(self)
            #if DEBUG
            HubSubscriptionTestHooks.didCreate? { [weak subscription] in
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
/// Task-scoped weak observation of the real Hub pin, matching the Job probe.
enum HubSubscriptionTestHooks {

    @TaskLocal
    static var didCreate: (@Sendable (@escaping @Sendable () -> Bool?) -> Void)?
}
#endif
