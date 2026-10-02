import Foundation

extension Their {

    /// Diagnostic metadata attached to a root `Job` or `Hub` for DEBUG-only
    /// top-level lifetime counting, and the only supported lifecycle diagnostic
    /// surface in TheirCore. Create it with `LifecycleLogging.common(...)` (which pins the
    /// caller's `#fileID`/`#line`) or the public initialiser, and pass it into a root
    /// `Their.Job(logging:...)` / `Their.Hub(logging:...)` at the construction site you
    /// want counted.
    ///
    /// What it counts: only root `.topLevel` `Job`/`Hub` init and deinit, keyed by
    /// `(file, line, label)`. Each event prints one line `~~| [<label>] (N)`, where
    /// `N` is the current number of live root objects at that key (`[no label]` when
    /// `label` is nil); `showOrigin` appends ` origin=<file>:<line>`. Other event
    /// strings handed to `logLifecycle(_:)` are accepted by the API but rejected by
    /// the store, so they are no-ops — keep emissions to `"job init"` / `"job deinit"`
    /// / `"hub init"` / `"hub deinit"`.
    ///
    /// Scope and threading: `logLifecycle(_:)` compiles to nothing outside `DEBUG`.
    /// In `DEBUG` the live-count store is a process-wide singleton behind a
    /// `Their.Lock`. Lines and their current output sinks enter one FIFO queue under that
    /// lock; a single drainer invokes sinks outside the lock, so concurrent output
    /// preserves count order and a sink may log reentrantly without deadlocking or
    /// recursively overtaking the line it is handling. Derived/internal wrappers
    /// must not copy active top-level logging — pass `logging.withoutTopLevel`, which
    /// strips an active `.topLevel` to `nil` but lets disabled logging (`options: []`)
    /// flow through as inert metadata. `resetForTests` / `setOutputForTests` are
    /// `DEBUG`-only test hooks; do not reintroduce broad subscribe/cancel/work/engine
    /// tracing here.
    ///
    /// Equality is structural over file/line/label/options/showOrigin without
    /// comparing `StaticString` identity.
    public struct LifecycleLogging: Equatable, Sendable {

        /// Source file the `LifecycleLogging` was constructed at; used as part of
        /// the live-counter key together with `line` and `label`.
        public let file: StaticString
        /// Optional human-readable label printed in every log line, for example
        /// `[Discovery]`. Pass `nil` to print `[no label]`.
        public let label: String?
        /// Source line; together with `file` and `label` identifies the
        /// top-level counter key.
        public let line: UInt
        /// Diagnostic mode. The default is `.topLevel`.
        public let options: Options
        /// When `true`, every log line is suffixed with `origin=<file>:<line>`.
        /// Off by default to keep top-level output compact.
        public let showOrigin: Bool

        public init(
            file: StaticString,
            line: UInt,
            label: String? = nil,
            options: Options = .topLevel,
            showOrigin: Bool = false
        ) {
            self.file = file
            self.label = label
            self.line = line
            self.options = options
            self.showOrigin = showOrigin
        }

        public static func == (lhs: Self, rhs: Self) -> Bool {
            String(describing: lhs.file) == String(describing: rhs.file)
                && lhs.label == rhs.label
                && lhs.line == rhs.line
                && lhs.options == rhs.options
                && lhs.showOrigin == rhs.showOrigin
        }
    }
}

// MARK: - Options

public extension Their.LifecycleLogging {

    /// Lifecycle diagnostic modes.
    struct Options: OptionSet, Sendable {
        public let rawValue: Int
        /// Counts only root `Job` / `Hub` init and deinit. Each line shows the
        /// current number of live root objects at this `(file, line, label)` call
        /// site.
        public static let topLevel = Options(rawValue: 1 << 0)

        public init(rawValue: Int) { self.rawValue = rawValue }
    }
}

// MARK: - Factory

public extension Their.LifecycleLogging {

    /// Canonical factory for new `LifecycleLogging` values; captures the
    /// caller's `#fileID` and `#line` so the origin is automatically pinned
    /// at the construction site without callers having to type them.
    static func common(
        file: StaticString = #fileID,
        line: UInt = #line,
        label: String? = nil,
        options: Options = .topLevel,
        showOrigin: Bool = false
    ) -> Self {
        .init(
            file: file,
            line: line,
            label: label,
            options: options,
            showOrigin: showOrigin
        )
    }
}

// MARK: - Logging

extension Their.LifecycleLogging {

    /// In DEBUG builds, prints `~~| [<label>] (N)` for root `Job` / `Hub`
    /// lifetimes. Other event strings are accepted by the API but intentionally
    /// rejected by the live-count store, so passing a non-`init`/`deinit` event
    /// is a no-op. Keep diagnostics quiet by only emitting `"job init"`,
    /// `"job deinit"`, `"hub init"` and `"hub deinit"`.
    func logLifecycle(_ event: String) {
#if DEBUG
        LifecycleLoggingStore.shared.log(event: event, logging: self)
#endif
    }
}

extension Optional where Wrapped == Their.LifecycleLogging {

    /// Strips active top-level logging before handing metadata to derived or
    /// internal wrappers. Disabled logging can still flow through as inert
    /// metadata.
    var withoutTopLevel: Their.LifecycleLogging? {
        guard let logging = self else {
            return nil
        }
        guard logging.options.contains(.topLevel) == false else {
            return nil
        }
        return logging
    }
}

#if DEBUG
extension Their.LifecycleLogging {

    static func isStoreLockAvailableForTests() -> Bool {
        LifecycleLoggingStore.shared.isLockAvailableForTests()
    }

    static func resetForTests() {
        LifecycleLoggingStore.shared.reset()
    }

    static func setOutputForTests(_ output: @escaping @Sendable (String) -> Void) {
        LifecycleLoggingStore.shared.setOutput(output)
    }
}

private enum LifecycleLoggingEvent: Sendable {

    case created
    case destroyed

    var delta: Int {
        switch self {
        case .created:
            return 1
        case .destroyed:
            return -1
        }
    }

    init?(_ rawValue: String) {
        switch rawValue.lowercased() {
        case "hub deinit", "job deinit":
            self = .destroyed
        case "hub init", "job init":
            self = .created
        default:
            return nil
        }
    }
}

private struct LifecycleLoggingKey: Hashable, Sendable {

    let file: String
    let label: String?
    let line: UInt
}

private struct LifecycleLoggingOutput: Sendable {

    let line: String
    let sink: @Sendable (String) -> Void
}

private final class LifecycleLoggingStore: Sendable {

    static let shared = LifecycleLoggingStore()

    private let state = Their.Lock(LifecycleLoggingState())

    private func drain() {
        while true {
            let output: LifecycleLoggingOutput? = state.withLock { state in
                guard let output = state.outputs.popFirst() else {
                    return nil
                }
                return output
            }
            guard let output else {
                return
            }
            output.sink(output.line)
        }
    }

    func isLockAvailableForTests() -> Bool {
        state.withLockIfAvailable { _ in true } ?? false
    }

    func log(
        event rawEvent: String,
        logging: Their.LifecycleLogging
    ) {
        guard logging.options.contains(.topLevel) else {
            return
        }
        guard let event = LifecycleLoggingEvent(rawEvent) else {
            return
        }
        let key = LifecycleLoggingKey(
            file: String(describing: logging.file),
            label: logging.label,
            line: logging.line
        )
        let shouldDrain = state.withLock { state in
            let live = state.adjust(key: key, delta: event.delta)
            let label = logging.label ?? "no label"
            let origin = logging.showOrigin ? " origin=\(key.file):\(key.line)" : ""
            let line = "~~| [\(label)] (\(live))\(origin)"
            return state.outputs.append(
                LifecycleLoggingOutput(
                    line: line,
                    sink: state.output
                )
            )
        }
        if shouldDrain {
            drain()
        }
    }

    func reset() {
        let oldOutput = state.withLock { state in
            // A reset can be requested reentrantly by the current output sink.
            // Keep the queued lines and drainer ownership intact so no line is
            // dropped and no second drainer can overtake the first one.
            let oldOutput = state.output
            state.counts = [:]
            state.output = { line in
                print(line)
            }
            return oldOutput
        }
        withExtendedLifetime(oldOutput) {}
    }

    func setOutput(_ output: @escaping @Sendable (String) -> Void) {
        let oldOutput = state.withLock { state in
            let oldOutput = state.output
            state.output = output
            return oldOutput
        }
        // A sink capture's destructor may log or replace the output again.
        withExtendedLifetime(oldOutput) {}
    }
}

private struct LifecycleLoggingState: Sendable {

    var counts = [LifecycleLoggingKey: Int]()
    var output: @Sendable (String) -> Void = { line in
        print(line)
    }
    var outputs = DrainQueue<LifecycleLoggingOutput>()

    mutating func adjust(
        key: LifecycleLoggingKey,
        delta: Int
    ) -> Int {
        let updated = (counts[key] ?? 0) + delta
        if updated <= 0 {
            counts.removeValue(forKey: key)
            return max(updated, 0)
        }
        counts[key] = updated
        return updated
    }
}
#endif
