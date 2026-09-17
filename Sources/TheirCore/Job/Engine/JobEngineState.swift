import Foundation

enum JobEngineState {

    case active(cancel: Their.WorkCancel)
    case idle
    /// Transient state used by `JobEngine.start()` to release the engine `Lock` while invoking the upstream
    /// `work(report:)` closure. Required because `work` may legitimately call `report` synchronously (e.g. an
    /// SDK preflight that emits an immediate failure when the resource is unavailable); the report is enqueued and
    /// drained without re-entering the engine `Lock`. Reads see `.starting` as "running" — a value report delivers,
    /// a failure report / `stop()` / `terminate()` move to `.terminated`, and the queued `.startReturned` input then
    /// invokes the cancel that `work` ultimately returned.
    case starting
    case terminated

    // The `isActive` / `isIdle` / `isTerminated` helpers below are used only
    // by tests for compact `#expect(state.isActive == true)` assertions.
    // Production code always switches over the cases so the compiler enforces
    // exhaustiveness when a new case is added.

    var isActive: Bool {
        switch self {
        case .active, .starting:
            return true
        case .idle, .terminated:
            return false
        }
    }

    var isIdle: Bool {
        switch self {
        case .idle:
            return true
        case .active, .starting, .terminated:
            return false
        }
    }

    var isTerminated: Bool {
        switch self {
        case .terminated:
            return true
        case .active, .idle, .starting:
            return false
        }
    }
}
