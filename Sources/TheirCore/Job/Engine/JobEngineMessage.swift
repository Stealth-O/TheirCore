import Foundation

/// Diagnostic outcome of a `JobEngine` input that produced no value or failure —
/// an emit/control ignored because the state was not active, or a stop/deinit
/// result. Surfaced only as a `JobEngineEvent.message` and dropped by `Job`.
enum JobEngineMessage: Equatable, Sendable {

    case deinitIgnoredBecauseStateIsNotActive
    case deinitStoppedWhileStarting
    case emitFailureIgnoredBecauseStateIsNotActive
    case emitFinishedIgnoredBecauseStateIsNotActive
    case emitOutputIgnoredBecauseStateIsNotActive
    case stopIgnoredBecauseStateIsNotActive
    case stopStopped
    case terminateIgnoredBecauseStateIsNotActive
    case terminateStopped
}
