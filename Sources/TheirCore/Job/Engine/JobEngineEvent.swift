import Foundation

/// Internal event the `JobEngine` delivers to its sink: a `.value`, a terminal
/// successful `.finished`, a terminal `.failure`, or a diagnostic `.message` (dropped
/// by the public `Job` facade).
enum JobEngineEvent<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

    case failure(Failure)
    case finished
    case message(JobEngineMessage)
    case value(Value)
}

extension JobEngineEvent: Equatable where Failure: Equatable, Value: Equatable {}
