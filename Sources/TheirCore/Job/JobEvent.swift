import Foundation

extension Their {

    /// Downstream event delivered to a `Job` subscriber: a `.value`, a terminal
    /// successful `.finished`, or a terminal `.failure`.
    public enum JobEvent<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

        case failure(Failure)
        case finished
        case value(Value)
    }
}

extension Their.JobEvent: Equatable where Failure: Equatable, Value: Equatable {}
