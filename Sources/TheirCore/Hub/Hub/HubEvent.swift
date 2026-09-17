import Foundation

extension Their {

    /// Downstream event delivered to a `Hub` subscriber: a broadcast `.value`, a
    /// terminal successful `.finished`, or a terminal `.failure`.
    public enum HubEvent<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

        case failure(Failure)
        case finished
        case value(Value)
    }
}

extension Their.HubEvent: Equatable where Failure: Equatable, Value: Equatable {}
