import Foundation

extension Their {

    /// Downstream subscriber closure for a `Hub`, receiving each `HubEvent`.
    public typealias HubSink<Value: Sendable, Failure: Swift.Error & Sendable> =
        @Sendable (Their.HubEvent<Value, Failure>) -> Void
}
