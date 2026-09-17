import Foundation

extension Their {

    /// Downstream subscriber closure for a `Job`, receiving each `JobEvent`.
    public typealias JobSink<Value: Sendable, Failure: Swift.Error & Sendable> = @Sendable (Their.JobEvent<Value, Failure>) -> Void
}
