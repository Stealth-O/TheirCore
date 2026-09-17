import Foundation

extension Their {

    /// Upstream outcome reported through `WorkReport`: a `.value`, a terminal
    /// successful `.finished`, or a terminal `.failure`.
    ///
    /// `.finished` is the producer-side way to say "no more values will ever be
    /// reported": the lifecycle terminates successfully and the subscriber
    /// receives a terminal `.finished` event. `.failure` terminates the lifecycle with a
    /// failure. Both are queued report inputs, so they stay FIFO-ordered behind
    /// values reported earlier and never overtake them; reports after a terminal
    /// outcome are ignored.
    public enum WorkOutput<Value: Sendable, Failure: Swift.Error & Sendable>: Sendable {

        case failure(Failure)
        case finished
        case value(Value)
    }
}

extension Their.WorkOutput: Equatable where Failure: Equatable, Value: Equatable {}
