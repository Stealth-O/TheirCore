import Foundation

extension Their {

    /// Reports an upstream job outcome: a `.value`, a terminal successful `.finished`,
    /// or a terminal `.failure`.
    ///
    /// Concurrent calls are accepted safely: each report is appended to the engine's FIFO queue under its lock, so
    /// reports are processed in append order and a later caller never overtakes an earlier queued report. Which of two
    /// genuinely concurrent callers appends first is decided by that lock, not by the domain — establish a domain order
    /// upstream when it matters. Once a terminal outcome (`.finished` or `.failure`) is processed, the job terminates and later
    /// reports are ignored.
    public typealias WorkReport<Value: Sendable, Failure: Swift.Error & Sendable> = @Sendable (Their.WorkOutput<Value, Failure>) -> Void
}
