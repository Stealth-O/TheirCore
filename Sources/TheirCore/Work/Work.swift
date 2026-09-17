import Foundation

extension Their {

    /// Upstream work closure: starts source work, reports `WorkOutput` outcomes —
    /// values, a terminal successful end, or a terminal failure — through the
    /// given `WorkReport`, and returns a `WorkCancel` that stops that work.
    public typealias Work<Value: Sendable, Failure: Swift.Error & Sendable> = @Sendable (@escaping Their.WorkReport<Value, Failure>) -> Their.WorkCancel
}
