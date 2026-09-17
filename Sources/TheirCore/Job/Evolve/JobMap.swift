import Foundation

public extension Their.Job {

    /// Stateless map of both value and failure: every upstream value
    /// produces one derived value (no filtering or accumulation). Built on root
    /// `evolve`; see its header for the lifecycle, cancellation and terminal contract.
    func map<NewValue: Sendable, NewFailure: Swift.Error & Sendable>(
        fileID: String = #fileID,
        failure: @escaping @Sendable (Failure) -> NewFailure,
        function: String = #function,
        line: UInt = #line,
        value: @escaping @Sendable (Value) -> NewValue
    ) -> Their.Job<NewValue, NewFailure> {
        evolve(
            fileID: fileID,
            failure: failure,
            function: function,
            initial: (),
            line: line
        ) { _, input in
            value(input)
        }
    }

    /// Stateless value map; failure type unchanged. Built on root
    /// `evolve`; see its header for the lifecycle, cancellation and terminal contract.
    func map<NewValue: Sendable>(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line,
        _ value: @escaping @Sendable (Value) -> NewValue
    ) -> Their.Job<NewValue, Failure> {
        evolve(
            fileID: fileID,
            function: function,
            initial: (),
            line: line
        ) { _, input in
            value(input)
        }
    }
}
