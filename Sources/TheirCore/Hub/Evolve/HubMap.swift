import Foundation

public extension Their.Hub {

    /// Stateless shared map of both value and failure: every upstream value
    /// produces one derived value (no filtering or accumulation). Built on root
    /// `evolve`; see its header for the shared lifecycle, cancellation and terminal contract.
    func map<NewValue: Sendable, NewFailure: Swift.Error & Sendable>(
        fileID: String = #fileID,
        failure: @escaping @Sendable (Failure) -> NewFailure,
        function: String = #function,
        line: UInt = #line,
        value: @escaping @Sendable (Value) -> NewValue
    ) -> Their.Hub<NewValue, NewFailure> {
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

    /// Stateless shared value map; failure type unchanged. Built on root
    /// `evolve`; see its header for the shared lifecycle, cancellation and terminal contract.
    func map<NewValue: Sendable>(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line,
        _ value: @escaping @Sendable (Value) -> NewValue
    ) -> Their.Hub<NewValue, Failure> {
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
