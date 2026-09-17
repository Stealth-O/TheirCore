import Foundation

public extension Their.Job {

    /// Stateless map of the failure type only; value type unchanged, for
    /// example wrapping an SDK error into a domain error. Built on root
    /// `evolve`; see its header for the lifecycle, cancellation and terminal contract.
    func mapError<NewFailure: Swift.Error & Sendable>(
        fileID: String = #fileID,
        _ failure: @escaping @Sendable (Failure) -> NewFailure,
        function: String = #function,
        line: UInt = #line
    ) -> Their.Job<Value, NewFailure> {
        evolve(
            fileID: fileID,
            failure: failure,
            function: function,
            initial: (),
            line: line
        ) { _, value in
            value
        }
    }
}
