import Foundation

public extension Their.Job {

    /// Maps each upstream value through a throwing transform, turning a thrown
    /// error into a terminal failure. The canonical operator for "decode every
    /// value, and a bad value ends the stream" — it replaces the hand-written
    /// `switch event { case .value: do { report(.value(try decode(...))) } catch
    /// { report(.failure(...)) } ... }` adapters that otherwise repeat per domain.
    ///
    /// Value: each upstream `.value` runs `transform`. A returned `NewValue` is
    /// emitted downstream as `.value`. A thrown error is mapped through `onThrow`
    /// into the derived job's `Failure`. The lifecycle then closes and cancels the
    /// still-live upstream subscription if its cancel is already stored. The
    /// terminal `.failure` is claimed after that teardown; cancellation of the
    /// derived subscription during teardown suppresses the pending callback.
    /// If the upstream cancel has not been stored yet, delivery does not wait
    /// for it: `.failure` can arrive before `upstream.subscribe` returns, and its
    /// late-returned cancel is invoked immediately afterward. That later
    /// teardown cannot suppress an already-delivered failure. This applies both
    /// to synchronous reports inside `subscribe` and to concurrent reports
    /// before its cancel is stored. Cancellation from `onThrow` itself also
    /// suppresses the failure. `onThrow` covers both a typed domain
    /// error and a generic fallback, e.g.
    /// `{ ($0 as? DomainError) ?? .invalidDocument(...) }`.
    ///
    /// Failure type: `tryMap` keeps the upstream `Failure` unchanged — an
    /// upstream `.failure` is forwarded as-is. Normalise the failure first with
    /// `mapError` when the domain failure differs, then `tryMap` only has to
    /// produce that same `Failure` from a thrown error:
    /// `upstream.mapError { .firestore($0) }.tryMap({ try decode($0) }) { ... }`.
    ///
    /// Execution and lifecycle: built on the root `evolve` machinery, so it
    /// inherits that contract — upstream events enter one FIFO queue, a single
    /// drainer reduces them in order, and `transform` / `onThrow` / the sink run
    /// with the internal lock released. A terminal (upstream `.finished` / `.failure`, or
    /// a value-driven `.failure`) is FIFO-ordered behind earlier values and never
    /// overtakes them; the derived job is single-subscriber and single-lifecycle
    /// and forwards misuse to the upstream's `MisuseHandler`; the returned cancel
    /// pins the chain like any `Job`. Keep `transform` cheap and non-blocking: it
    /// runs under the drainer — route heavy decode to an effect boundary if it is
    /// not trivial. Tested in `JobTryMapTests`.
    ///
    /// - Parameters:
    ///   - transform: Produces the next derived value, or throws to terminate.
    ///   - onThrow: Maps a thrown error into the derived job's terminal failure.
    func tryMap<NewValue: Sendable>(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line,
        _ transform: @escaping @Sendable (Value) throws -> NewValue,
        onThrow: @escaping @Sendable (Swift.Error) -> Failure
    ) -> Their.Job<NewValue, Failure> {
        let misuseLocation = Their.MisuseLocation(
            fileID: fileID,
            function: function,
            line: line
        )
        return evolveOutcome(
            failure: { $0 },
            initialState: (),
            misuseLocation: misuseLocation
        ) { _, input in
            do {
                return .emit(try transform(input))
            } catch {
                return .failure(onThrow(error))
            }
        }
    }
}
