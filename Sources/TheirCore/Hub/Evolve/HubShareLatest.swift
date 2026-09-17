import Foundation

public extension Their.Hub {

    /// Explicit latest-value replay operator. A late subscriber receives the
    /// last successful value (if any) exactly once; failures and debug
    /// messages are never replayed. Replay is delivered through the shared
    /// evolution's FIFO queue, so it is ordered with live broadcasts: the
    /// joiner never receives a stale replay after a newer broadcast it
    /// already saw, and never receives the same value twice. The cached value
    /// is cleared on a terminal outcome (`.finished` / `.failure`) and when the last
    /// subscriber unsubscribes, so a later first subscriber starts a fresh
    /// lifecycle with no replay until a new value is observed.
    ///
    /// Use this at call sites that need latest-value-on-subscribe semantics;
    /// the base `Hub` is intentionally live-only. Implemented as the
    /// `replayLatest` evolution over the root `evolve`; keep replay here, never
    /// in `HubEngine`.
    func shareLatest(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line
    ) -> Their.Hub<Value, Failure> {
        evolve(
            fileID: fileID,
            failure: { $0 },
            function: function,
            initial: (),
            line: line,
            replayLatest: true
        ) { _, value in
            value
        }
    }
}
