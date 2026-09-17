import Foundation

public extension Their.Job {

    /// Creates an inert `Job` that never reports values, failures or `.finished`.
    ///
    /// Use this for dependency defaults, previews and tests where a live
    /// listener should stay silent until the owner cancels it. The lifecycle is
    /// still the standard single-subscriber `Job` lifecycle: subscribing starts
    /// one empty `Work`, cancel/deinit stop it, and a second subscribe reports
    /// normal `Job` misuse.
    static func never(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line,
        logging: Their.LifecycleLogging? = nil,
        misuseHandler: @escaping Their.MisuseHandler = Their.MisuseHandlers.fatal
    ) -> Their.Job<Value, Failure> {
        Their.Job(
            fileID: fileID,
            function: function,
            line: line,
            logging: logging,
            misuseHandler: misuseHandler,
            work: { _ in {} }
        )
    }
}
