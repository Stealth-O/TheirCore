import Foundation

public extension Their.Job {

    /// Wraps one async operation into a finite one-shot `Their.Job`.
    ///
    /// Lifecycle: `subscribe` starts one `Task` running `operation`. A successful
    /// result is reported as one `.value` followed by the terminal `.finished`; a
    /// thrown error is mapped through `failure` into the job's typed failure and
    /// reported as the terminal `.failure`. Both reports go through the engine
    /// FIFO, so the value is always delivered before `.finished` and a terminal
    /// outcome never overtakes it. `Their.Job.once { ... }.stream()` therefore
    /// produces a `for await` loop that ends naturally on both outcomes.
    ///
    /// Error typing: the operation is untyped `throws` and the `failure` mapper
    /// owns the domain mapping, mirroring `evolve(failure:)`. Typed
    /// `throws(Failure)` was deliberately not used: closure conversions into a
    /// generic typed-throws parameter (`Never`/`any Error` into `Failure`) are
    /// still rejected by the compiler, which would force explicit signatures at
    /// every call site. The non-throwing overload below covers
    /// `Failure == Never` sources.
    ///
    /// Cancellation: the returned lifecycle's `WorkCancel` cancels the `Task`
    /// through standard Swift cooperative cancellation — `operation` decides how
    /// it observes `Task.isCancelled`/cancellation handlers. An outcome the
    /// operation still reports after cancel is dropped by the already-terminated
    /// engine, so no event reaches the subscriber after cancellation.
    ///
    /// Everything else is the standard `Their.Job` contract: single-subscriber,
    /// single-lifecycle, misuse on a second `subscribe`, the cancel/pin behavior
    /// and `deinit` cancelling an active lifecycle. Tested in `JobOnceTests`.
    static func once(
        fileID: String = #fileID,
        failure: @escaping @Sendable (any Error) -> Failure,
        function: String = #function,
        line: UInt = #line,
        logging: Their.LifecycleLogging? = nil,
        misuseHandler: @escaping Their.MisuseHandler = Their.MisuseHandlers.fatal,
        _ operation: @escaping @Sendable () async throws -> Value
    ) -> Their.Job<Value, Failure> {
        Their.Job(
            fileID: fileID,
            function: function,
            line: line,
            logging: logging,
            misuseHandler: misuseHandler,
            work: { report in
                let task = Task {
                    do {
                        let value = try await operation()
                        report(.value(value))
                        report(.finished)
                    } catch {
                        report(.failure(failure(error)))
                    }
                }
                return {
                    task.cancel()
                }
            }
        )
    }
}

public extension Their.Job where Failure == Never {

    /// Non-throwing one-shot overload: a `Failure == Never` job that reports one
    /// `.value` then the terminal `.finished`. See the throwing overload above for
    /// the full lifecycle, cancellation and FIFO contract.
    static func once(
        fileID: String = #fileID,
        function: String = #function,
        line: UInt = #line,
        logging: Their.LifecycleLogging? = nil,
        misuseHandler: @escaping Their.MisuseHandler = Their.MisuseHandlers.fatal,
        _ operation: @escaping @Sendable () async -> Value
    ) -> Their.Job<Value, Never> {
        Their.Job(
            fileID: fileID,
            function: function,
            line: line,
            logging: logging,
            misuseHandler: misuseHandler,
            work: { report in
                let task = Task {
                    let value = await operation()
                    report(.value(value))
                    report(.finished)
                }
                return {
                    task.cancel()
                }
            }
        )
    }
}
