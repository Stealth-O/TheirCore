import Foundation

extension Their {

    /// Sendable handler that receives `Misuse` reports raised by TheirCore primitives
    /// (e.g. a second subscribe on a single-subscriber `Job`). The default
    /// handler is `MisuseHandlers.fatal`; tests inject a recording handler such
    /// as `TestMisuseRecorder.handler`.
    public typealias MisuseHandler = @Sendable (Their.Misuse) -> Void

    /// Incorrect use of a TheirCore primitive — not a normal domain failure. Misuse
    /// is intentionally outside `JobEvent<Value, Failure>` and `HubEvent<Value, Failure>`
    /// so that the value/failure channels keep their domain meaning; misuse goes
    /// to a separate `MisuseHandler`.
    public struct Misuse: Equatable, Sendable, Swift.Error {

        public let message: String
        public let origin: Their.MisuseLocation
        public let trace: [Their.MisuseLocation]

        public init(
            message: String,
            fileID: String = #fileID,
            function: String = #function,
            line: UInt = #line,
            trace: [Their.MisuseLocation] = []
        ) {
            self.message = message
            self.origin = Their.MisuseLocation(
                fileID: fileID,
                function: function,
                line: line
            )
            self.trace = trace
        }

        public init(
            message: String,
            origin: Their.MisuseLocation,
            trace: [Their.MisuseLocation] = []
        ) {
            self.message = message
            self.origin = origin
            self.trace = trace
        }
    }
}

extension Their.Misuse: CustomStringConvertible {

    public var description: String {
        let traceDescription = trace.map(\.description).joined(separator: "\n")
        guard traceDescription.isEmpty == false else {
            return "\(message)\norigin: \(origin)"
        }
        return "\(message)\norigin: \(origin)\ntrace:\n\(traceDescription)"
    }
}

extension Their {

    public enum MisuseHandlers {

        public static let fatal: Their.MisuseHandler = { misuse in
            fatalError(misuse.description)
        }
    }

    public struct MisuseLocation: Equatable, Sendable {

        public let fileID: String
        public let function: String
        public let line: UInt

        public init(
            fileID: String = #fileID,
            function: String = #function,
            line: UInt = #line
        ) {
            self.fileID = fileID
            self.function = function
            self.line = line
        }
    }
}

extension Their.MisuseLocation: CustomStringConvertible {

    public var description: String {
        "\(fileID):\(line) \(function)"
    }
}
