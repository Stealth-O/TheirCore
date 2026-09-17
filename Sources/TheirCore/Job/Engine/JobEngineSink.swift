import Foundation

/// Internal closure the `JobEngine` calls to deliver each `JobEngineEvent`.
typealias JobEngineSink<Value: Sendable, Failure: Swift.Error & Sendable> = @Sendable (JobEngineEvent<Value, Failure>) -> Void
