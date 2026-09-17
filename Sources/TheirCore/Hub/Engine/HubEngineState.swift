import Foundation

/// Snapshot of `HubEngine` lifecycle state for tests: whether an inner
/// `JobEngine` is running and how many subscribers are attached.
struct HubEngineState: Equatable, Sendable {

    let isRunning: Bool
    let subscribersCount: Int

    init(
        isRunning: Bool,
        subscribersCount: Int
    ) {
        self.isRunning = isRunning
        self.subscribersCount = subscribersCount
    }
}
