import Foundation

extension Their {

    /// Cancels this hub subscription immediately.
    ///
    /// Future events are suppressed for the canceled subscriber synchronously, and the subscriber is removed from the
    /// `HubEngine` registry synchronously. When this was the last subscriber of an already-running shared lifecycle,
    /// the underlying `JobEngine.stop()` runs inline before `cancel()` returns; if the last subscriber leaves while the
    /// lifecycle is still starting, the in-progress starter stops it on commit instead. Either way the upstream is
    /// stopped exactly once.
    /// The subscription also drops its reference to the sink. A retained cancel
    /// does not retain sink captures after cancellation or terminal delivery;
    /// an already-running callback may retain them until it returns.
    public typealias HubCancel = Their.WorkCancel
}
