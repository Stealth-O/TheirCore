import Foundation

/// Common inline subscription bridge for the public Job/Hub stream adapters.
/// The caller supplies only its event alphabet's terminal test. `Resource`
/// owns the returned cancel, including a terminal event arriving synchronously
/// before subscribe returns it. Stream/iterator copies share the continuation
/// context, whose termination releases that cancel and its facade pin.
/// Events keep the default unbounded buffer; terminal events are yielded before
/// finishing. No scheduler, extra lifecycle or event transformation is added.
func makeSubscriptionStream<Event: Sendable>(
    isTerminal: @escaping @Sendable (Event) -> Bool,
    subscribe: (@escaping @Sendable (Event) -> Void) -> Their.WorkCancel
) -> AsyncStream<Event> {
    AsyncStream { continuation in
        let subscription = Their.Resource<Their.WorkCancel>(release: { $0() })
        continuation.onTermination = { _ in
            subscription.cancel()
        }
        let cancel = subscribe { event in
            continuation.yield(event)
            if isTerminal(event) {
                continuation.finish()
            }
        }
        subscription.set(cancel)
    }
}
