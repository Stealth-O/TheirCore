import Foundation
import Testing
@testable
import TheirCore
import TheirCoreTesting

@Suite
struct SubscriptionStreamTests {

    /// Exercise the bridge directly, without a Job/Hub engine filtering late
    /// callbacks: releasing a late-returned cancel may synchronously report
    /// again, but the already-finished stream retains only its original FIFO.
    @Test func synchronousTerminalReleasesReturnedCancelAndDropsReentrantOutput() async throws {
        try await Their.stress {
            let cancels = Their.TestCountRecorder()
            let stream = makeSubscriptionStream(
                isTerminal: { (event: Int) in event < 0 },
                subscribe: { report in
                    report(1)
                    report(-1)
                    return {
                        _ = cancels.increment()
                        report(2)
                        report(-2)
                    }
                }
            )
            #expect(cancels.count == 1)

            var iterator = stream.makeAsyncIterator()
            #expect(await iterator.next() == 1)
            #expect(await iterator.next() == -1)
            #expect(await iterator.next() == nil)
            #expect(cancels.count == 1)
        }
    }
}
