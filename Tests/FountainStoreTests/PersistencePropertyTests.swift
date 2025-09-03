import XCTest
@testable import FountainStore

final class PersistencePropertyTests: XCTestCase {
    struct Doc: Codable, Identifiable, Equatable { var id: Int; var value: Int }

    struct LCG: RandomNumberGenerator {
        var state: UInt64
        mutating func next() -> UInt64 {
            state = 6364136223846793005 &* state &+ 1
            return state
        }
    }

    func test_randomized_crash_recovery() async throws {
        var rng = LCG(state: 42)
        let (store, dir) = try await makeTempStore()
        defer { try? FileManager.default.removeItem(at: dir) }
        var s: FountainStore? = store
        let coll = await s!.collection("docs", of: Doc.self)
        var expected: [Int: Doc] = [:]
        for _ in 0..<100 {
            let id = Int(rng.next() % 50)
            let value = Int(rng.next() % 1000)
            let doc = Doc(id: id, value: value)
            try await coll.put(doc)
            expected[id] = doc
        }
        try await triggerMemtableFlush(s!)
        s = nil
        let reopened = try await reopenStore(at: dir)
        let coll2 = await reopened.collection("docs", of: Doc.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        for (id, doc) in expected {
            let got = try await coll2.get(id: id)
            XCTAssertEqual(got, doc)
        }
    }
}
