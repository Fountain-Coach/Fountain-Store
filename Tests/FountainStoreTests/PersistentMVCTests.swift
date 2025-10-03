import XCTest
@testable import FountainStore

final class PersistentMVCTests: XCTestCase {
    struct Doc: Codable, Identifiable, Equatable { var id: Int; var val: String }

    func test_snapshot_across_restart_preserves_history() async throws {
        // Open fresh store and create two versions of the same doc.
        let (store, dir) = try await makeTempStore()
        defer { try? FileManager.default.removeItem(at: dir) }
        let coll = await store.collection("docs", of: Doc.self)

        try await coll.put(.init(id: 1, val: "v1"))
        let s1 = await store.snapshot()
        try await coll.put(.init(id: 1, val: "v2"))
        let s2 = await store.snapshot()

        // Force a memtable flush so versions persist in SSTables with sequences.
        try await triggerMemtableFlush(store)

        // Re-open and verify snapshot reads see the correct historical values.
        let reopened = try await reopenStore(at: dir)
        let coll2 = await reopened.collection("docs", of: Doc.self)
        try await Task.sleep(nanoseconds: 1_000_000)

        let atS1 = try await coll2.get(id: 1, snapshot: s1)
        XCTAssertEqual(atS1, Doc(id: 1, val: "v1"))

        let atS2 = try await coll2.get(id: 1, snapshot: s2)
        XCTAssertEqual(atS2, Doc(id: 1, val: "v2"))
    }
}

