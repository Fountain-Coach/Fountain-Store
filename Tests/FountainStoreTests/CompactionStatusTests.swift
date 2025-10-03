import XCTest
@testable import FountainStore

final class CompactionStatusTests: XCTestCase {
    struct D: Codable, Identifiable { let id: Int }

    func test_compaction_status_reports_levels() async throws {
        let (store, _) = try await makeTempStore()
        // Generate multiple SSTables by exceeding memtable limit several times.
        let coll = await store.collection("d", of: D.self)
        let limit = await store.memtable.limit
        for i in 0..<(limit * 3) { try await coll.put(.init(id: i)) }
        try await store.flushMemtableIfNeeded()
        // Trigger a compaction tick to update pending tables count.
        await store.compactor.tick()
        let st = try await store.compactionStatus()
        XCTAssertGreaterThanOrEqual(st.levels.count, 1)
        let totalTables = st.levels.reduce(0) { $0 + $1.tables }
        XCTAssertGreaterThanOrEqual(totalTables, 1)
    }
}

