import XCTest
@testable import FountainStore

final class StressTests: XCTestCase {
    struct Doc: Codable, Identifiable, Equatable { var id: Int; var v: String }

    func test_wal_rotation_and_compaction_under_load() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        // Open with small WAL segment size to force rotation quickly.
        let store = try await FountainStore.open(.init(path: dir, cacheBytes: 1 << 20, logger: nil, defaultScanLimit: 100, walSegmentBytes: 8 << 10))
        let coll = await store.collection("docs", of: Doc.self)

        // Write a few thousand records with intermittent compaction ticks.
        let total = 2_000
        for i in 0..<total {
            try await coll.put(.init(id: i, v: "x\(i)"))
            if i % 200 == 0 { await store.compactor.tick() }
        }
        try await store.flushMemtableIfNeeded()
        await store.compactor.tick()

        // Reopen and validate a sample of records exist.
        let reopened = try await FountainStore.open(.init(path: dir))
        let reopenedColl = await reopened.collection("docs", of: Doc.self)
        for i in [0, 500, 1000] {
            let v = try await reopenedColl.get(id: i)
            XCTAssertEqual(v?.id, i)
        }
    }
}
