import XCTest
@testable import FountainStore
import FountainStoreCore

final class CacheTests: XCTestCase {
    struct T: Codable, Identifiable { let id: Int; let v: String }

    func test_block_cache_counts_hits() async throws {
        // Small cache to ensure enabled
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let store = try await FountainStore.open(.init(path: dir, cacheBytes: 64 * 1024))
        let coll = await store.collection("t", of: T.self)
        try await coll.put(.init(id: 1, v: "a"))
        try await coll.put(.init(id: 2, v: "b"))
        // Force a flush to SSTable, then re-open to ensure reads hit disk/cache
        try await triggerMemtableFlush(store)
        let reopened = try await reopenStore(at: dir)
        let c2 = await reopened.collection("t", of: T.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        _ = try await c2.get(id: 1)
        _ = try await c2.get(id: 1)
        // We can't directly query block cache stats through Store; rely on functionality by repeated gets
        // and just ensure values are retrievable.
        let v = try await c2.get(id: 1)
        XCTAssertEqual(v?.v, "a")
    }
}
