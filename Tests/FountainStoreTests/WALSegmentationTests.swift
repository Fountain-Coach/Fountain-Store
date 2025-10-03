import XCTest
@testable import FountainStore

final class WALSegmentationTests: XCTestCase {
    struct D: Codable, Identifiable, Equatable { var id: Int }

    func test_wal_segment_rotation_and_replay() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        // Small segment size to force rotation quickly.
        let store = try await FountainStore.open(.init(path: dir, walSegmentBytes: 1 << 10))
        let c = await store.collection("d", of: D.self)
        // Write enough records to exceed 1KB WAL.
        for i in 0..<200 { try await c.put(.init(id: i)) }
        // Reopen and verify a few random records are still present.
        let reopened = try await reopenStore(at: dir)
        let c2 = await reopened.collection("d", of: D.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        for id in [0, 50, 199] {
            let v = try await c2.get(id: id)
            XCTAssertEqual(v, D(id: id))
        }
    }
}

