import XCTest
@testable import FountainStore

final class BackupRestoreTests: XCTestCase {
    struct Item: Codable, Identifiable, Equatable { var id: Int; var body: String }

    func test_backup_and_restore_roundtrip() async throws {
        let (store, dir) = try await makeTempStore()
        defer { try? FileManager.default.removeItem(at: dir) }
        let items = await store.collection("items", of: Item.self)
        try await items.put(.init(id: 1, body: "a"))
        try await items.put(.init(id: 2, body: "b"))
        // Create backup
        let ref = try await store.createBackup(note: "initial")
        XCTAssertFalse(ref.id.isEmpty)

        // Mutate state after backup
        try await items.delete(id: 1)
        try await items.put(.init(id: 3, body: "c"))
        var now = try await items.scan().map { $0.id }.sorted()
        XCTAssertEqual(now, [2,3])

        // Restore
        try await store.restoreBackup(id: ref.id)
        let reopened = try await reopenStore(at: dir)
        let items2 = await reopened.collection("items", of: Item.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        let after = try await items2.scan().map { $0.id }.sorted()
        XCTAssertEqual(after, [1,2])
    }
}

