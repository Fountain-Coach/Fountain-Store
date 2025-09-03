@testable import FountainStore
import XCTest

final class CrashRecoveryTests: XCTestCase {
    struct Doc: Codable, Identifiable { let id: Int; var val: String }

    private func tempDir() throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    func testPartialWALRecordIgnored() async throws {
        let dir = try tempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await FountainStore.open(StoreOptions(path: dir))
        let coll = await store.collection("docs", of: Doc.self)
        try await coll.put(Doc(id: 1, val: "a"))
        // Write a partial WAL record (truncated bytes).
        let walPath = dir.appendingPathComponent("wal.log")
        let h = try FileHandle(forWritingTo: walPath)
        try h.seekToEnd()
        h.write(Data([0x00]))
        try h.close()
        let reopened = try await FountainStore.open(StoreOptions(path: dir))
        let coll2 = await reopened.collection("docs", of: Doc.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        let v = try await coll2.get(id: 1)
        XCTAssertEqual(v?.val, "a")
    }

    func testRecoveryFromFlushedSSTable() async throws {
        let (store, dir) = try await makeTempStore()
        var s: FountainStore? = store
        let coll = await s!.collection("docs", of: Doc.self)
        try await coll.put(Doc(id: 1, val: "a"))
        try await triggerMemtableFlush(s!)
        s = nil
        let reopened = try await reopenStore(at: dir)
        let coll2 = await reopened.collection("docs", of: Doc.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        let v = try await coll2.get(id: 1)
        XCTAssertEqual(v?.val, "a")
    }
}

