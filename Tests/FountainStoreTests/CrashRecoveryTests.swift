@testable import FountainStore
import FountainStoreCore
import XCTest

final class CrashRecoveryTests: XCTestCase {
    struct Doc: Codable, Identifiable { let id: Int; var val: String }

    private func tempDir() throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private func runCrash(point: String, requiresFlush: Bool) async throws {
        let dir = try tempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await FountainStore.open(StoreOptions(path: dir))
        let coll = await store.collection("docs", of: Doc.self)
        if requiresFlush {
            for i in 0..<1024 {
                await store.memtable.put(MemtableEntry(key: Data("d\(i)".utf8), value: Data("x".utf8), sequence: 0))
            }
        }
        CrashPoints.active = point
        do {
            try await coll.put(Doc(id: 0, val: "v0"))
            XCTFail("expected crash")
        } catch is CrashError {}
        CrashPoints.active = nil
        let reopened = try await FountainStore.open(StoreOptions(path: dir))
        let coll2 = await reopened.collection("docs", of: Doc.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        let v = try await coll2.get(id: 0)
        if requiresFlush {
            XCTAssertNil(v)
            let m = try await reopened.manifest.load()
            XCTAssertEqual(m.sequence, 1)
        } else {
            XCTAssertEqual(v?.val, "v0")
        }
    }

    func testCrashMatrix() async throws {
        try await runCrash(point: "wal_append", requiresFlush: false)
        try await runCrash(point: "wal_fsync", requiresFlush: false)
        try await runCrash(point: "manifest_save", requiresFlush: true)
        try await runCrash(point: "memtable_flush", requiresFlush: true)
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
}

