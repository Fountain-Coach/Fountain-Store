import XCTest
@testable import FountainStore
import FountainStoreCore

final class TransactionalRecoveryTests: XCTestCase {
    struct Doc: Codable, Identifiable, Equatable { var id: Int; var val: String }
    private struct Frame: Codable { let type: String; let txid: String?; let key: Data?; let value: Data?? }

    private func key(_ coll: String, _ id: Int) throws -> Data {
        var k = Data(coll.utf8)
        k.append(0)
        k.append(try JSONEncoder().encode(id))
        return k
    }

    func test_uncommitted_transaction_is_ignored_on_replay() async throws {
        let (store, dir) = try await makeTempStore()
        defer { try? FileManager.default.removeItem(at: dir) }
        let tx = UUID().uuidString
        // BEGIN + OP without COMMIT
        let begin = try JSONEncoder().encode(Frame(type: "begin", txid: tx, key: nil, value: nil))
        try await store.wal.append(WALRecord(sequence: 0, payload: begin, crc32: 0))
        let op = try JSONEncoder().encode(Frame(type: "op", txid: tx, key: try key("docs", 1), value: try JSONEncoder().encode(Doc(id: 1, val: "a"))))
        try await store.wal.append(WALRecord(sequence: 1, payload: op, crc32: 0))
        try await store.wal.sync()
        // Reopen and verify doc not visible.
        let reopened = try await reopenStore(at: dir)
        let coll = await reopened.collection("docs", of: Doc.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        let v = try await coll.get(id: 1)
        XCTAssertNil(v)
    }

    func test_committed_transaction_applies_on_replay() async throws {
        let (store, dir) = try await makeTempStore()
        defer { try? FileManager.default.removeItem(at: dir) }
        let tx = UUID().uuidString
        let begin = try JSONEncoder().encode(Frame(type: "begin", txid: tx, key: nil, value: nil))
        try await store.wal.append(WALRecord(sequence: 0, payload: begin, crc32: 0))
        let op = try JSONEncoder().encode(Frame(type: "op", txid: tx, key: try key("docs", 2), value: try JSONEncoder().encode(Doc(id: 2, val: "b"))))
        try await store.wal.append(WALRecord(sequence: 10, payload: op, crc32: 0))
        let commit = try JSONEncoder().encode(Frame(type: "commit", txid: tx, key: nil, value: nil))
        try await store.wal.append(WALRecord(sequence: 11, payload: commit, crc32: 0))
        try await store.wal.sync()
        // Reopen and verify doc visible.
        let reopened = try await reopenStore(at: dir)
        let coll = await reopened.collection("docs", of: Doc.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        let v = try await coll.get(id: 2)
        XCTAssertEqual(v, Doc(id: 2, val: "b"))
    }
}
