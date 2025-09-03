
import XCTest
@testable import FountainStore
import FountainStoreCore

final class StoreBasicsTests: XCTestCase {
    struct Note: Codable, Identifiable, Equatable {
        var id: UUID
        var title: String
        var body: String
    }

    func test_open_and_snapshot() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let start = await store.snapshot()
        let notes = await store.collection("notes", of: Note.self)
        try await notes.put(.init(id: UUID(), title: "t", body: "b"))
        let end = await store.snapshot()
        XCTAssertGreaterThan(end.sequence, start.sequence)
    }

    func test_collection_put_get() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let notes = await store.collection("notes", of: Note.self)
        let note = Note(id: UUID(), title: "hello", body: "world")
        try await notes.put(note)
        let loaded = try await notes.get(id: note.id)
        XCTAssertEqual(loaded, note)
    }

    func test_collection_delete() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let notes = await store.collection("notes", of: Note.self)
        let note = Note(id: UUID(), title: "hello", body: "world")
        try await notes.put(note)
        try await notes.delete(id: note.id)
        let loaded = try await notes.get(id: note.id)
        XCTAssertNil(loaded)
    }

    func test_snapshot_isolation() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let notes = await store.collection("notes", of: Note.self)
        let note = Note(id: UUID(), title: "hello", body: "world")
        try await notes.put(note)
        let snap = await store.snapshot()
        try await notes.delete(id: note.id)
        let current = try await notes.get(id: note.id)
        let snapValue = try await notes.get(id: note.id, snapshot: snap)
        XCTAssertNil(current)
        XCTAssertEqual(snapValue, note)
    }

    func test_history_tracking() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let notes = await store.collection("notes", of: Note.self)
        let id = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        let v1 = Note(id: id, title: "t1", body: "b1")
        try await notes.put(v1)
        var v2 = v1; v2.body = "b2"
        try await notes.put(v2)
        let snap = await store.snapshot()
        try await notes.delete(id: id)
        let all = try await notes.history(id: id)
        XCTAssertEqual(all.map { $0.1 }, [v1, v2, nil])
        XCTAssertEqual(all.map { $0.0 }, [1, 2, 3])
        let snapHist = try await notes.history(id: id, snapshot: snap)
        XCTAssertEqual(snapHist.map { $0.1 }, [v1, v2])
    }

    func test_scan_respects_snapshot_and_limit() async throws {
        struct Item: Codable, Identifiable, Equatable {
            var id: Int
            var body: String
        }
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let items = await store.collection("items", of: Item.self)

        try await items.put(.init(id: 1, body: "a"))
        try await items.put(.init(id: 2, body: "b"))
        try await items.put(.init(id: 3, body: "c"))
        let snap = await store.snapshot()
        try await items.delete(id: 2)
        try await items.put(.init(id: 3, body: "c2"))
        try await items.put(.init(id: 4, body: "d"))

        let current = try await items.scan().map { $0.id }
        XCTAssertEqual(current, [1, 3, 4])

        let snapScan = try await items.scan(snapshot: snap).map { $0.id }
        XCTAssertEqual(snapScan, [1, 2, 3])

        let limited = try await items.scan(limit: 2).map { $0.id }
        XCTAssertEqual(limited, [1, 3])
    }

    func test_unique_index_lookup_and_snapshot() async throws {
        struct User: Codable, Identifiable, Equatable {
            var id: UUID
            var email: String
        }
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let users = await store.collection("users", of: User.self)
        try await users.define(.init(name: "byEmail", kind: .unique(\User.email)))
        let id = UUID(uuidString: "00000000-0000-0000-0000-000000000010")!
        let original = User(id: id, email: "a@example.com")
        try await users.put(original)
        let snap = await store.snapshot()
        var updated = original
        updated.email = "b@example.com"
        try await users.put(updated)
        let currentA = try await users.byIndex("byEmail", equals: "a@example.com")
        XCTAssertTrue(currentA.isEmpty)
        let currentB = try await users.byIndex("byEmail", equals: "b@example.com")
        XCTAssertEqual(currentB, [updated])
        let snapA = try await users.byIndex("byEmail", equals: "a@example.com", snapshot: snap)
        XCTAssertEqual(snapA, [original])
        try await users.delete(id: id)
        let afterDel = try await users.byIndex("byEmail", equals: "b@example.com")
        XCTAssertTrue(afterDel.isEmpty)
    }

    func test_multi_index_lookup() async throws {
        struct Doc: Codable, Identifiable, Equatable {
            var id: Int
            var tag: String
        }
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let docs = await store.collection("docs", of: Doc.self)
        try await docs.define(.init(name: "byTag", kind: .multi(\Doc.tag)))
        try await docs.put(.init(id: 1, tag: "a"))
        try await docs.put(.init(id: 2, tag: "a"))
        try await docs.put(.init(id: 3, tag: "b"))
        let snap = await store.snapshot()
        try await docs.delete(id: 1)
        try await docs.put(.init(id: 2, tag: "b"))
        let currentA = try await docs.byIndex("byTag", equals: "a").map { $0.id }
        XCTAssertEqual(currentA, [])
        let currentB = try await docs.byIndex("byTag", equals: "b").map { $0.id }.sorted()
        XCTAssertEqual(currentB, [2, 3])
        let snapA = try await docs.byIndex("byTag", equals: "a", snapshot: snap).map { $0.id }.sorted()
        XCTAssertEqual(snapA, [1, 2])
    }

    func test_unique_index_scan_prefix_and_limit() async throws {
        struct User: Codable, Identifiable, Equatable {
            var id: Int
            var email: String
        }
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let users = await store.collection("users", of: User.self)
        try await users.define(.init(name: "byEmail", kind: .unique(\User.email)))
        try await users.put(.init(id: 1, email: "a@example.com"))
        try await users.put(.init(id: 2, email: "aa@example.com"))
        try await users.put(.init(id: 3, email: "b@example.com"))
        let res = try await users.scanIndex("byEmail", prefix: "a").map { $0.id }
        XCTAssertEqual(res, [1, 2])
        let limited = try await users.scanIndex("byEmail", prefix: "a", limit: 1).map { $0.id }
        XCTAssertEqual(limited, [1])
    }

    func test_multi_index_scan_prefix() async throws {
        struct Doc: Codable, Identifiable, Equatable {
            var id: Int
            var tag: String
        }
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let docs = await store.collection("docs", of: Doc.self)
        try await docs.define(.init(name: "byTag", kind: .multi(\Doc.tag)))
        try await docs.put(.init(id: 1, tag: "a1"))
        try await docs.put(.init(id: 2, tag: "a2"))
        try await docs.put(.init(id: 3, tag: "b1"))
        let res = try await docs.scanIndex("byTag", prefix: "a").map { $0.id }.sorted()
        XCTAssertEqual(res, [1, 2])
    }

    // MARK: - Persistence Across Restart

    func test_records_and_indexes_persist_across_restart() async throws {
        struct User: Codable, Identifiable, Equatable { var id: Int; var email: String }
        struct Doc: Codable, Identifiable, Equatable { var id: Int; var tag: String }
        struct Text: Codable, Identifiable, Equatable { var id: Int; var text: String }
        struct Vec: Codable, Identifiable, Equatable { var id: String; var embedding: [Double] }

        let (initial, dir) = try await makeTempStore()
        var store: FountainStore? = initial
        let users = await store!.collection("users", of: User.self)
        try await users.define(.init(name: "byEmail", kind: .unique(\User.email)))
        try await users.put(.init(id: 1, email: "a@example.com"))

        let docs = await store!.collection("docs", of: Doc.self)
        try await docs.define(.init(name: "byTag", kind: .multi(\Doc.tag)))
        try await docs.put(.init(id: 1, tag: "t1"))

        let texts = await store!.collection("texts", of: Text.self)
        try await texts.define(.init(name: "fts", kind: .fts(\Text.text)))
        try await texts.put(.init(id: 1, text: "hello world"))

        let vecs = await store!.collection("vecs", of: Vec.self)
        try await vecs.define(.init(name: "vec", kind: .vector(\Vec.embedding)))
        try await vecs.put(.init(id: "a", embedding: [0.0, 0.0]))

        store = nil
        let reopened = try await reopenStore(at: dir)
        let rUsers = await reopened.collection("users", of: User.self)
        try await rUsers.define(.init(name: "byEmail", kind: .unique(\User.email)))
        let rDocs = await reopened.collection("docs", of: Doc.self)
        try await rDocs.define(.init(name: "byTag", kind: .multi(\Doc.tag)))
        let rTexts = await reopened.collection("texts", of: Text.self)
        try await rTexts.define(.init(name: "fts", kind: .fts(\Text.text)))
        let rVecs = await reopened.collection("vecs", of: Vec.self)
        try await rVecs.define(.init(name: "vec", kind: .vector(\Vec.embedding)))

        try await Task.sleep(nanoseconds: 1_000_000)

        let uVal = try await rUsers.get(id: 1)
        let dVal = try await rDocs.get(id: 1)
        let tVal = try await rTexts.get(id: 1)
        let vVal = try await rVecs.get(id: "a")
        XCTAssertNotNil(uVal)
        XCTAssertNotNil(dVal)
        XCTAssertNotNil(tVal)
        XCTAssertNotNil(vVal)

        let uRes = try await rUsers.byIndex("byEmail", equals: "a@example.com").map { $0.id }
        XCTAssertEqual(uRes, [1])
        let dRes = try await rDocs.byIndex("byTag", equals: "t1").map { $0.id }
        XCTAssertEqual(dRes, [1])
        let tRes = try await rTexts.searchText("fts", query: "hello").map { $0.id }
        XCTAssertEqual(tRes, [1])
        let vRes = try await rVecs.vectorSearch("vec", query: [0.1, 0.1], k: 1).map { $0.id }
        XCTAssertEqual(vRes, ["a"])
    }

    // MARK: - Property Tests

    func test_wal_corruption_detection_property() async throws {
        for _ in 0..<10 {
            let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            let wal = WAL(path: url)
            defer { try? FileManager.default.removeItem(at: url) }
            let payload = Data((0..<16).map { _ in UInt8.random(in: 0...255) })
            try await wal.append(WALRecord(sequence: 1, payload: payload, crc32: 0))
            try await wal.sync()
            var data = try Data(contentsOf: url)
            // Skip header (seq + len) and corrupt payload or CRC.
            if data.count > 12 {
                let idx = Int.random(in: 12..<data.count)
                data[idx] ^= 0xFF
            }
            try data.write(to: url)
            let recs = try await wal.replay()
            XCTAssertTrue(recs.isEmpty)
        }
    }

    func test_manifest_sequence_monotonicity_property() async throws {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let manifest = ManifestStore(url: url)
        defer { try? FileManager.default.removeItem(at: url) }
        var last: UInt64 = 0
        for _ in 0..<20 {
            last &+= UInt64.random(in: 0...5)
            try await manifest.save(Manifest(sequence: last))
            let loaded = try await manifest.load()
            XCTAssertGreaterThanOrEqual(loaded.sequence, last)
            XCTAssertEqual(loaded.sequence, last)
        }
    }
}
