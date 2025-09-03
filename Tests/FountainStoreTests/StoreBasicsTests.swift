
import XCTest
@testable import FountainStore

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
}
