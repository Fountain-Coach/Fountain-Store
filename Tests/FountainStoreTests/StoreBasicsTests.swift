
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
}
