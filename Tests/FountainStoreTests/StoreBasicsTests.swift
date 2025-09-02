
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
}
