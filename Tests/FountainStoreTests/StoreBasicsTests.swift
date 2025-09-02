
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
        _ = await store.snapshot()
        // .todo: assert sequence changes after writes once implemented
        XCTAssertTrue(true)
    }

    func test_collection_compile() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let notes = await store.collection("notes", of: Note.self)
        _ = try await notes.get(id: UUID())
        XCTAssertTrue(true)
    }
}
