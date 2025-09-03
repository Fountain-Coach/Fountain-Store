import XCTest
@testable import FountainStore

final class TransactionTests: XCTestCase {
    struct Item: Codable, Identifiable, Equatable {
        var id: Int
        var body: String
    }

    func test_batch_put_delete_atomicity() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let items = await store.collection("items", of: Item.self)

        let a = Item(id: 1, body: "a")
        let b = Item(id: 2, body: "b")

        let snap = await store.snapshot()
        try await items.batch([.put(a), .put(b), .delete(a.id)])

        let current = try await items.scan().map { $0.id }.sorted()
        XCTAssertEqual(current, [2])

        let snapScan = try await items.scan(snapshot: snap).map { $0.id }
        XCTAssertEqual(snapScan, [])

        let end = await store.snapshot()
        XCTAssertEqual(end.sequence, snap.sequence + 3)
    }

    func test_unique_index_enforcement() async throws {
        struct User: Codable, Identifiable, Equatable {
            var id: Int
            var email: String
        }
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let users = await store.collection("users", of: User.self)
        try await users.define(.init(name: "byEmail", kind: .unique(\User.email)))
        try await users.put(.init(id: 1, email: "a@example.com"))
        do {
            try await users.put(.init(id: 2, email: "a@example.com"))
            XCTFail("expected unique violation")
        } catch CollectionError.uniqueConstraintViolation(let index, let key) {
            XCTAssertEqual(index, "byEmail")
            XCTAssertEqual(key, "a@example.com")
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }
}

