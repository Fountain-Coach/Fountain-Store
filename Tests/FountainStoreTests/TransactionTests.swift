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
}

