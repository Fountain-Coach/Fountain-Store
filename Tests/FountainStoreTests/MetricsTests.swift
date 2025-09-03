import XCTest
@testable import FountainStore

final class MetricsTests: XCTestCase {
    struct Item: Codable, Identifiable, Equatable {
        var id: Int
        var body: String
    }

    func test_operation_counters() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let items = await store.collection("items", of: Item.self)
        try await items.define(.init(name: "byBody", kind: .unique(\Item.body)))
        try await items.put(.init(id: 1, body: "a"))
        _ = try await items.get(id: 1)
        _ = try await items.scan()
        _ = try await items.byIndex("byBody", equals: "a")
        try await items.delete(id: 1)
        let m = await store.metricsSnapshot()
        XCTAssertEqual(m.puts, 1)
        XCTAssertEqual(m.gets, 2)
        XCTAssertEqual(m.scans, 1)
        XCTAssertEqual(m.indexLookups, 1)
        XCTAssertEqual(m.deletes, 1)
        XCTAssertEqual(m.batches, 0)
    }

    func test_batch_operation_counters() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let items = await store.collection("items", of: Item.self)
        try await items.batch([
            .put(.init(id: 1, body: "a")),
            .put(.init(id: 2, body: "b")),
            .delete(1)
        ])
        let m = await store.metricsSnapshot()
        XCTAssertEqual(m.batches, 1)
        XCTAssertEqual(m.puts, 2)
        XCTAssertEqual(m.deletes, 1)
    }
}
