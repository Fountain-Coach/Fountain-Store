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
        _ = try await items.scanIndex("byBody", prefix: "a")
        _ = try await items.history(id: 1)
        try await items.delete(id: 1)
        let m = await store.metricsSnapshot()
        XCTAssertEqual(m.puts, 1)
        XCTAssertEqual(m.gets, 3)
        XCTAssertEqual(m.scans, 1)
        XCTAssertEqual(m.indexLookups, 2)
        XCTAssertEqual(m.deletes, 1)
        XCTAssertEqual(m.batches, 0)
        XCTAssertEqual(m.histories, 1)
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

    func test_reset_metrics() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let items = await store.collection("items", of: Item.self)
        try await items.put(.init(id: 1, body: "a"))
        let snap = await store.resetMetrics()
        XCTAssertEqual(snap.puts, 1)
        XCTAssertEqual(snap.histories, 0)
        let m = await store.metricsSnapshot()
        XCTAssertEqual(m.puts, 0)
        XCTAssertEqual(m.gets, 0)
        XCTAssertEqual(m.deletes, 0)
        XCTAssertEqual(m.scans, 0)
        XCTAssertEqual(m.indexLookups, 0)
        XCTAssertEqual(m.batches, 0)
        XCTAssertEqual(m.histories, 0)
    }

    func test_metrics_codable_roundtrip() throws {
        var m = Metrics()
        m.puts = 2
        m.gets = 3
        m.histories = 4
        let data = try JSONEncoder().encode(m)
        let decoded = try JSONDecoder().decode(Metrics.self, from: data)
        XCTAssertEqual(decoded, m)
    }
}
