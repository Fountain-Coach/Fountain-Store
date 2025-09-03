import XCTest
import Foundation
@testable import FountainStore

final class LoggingTests: XCTestCase {
    struct Item: Codable, Identifiable, Equatable {
        var id: Int
        var body: String
    }

    final class Sink: @unchecked Sendable {
        private var events: [LogEvent] = []
        private let lock = NSLock()
        func append(_ e: LogEvent) {
            lock.lock(); defer { lock.unlock() }
            events.append(e)
        }
        func snapshot() -> [LogEvent] {
            lock.lock(); defer { lock.unlock() }
            return events
        }
    }

    func test_operation_logs() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let sink = Sink()
        let store = try await FountainStore.open(.init(path: tmp, logger: { sink.append($0) }))
        let items = await store.collection("items", of: Item.self)
        try await items.define(.init(name: "byBody", kind: .unique(\Item.body)))
        try await items.put(.init(id: 1, body: "a"))
        _ = try await items.get(id: 1)
        _ = try await items.byIndex("byBody", equals: "a")
        _ = try await items.scanIndex("byBody", prefix: "a")
        _ = try await items.scan()
        _ = try await items.history(id: 1)
        try await items.delete(id: 1)
        try await items.batch([.put(.init(id: 2, body: "b")), .delete(2)])
        let events = sink.snapshot()
        let expected: [LogEvent] = [
            .put(collection: "items"),
            .get(collection: "items"),
            .indexLookup(collection: "items", index: "byBody"),
            .get(collection: "items"),
            .indexLookup(collection: "items", index: "byBody"),
            .get(collection: "items"),
            .scan(collection: "items"),
            .history(collection: "items"),
            .delete(collection: "items"),
            .batch(collection: "items", count: 2),
            .put(collection: "items"),
            .delete(collection: "items"),
        ]
        XCTAssertEqual(events, expected)
    }

    func test_log_event_codable_roundtrip() throws {
        let event: LogEvent = .history(collection: "c")
        let data = try JSONEncoder().encode(event)
        let decoded = try JSONDecoder().decode(LogEvent.self, from: data)
        XCTAssertEqual(decoded, event)
    }
}
