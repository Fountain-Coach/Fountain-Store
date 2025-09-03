import XCTest
@testable import FountainStore

final class ConfigurationTests: XCTestCase {
    func test_default_scan_limit_option() async throws {
        struct Item: Codable, Identifiable { let id: Int }
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp, defaultScanLimit: 5))
        let coll = await store.collection("items", of: Item.self)
        for i in 0..<10 { try await coll.put(Item(id: i)) }
        let res = try await coll.scan()
        XCTAssertEqual(res.count, 5)
    }
}
