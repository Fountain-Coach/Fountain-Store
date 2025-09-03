import XCTest
@testable import FountainStore

final class OptionalModulesIntegrationTests: XCTestCase {
    struct TextDoc: Codable, Identifiable, Equatable {
        var id: Int
        var text: String
    }

    struct VecDoc: Codable, Identifiable, Equatable {
        var id: String
        var embedding: [Double]
    }

    func test_fts_index_search() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let docs = await store.collection("docs", of: TextDoc.self)
        try await docs.define(.init(name: "fts", kind: .fts(\TextDoc.text)))
        try await docs.put(.init(id: 1, text: "hello world"))
        try await docs.put(.init(id: 2, text: "swift world"))
        let res = try await docs.searchText("fts", query: "hello").map { $0.id }
        XCTAssertEqual(res, [1])
    }

    func test_vector_index_search() async throws {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(.init(path: tmp))
        let items = await store.collection("items", of: VecDoc.self)
        try await items.define(.init(name: "vec", kind: .vector(\VecDoc.embedding)))
        try await items.put(.init(id: "a", embedding: [0.0, 0.0]))
        try await items.put(.init(id: "b", embedding: [1.0, 1.0]))
        let res = try await items.vectorSearch("vec", query: [0.1, 0.1], k: 1).map { $0.id }
        XCTAssertEqual(res, ["a"])
    }
}
