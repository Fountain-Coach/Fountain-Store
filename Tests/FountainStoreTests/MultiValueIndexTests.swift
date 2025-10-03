import XCTest
@testable import FountainStore

final class MultiValueIndexTests: XCTestCase {
    struct Doc: Codable, Identifiable, Equatable { var id: Int; var tags: [String] }

    func test_multi_array_keypath_index() async throws {
        let (store, _) = try await makeTempStore()
        let coll = await store.collection("docs", of: Doc.self)
        try await coll.define(.init(name: "byTag", kind: .multi(\Doc.tags)))
        try await coll.put(.init(id: 1, tags: ["a", "b"]))
        var r = try await coll.byIndex("byTag", equals: "a").map { $0.id }
        XCTAssertEqual(r, [1])
        r = try await coll.byIndex("byTag", equals: "b").map { $0.id }
        XCTAssertEqual(r, [1])
        // Update tags to drop "a"
        try await coll.put(.init(id: 1, tags: ["b"]))
        r = try await coll.byIndex("byTag", equals: "a").map { $0.id }
        XCTAssertEqual(r, [])
        r = try await coll.byIndex("byTag", equals: "b").map { $0.id }
        XCTAssertEqual(r, [1])
    }

    func test_multi_values_extractor_index() async throws {
        let (store, _) = try await makeTempStore()
        let coll = await store.collection("docs", of: Doc.self)
        try await coll.define(.init(name: "byTag2", kind: .multiValues { $0.tags }))
        try await coll.put(.init(id: 2, tags: ["x"]))
        let r = try await coll.byIndex("byTag2", equals: "x").map { $0.id }
        XCTAssertEqual(r, [2])
    }
}

