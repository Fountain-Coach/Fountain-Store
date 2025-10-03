import XCTest
@testable import FountainStore

final class IndexPersistenceTests: XCTestCase {
    struct User: Codable, Identifiable, Equatable { var id: Int; var email: String }
    struct Doc: Codable, Identifiable, Equatable { var id: Int; var tag: String }

    func test_index_definitions_persist_in_manifest() async throws {
        let (store, dir) = try await makeTempStore()
        defer { try? FileManager.default.removeItem(at: dir) }
        let users = await store.collection("users", of: User.self)
        try await users.define(.init(name: "byEmail", kind: .unique(\User.email)))
        let docs = await store.collection("docs", of: Doc.self)
        try await docs.define(.init(name: "byTag", kind: .multi(\Doc.tag)))

        let listUsers = try await store.listIndexDefinitions("users")
        let listDocs = try await store.listIndexDefinitions("docs")
        XCTAssertTrue(listUsers.contains(where: { $0.name == "byEmail" && $0.kind == "unique" }))
        XCTAssertTrue(listDocs.contains(where: { $0.name == "byTag" && $0.kind == "multi" }))

        // Re-open and verify catalog remains.
        let reopened = try await reopenStore(at: dir)
        let ru = try await reopened.listIndexDefinitions("users")
        XCTAssertTrue(ru.contains(where: { $0.name == "byEmail" && $0.kind == "unique" }))
    }
}

