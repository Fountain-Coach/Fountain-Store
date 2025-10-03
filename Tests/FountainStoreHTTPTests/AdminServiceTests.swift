import XCTest
@testable import FountainStoreHTTP
@testable import FountainStore
import Foundation
import FountainStore

final class AdminServiceTests: XCTestCase {
    func test_health_status_and_crud() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let store = try await FountainStore.open(.init(path: dir))
        let admin = await AdminService(store: store)
        let h = await admin.health()
        XCTAssertEqual(h.status, "ok")

        let name = await admin.createCollection("docs")
        XCTAssertEqual(name, "docs")
        _ = await admin.listCollections()

        let doc = try await admin.putRecord(collection: "docs", id: "1", data: AnyJSON.object(["a": AnyJSON.string("b")]))
        XCTAssertEqual(doc.id, "1")
        let got = try await admin.getRecord(collection: "docs", id: "1")
        XCTAssertEqual(got?.id, "1")
        try await admin.deleteRecord(collection: "docs", id: "1")
        let gone = try await admin.getRecord(collection: "docs", id: "1")
        XCTAssertNil(gone)
        let st = await admin.status()
        XCTAssertGreaterThanOrEqual(st.collectionsCount, 1)
    }
}
