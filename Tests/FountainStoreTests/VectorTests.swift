import XCTest
@testable import FountainVector

final class VectorTests: XCTestCase {
    func test_add_and_search() {
        var idx = HNSWIndex()
        idx.add(id: "a", vector: [0.0, 0.0])
        idx.add(id: "b", vector: [1.0, 1.0])
        idx.add(id: "c", vector: [0.5, 0.5])
        let res = idx.search([0.6, 0.6], k: 2)
        XCTAssertEqual(res, ["c", "b"])
    }

    func test_remove_vector() {
        var idx = HNSWIndex()
        idx.add(id: "a", vector: [0.0, 0.0])
        idx.add(id: "b", vector: [1.0, 1.0])
        idx.remove(id: "a")
        let res = idx.search([0.0, 0.0], k: 1)
        XCTAssertEqual(res, ["b"])
    }
}
