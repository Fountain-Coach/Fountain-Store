import XCTest
@testable import FountainVector

final class VectorPropertyTests: XCTestCase {
    func test_search_returns_nearest_neighbor() {
        var idx = HNSWIndex()
        let data: [(String, [Double])] = [
            ("a", [0.0, 0.0]),
            ("b", [1.0, 1.0]),
            ("c", [0.2, 0.2])
        ]
        for (id, vec) in data { idx.add(id: id, vector: vec) }
        for (id, vec) in data {
            XCTAssertEqual(idx.search(vec, k: 1).first, id)
        }
    }
}
