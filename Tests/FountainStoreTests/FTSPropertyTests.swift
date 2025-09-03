import XCTest
@testable import FountainFTS

final class FTSPropertyTests: XCTestCase {
    func test_term_lookup_matches_documents() {
        var idx = FTSIndex()
        let docs = [
            "swift storage engine",
            "vector search index",
            "swift swift replication"
        ]
        for (i, text) in docs.enumerated() {
            idx.add(docID: "\(i)", text: text)
        }
        for (i, text) in docs.enumerated() {
            let tokens = Set(text.split(separator: " ").map(String.init))
            for t in tokens {
                XCTAssertTrue(idx.search(t).contains("\(i)"))
            }
        }
    }
}
