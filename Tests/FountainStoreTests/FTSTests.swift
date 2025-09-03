import XCTest
@testable import FountainFTS

final class FTSTests: XCTestCase {
    func test_basic_index_and_search() {
        var idx = FTSIndex()
        idx.add(docID: "1", text: "hello world")
        idx.add(docID: "2", text: "hello swift")
        XCTAssertEqual(Set(idx.search("hello")), ["1", "2"])
        XCTAssertEqual(Set(idx.search("world")), ["1"])
        XCTAssertTrue(idx.search("swift world").isEmpty)
    }

    func test_remove_document() {
        var idx = FTSIndex()
        idx.add(docID: "1", text: "hello world")
        idx.add(docID: "2", text: "hello swift")
        idx.remove(docID: "1")
        XCTAssertEqual(Set(idx.search("hello")), ["2"])
        XCTAssertTrue(idx.search("world").isEmpty)
    }
}
