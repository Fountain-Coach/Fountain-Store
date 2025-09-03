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

    func test_bm25_ranking() {
        var idx = FTSIndex()
        idx.add(docID: "A", text: "swift codex swift")
        idx.add(docID: "B", text: "swift codex")
        let res = idx.search("swift")
        XCTAssertEqual(res, ["A", "B"])
    }

    func test_search_limit() {
        var idx = FTSIndex()
        idx.add(docID: "1", text: "hello world")
        idx.add(docID: "2", text: "hello hello world")
        let res = idx.search("hello", limit: 1)
        XCTAssertEqual(res, ["2"])
    }

    func test_stopword_analyzer() {
        let stopwords: Set<String> = ["the", "and"]
        var idx = FTSIndex(analyzer: FTSIndex.stopwordAnalyzer(stopwords))
        idx.add(docID: "1", text: "the quick brown fox")
        XCTAssertTrue(idx.search("the").isEmpty)
        XCTAssertEqual(Set(idx.search("quick")), ["1"])
    }
}
