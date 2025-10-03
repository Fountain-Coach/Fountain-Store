import XCTest
@testable import FountainStore

final class MultiCollectionBatchTests: XCTestCase {
    struct A: Codable, Identifiable, Equatable { var id: Int; var val: String }
    struct B: Codable, Identifiable, Equatable { var id: String; var n: Int }

    func test_store_level_batch_commits_across_collections() async throws {
        let (store, _) = try await makeTempStore()
        let ca = await store.collection("A", of: A.self)
        let cb = await store.collection("B", of: B.self)

        let ops: [FountainStore.StoreOp] = [
            try ca.makeStoreOpPut(.init(id: 1, val: "x")),
            try cb.makeStoreOpPut(.init(id: "k", n: 7)),
        ]
        try await store.batch(ops)

        let a = try await ca.get(id: 1)
        let b = try await cb.get(id: "k")
        XCTAssertEqual(a, A(id: 1, val: "x"))
        XCTAssertEqual(b, B(id: "k", n: 7))
    }
}
