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

    func test_store_level_batch_crash_after_fsync_replays_on_start() async throws {
        let (store, dir) = try await makeTempStore()
        let ca = await store.collection("A", of: A.self)
        let cb = await store.collection("B", of: B.self)
        CrashPoints.active = "wal_fsync"
        do {
            let ops: [FountainStore.StoreOp] = [
                try ca.makeStoreOpPut(.init(id: 2, val: "y")),
                try cb.makeStoreOpPut(.init(id: "z", n: 9)),
            ]
            try await store.batch(ops)
            XCTFail("expected crash after fsync")
        } catch {
            // Expected crash simulation
        }
        CrashPoints.active = nil
        let reopened = try await reopenStore(at: dir)
        let ra = await reopened.collection("A", of: A.self)
        let rb = await reopened.collection("B", of: B.self)
        try await Task.sleep(nanoseconds: 1_000_000)
        let va = try await ra.get(id: 2)
        let vb = try await rb.get(id: "z")
        XCTAssertEqual(va, A(id: 2, val: "y"))
        XCTAssertEqual(vb, B(id: "z", n: 9))
    }
}
