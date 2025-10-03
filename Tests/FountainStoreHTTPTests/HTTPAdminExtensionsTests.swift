import XCTest
@testable import FountainStoreHTTP
@testable import FountainStore
import Foundation

final class HTTPAdminExtensionsTests: XCTestCase {
    private func makeStoreAndAdmin() async throws -> (FountainStore, URL, AdminService) {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let store = try await FountainStore.open(.init(path: dir))
        let admin = AdminService(store: store)
        return (store, dir, admin)
    }

    func test_define_index_and_query() async throws {
        let (_, dir, admin) = try await makeStoreAndAdmin()
        defer { try? FileManager.default.removeItem(at: dir) }
        _ = await admin.createCollection("docs")
        // Put two docs with tags
        _ = try await admin.putRecord(collection: "docs", id: "a", data: .object(["tags": .array([.string("x"), .string("y")])]))
        _ = try await admin.putRecord(collection: "docs", id: "b", data: .object(["tags": .array([.string("y")])]))
        // Define multi index on tags[]
        let def = AdminService.IndexDefinition(name: "byTags", kind: "multi", keyPath: ".tags[]")
        _ = try await admin.defineIndex(collection: "docs", def: def)

        // Query by index equals
        let q = AdminService.Query.indexEquals(index: "byTags", key: "y", pageSize: 10, pageToken: nil)
        let res = try await admin.query(collection: "docs", query: q, snapshotId: nil)
        let ids = Set(res.items.map { $0.id })
        XCTAssertEqual(ids, Set(["a", "b"]))
    }

    func test_transactions_across_collections_and_guard() async throws {
        let (store, dir, admin) = try await makeStoreAndAdmin()
        defer { try? FileManager.default.removeItem(at: dir) }
        _ = await admin.createCollection("a")
        _ = await admin.createCollection("b")
        let seq = await store.snapshot().sequence
        let ops: [AdminService.Operation] = [
            .put(collection: "a", record: .init(id: "1", data: .object(["v": .string("a1")])))
            , .put(collection: "b", record: .init(id: "2", data: .object(["v": .string("b2")])))]
        let tx = AdminService.Transaction(operations: ops, requireSequenceAtLeast: seq)
        let res = await admin.commitTransaction(tx)
        XCTAssertGreaterThanOrEqual(res.committedSequence, seq)

        // Guard higher than current should 409
        let bad = AdminService.Transaction(operations: ops, requireSequenceAtLeast: UInt64.max)
        let r2 = await admin.commitTransaction(bad)
        XCTAssertEqual(r2.results.first?.status, "error")
    }

    func test_snapshots_and_query_by_snapshot() async throws {
        let (_, dir, admin) = try await makeStoreAndAdmin()
        defer { try? FileManager.default.removeItem(at: dir) }
        _ = await admin.createCollection("docs")
        _ = try await admin.putRecord(collection: "docs", id: "1", data: .object(["val": .string("v1")]))
        let snap = await admin.createSnapshot()
        _ = try await admin.putRecord(collection: "docs", id: "1", data: .object(["val": .string("v2")]))
        let byId = AdminService.Query.byId(id: "1")
        let rSnap = try await admin.query(collection: "docs", query: byId, snapshotId: snap.id)
        let rNow = try await admin.query(collection: "docs", query: byId, snapshotId: nil)
        XCTAssertEqual(rSnap.items.first?.data, .object(["val": .string("v1")]))
        XCTAssertEqual(rNow.items.first?.data, .object(["val": .string("v2")]))
        let released = await admin.releaseSnapshot(snap.id)
        XCTAssertTrue(released)
    }

    func test_scan_query_by_prefix() async throws {
        let (_, dir, admin) = try await makeStoreAndAdmin()
        defer { try? FileManager.default.removeItem(at: dir) }
        _ = await admin.createCollection("docs")
        for i in 1...5 { _ = try await admin.putRecord(collection: "docs", id: "item_\(i)", data: .object(["i": .number(Double(i))])) }
        let q = AdminService.Query.scan(prefix: "item_1", startAfter: nil, limit: 10)
        let res = try await admin.query(collection: "docs", query: q, snapshotId: nil)
        XCTAssertTrue(res.items.allSatisfy { $0.id.hasPrefix("item_1") })
    }
}

