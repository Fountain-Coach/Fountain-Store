import Foundation
import FountainStore

public struct HTTPDoc: Codable, Identifiable, Equatable, Sendable {
    public var id: String
    public var data: AnyJSON
    public var version: String?
    public init(id: String, data: AnyJSON, version: String? = nil) { self.id = id; self.data = data; self.version = version }
}

public actor AdminService {
    private let store: FountainStore
    public init(store: FountainStore) { self.store = store }

    // Health
    public struct Health: Codable, Sendable { public let status: String; public let sequence: UInt64 }
    public func health() async -> Health {
        let seq = await store.snapshot().sequence
        return Health(status: "ok", sequence: seq)
    }

    // Status
    public struct CollectionRef: Codable, Sendable { public let name: String; public let recordsApprox: Int }
    public struct StoreStatus: Codable, Sendable { public let sequence: UInt64; public let collectionsCount: Int; public let collections: [CollectionRef] }
    public func status() async -> StoreStatus {
        let seq = await store.snapshot().sequence
        let names = await store.listCollections()
        let cols = names.map { CollectionRef(name: $0, recordsApprox: 0) }
        return StoreStatus(sequence: seq, collectionsCount: cols.count, collections: cols)
    }

    // Collections
    public func listCollections() async -> [String] { await store.listCollections() }
    public func createCollection(_ name: String) async -> String {
        _ = await store.collection(name, of: HTTPDoc.self)
        return name
    }

    // Records
    public func putRecord(collection: String, id: String, data: AnyJSON) async throws -> HTTPDoc {
        let coll = await store.collection(collection, of: HTTPDoc.self)
        let doc = HTTPDoc(id: id, data: data)
        try await coll.put(doc)
        return doc
    }
    public func getRecord(collection: String, id: String) async throws -> HTTPDoc? {
        let coll = await store.collection(collection, of: HTTPDoc.self)
        return try await coll.get(id: id)
    }
    public func deleteRecord(collection: String, id: String) async throws {
        let coll = await store.collection(collection, of: HTTPDoc.self)
        try await coll.delete(id: id)
    }

    // Metrics
    public func metrics() async -> Metrics { await store.metricsSnapshot() }

    // Compaction
    public func compactionStatus() async throws -> FountainStore.CompactionStatus { try await store.compactionStatus() }
    public func compactionTick() async { _ = try? await store.compactionStatus(); /* no-op trigger */ }
}
