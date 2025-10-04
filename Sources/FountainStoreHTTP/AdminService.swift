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
    public func underlyingStore() -> FountainStore { store }

    // MARK: - Health / Status / Metrics
    public struct Health: Codable, Sendable { public let status: String; public let sequence: UInt64 }
    public func health() async -> Health {
        let seq = await store.snapshot().sequence
        return Health(status: "ok", sequence: seq)
    }

    public struct CollectionRef: Codable, Sendable { public let name: String; public let recordsApprox: Int }
    public struct StoreStatus: Codable, Sendable { public let sequence: UInt64; public let collectionsCount: Int; public let collections: [CollectionRef] }
    public func status() async -> StoreStatus {
        let seq = await store.snapshot().sequence
        let names = await store.listCollections()
        let cols = names.map { CollectionRef(name: $0, recordsApprox: 0) }
        return StoreStatus(sequence: seq, collectionsCount: cols.count, collections: cols)
    }

    public func metrics() async -> Metrics { await store.metricsSnapshot() }

    // MARK: - Collections
    public func listCollections() async -> [String] { await store.listCollections() }
    public func createCollection(_ name: String) async -> String {
        _ = await store.collection(name, of: HTTPDoc.self)
        return name
    }
    public func dropCollection(_ name: String) async throws {
        // Best effort: delete all HTTPDoc records in this collection, then drop from catalog.
        let coll = await store.collection(name, of: HTTPDoc.self)
        // Attempt to grab a large batch to cover all items.
        if let all = try? await coll.scan(prefix: nil, limit: Int.max, snapshot: nil) {
            for doc in all {
                try? await coll.delete(id: doc.id)
            }
        }
        try await store.dropCollection(name)
    }

    // MARK: - Indexes
    public struct IndexDefinition: Codable, Sendable, Equatable {
        public let name: String
        public let kind: String // unique | multi
        public let keyPath: String
        public var options: [String: AnyJSON]?
        public init(name: String, kind: String, keyPath: String, options: [String: AnyJSON]? = nil) {
            self.name = name
            self.kind = kind
            self.keyPath = keyPath
            self.options = options
        }
    }

    public func listIndexNames(_ collection: String) async -> [String] {
        (try? await store.listIndexDefinitions(collection).map { $0.name }) ?? []
    }
    public func listIndexDefinitions(_ collection: String) async -> [IndexDefinition] {
        let defs = (try? await store.listIndexDefinitions(collection)) ?? []
        return defs.map { IndexDefinition(name: $0.name, kind: $0.kind, keyPath: $0.field ?? $0.name) }
    }

    public func defineIndex(collection: String, def: IndexDefinition) async throws -> IndexDefinition {
        let coll = await store.collection(collection, of: HTTPDoc.self)
        // Support dynamic JSON extraction for multi via closure; unique falls back to multi.
        let extractor: @Sendable (HTTPDoc) -> [String] = { doc in
            return AdminService.extractStrings(from: doc, by: def.keyPath)
        }
        // Core store lacks dynamic unique; use multiValues for both and persist whatever core records
        try await coll.define(.init(name: def.name, kind: .multiValues(extractor)))
        _ = collection // silence unused when compiled without extra uses
        return def
    }

    private static func extractStrings(from doc: HTTPDoc, by keyPath: String) -> [String] {
        // Supports simple paths like ".field" and ".nested.field" and array ".tags[]"
        func descend(_ value: AnyJSON, components: [String]) -> [AnyJSON] {
            guard !components.isEmpty else { return [value] }
            var comps = components
            let head = comps.removeFirst()
            let isArray = head.hasSuffix("[]")
            let key = isArray ? String(head.dropLast(2)) : head
            switch value {
            case .object(let obj):
                guard let next = obj[key] else { return [] }
                if isArray, case .array(let arr) = next {
                    return arr.flatMap { descend($0, components: comps) }
                } else {
                    return descend(next, components: comps)
                }
            default:
                return []
            }
        }
        // Remove leading "." and split by "."
        var path = keyPath
        if path.hasPrefix(".") { path.removeFirst() }
        if path == "id" { return [doc.id] }
        if path == "version" { return doc.version.map { [$0] } ?? [] }
        let parts = path.isEmpty ? [] : path.split(separator: ".").map(String.init)
        let hits = descend(doc.data, components: parts)
        var out: [String] = []
        for h in hits {
            if case .string(let s) = h { out.append(s) }
        }
        return out
    }

    // MARK: - Records
    public func putRecord(collection: String, id: String, data: AnyJSON) async throws -> HTTPDoc {
        let coll = await store.collection(collection, of: HTTPDoc.self)
        let doc = HTTPDoc(id: id, data: data)
        try await coll.put(doc)
        return doc
    }
    public func getRecord(collection: String, id: String, snapshotId: String? = nil) async throws -> HTTPDoc? {
        let coll = await store.collection(collection, of: HTTPDoc.self)
        let snap = snapshotId.flatMap { snapshots[$0] }
        return try await coll.get(id: id, snapshot: snap)
    }
    public func deleteRecord(collection: String, id: String) async throws {
        let coll = await store.collection(collection, of: HTTPDoc.self)
        try await coll.delete(id: id)
    }

    // MARK: - Queries
    public enum Query: Codable, Sendable, Equatable {
        case byId(id: String)
        case indexEquals(index: String, key: String, pageSize: Int?, pageToken: String?)
        case scan(prefix: String?, startAfter: String?, limit: Int?)

        private enum CodingKeys: String, CodingKey { case type, id, index, key, pageSize, pageToken, prefix, startAfter, limit }
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            let type = try c.decode(String.self, forKey: .type)
            switch type {
            case "byId":
                let id = try c.decode(String.self, forKey: .id)
                self = .byId(id: id)
            case "indexEquals":
                let index = try c.decode(String.self, forKey: .index)
                let key = try c.decode(String.self, forKey: .key)
                let ps = try c.decodeIfPresent(Int.self, forKey: .pageSize)
                let pt = try c.decodeIfPresent(String.self, forKey: .pageToken)
                self = .indexEquals(index: index, key: key, pageSize: ps, pageToken: pt)
            case "scan":
                let prefix = try c.decodeIfPresent(String.self, forKey: .prefix)
                let startAfter = try c.decodeIfPresent(String.self, forKey: .startAfter)
                let limit = try c.decodeIfPresent(Int.self, forKey: .limit)
                self = .scan(prefix: prefix, startAfter: startAfter, limit: limit)
            default:
                throw DecodingError.dataCorruptedError(forKey: .type, in: c, debugDescription: "unknown query type")
            }
        }
        public func encode(to encoder: Encoder) throws {
            var c = encoder.container(keyedBy: CodingKeys.self)
            switch self {
            case .byId(let id):
                try c.encode("byId", forKey: .type)
                try c.encode(id, forKey: .id)
            case .indexEquals(let index, let key, let ps, let pt):
                try c.encode("indexEquals", forKey: .type)
                try c.encode(index, forKey: .index)
                try c.encode(key, forKey: .key)
                try c.encodeIfPresent(ps, forKey: .pageSize)
                try c.encodeIfPresent(pt, forKey: .pageToken)
            case .scan(let prefix, let startAfter, let limit):
                try c.encode("scan", forKey: .type)
                try c.encodeIfPresent(prefix, forKey: .prefix)
                try c.encodeIfPresent(startAfter, forKey: .startAfter)
                try c.encodeIfPresent(limit, forKey: .limit)
            }
        }
    }
    public struct QueryResponse: Codable, Sendable, Equatable { public let items: [HTTPDoc]; public let nextPageToken: String? }

    public func query(collection: String, query: Query, snapshotId: String?) async throws -> QueryResponse {
        let coll = await store.collection(collection, of: HTTPDoc.self)
        let snap = snapshotId.flatMap { snapshots[$0] }
        switch query {
        case .byId(let id):
            if let v = try await coll.get(id: id, snapshot: snap) { return QueryResponse(items: [v], nextPageToken: nil) }
            return QueryResponse(items: [], nextPageToken: nil)
        case .indexEquals(let index, let key, let pageSize, let pageToken):
            var items = try await coll.byIndex(index, equals: key, snapshot: snap)
            // Ensure a stable order by id to paginate deterministically
            items.sort { $0.id < $1.id }
            let limit = max(1, (pageSize ?? 100))
            let startIndex: Int
            if let token = pageToken, let idx = items.firstIndex(where: { $0.id > token }) {
                startIndex = idx
            } else {
                startIndex = 0
            }
            let endIndex = min(startIndex + limit, items.count)
            let pageItems = (startIndex < endIndex) ? Array(items[startIndex..<endIndex]) : []
            let hasMore = endIndex < items.count
            let next = hasMore ? pageItems.last?.id : nil
            return QueryResponse(items: pageItems, nextPageToken: next)
        case .scan(let prefix, let startAfter, let limit):
            var prefData: Data? = nil
            if let p = prefix { prefData = try? JSONEncoder().encode(p) }
            // Fetch full set under prefix, then apply startAfter + limit. Acceptable for modest datasets.
            var items = try await coll.scan(prefix: prefData, limit: nil, snapshot: snap)
            items.sort { $0.id < $1.id }
            let startIdx: Int
            if let sa = startAfter, let idx = items.firstIndex(where: { $0.id > sa }) {
                startIdx = idx
            } else {
                startIdx = 0
            }
            let pageLimit = max(1, (limit ?? 100))
            let endIdx = min(startIdx + pageLimit, items.count)
            let pageItems = (startIdx < endIdx) ? Array(items[startIdx..<endIdx]) : []
            let hasMore = endIdx < items.count
            let next = hasMore ? pageItems.last?.id : nil
            return QueryResponse(items: pageItems, nextPageToken: next)
        }
    }

    // MARK: - Transactions
    public enum Operation: Codable, Sendable, Equatable {
        case put(collection: String, record: HTTPDoc)
        case delete(collection: String, id: String)
        case defineIndex(collection: String, index: IndexDefinition)

        private enum CodingKeys: String, CodingKey { case op, collection, record, id, index }
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            let type = try c.decode(String.self, forKey: .op)
            switch type {
            case "put":
                let coll = try c.decode(String.self, forKey: .collection)
                let rec = try c.decode(HTTPDoc.self, forKey: .record)
                self = .put(collection: coll, record: rec)
            case "delete":
                let coll = try c.decode(String.self, forKey: .collection)
                let id = try c.decode(String.self, forKey: .id)
                self = .delete(collection: coll, id: id)
            case "defineIndex":
                let coll = try c.decode(String.self, forKey: .collection)
                let idx = try c.decode(IndexDefinition.self, forKey: .index)
                self = .defineIndex(collection: coll, index: idx)
            default:
                throw DecodingError.dataCorruptedError(forKey: .op, in: c, debugDescription: "unknown op")
            }
        }
        public func encode(to encoder: Encoder) throws {
            var c = encoder.container(keyedBy: CodingKeys.self)
            switch self {
            case .put(let collection, let record):
                try c.encode("put", forKey: .op)
                try c.encode(collection, forKey: .collection)
                try c.encode(record, forKey: .record)
            case .delete(let collection, let id):
                try c.encode("delete", forKey: .op)
                try c.encode(collection, forKey: .collection)
                try c.encode(id, forKey: .id)
            case .defineIndex(let collection, let index):
                try c.encode("defineIndex", forKey: .op)
                try c.encode(collection, forKey: .collection)
                try c.encode(index, forKey: .index)
            }
        }
    }
    public struct Transaction: Codable, Sendable, Equatable {
        public let operations: [Operation]
        public let requireSequenceAtLeast: UInt64?
        public init(operations: [Operation], requireSequenceAtLeast: UInt64? = nil) {
            self.operations = operations
            self.requireSequenceAtLeast = requireSequenceAtLeast
        }
    }
    public struct TransactionResult: Codable, Sendable, Equatable {
        public struct OpResult: Codable, Sendable, Equatable {
            public let opIndex: Int
            public let status: String // ok | error
            public let record: HTTPDoc?
            public let error: Problem?
        }
        public let committedSequence: UInt64
        public let results: [OpResult]
    }

    public struct Problem: Codable, Sendable, Error, Equatable {
        public let type: String
        public let title: String
        public let status: Int
        public let detail: String?
        public let instance: String?
        public init(type: String = "about:blank", title: String, status: Int, detail: String? = nil, instance: String? = nil) {
            self.type = type
            self.title = title
            self.status = status
            self.detail = detail
            self.instance = instance
        }
    }

    public func commitTransaction(_ tx: Transaction) async -> TransactionResult {
        var storeOps: [FountainStore.StoreOp] = []
        var results: [TransactionResult.OpResult] = []
        // Build store ops and also perform index definition side-effects immediately.
        for (i, op) in tx.operations.enumerated() {
            switch op {
            case .put(let collection, let record):
                do {
                    let coll = await store.collection(collection, of: HTTPDoc.self)
                    let op = try coll.makeStoreOpPut(record)
                    storeOps.append(op)
                    results.append(.init(opIndex: i, status: "ok", record: record, error: nil))
                } catch {
                    results.append(.init(opIndex: i, status: "error", record: nil, error: Problem(title: "encode", status: 500, detail: "failed to encode record", instance: nil)))
                }
            case .delete(let collection, let id):
                do {
                    let coll = await store.collection(collection, of: HTTPDoc.self)
                    let op = try coll.makeStoreOpDelete(id)
                    storeOps.append(op)
                    results.append(.init(opIndex: i, status: "ok", record: nil, error: nil))
                } catch {
                    results.append(.init(opIndex: i, status: "error", record: nil, error: Problem(title: "encode", status: 500, detail: "failed to encode id", instance: nil)))
                }
            case .defineIndex(let collection, let index):
                do {
                    _ = try await defineIndex(collection: collection, def: index)
                    results.append(.init(opIndex: i, status: "ok", record: nil, error: nil))
                } catch {
                    results.append(.init(opIndex: i, status: "error", record: nil, error: Problem(title: "defineIndex", status: 500, detail: "failed to define index", instance: nil)))
                }
            }
        }
        // Commit atomically.
        do {
            try await store.batch(storeOps, requireSequenceAtLeast: tx.requireSequenceAtLeast)
        } catch let e as TransactionError {
            switch e {
            case .sequenceTooLow(let req, let cur):
                return TransactionResult(committedSequence: await store.snapshot().sequence, results: [TransactionResult.OpResult(opIndex: -1, status: "error", record: nil, error: Problem(title: "conflict", status: 409, detail: "sequence too low (required: \(req), current: \(cur))", instance: nil))])
            }
        } catch let e as CollectionError {
            switch e {
            case .uniqueConstraintViolation(let index, let key):
                return TransactionResult(committedSequence: await store.snapshot().sequence, results: [TransactionResult.OpResult(opIndex: -1, status: "error", record: nil, error: Problem(title: "unique constraint", status: 409, detail: "\(index) key=\(key)", instance: nil))])
            }
        } catch {
            return TransactionResult(committedSequence: await store.snapshot().sequence, results: [TransactionResult.OpResult(opIndex: -1, status: "error", record: nil, error: Problem(title: "commit", status: 500, detail: "unknown error", instance: nil))])
        }
        return TransactionResult(committedSequence: await store.snapshot().sequence, results: results)
    }

    // MARK: - Snapshots
    private var snapshots: [String: Snapshot] = [:]
    public struct SnapshotInfo: Codable, Sendable, Equatable { public let id: String; public let sequence: UInt64 }
    public func createSnapshot() async -> SnapshotInfo {
        let snap = await store.snapshot()
        let id = UUID().uuidString
        snapshots[id] = snap
        return SnapshotInfo(id: id, sequence: snap.sequence)
    }
    public func releaseSnapshot(_ id: String) async -> Bool {
        return snapshots.removeValue(forKey: id) != nil
    }

    // MARK: - Compaction
    public func compactionStatus() async throws -> FountainStore.CompactionStatus { try await store.compactionStatus() }
    public func compactionTick() async { _ = try? await store.compactionStatus(); /* no-op trigger */ }
}
