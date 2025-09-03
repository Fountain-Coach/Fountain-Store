
//
//  Store.swift
//  FountainStore
//
//  Public API surface for the pure‑Swift embedded store.
//

import Foundation

public struct StoreOptions: Sendable {
    public let path: URL
    public let cacheBytes: Int
    public let logger: (@Sendable (LogEvent) -> Void)?
    public let defaultScanLimit: Int
    public init(path: URL, cacheBytes: Int = 64 << 20, logger: (@Sendable (LogEvent) -> Void)? = nil, defaultScanLimit: Int = 100) {
        self.path = path
        self.cacheBytes = cacheBytes
        self.logger = logger
        self.defaultScanLimit = defaultScanLimit
    }
}

public struct Snapshot: Sendable, Hashable {
    public let sequence: UInt64
    public init(sequence: UInt64) { self.sequence = sequence }
}

public struct Metrics: Sendable, Hashable, Codable {
    public var puts: UInt64 = 0
    public var gets: UInt64 = 0
    public var deletes: UInt64 = 0
    public var scans: UInt64 = 0
    public var indexLookups: UInt64 = 0
    public var batches: UInt64 = 0
    public init() {}
}

public enum LogEvent: Sendable, Hashable, Codable {
    case put(collection: String)
    case get(collection: String)
    case delete(collection: String)
    case scan(collection: String)
    case indexLookup(collection: String, index: String)
    case batch(collection: String, count: Int)

    private enum CodingKeys: String, CodingKey {
        case type, collection, index, count
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .put(let collection):
            try container.encode("put", forKey: .type)
            try container.encode(collection, forKey: .collection)
        case .get(let collection):
            try container.encode("get", forKey: .type)
            try container.encode(collection, forKey: .collection)
        case .delete(let collection):
            try container.encode("delete", forKey: .type)
            try container.encode(collection, forKey: .collection)
        case .scan(let collection):
            try container.encode("scan", forKey: .type)
            try container.encode(collection, forKey: .collection)
        case .indexLookup(let collection, let index):
            try container.encode("indexLookup", forKey: .type)
            try container.encode(collection, forKey: .collection)
            try container.encode(index, forKey: .index)
        case .batch(let collection, let count):
            try container.encode("batch", forKey: .type)
            try container.encode(collection, forKey: .collection)
            try container.encode(count, forKey: .count)
        }
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(String.self, forKey: .type)
        switch type {
        case "put":
            let collection = try container.decode(String.self, forKey: .collection)
            self = .put(collection: collection)
        case "get":
            let collection = try container.decode(String.self, forKey: .collection)
            self = .get(collection: collection)
        case "delete":
            let collection = try container.decode(String.self, forKey: .collection)
            self = .delete(collection: collection)
        case "scan":
            let collection = try container.decode(String.self, forKey: .collection)
            self = .scan(collection: collection)
        case "indexLookup":
            let collection = try container.decode(String.self, forKey: .collection)
            let index = try container.decode(String.self, forKey: .index)
            self = .indexLookup(collection: collection, index: index)
        case "batch":
            let collection = try container.decode(String.self, forKey: .collection)
            let count = try container.decode(Int.self, forKey: .count)
            self = .batch(collection: collection, count: count)
        default:
            let context = DecodingError.Context(codingPath: [CodingKeys.type], debugDescription: "Unknown log event type: \(type)")
            throw DecodingError.dataCorrupted(context)
        }
    }
}

public enum CollectionError: Error, Sendable {
    case uniqueConstraintViolation(index: String, key: String)
}

public enum TransactionError: Error, Sendable {
    case sequenceTooLow(required: UInt64, current: UInt64)
}

public struct Index<C>: Sendable {
    public enum Kind: @unchecked Sendable {
        case unique(PartialKeyPath<C>)
        case multi(PartialKeyPath<C>)
    }
    public let name: String
    public let kind: Kind
    public init(name: String, kind: Kind) {
        self.name = name
        self.kind = kind
    }
}

public struct Transaction: Sendable {
    // Marker type for now; expanded at M3.
    public init() {}
}

public actor FountainStore {
    public static func open(_ opts: StoreOptions) async throws -> FountainStore {
        return FountainStore(options: opts)
    }

    public func snapshot() -> Snapshot {
        return Snapshot(sequence: sequence)
    }

    public func collection<C: Codable & Identifiable>(_ name: String, of: C.Type) -> Collection<C> {
        return Collection<C>(name: name, store: self)
    }

    // MARK: - Internals
    private let options: StoreOptions
    private var sequence: UInt64 = 0
    private var metrics = Metrics()

    fileprivate func nextSequence() -> UInt64 {
        allocateSequences(1)
    }

    fileprivate func allocateSequences(_ count: Int) -> UInt64 {
        let start = sequence &+ 1
        sequence &+= UInt64(count)
        return start
    }

    public func metricsSnapshot() -> Metrics {
        metrics
    }

    public func resetMetrics() -> Metrics {
        let snap = metrics
        metrics = Metrics()
        return snap
    }

    internal func defaultScanLimit() -> Int {
        options.defaultScanLimit
    }

    internal enum Metric {
        case put, get, delete, scan, indexLookup, batch
    }

    internal func record(_ metric: Metric, _ count: UInt64 = 1) {
        switch metric {
        case .put:
            metrics.puts &+= count
        case .get:
            metrics.gets &+= count
        case .delete:
            metrics.deletes &+= count
        case .scan:
            metrics.scans &+= count
        case .indexLookup:
            metrics.indexLookups &+= count
        case .batch:
            metrics.batches &+= count
        }
    }

    internal func log(_ event: LogEvent) {
        options.logger?(event)
    }

    private init(options: StoreOptions) { self.options = options }
}

public actor Collection<C: Codable & Identifiable> where C.ID: Codable & Hashable {
    public let name: String
    private let store: FountainStore
    private var data: [C.ID: [(UInt64, C?)]] = [:]
    
    private enum IndexStorage {
        final class Unique {
            let keyPath: KeyPath<C, String>
            var map: [String: [(UInt64, C.ID?)]] = [:]
            init(keyPath: KeyPath<C, String>) { self.keyPath = keyPath }
        }
        final class Multi {
            let keyPath: KeyPath<C, String>
            var map: [String: [(UInt64, [C.ID])]] = [:]
            init(keyPath: KeyPath<C, String>) { self.keyPath = keyPath }
        }
        case unique(Unique)
        case multi(Multi)
    }
    private var indexes: [String: IndexStorage] = [:]

    public init(name: String, store: FountainStore) {
        self.name = name
        self.store = store
    }

    public enum BatchOp {
        case put(C)
        case delete(C.ID)
    }

    public func define(_ index: Index<C>) async throws {
        switch index.kind {
        case .unique(let path):
            guard let kp = path as? KeyPath<C, String> else { return }
            let idx = IndexStorage.Unique(keyPath: kp)
            for (id, versions) in data {
                guard let (seq, val) = versions.last, let v = val else { continue }
                let key = v[keyPath: kp]
                idx.map[key, default: []].append((seq, id))
            }
            indexes[index.name] = .unique(idx)
        case .multi(let path):
            guard let kp = path as? KeyPath<C, String> else { return }
            let idx = IndexStorage.Multi(keyPath: kp)
            for (id, versions) in data {
                guard let (seq, val) = versions.last, let v = val else { continue }
                let key = v[keyPath: kp]
                var arr = idx.map[key]?.last?.1 ?? []
                arr.append(id)
                idx.map[key, default: []].append((seq, arr))
            }
            indexes[index.name] = .multi(idx)
        }
    }

    public func batch(_ ops: [BatchOp], requireSequenceAtLeast: UInt64? = nil) async throws {
        guard !ops.isEmpty else { return }
        if let req = requireSequenceAtLeast {
            let current = await store.snapshot().sequence
            guard current >= req else {
                throw TransactionError.sequenceTooLow(required: req, current: current)
            }
        }
        await store.record(.batch)
        await store.log(.batch(collection: name, count: ops.count))
        let start = await store.allocateSequences(ops.count)
        var seq = start
        for op in ops {
            switch op {
            case .put(let v):
                try await put(v, sequence: seq)
            case .delete(let id):
                try await delete(id: id, sequence: seq)
            }
            seq &+= 1
        }
    }

    public func put(_ value: C, sequence: UInt64? = nil) async throws {
        await store.record(.put)
        await store.log(.put(collection: name))
        let seq: UInt64
        if let s = sequence {
            seq = s
        } else {
            seq = await store.nextSequence()
        }
        let old = data[value.id]?.last?.1
        for (name, storage) in indexes {
            switch storage {
            case .unique(let idx):
                let key = value[keyPath: idx.keyPath]
                if let existing = idx.map[key]?.last?.1, existing != value.id {
                    throw CollectionError.uniqueConstraintViolation(index: name, key: key)
                }
            case .multi:
                continue
            }
        }
        data[value.id, default: []].append((seq, value))
        for storage in indexes.values {
            switch storage {
            case .unique(let idx):
                let key = value[keyPath: idx.keyPath]
                if let old = old {
                    let oldKey = old[keyPath: idx.keyPath]
                    if oldKey != key {
                        idx.map[oldKey, default: []].append((seq, nil))
                    }
                }
                idx.map[key, default: []].append((seq, value.id))
            case .multi(let idx):
                let key = value[keyPath: idx.keyPath]
                if let old = old {
                    let oldKey = old[keyPath: idx.keyPath]
                    if oldKey != key {
                        var oldArr = idx.map[oldKey]?.last?.1 ?? []
                        if let pos = oldArr.firstIndex(of: value.id) { oldArr.remove(at: pos) }
                        idx.map[oldKey, default: []].append((seq, oldArr))
                    }
                }
                var arr = idx.map[key]?.last?.1 ?? []
                if !arr.contains(value.id) { arr.append(value.id) }
                idx.map[key, default: []].append((seq, arr))
            }
        }
    }

    public func get(id: C.ID, snapshot: Snapshot? = nil) async throws -> C? {
        await store.record(.get)
        await store.log(.get(collection: name))
        guard let versions = data[id] else { return nil }
        let limit = snapshot?.sequence ?? UInt64.max
        return versions.last(where: { $0.0 <= limit })?.1
    }

    public func history(id: C.ID, snapshot: Snapshot? = nil) async throws -> [(UInt64, C?)] {
        guard let versions = data[id] else { return [] }
        let limit = snapshot?.sequence ?? UInt64.max
        return versions.filter { $0.0 <= limit }
    }

    public func delete(id: C.ID, sequence: UInt64? = nil) async throws {
        await store.record(.delete)
        await store.log(.delete(collection: name))
        let seq: UInt64
        if let s = sequence {
            seq = s
        } else {
            seq = await store.nextSequence()
        }
        let old = data[id]?.last?.1
        data[id, default: []].append((seq, nil))
        guard let oldVal = old else { return }
        for storage in indexes.values {
            switch storage {
            case .unique(let idx):
                let key = oldVal[keyPath: idx.keyPath]
                idx.map[key, default: []].append((seq, nil))
            case .multi(let idx):
                let key = oldVal[keyPath: idx.keyPath]
                var arr = idx.map[key]?.last?.1 ?? []
                if let pos = arr.firstIndex(of: id) { arr.remove(at: pos) }
                idx.map[key, default: []].append((seq, arr))
            }
        }
    }

    public func byIndex(_ name: String, equals key: String, snapshot: Snapshot? = nil) async throws -> [C] {
        await store.record(.indexLookup)
        await store.log(.indexLookup(collection: self.name, index: name))
        guard let storage = indexes[name] else { return [] }
        let limit = snapshot?.sequence ?? UInt64.max
        switch storage {
        case .unique(let idx):
            guard let versions = idx.map[key],
                  let id = versions.last(where: { $0.0 <= limit })?.1 else { return [] }
            if let val = try await get(id: id, snapshot: snapshot) { return [val] }
            return []
        case .multi(let idx):
            guard let versions = idx.map[key],
                  let ids = versions.last(where: { $0.0 <= limit })?.1 else { return [] }
            var res: [C] = []
            for id in ids {
                if let val = try await get(id: id, snapshot: snapshot) { res.append(val) }
            }
            return res
        }
    }

    public func scanIndex(_ name: String, prefix: String, limit: Int? = nil, snapshot: Snapshot? = nil) async throws -> [C] {
        await store.record(.indexLookup)
        await store.log(.indexLookup(collection: self.name, index: name))
        guard let storage = indexes[name] else { return [] }
        let seqLimit = snapshot?.sequence ?? UInt64.max
        let maxItems: Int
        if let limit = limit {
            maxItems = limit
        } else {
            maxItems = await store.defaultScanLimit()
        }
        var items: [(String, C)] = []
        switch storage {
        case .unique(let idx):
            for (key, versions) in idx.map {
                guard key.hasPrefix(prefix),
                      let id = versions.last(where: { $0.0 <= seqLimit })?.1,
                      let val = try await get(id: id, snapshot: snapshot) else { continue }
                items.append((key, val))
            }
        case .multi(let idx):
            let encoder = JSONEncoder()
            for (key, versions) in idx.map {
                guard key.hasPrefix(prefix),
                      let ids = versions.last(where: { $0.0 <= seqLimit })?.1 else { continue }
                var pairs: [(Data, C)] = []
                for id in ids {
                    if let val = try await get(id: id, snapshot: snapshot) {
                        let data = try encoder.encode(id)
                        pairs.append((data, val))
                    }
                }
                pairs.sort { $0.0.lexicographicallyPrecedes($1.0) }
                for (_, val) in pairs { items.append((key, val)) }
            }
        }
        items.sort { $0.0 < $1.0 }
        return items.prefix(maxItems).map { $0.1 }
    }

    public func scan(prefix: Data? = nil, limit: Int? = nil, snapshot: Snapshot? = nil) async throws -> [C] {
        await store.record(.scan)
        await store.log(.scan(collection: name))
        // Collect latest visible version for each key and filter by prefix.
        let encoder = JSONEncoder()
        let seqLimit = snapshot?.sequence ?? UInt64.max
        let maxItems: Int
        if let limit = limit {
            maxItems = limit
        } else {
            maxItems = await store.defaultScanLimit()
        }
        var items: [(Data, C)] = []

        for (id, versions) in data {
            guard let hit = versions.last(where: { $0.0 <= seqLimit }),
                  let value = hit.1 else { continue }
            let keyData = try encoder.encode(id)
            if let p = prefix, !keyData.starts(with: p) { continue }
            items.append((keyData, value))
        }

        items.sort { $0.0.lexicographicallyPrecedes($1.0) }
        return items.prefix(maxItems).map { $0.1 }
    }
}
