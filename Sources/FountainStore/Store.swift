
//
//  Store.swift
//  FountainStore
//
//  Public API surface for the pure‑Swift embedded store.
//

import Foundation
import FountainStoreCore
import FountainFTS
import FountainVector

// Crash injection helper used in tests.
internal enum CrashError: Error { case triggered }
internal enum CrashPoints {
    nonisolated(unsafe) static var active: String?
    nonisolated static func hit(_ id: String) throws {
        if active == id { throw CrashError.triggered }
    }
}

// MARK: - Store-level multi-collection batch API

public extension FountainStore {
    enum StoreOp: Sendable {
        case put(collection: String, id: Data, value: Data)
        case delete(collection: String, id: Data)
    }

    func batch(_ ops: [StoreOp], requireSequenceAtLeast: UInt64? = nil) async throws {
        guard !ops.isEmpty else { return }
        await applyBackpressureIfNeeded()
        if let req = requireSequenceAtLeast {
            let current = await snapshot().sequence
            guard current >= req else { throw TransactionError.sequenceTooLow(required: req, current: current) }
        }
        record(.batch)
        // Pre-commit validation: unique constraints per collection across the batch.
        var perCollection: [String: [(Bool, Data, Data?)]] = [:]
        for op in ops {
            switch op {
            case .put(let coll, let idData, let valueData):
                perCollection[coll, default: []].append((true, idData, valueData))
            case .delete(let coll, let idData):
                perCollection[coll, default: []].append((false, idData, nil))
            }
        }
        for (coll, items) in perCollection {
            if let validate = validateHooks[coll] {
                try await validate(items)
            }
        }
        // Allocate sequences for ops in order.
        let start = allocateSequences(ops.count)
        var seq = start
        // Build and append transactional WAL frames.
        let txid = UUID().uuidString
        let begin = WALFrame(type: "begin", txid: txid, key: nil, value: nil)
        try await wal.append(WALRecord(sequence: 0, payload: try JSONEncoder().encode(begin), crc32: 0))
        var memEntries: [(String, Data, Data?, UInt64)] = []
        for op in ops {
            switch op {
            case .put(let coll, let idData, let valueData):
                let baseKey = makeBaseKey(collection: coll, idData: idData)
                let frame = WALFrame(type: "op", txid: txid, key: baseKey, value: valueData)
                try await wal.append(WALRecord(sequence: seq, payload: try JSONEncoder().encode(frame), crc32: 0))
                memEntries.append((coll, baseKey, valueData, seq))
            case .delete(let coll, let idData):
                let baseKey = makeBaseKey(collection: coll, idData: idData)
                let frame = WALFrame(type: "op", txid: txid, key: baseKey, value: nil)
                try await wal.append(WALRecord(sequence: seq, payload: try JSONEncoder().encode(frame), crc32: 0))
                memEntries.append((coll, baseKey, nil, seq))
            }
            seq &+= 1
        }
        let commit = WALFrame(type: "commit", txid: txid, key: nil, value: nil)
        try await wal.append(WALRecord(sequence: 0, payload: try JSONEncoder().encode(commit), crc32: 0))
        try await wal.sync()
        try CrashPoints.hit("wal_fsync")

        // Apply to memtable and in-memory collections.
        for (coll, baseKey, value, s) in memEntries {
            await memtable.put(MemtableEntry(key: baseKey, value: value, sequence: s))
            if let hook = applyHooks[coll] {
                if let (_, idData) = splitKey(baseKey) { await hook(idData, value, s) }
            }
        }
        try await flushMemtableIfNeeded()
    }
}

public extension Collection {
    nonisolated func makeStoreOpPut(_ value: C) throws -> FountainStore.StoreOp {
        let idData = try JSONEncoder().encode(value.id)
        let valData = try JSONEncoder().encode(value)
        return .put(collection: name, id: idData, value: valData)
    }
    nonisolated func makeStoreOpDelete(_ id: C.ID) throws -> FountainStore.StoreOp {
        let idData = try JSONEncoder().encode(id)
        return .delete(collection: name, id: idData)
    }
}

// MARK: - Compaction status

public extension FountainStore {
    struct CompactionLevelStatus: Sendable, Hashable, Codable {
        public let level: Int
        public let tables: Int
        public let sizeBytes: Int64
    }
    struct CompactionStatus: Sendable, Hashable, Codable {
        public let running: Bool
        public let pendingTables: Int
        public let levels: [CompactionLevelStatus]
        public let debtBytes: Int64
    }
    func compactionStatus() async throws -> CompactionStatus {
        let st = try await compactor.status()
        let levels = st.levels.map { CompactionLevelStatus(level: $0.level, tables: $0.tables, sizeBytes: $0.sizeBytes) }
        return CompactionStatus(running: st.running, pendingTables: st.pendingTables, levels: levels, debtBytes: st.debtBytes)
    }
}

/// Configuration parameters for opening a `FountainStore`.
public struct StoreOptions: Sendable {
    public let path: URL
    public let cacheBytes: Int
    public let logger: (@Sendable (LogEvent) -> Void)?
    public let defaultScanLimit: Int
    public let walSegmentBytes: Int
    public init(path: URL, cacheBytes: Int = 64 << 20, logger: (@Sendable (LogEvent) -> Void)? = nil, defaultScanLimit: Int = 100, walSegmentBytes: Int = 4 << 20) {
        self.path = path
        self.cacheBytes = cacheBytes
        self.logger = logger
        self.defaultScanLimit = defaultScanLimit
        self.walSegmentBytes = walSegmentBytes
    }
    // Backward-compatible initializer without walSegmentBytes parameter.
    public init(path: URL, cacheBytes: Int = 64 << 20, logger: (@Sendable (LogEvent) -> Void)? = nil, defaultScanLimit: Int = 100) {
        self.path = path
        self.cacheBytes = cacheBytes
        self.logger = logger
        self.defaultScanLimit = defaultScanLimit
        self.walSegmentBytes = 4 << 20
    }
}

/// Immutable view of the store at a specific sequence number.
public struct Snapshot: Sendable, Hashable {
    public let sequence: UInt64
    public init(sequence: UInt64) { self.sequence = sequence }
}

/// Aggregated counters for store operations.
public struct Metrics: Sendable, Hashable, Codable {
    public var puts: UInt64 = 0
    public var gets: UInt64 = 0
    public var deletes: UInt64 = 0
    public var scans: UInt64 = 0
    public var indexLookups: UInt64 = 0
    public var batches: UInt64 = 0
    public var histories: UInt64 = 0
    public init() {}
}

private struct WALPayload: Codable {
    let key: Data
    let value: Data?
}

// Transactional WAL frames for atomic multi-op commits during recovery.
// Backward compatible: old records without a `type` are treated as committed ops.
private struct WALFrame: Codable {
    let type: String // "begin" | "op" | "commit"
    let txid: String?
    let key: Data?
    let value: Data??
}

/// Structured log events emitted by the store.
public enum LogEvent: Sendable, Hashable, Codable {
    case put(collection: String)
    case get(collection: String)
    case delete(collection: String)
    case scan(collection: String)
    case indexLookup(collection: String, index: String)
    case batch(collection: String, count: Int)
    case history(collection: String)

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
        case .history(let collection):
            try container.encode("history", forKey: .type)
            try container.encode(collection, forKey: .collection)
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
        case "history":
            let collection = try container.decode(String.self, forKey: .collection)
            self = .history(collection: collection)
        default:
            let context = DecodingError.Context(codingPath: [CodingKeys.type], debugDescription: "Unknown log event type: \(type)")
            throw DecodingError.dataCorrupted(context)
        }
    }
}

/// Errors thrown when operating on collections.
public enum CollectionError: Error, Sendable {
    case uniqueConstraintViolation(index: String, key: String)
}

/// Errors related to transaction sequencing.
public enum TransactionError: Error, Sendable {
    case sequenceTooLow(required: UInt64, current: UInt64)
}

/// Definition for a secondary index over documents of type `C`.
public struct Index<C>: Sendable {
    public enum Kind: @unchecked Sendable {
        case unique(PartialKeyPath<C>)
        case multi(PartialKeyPath<C>)
        case multiValues(@Sendable (C) -> [String])
        case fts(PartialKeyPath<C>, analyzer: @Sendable (String) -> [String] = FTSIndex.defaultAnalyzer)
        case vector(PartialKeyPath<C>)
    }
    public let name: String
    public let kind: Kind
    public init(name: String, kind: Kind) {
        self.name = name
        self.kind = kind
    }
}

/// Marker type representing a transactional batch.
public struct Transaction: Sendable {
    public init() {}
}

/// Top-level actor managing collections and persistence.
public actor FountainStore {
    /// Opens or creates a store at the given path.
    public static func open(_ opts: StoreOptions) async throws -> FountainStore {
        let fm = FileManager.default
        try fm.createDirectory(at: opts.path, withIntermediateDirectories: true)
        let wal = WAL(path: opts.path.appendingPathComponent("wal.log"), rotateBytes: opts.walSegmentBytes)
        let manifest = ManifestStore(url: opts.path.appendingPathComponent("MANIFEST.json"))
        let memtable = Memtable(limit: 1024)
        let compactor = Compactor(directory: opts.path, manifest: manifest)
        let store = FountainStore(options: opts, wal: wal, manifest: manifest, memtable: memtable, compactor: compactor)
        // Configure SSTable block cache based on options.
        SSTable.configureCache(capacityBytes: opts.cacheBytes)

        // Load manifest to seed sequence and discover existing tables.
        let m = try await manifest.load()
        await store.setSequence(m.sequence)
        try await store.loadSSTables(m)

        // Replay WAL records newer than the manifest sequence.
        let recs = try await wal.replay()
        for r in recs {
            try await store.replayRecord(r, manifestSequence: m.sequence)
        }
        return store
    }

    /// Returns a snapshot representing the current sequence.
    public func snapshot() -> Snapshot {
        Snapshot(sequence: sequence)
    }

    /// Returns a handle to the named collection for document type `C`.
    public func collection<C: Codable & Identifiable>(_ name: String, of: C.Type) -> Collection<C> {
        if let any = collectionsCache[name], let cached = any as? Collection<C> {
            return cached
        }
        let coll = Collection<C>(name: name, store: self)
        if let items = bootstrap.removeValue(forKey: name) {
            Task { await coll.bootstrap(items) }
        }
        registerApplyHook(name) { idData, valueData, seq in
            await coll.applyCommittedRaw(idData: idData, valueData: valueData, sequence: seq)
        }
        registerValidateHook(name) { rawOps in
            try await coll.prevalidateUnique(rawOps: rawOps)
        }
        collectionsCache[name] = coll
        return coll
    }

    // MARK: - Internals
    private let options: StoreOptions
    internal let wal: WAL
    internal let manifest: ManifestStore
    internal let memtable: Memtable
    internal let compactor: Compactor
    private var bootstrap: [String: [(Data, Data?, UInt64)]] = [:]
    private var sequence: UInt64 = 0
    private var metrics = Metrics()
    // Hook to apply committed ops to live collections' in-memory state.
    private var applyHooks: [String: @Sendable (Data, Data?, UInt64) async -> Void] = [:]
    // Hook to prevalidate unique constraints for a batch (per collection).
    private var validateHooks: [String: @Sendable ([(Bool, Data, Data?)]) async throws -> Void] = [:]
    // Cache of live collection actors by name (type-erased).
    private var collectionsCache: [String: Any] = [:]
    // Replay-time transaction buffers (BEGIN/OP/COMMIT); cleared after open.
    private var replayActiveTx: Set<String> = []
    private var replayPendingOps: [String: [(UInt64, Data, Data?)]] = [:]

    fileprivate func nextSequence() -> UInt64 {
        allocateSequences(1)
    }

    fileprivate func allocateSequences(_ count: Int) -> UInt64 {
        let start = sequence &+ 1
        sequence &+= UInt64(count)
        return start
    }

    /// Returns current metrics without resetting them.
    public func metricsSnapshot() -> Metrics {
        metrics
    }

    /// Resets metrics counters and returns their previous values.
    public func resetMetrics() -> Metrics {
        let snap = metrics
        metrics = Metrics()
        return snap
    }

    internal func defaultScanLimit() -> Int {
        options.defaultScanLimit
    }

    internal enum Metric {
        case put, get, delete, scan, indexLookup, batch, history
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
        case .history:
            metrics.histories &+= count
        }
    }

    internal func log(_ event: LogEvent) {
        options.logger?(event)
    }

    internal func applyBackpressureIfNeeded() async {
        // Scale backpressure sleep with compaction debt to smooth spikes.
        if let st = try? await compactor.status(), st.debtBytes > (512 * 1024) {
            let kb = max(1, st.debtBytes / 1024)
            let factor = min(5_000_000, kb * 1000) // up to 5ms
            try? await Task.sleep(nanoseconds: UInt64(factor))
        }
    }
    
    internal func flushMemtableIfNeeded() async throws {
        if await memtable.isOverLimit() {
            try await flushMemtable()
        }
    }

    private func setSequence(_ seq: UInt64) {
        self.sequence = seq
    }

    private func addBootstrap(collection: String, id: Data, value: Data?, sequence: UInt64) {
        bootstrap[collection, default: []].append((id, value, sequence))
    }

    private func registerApplyHook(_ collection: String, _ hook: @escaping @Sendable (Data, Data?, UInt64) async -> Void) {
        applyHooks[collection] = hook
    }
    public func listCollections() -> [String] {
        Array(applyHooks.keys).sorted()
    }

    /// Drops a collection from the live store catalog. This unregisters live hooks and
    /// removes index catalog entries. It does not rewrite existing SSTables; use higher
    /// level deletion to remove records if desired.
    public func dropCollection(_ name: String) async throws {
        collectionsCache.removeValue(forKey: name)
        applyHooks.removeValue(forKey: name)
        validateHooks.removeValue(forKey: name)
        // Remove index definitions from manifest catalog for this collection.
        var m = try await manifest.load()
        m.indexCatalog.removeValue(forKey: name)
        try await manifest.save(m)
    }
    private func registerValidateHook(_ collection: String, _ hook: @escaping @Sendable ([(Bool, Data, Data?)]) async throws -> Void) {
        validateHooks[collection] = hook
    }
    
    internal func loadSSTables(_ manifest: Manifest) async throws {
        for (id, url) in manifest.tables {
            let handle = SSTableHandle(id: id, path: url)
            let entries = try SSTable.scan(handle)
            for (k, v) in entries {
                // Decode SSTable key which may include an appended sequence number.
                if let decoded = decodeSSTableKey(k.raw) {
                    let seq = decoded.seq ?? manifest.sequence
                    let val: Data? = v.raw.isEmpty ? nil : v.raw
                    addBootstrap(collection: decoded.collection, id: decoded.idData, value: val, sequence: seq)
                }
            }
        }
    }

    internal func replayRecord(_ r: WALRecord, manifestSequence: UInt64) async throws {
        // Try transactional frame first; fallback to legacy payload.
        if let frame = try? JSONDecoder().decode(WALFrame.self, from: r.payload) {
            switch frame.type {
            case "begin":
                if let tx = frame.txid { replayActiveTx.insert(tx); replayPendingOps[tx] = [] }
                return
            case "op":
                guard let key = frame.key else { return }
                let value: Data?
                if let vv = frame.value { value = vv } else { value = nil }
                if r.sequence <= manifestSequence {
                    return // already materialized
                }
                if let tx = frame.txid, replayActiveTx.contains(tx) {
                    replayPendingOps[tx, default: []].append((r.sequence, key, value))
                    return
                }
                // No active tx: treat as committed op.
                await memtable.put(MemtableEntry(key: key, value: value, sequence: r.sequence))
                if let (col, idData) = splitKey(key) {
                    addBootstrap(collection: col, id: idData, value: value, sequence: r.sequence)
                }
                return
            case "commit":
                if let tx = frame.txid, let ops = replayPendingOps.removeValue(forKey: tx) {
                    replayActiveTx.remove(tx)
                    for (seq, key, value) in ops.sorted(by: { $0.0 < $1.0 }) {
                        await memtable.put(MemtableEntry(key: key, value: value, sequence: seq))
                        if let (col, idData) = splitKey(key) {
                            addBootstrap(collection: col, id: idData, value: value, sequence: seq)
                        }
                    }
                }
                return
            default:
                break
            }
        }
        // Legacy single-op payload: apply immediately (committed).
        let payload = try JSONDecoder().decode(WALPayload.self, from: r.payload)
        if r.sequence > manifestSequence {
            await memtable.put(MemtableEntry(key: payload.key, value: payload.value, sequence: r.sequence))
            if let (col, idData) = splitKey(payload.key) {
                addBootstrap(collection: col, id: idData, value: payload.value, sequence: r.sequence)
            }
        }
    }

    private func flushMemtable() async throws {
        let drained = await memtable.drain()
        guard !drained.isEmpty else { return }
        // Write a new SSTable.
        let url = options.path.appendingPathComponent(UUID().uuidString + ".sst")
        let entries = drained.map {
            let k = sstableKeyAppendingSeq($0.key, seq: $0.sequence)
            return (TableKey(raw: k), TableValue(raw: $0.value ?? Data()))
        }
            .sorted { $0.0.raw.lexicographicallyPrecedes($1.0.raw) }
        let handle = try await SSTable.create(at: url, entries: entries)
        var m = try await manifest.load()
        m.sequence = sequence
        m.tables[handle.id] = handle.path
        try await manifest.save(m)
        // GC old WAL segments that are safely beyond the manifest sequence.
        await wal.gc(manifestSequence: m.sequence)
        // CRASH_POINT(id: manifest_save)
        try CrashPoints.hit("manifest_save")
        await memtable.fireFlushCallbacks(drained)
        // CRASH_POINT(id: memtable_flush)
        try CrashPoints.hit("memtable_flush")
        // Schedule background compaction.
        Task { await compactor.tick() }
    }

    private func splitKey(_ data: Data) -> (String, Data)? {
        guard let idx = data.firstIndex(of: 0) else { return nil }
        let nameData = data[..<idx]
        let idData = data[data.index(after: idx)...]
        guard let name = String(data: nameData, encoding: .utf8) else { return nil }
        return (name, Data(idData))
    }

    // MARK: - SSTable key encoding/decoding (MVCC persistent sequences)

    /// Returns a new key by appending a separator and the big-endian sequence number.
    /// Base key format: collectionName\0idJSON
    /// Appended format:  + "\0" + seq(8 bytes, big endian)
    private func sstableKeyAppendingSeq(_ baseKey: Data, seq: UInt64) -> Data {
        var out = baseKey
        out.append(0)
        var be = seq.bigEndian
        withUnsafeBytes(of: &be) { out.append(contentsOf: $0) }
        return out
    }

    /// Decodes an SSTable key which may or may not include an appended sequence number.
    /// Returns collection name, id JSON bytes, and optional sequence.
    private func decodeSSTableKey(_ data: Data) -> (collection: String, idData: Data, seq: UInt64?)? {
        guard let firstSep = data.firstIndex(of: 0) else { return nil }
        let nameData = data[..<firstSep]
        guard let name = String(data: nameData, encoding: .utf8) else { return nil }
        // Check for trailing "\0" + 8-byte seq.
        if data.count >= firstSep + 1 + 1 + 8 {
            let sep2Pos = data.count - 9
            if data[sep2Pos] == 0 {
                let idBytes = data[(firstSep + 1)..<sep2Pos]
                let seqBytes = data[(sep2Pos + 1)..<data.count]
                let seq = seqBytes.withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.bigEndian
                return (name, Data(idBytes), seq)
            }
        }
        let idBytes = data[(firstSep + 1)..<data.count]
        return (name, Data(idBytes), nil)
    }

    private func makeBaseKey(collection: String, idData: Data) -> Data {
        var data = Data(collection.utf8)
        data.append(0)
        data.append(idData)
        return data
    }

    // Persist or update index definition for a collection in the manifest catalog.
    internal func persistIndexDefinition(collection: String, name: String, kind: String, field: String? = nil) async throws {
        var m = try await manifest.load()
        var defs = m.indexCatalog[collection] ?? []
        if let idx = defs.firstIndex(where: { $0.name == name }) {
            defs[idx] = IndexDef(name: name, kind: kind, field: field)
        } else {
            defs.append(IndexDef(name: name, kind: kind, field: field))
        }
        m.indexCatalog[collection] = defs
        try await manifest.save(m)
    }

    public func listIndexDefinitions(_ collection: String) async throws -> [IndexDef] {
        let m = try await manifest.load()
        return m.indexCatalog[collection] ?? []
    }

    /// Public wrapper to persist or update an index definition in the manifest catalog.
    public func saveIndexDefinition(collection: String, name: String, kind: String, field: String? = nil) async {
        try? await persistIndexDefinition(collection: collection, name: name, kind: kind, field: field)
    }

    // MARK: - Backups
    public struct BackupRef: Sendable, Hashable, Codable {
        public let id: String
        public let createdAt: String
        public let note: String?
        public let sizeBytes: Int64
    }

    public func createBackup(note: String? = nil) async throws -> BackupRef {
        // Quiesce writes (we are in store actor). Make state durable.
        try await wal.sync()
        // Flush current memtable (if any) to reduce WAL replay on restore.
        try await flushMemtable()
        try await wal.sync()
        let m = try await manifest.load()
        let fm = FileManager.default
        let backupsDir = options.path.appendingPathComponent("backups")
        try? fm.createDirectory(at: backupsDir, withIntermediateDirectories: true)
        let bid = UUID().uuidString
        let dir = backupsDir.appendingPathComponent(bid)
        try fm.createDirectory(at: dir, withIntermediateDirectories: true)
        var total: Int64 = 0
        func copy(_ src: URL, _ dst: URL) throws {
            if fm.fileExists(atPath: dst.path) { try fm.removeItem(at: dst) }
            try fm.copyItem(at: src, to: dst)
            if let s = try? fm.attributesOfItem(atPath: dst.path)[.size] as? NSNumber { total += s.int64Value }
        }
        let manifestPath = options.path.appendingPathComponent("MANIFEST.json")
        let walPath = options.path.appendingPathComponent("wal.log")
        if fm.fileExists(atPath: manifestPath.path) { try copy(manifestPath, dir.appendingPathComponent("MANIFEST.json")) }
        if fm.fileExists(atPath: walPath.path) { try copy(walPath, dir.appendingPathComponent("wal.log")) }
        for (_, url) in m.tables {
            let name = url.lastPathComponent
            if fm.fileExists(atPath: url.path) {
                try copy(url, dir.appendingPathComponent(name))
            }
        }
        let ref = BackupRef(id: bid, createdAt: ISO8601DateFormatter().string(from: Date()), note: note, sizeBytes: total)
        let meta = try JSONEncoder().encode(ref)
        try meta.write(to: dir.appendingPathComponent("backup.json"))
        return ref
    }

    public func listBackups() -> [BackupRef] {
        let fm = FileManager.default
        let backupsDir = options.path.appendingPathComponent("backups")
        guard let contents = try? fm.contentsOfDirectory(at: backupsDir, includingPropertiesForKeys: nil) else { return [] }
        var refs: [BackupRef] = []
        for folder in contents {
            let meta = folder.appendingPathComponent("backup.json")
            if let data = try? Data(contentsOf: meta), let ref = try? JSONDecoder().decode(BackupRef.self, from: data) {
                refs.append(ref)
            }
        }
        return refs.sorted { $0.createdAt < $1.createdAt }
    }

    public func restoreBackup(id: String) async throws {
        // Quiesce writes
        let fm = FileManager.default
        let backupsDir = options.path.appendingPathComponent("backups")
        let dir = backupsDir.appendingPathComponent(id)
        // Copy MANIFEST and referenced SSTables and wal
        let manifestSrc = dir.appendingPathComponent("MANIFEST.json")
        let walSrc = dir.appendingPathComponent("wal.log")
        // Remove current SSTables referenced by manifest
        if let current = try? await manifest.load() {
            for (_, url) in current.tables { try? fm.removeItem(at: url) }
        }
        // Copy SSTables from backup folder (any .sst file)
        if let files = try? fm.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) {
            for f in files where f.pathExtension == "sst" {
                let dst = options.path.appendingPathComponent(f.lastPathComponent)
                if fm.fileExists(atPath: dst.path) { try? fm.removeItem(at: dst) }
                try fm.copyItem(at: f, to: dst)
            }
        }
        // Write manifest with corrected URLs
        if fm.fileExists(atPath: manifestSrc.path) {
            let data = try Data(contentsOf: manifestSrc)
            let old = try JSONDecoder().decode(Manifest.self, from: data)
            var fixed = Manifest(sequence: old.sequence, tables: [:], indexCatalog: old.indexCatalog)
            for (id, url) in old.tables {
                let dst = options.path.appendingPathComponent(url.lastPathComponent)
                fixed.tables[id] = dst
            }
            try await manifest.save(fixed)
        }
        // Replace WAL
        let walDst = options.path.appendingPathComponent("wal.log")
        if fm.fileExists(atPath: walSrc.path) {
            if fm.fileExists(atPath: walDst.path) { try? fm.removeItem(at: walDst) }
            try fm.copyItem(at: walSrc, to: walDst)
        }
        // Reset bootstrap; on next collection() calls, data will reflect restored state after replay
        bootstrap.removeAll()
        let m = try await manifest.load()
        try await loadSSTables(m)
    }

    private init(options: StoreOptions, wal: WAL, manifest: ManifestStore, memtable: Memtable, compactor: Compactor) {
        self.options = options
        self.wal = wal
        self.manifest = manifest
        self.memtable = memtable
        self.compactor = compactor
    }
}

/// Provides CRUD and indexing operations for a document collection.
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
            let stringKeyPath: KeyPath<C, String>?
            let arrayKeyPath: KeyPath<C, [String]>?
            let extractor: (@Sendable (C) -> [String])?
            var map: [String: [(UInt64, [C.ID])]] = [:]
            init(stringKeyPath: KeyPath<C, String>) { self.stringKeyPath = stringKeyPath; self.arrayKeyPath = nil; self.extractor = nil }
            init(arrayKeyPath: KeyPath<C, [String]>) { self.arrayKeyPath = arrayKeyPath; self.stringKeyPath = nil; self.extractor = nil }
            init(extractor: @escaping @Sendable (C) -> [String]) { self.extractor = extractor; self.stringKeyPath = nil; self.arrayKeyPath = nil }
            func keys(for value: C) -> [String] {
                if let kp = stringKeyPath { return [value[keyPath: kp]] }
                if let akp = arrayKeyPath { return value[keyPath: akp] }
                if let ex = extractor { return ex(value) }
                return []
            }
        }
        final class FTS {
            let keyPath: KeyPath<C, String>
            var index: FTSIndex
            var idMap: [String: C.ID] = [:]
            init(keyPath: KeyPath<C, String>, analyzer: @escaping @Sendable (String) -> [String]) {
                self.keyPath = keyPath
                self.index = FTSIndex(analyzer: analyzer)
            }
        }
        final class Vector {
            let keyPath: KeyPath<C, [Double]>
            var index = HNSWIndex()
            var idMap: [String: C.ID] = [:]
            init(keyPath: KeyPath<C, [Double]>) { self.keyPath = keyPath }
        }
        case unique(Unique)
        case multi(Multi)
        case fts(FTS)
        case vector(Vector)
    }
    private var indexes: [String: IndexStorage] = [:]

    public init(name: String, store: FountainStore) {
        self.name = name
        self.store = store
    }

    /// Returns the last committed sequence number for the given id, respecting an optional snapshot.
    public func lastSequence(of id: C.ID, snapshot: Snapshot? = nil) async -> UInt64? {
        let limit = snapshot?.sequence ?? UInt64.max
        guard let versions = data[id] else { return nil }
        return versions.last(where: { $0.0 <= limit })?.0
    }

    private func encodeKey(_ id: C.ID) throws -> Data {
        var data = Data(name.utf8)
        data.append(0)
        data.append(try JSONEncoder().encode(id))
        return data
    }

    // Apply a committed op (durable in WAL) to in-memory state.
    internal func applyCommittedRaw(idData: Data, valueData: Data?, sequence: UInt64) async {
        guard let id = try? JSONDecoder().decode(C.ID.self, from: idData) else { return }
        if let vd = valueData, let value = try? JSONDecoder().decode(C.self, from: vd) {
            performPut(value, sequence: sequence)
        } else {
            performDelete(id: id, sequence: sequence)
        }
    }

    // Validate unique indexes across a batch of raw ops (put/delete).
    internal func prevalidateUnique(rawOps: [(Bool, Data, Data?)]) throws {
        // Build overlay per unique index name: key -> optional id
        var overlays: [String: [String: C.ID?]] = [:]
        func idxKey(_ name: String) -> String { name }

        func effective(_ idx: IndexStorage.Unique, _ idxName: String, _ key: String) -> C.ID? {
            if let over = overlays[idxKey(idxName)]?[key] { return over }
            return idx.map[key]?.last?.1
        }

        for (isPut, idData, valueData) in rawOps {
            guard let id = try? JSONDecoder().decode(C.ID.self, from: idData) else { continue }
            if isPut {
                guard let vd = valueData, let value = try? JSONDecoder().decode(C.self, from: vd) else { continue }
                for (name, storage) in indexes {
                    if case .unique(let idx) = storage {
                        let newKey = value[keyPath: idx.keyPath]
                        // Remove old mapping if key changed.
                        if let oldVal = data[id]?.last?.1 {
                            let oldKey = oldVal[keyPath: idx.keyPath]
                            if oldKey != newKey {
                                if effective(idx, name, oldKey) == id {
                                    var m = overlays[idxKey(name)] ?? [:]
                                    m[oldKey] = nil
                                    overlays[idxKey(name)] = m
                                }
                            }
                        }
                        // Check conflict on newKey
                        if let existing = effective(idx, name, newKey), existing != id {
                            throw CollectionError.uniqueConstraintViolation(index: name, key: newKey)
                        }
                        var m = overlays[idxKey(name)] ?? [:]
                        m[newKey] = id
                        overlays[idxKey(name)] = m
                    }
                }
            } else {
                // delete
                if let oldVal = data[id]?.last?.1 {
                    for (name, storage) in indexes {
                        if case .unique(let idx) = storage {
                            let oldKey = oldVal[keyPath: idx.keyPath]
                            if effective(idx, name, oldKey) == id {
                                var m = overlays[idxKey(name)] ?? [:]
                                m[oldKey] = nil
                                overlays[idxKey(name)] = m
                            }
                        }
                    }
                }
            }
        }
    }

    private func performPut(_ value: C, sequence: UInt64) {
        let old = data[value.id]?.last?.1
        data[value.id, default: []].append((sequence, value))
        for storage in indexes.values {
            switch storage {
            case .unique(let idx):
                let key = value[keyPath: idx.keyPath]
                if let old = old {
                    let oldKey = old[keyPath: idx.keyPath]
                    if oldKey != key {
                        idx.map[oldKey, default: []].append((sequence, nil))
                    }
                }
                idx.map[key, default: []].append((sequence, value.id))
            case .multi(let idx):
                let newKeys = idx.keys(for: value)
                if let old = old {
                    let oldKeys = idx.keys(for: old)
                    // Remove from keys no longer present
                    for k in oldKeys where !newKeys.contains(k) {
                        var arr = idx.map[k]?.last?.1 ?? []
                        if let pos = arr.firstIndex(of: value.id) { arr.remove(at: pos) }
                        idx.map[k, default: []].append((sequence, arr))
                    }
                }
                for k in newKeys {
                    var arr = idx.map[k]?.last?.1 ?? []
                    if !arr.contains(value.id) { arr.append(value.id) }
                    idx.map[k, default: []].append((sequence, arr))
                }
            case .fts(let idx):
                let docID = "\(value.id)"
                if old != nil { idx.index.remove(docID: docID) }
                idx.index.add(docID: docID, text: value[keyPath: idx.keyPath])
                idx.idMap[docID] = value.id
            case .vector(let idx):
                let docID = "\(value.id)"
                if old != nil { idx.index.remove(id: docID) }
                idx.index.add(id: docID, vector: value[keyPath: idx.keyPath])
                idx.idMap[docID] = value.id
            }
        }
    }

    private func performDelete(id: C.ID, sequence: UInt64) {
        let old = data[id]?.last?.1
        data[id, default: []].append((sequence, nil))
        guard let oldVal = old else { return }
        for storage in indexes.values {
            switch storage {
            case .unique(let idx):
                let key = oldVal[keyPath: idx.keyPath]
                idx.map[key, default: []].append((sequence, nil))
            case .multi(let idx):
                let keys = idx.keys(for: oldVal)
                for key in keys {
                    var arr = idx.map[key]?.last?.1 ?? []
                    if let pos = arr.firstIndex(of: id) { arr.remove(at: pos) }
                    idx.map[key, default: []].append((sequence, arr))
                }
            case .fts(let idx):
                let docID = "\(id)"
                idx.index.remove(docID: docID)
                idx.idMap.removeValue(forKey: docID)
            case .vector(let idx):
                let docID = "\(id)"
                idx.index.remove(id: docID)
                idx.idMap.removeValue(forKey: docID)
            }
        }
    }

    internal func bootstrap(_ items: [(Data, Data?, UInt64)]) async {
        for (idData, valData, seq) in items {
            guard let id = try? JSONDecoder().decode(C.ID.self, from: idData) else { continue }
            if let vd = valData, let value = try? JSONDecoder().decode(C.self, from: vd) {
                performPut(value, sequence: seq)
            } else {
                performDelete(id: id, sequence: seq)
            }
        }
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
            try? await store.persistIndexDefinition(collection: name, name: index.name, kind: "unique")
        case .multi(let path):
            let idx: IndexStorage.Multi
            if let kp = path as? KeyPath<C, String> {
                idx = IndexStorage.Multi(stringKeyPath: kp)
            } else if let akp = path as? KeyPath<C, [String]> {
                idx = IndexStorage.Multi(arrayKeyPath: akp)
            } else { return }
            for (id, versions) in data {
                guard let (seq, val) = versions.last, let v = val else { continue }
                let keys = idx.keys(for: v)
                for key in keys {
                    var arr = idx.map[key]?.last?.1 ?? []
                    arr.append(id)
                    idx.map[key, default: []].append((seq, arr))
                }
            }
            indexes[index.name] = .multi(idx)
            try? await store.persistIndexDefinition(collection: name, name: index.name, kind: "multi")
        case .multiValues(let extractor):
            let idx = IndexStorage.Multi(extractor: extractor)
            for (id, versions) in data {
                guard let (seq, val) = versions.last, let v = val else { continue }
                let keys = idx.keys(for: v)
                for key in keys {
                    var arr = idx.map[key]?.last?.1 ?? []
                    arr.append(id)
                    idx.map[key, default: []].append((seq, arr))
                }
            }
            indexes[index.name] = .multi(idx)
            try? await store.persistIndexDefinition(collection: name, name: index.name, kind: "multi")
        case .fts(let path, analyzer: let analyzer):
            guard let kp = path as? KeyPath<C, String> else { return }
            let idx = IndexStorage.FTS(keyPath: kp, analyzer: analyzer)
            for (id, versions) in data {
                guard let (_, val) = versions.last, let v = val else { continue }
                let docID = "\(id)"
                idx.index.add(docID: docID, text: v[keyPath: kp])
                idx.idMap[docID] = id
            }
            indexes[index.name] = .fts(idx)
            try? await store.persistIndexDefinition(collection: name, name: index.name, kind: "fts")
        case .vector(let path):
            guard let kp = path as? KeyPath<C, [Double]> else { return }
            let idx = IndexStorage.Vector(keyPath: kp)
            for (id, versions) in data {
                guard let (_, val) = versions.last, let v = val else { continue }
                let docID = "\(id)"
                idx.index.add(id: docID, vector: v[keyPath: kp])
                idx.idMap[docID] = id
            }
            indexes[index.name] = .vector(idx)
            try? await store.persistIndexDefinition(collection: name, name: index.name, kind: "vector")
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

    /// Inserts or updates a document in the collection.
    public func put(_ value: C, sequence: UInt64? = nil) async throws {
        await store.applyBackpressureIfNeeded()
        await store.record(.put)
        await store.log(.put(collection: name))
        let seq: UInt64
        if let s = sequence {
            seq = s
        } else {
            seq = await store.nextSequence()
        }

        // Check unique constraints before persisting.
        for (name, storage) in indexes {
            switch storage {
            case .unique(let idx):
                let key = value[keyPath: idx.keyPath]
                if let existing = idx.map[key]?.last?.1, existing != value.id {
                    throw CollectionError.uniqueConstraintViolation(index: name, key: key)
                }
            case .multi, .fts, .vector:
                continue
            }
        }

        // WAL + memtable
        let keyData = try encodeKey(value.id)
        let valData = try JSONEncoder().encode(value)
        let payload = WALPayload(key: keyData, value: valData)
        try await store.wal.append(WALRecord(sequence: seq, payload: try JSONEncoder().encode(payload), crc32: 0))
        // CRASH_POINT(id: wal_append)
        try CrashPoints.hit("wal_append")
        try await store.wal.sync()
        // CRASH_POINT(id: wal_fsync)
        try CrashPoints.hit("wal_fsync")
        await store.memtable.put(MemtableEntry(key: keyData, value: valData, sequence: seq))
        try await store.flushMemtableIfNeeded()

        // Apply in-memory structures.
        performPut(value, sequence: seq)
    }

    /// Retrieves a document by identifier, optionally from a snapshot.
    public func get(id: C.ID, snapshot: Snapshot? = nil) async throws -> C? {
        await store.record(.get)
        await store.log(.get(collection: name))
        guard let versions = data[id] else { return nil }
        let limit = snapshot?.sequence ?? UInt64.max
        return versions.last(where: { $0.0 <= limit })?.1
    }

    public func history(id: C.ID, snapshot: Snapshot? = nil) async throws -> [(UInt64, C?)] {
        await store.record(.history)
        await store.log(.history(collection: name))
        guard let versions = data[id] else { return [] }
        let limit = snapshot?.sequence ?? UInt64.max
        return versions.filter { $0.0 <= limit }
    }

    /// Removes a document by identifier.
    public func delete(id: C.ID, sequence: UInt64? = nil) async throws {
        await store.applyBackpressureIfNeeded()
        await store.record(.delete)
        await store.log(.delete(collection: name))
        let seq: UInt64
        if let s = sequence {
            seq = s
        } else {
            seq = await store.nextSequence()
        }

        let keyData = try encodeKey(id)
        let payload = WALPayload(key: keyData, value: nil)
        try await store.wal.append(WALRecord(sequence: seq, payload: try JSONEncoder().encode(payload), crc32: 0))
        // CRASH_POINT(id: wal_append)
        try CrashPoints.hit("wal_append")
        try await store.wal.sync()
        // CRASH_POINT(id: wal_fsync)
        try CrashPoints.hit("wal_fsync")
        await store.memtable.put(MemtableEntry(key: keyData, value: nil, sequence: seq))
        try await store.flushMemtableIfNeeded()

        performDelete(id: id, sequence: seq)
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
        case .fts, .vector:
            return []
        }
    }

    /// Performs a full-text search against the given index.
    public func searchText(_ name: String, query: String, limit: Int? = nil) async throws -> [C] {
        await store.record(.indexLookup)
        await store.log(.indexLookup(collection: self.name, index: name))
        guard let storage = indexes[name] else { return [] }
        guard case .fts(let idx) = storage else { return [] }
        let ids = idx.index.search(query, limit: limit)
        var res: [C] = []
        for doc in ids {
            if let real = idx.idMap[doc], let val = try await get(id: real) {
                res.append(val)
            }
        }
        return res
    }

    /// Performs a nearest-neighbor vector search using the specified index.
    public func vectorSearch(_ name: String, query: [Double], k: Int, metric: HNSWIndex.DistanceMetric = .l2) async throws -> [C] {
        await store.record(.indexLookup)
        await store.log(.indexLookup(collection: self.name, index: name))
        guard let storage = indexes[name] else { return [] }
        guard case .vector(let idx) = storage else { return [] }
        let ids = idx.index.search(query, k: k, metric: metric)
        var res: [C] = []
        for doc in ids {
            if let real = idx.idMap[doc], let val = try await get(id: real) {
                res.append(val)
            }
        }
        return res
    }

    /// Scans a secondary index by key prefix.
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
        case .fts, .vector:
            break
        }
        items.sort { $0.0 < $1.0 }
        return items.prefix(maxItems).map { $0.1 }
    }

    /// Scans documents by key prefix, respecting an optional snapshot.
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
