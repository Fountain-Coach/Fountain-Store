
//
//  Store.swift
//  FountainStore
//
//  Public API surface for the pure‑Swift embedded store.
//

import Foundation

public struct StoreOptions: Sendable, Hashable {
    public let path: URL
    public let cacheBytes: Int
    public init(path: URL, cacheBytes: Int = 64 << 20) {
        self.path = path
        self.cacheBytes = cacheBytes
    }
}

public struct Snapshot: Sendable, Hashable {
    public let sequence: UInt64
    public init(sequence: UInt64) { self.sequence = sequence }
}

public struct Index<C>: Sendable {
    public enum Kind {
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
        // Placeholder until MVCC is implemented.
        return Snapshot(sequence: 0)
    }

    public func collection<C: Codable & Identifiable>(_ name: String, of: C.Type) -> Collection<C> {
        return Collection<C>(name: name)
    }

    // MARK: - Internals
    private let options: StoreOptions
    private init(options: StoreOptions) { self.options = options }
}

public actor Collection<C: Codable & Identifiable> where C.ID: Codable {
    public let name: String
    public init(name: String) { self.name = name }

    public func define(_ index: Index<C>) async throws {
        // TODO: register index in manifest and create structures.
        // For now, placeholder.
    }

    public func put(_ value: C) async throws {
        // TODO: WAL append + memtable apply.
        fatalError("Unimplemented: put")
    }

    public func get(id: C.ID, snapshot: Snapshot? = nil) async throws -> C? {
        // TODO: probe memtable + SSTables using MVCC snapshot if provided.
        return nil
    }

    public func delete(id: C.ID) async throws {
        // TODO: tombstone in WAL and memtable.
        fatalError("Unimplemented: delete")
    }

    public func byIndex(_ name: String, equals key: String, snapshot: Snapshot? = nil) async throws -> [C] {
        // TODO: lookup in secondary index collection.
        return []
    }

    public func scan(prefix: Data? = nil, limit: Int = 100, snapshot: Snapshot? = nil) async throws -> [C] {
        // TODO: ordered iteration across memtable + SSTables.
        return []
    }
}
