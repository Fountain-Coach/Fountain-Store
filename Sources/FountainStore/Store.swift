
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

    fileprivate func nextSequence() -> UInt64 {
        sequence &+= 1
        return sequence
    }

    private init(options: StoreOptions) { self.options = options }
}

public actor Collection<C: Codable & Identifiable> where C.ID: Codable & Hashable {
    public let name: String
    private let store: FountainStore
    private var data: [C.ID: [(UInt64, C?)]] = [:]

    public init(name: String, store: FountainStore) {
        self.name = name
        self.store = store
    }

    public func define(_ index: Index<C>) async throws {
        // TODO: register index in manifest and create structures.
    }

    public func put(_ value: C) async throws {
        let seq = await store.nextSequence()
        data[value.id, default: []].append((seq, value))
    }

    public func get(id: C.ID, snapshot: Snapshot? = nil) async throws -> C? {
        guard let versions = data[id] else { return nil }
        let limit = snapshot?.sequence ?? UInt64.max
        return versions.last(where: { $0.0 <= limit })?.1
    }

    public func history(id: C.ID, snapshot: Snapshot? = nil) async throws -> [(UInt64, C?)] {
        guard let versions = data[id] else { return [] }
        let limit = snapshot?.sequence ?? UInt64.max
        return versions.filter { $0.0 <= limit }
    }

    public func delete(id: C.ID) async throws {
        let seq = await store.nextSequence()
        data[id, default: []].append((seq, nil))
    }

    public func byIndex(_ name: String, equals key: String, snapshot: Snapshot? = nil) async throws -> [C] {
        return []
    }

    public func scan(prefix: Data? = nil, limit: Int = 100, snapshot: Snapshot? = nil) async throws -> [C] {
        return []
    }
}
