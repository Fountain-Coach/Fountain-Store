
//
//  Memtable.swift
//  FountainStoreCore
//
//  In‑memory sorted map (placeholder implementation).
//

import Foundation

public struct MemtableEntry: Sendable, Hashable {
    public let key: Data
    public let value: Data?
    public let sequence: UInt64
    public init(key: Data, value: Data?, sequence: UInt64) {
        self.key = key; self.value = value; self.sequence = sequence
    }
}

public actor Memtable {
    private var entries: [MemtableEntry] = []
    public init() {}
    public func put(_ e: MemtableEntry) async {
        entries.append(e)
        // Keep it sorted by key for now; optimize later.
        entries.sort { $0.key.lexicographicallyPrecedes($1.key) }
    }
    public func get(_ key: Data) async -> MemtableEntry? {
        // Linear for now; replace with binary search later.
        return entries.last(where: { $0.key == key })
    }
    public func scan(prefix: Data?) async -> [MemtableEntry] {
        guard let p = prefix else { return entries }
        return entries.filter { $0.key.starts(with: p) }
    }
    public func isOverSize(limit: Int) async -> Bool {
        return entries.count > limit
    }
    public func drain() async -> [MemtableEntry] {
        let out = entries
        entries.removeAll(keepingCapacity: true)
        return out
    }
}
