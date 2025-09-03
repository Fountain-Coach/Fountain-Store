
//
//  Memtable.swift
//  FountainStoreCore
//
//  In-memory sorted map backing the write buffer before flush to SSTables.
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
    public typealias FlushCallback = @Sendable ([MemtableEntry]) async -> Void

    private var entries: [MemtableEntry] = []
    private var callbacks: [FlushCallback] = []

    /// Capacity limit after which a flush should be triggered.
    public let limit: Int

    public init(limit: Int = 1024) { self.limit = limit }

    public func put(_ e: MemtableEntry) async {
        entries.append(e)
        // Maintain entries sorted by key using a full array sort.
        entries.sort { $0.key.lexicographicallyPrecedes($1.key) }
    }

    public func get(_ key: Data) async -> MemtableEntry? {
        // Linear scan over the sorted array; efficient enough for small tables.
        return entries.last(where: { $0.key == key })
    }

    public func scan(prefix: Data?) async -> [MemtableEntry] {
        guard let p = prefix else { return entries }
        return entries.filter { $0.key.starts(with: p) }
    }

    /// Returns true when the number of entries exceeds the configured limit.
    public func isOverLimit() async -> Bool {
        return entries.count > limit
    }

    public func drain() async -> [MemtableEntry] {
        let out = entries
        entries.removeAll(keepingCapacity: true)
        return out
    }

    /// Register a callback that is invoked after the memtable was flushed.
    public func onFlush(_ cb: @escaping FlushCallback) {
        callbacks.append(cb)
    }

    /// Invoke all registered flush callbacks with the drained entries.
    public func fireFlushCallbacks(_ drained: [MemtableEntry]) async {
        for cb in callbacks { await cb(drained) }
    }
}
