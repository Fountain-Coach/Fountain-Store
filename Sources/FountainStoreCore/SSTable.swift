
//
//  SSTable.swift
//  FountainStoreCore
//
//  Immutable sorted table files with block index and bloom filter.
//

import Foundation

public struct SSTableHandle: Sendable, Hashable {
    public let id: UUID
    public let path: URL
    public init(id: UUID, path: URL) {
        self.id = id; self.path = path
    }
}

public struct TableKey: Sendable, Hashable, Comparable {
    public let raw: Data
    public static func < (lhs: TableKey, rhs: TableKey) -> Bool { lhs.raw.lexicographicallyPrecedes(rhs.raw) }
}

public struct TableValue: Sendable, Hashable {
    public let raw: Data
}

public enum SSTableError: Error { case corrupt, notFound }

public actor SSTable {
    public static func create(at url: URL, entries: [(TableKey, TableValue)]) async throws -> SSTableHandle {
        // TODO: write blocks + index + footer.
        return SSTableHandle(id: UUID(), path: url)
    }
    public static func get(_ handle: SSTableHandle, key: TableKey) async throws -> TableValue? {
        // TODO: binary search via block index; bloom precheck.
        return nil
    }
}
