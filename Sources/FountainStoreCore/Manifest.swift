
//
//  Manifest.swift
//  FountainStoreCore
//
//  Tracks live SSTables and global sequence numbers.
//

import Foundation

public struct Manifest: Codable, Sendable {
    public var sequence: UInt64
    public var tables: [UUID: URL]
    public init(sequence: UInt64 = 0, tables: [UUID: URL] = [:]) {
        self.sequence = sequence
        self.tables = tables
    }
}

public enum ManifestError: Error { case corrupt }

public actor ManifestStore {
    private let url: URL
    public init(url: URL) { self.url = url }
    public func load() async throws -> Manifest {
        // TODO: load or initialize new manifest
        return Manifest()
    }
    public func save(_ m: Manifest) async throws {
        // TODO: atomic write: write temp then rename.
    }
}
