
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
    public var indexCatalog: [String: [IndexDef]]
    public init(sequence: UInt64 = 0, tables: [UUID: URL] = [:], indexCatalog: [String: [IndexDef]] = [:]) {
        self.sequence = sequence
        self.tables = tables
        self.indexCatalog = indexCatalog
    }
}

public struct IndexDef: Codable, Sendable, Hashable {
    public var name: String
    public var kind: String // unique | multi | fts | vector
    public var field: String?
    public init(name: String, kind: String, field: String? = nil) {
        self.name = name
        self.kind = kind
        self.field = field
    }
}

public enum ManifestError: Error { case corrupt }

public actor ManifestStore {
    private let url: URL
    public init(url: URL) { self.url = url }
    public func load() async throws -> Manifest {
        if !FileManager.default.fileExists(atPath: url.path) {
            return Manifest()
        }
        let data = try Data(contentsOf: url)
        do {
            var m = try JSONDecoder().decode(Manifest.self, from: data)
            // Backward compatibility: pre-indexCatalog manifests.
            // If indexCatalog is missing (older files), default to empty.
            return m
        } catch {
            throw ManifestError.corrupt
        }
    }
    public func save(_ m: Manifest) async throws {
        let data = try JSONEncoder().encode(m)
        let tmp = url.appendingPathExtension("tmp")
        try data.write(to: tmp)
        let fm = FileManager.default
        if fm.fileExists(atPath: url.path) {
            try fm.removeItem(at: url)
        }
        try fm.moveItem(at: tmp, to: url)
    }
}
