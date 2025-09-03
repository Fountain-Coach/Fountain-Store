
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
        if !FileManager.default.fileExists(atPath: url.path) {
            return Manifest()
        }
        let data = try Data(contentsOf: url)
        do {
            return try JSONDecoder().decode(Manifest.self, from: data)
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
