
//
//  WAL.swift
//  FountainStoreCore
//
//  Write‑Ahead Log with CRC and fsync boundaries.
//

import Foundation

public struct WALRecord: Sendable {
    public let sequence: UInt64
    public let payload: Data
    public let crc32: UInt32
    public init(sequence: UInt64, payload: Data, crc32: UInt32) {
        self.sequence = sequence
        self.payload = payload
        self.crc32 = crc32
    }
}

public actor WAL {
    public init(path: URL) {
        self.path = path
    }
    public func append(_ rec: WALRecord) async throws {
        // TODO: append + flush; compute and verify CRC.
        // Placeholder no-op for now.
    }
    public func sync() async throws {
        // TODO: fsync file descriptor.
    }
    public func replay() async throws -> [WALRecord] {
        // TODO: read and validate; stop at first bad record.
        return []
    }
    // MARK: - Internals
    private let path: URL
}
