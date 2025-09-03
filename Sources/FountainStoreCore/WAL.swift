
//
//  WAL.swift
//  FountainStoreCore
//
//  Write‑Ahead Log with CRC and fsync boundaries.
//

import Foundation

// Precomputed CRC32 table for polynomial 0xEDB88320
private let crc32Table: [UInt32] = {
    (0...255).map { i -> UInt32 in
        var c = UInt32(i)
        for _ in 0..<8 {
            if c & 1 == 1 {
                c = 0xEDB88320 ^ (c >> 1)
            } else {
                c = c >> 1
            }
        }
        return c
    }
}()

private func crc32(_ data: Data) -> UInt32 {
    var c: UInt32 = 0xFFFFFFFF
    for b in data {
        let idx = Int((c ^ UInt32(b)) & 0xFF)
        c = crc32Table[idx] ^ (c >> 8)
    }
    return c ^ 0xFFFFFFFF
}

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
        let handle = try ensureHandle()
        let crc = crc32(rec.payload)
        if rec.crc32 != 0 && rec.crc32 != crc {
            throw WALError.crcMismatch
        }
        var data = Data()
        var seq = rec.sequence.bigEndian
        var len = UInt32(rec.payload.count).bigEndian
        var crcBE = crc.bigEndian
        data.append(Data(bytes: &seq, count: MemoryLayout<UInt64>.size))
        data.append(Data(bytes: &len, count: MemoryLayout<UInt32>.size))
        data.append(rec.payload)
        data.append(Data(bytes: &crcBE, count: MemoryLayout<UInt32>.size))
        try handle.write(contentsOf: data)
    }
    public func sync() async throws {
        if let h = handle {
            try h.synchronize()
        }
    }
    public func replay() async throws -> [WALRecord] {
        guard FileManager.default.fileExists(atPath: path.path) else { return [] }
        let data = try Data(contentsOf: path)
        var offset = 0
        var res: [WALRecord] = []
        while offset + 16 <= data.count {
            let seq = UInt64(bigEndian: data[offset..<(offset+8)].withUnsafeBytes { $0.load(as: UInt64.self) })
            offset += 8
            let len = UInt32(bigEndian: data[offset..<(offset+4)].withUnsafeBytes { $0.load(as: UInt32.self) })
            offset += 4
            if offset + Int(len) + 4 > data.count { break }
            let payload = data[offset..<(offset+Int(len))]
            offset += Int(len)
            let stored = UInt32(bigEndian: data[offset..<(offset+4)].withUnsafeBytes { $0.load(as: UInt32.self) })
            offset += 4
            if crc32(Data(payload)) != stored { break }
            res.append(WALRecord(sequence: seq, payload: Data(payload), crc32: stored))
        }
        return res
    }
    // MARK: - Internals
    private let path: URL
    private var handle: FileHandle?

    private func ensureHandle() throws -> FileHandle {
        if let h = handle { return h }
        let fm = FileManager.default
        if !fm.fileExists(atPath: path.path) {
            _ = fm.createFile(atPath: path.path, contents: nil)
        }
        let h = try FileHandle(forUpdating: path)
        try h.seekToEnd()
        handle = h
        return h
    }
}

public enum WALError: Error { case crcMismatch }
