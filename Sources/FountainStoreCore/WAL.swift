
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
    public init(path: URL, rotateBytes: Int = 0) {
        self.path = path
        self.rotateBytes = max(0, rotateBytes)
        self.dir = path.deletingLastPathComponent()
        let base = path.deletingPathExtension().lastPathComponent // e.g., "wal"
        self.basePrefix = base
        self.activeName = path.lastPathComponent
        self.segmentPattern = base + ".%06d.log"
        self.segmentIndex = 1 + Self.discoverMaxIndex(in: dir, base: base)
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
        try maybeRotate()
    }
    public func sync() async throws {
        if let h = handle {
            try h.synchronize()
        }
    }
    public func replay() async throws -> [WALRecord] {
        var files: [URL] = []
        let fm = FileManager.default
        if let items = try? fm.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) {
            let segs = items.filter { $0.lastPathComponent.hasPrefix(basePrefix + ".") && $0.pathExtension == "log" && $0.lastPathComponent != activeName }
                .sorted { $0.lastPathComponent < $1.lastPathComponent }
            files.append(contentsOf: segs)
        }
        // Add active file last
        let active = dir.appendingPathComponent(activeName)
        if fm.fileExists(atPath: active.path) { files.append(active) }
        var res: [WALRecord] = []
        for f in files {
            let data = try Data(contentsOf: f)
            var offset = 0
            while offset + 16 <= data.count {
                let seq = UInt64(bigEndian: data[offset..<(offset+8)].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) })
                offset += 8
                let len = UInt32(bigEndian: data[offset..<(offset+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) })
                offset += 4
                if offset + Int(len) + 4 > data.count { break }
                let payload = data[offset..<(offset+Int(len))]
                offset += Int(len)
                let stored = UInt32(bigEndian: data[offset..<(offset+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) })
                offset += 4
                if crc32(Data(payload)) != stored { break }
                res.append(WALRecord(sequence: seq, payload: Data(payload), crc32: stored))
            }
        }
        return res
    }
    public func gc(manifestSequence: UInt64) async {
        let fm = FileManager.default
        guard let items = try? fm.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) else { return }
        for f in items where f.lastPathComponent.hasPrefix(basePrefix + ".") && f.pathExtension == "log" {
            // compute max seq in this segment
            guard let data = try? Data(contentsOf: f) else { continue }
            var offset = 0
            var maxSeq: UInt64 = 0
            while offset + 16 <= data.count {
                let seq = UInt64(bigEndian: data[offset..<(offset+8)].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) })
                offset += 8
                let len = Int(UInt32(bigEndian: data[offset..<(offset+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }))
                offset += 4
                if offset + len + 4 > data.count { break }
                offset += len + 4
                if seq > maxSeq { maxSeq = seq }
            }
            if maxSeq <= manifestSequence {
                try? fm.removeItem(at: f)
            }
        }
    }
    // MARK: - Internals
    private let path: URL
    private let dir: URL
    private let basePrefix: String
    private let activeName: String
    private let segmentPattern: String
    private var segmentIndex: Int
    private let rotateBytes: Int
    private var handle: FileHandle?

    private func ensureHandle() throws -> FileHandle {
        if let h = handle { return h }
        let fm = FileManager.default
        let active = dir.appendingPathComponent(activeName)
        if !fm.fileExists(atPath: active.path) {
            _ = fm.createFile(atPath: active.path, contents: nil)
        }
        let h = try FileHandle(forUpdating: active)
        try h.seekToEnd()
        handle = h
        return h
    }

    private func maybeRotate() throws {
        guard rotateBytes > 0 else { return }
        let active = dir.appendingPathComponent(activeName)
        guard let attrs = try? FileManager.default.attributesOfItem(atPath: active.path) else { return }
        if let size = (attrs[.size] as? NSNumber)?.intValue, size >= rotateBytes {
            try handle?.close()
            handle = nil
            let segName = String(format: segmentPattern, segmentIndex)
            segmentIndex += 1
            let dst = dir.appendingPathComponent(segName)
            try FileManager.default.moveItem(at: active, to: dst)
            _ = FileManager.default.createFile(atPath: active.path, contents: nil)
        }
    }

    private static func discoverMaxIndex(in dir: URL, base: String) -> Int {
        let fm = FileManager.default
        guard let items = try? fm.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) else { return 0 }
        var maxIdx = 0
        for u in items {
            let name = u.lastPathComponent
            if name.hasPrefix(base + "."), name.hasSuffix(".log") {
                let mid = name.dropFirst(base.count + 1).dropLast(4)
                if let idx = Int(mid) { if idx > maxIdx { maxIdx = idx } }
            }
        }
        return maxIdx
    }
}

public enum WALError: Error { case crcMismatch }
