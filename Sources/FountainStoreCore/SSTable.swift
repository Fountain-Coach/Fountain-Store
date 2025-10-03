
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
    public init(raw: Data) { self.raw = raw }
    public static func < (lhs: TableKey, rhs: TableKey) -> Bool { lhs.raw.lexicographicallyPrecedes(rhs.raw) }
}

public struct TableValue: Sendable, Hashable {
    public let raw: Data
    public init(raw: Data) { self.raw = raw }
}

public enum SSTableError: Error { case corrupt, notFound }

public actor SSTable {
    // CRC32 utility (polynomial 0xEDB88320)
    private static let crc32Table: [UInt32] = {
        (0...255).map { i -> UInt32 in
            var c = UInt32(i)
            for _ in 0..<8 { c = (c & 1) == 1 ? (0xEDB88320 ^ (c >> 1)) : (c >> 1) }
            return c
        }
    }()
    private static func crc32(_ data: Data) -> UInt32 {
        var c: UInt32 = 0xFFFFFFFF
        for b in data { let idx = Int((c ^ UInt32(b)) & 0xFF); c = crc32Table[idx] ^ (c >> 8) }
        return c ^ 0xFFFFFFFF
    }
    /// Create an immutable sorted table file at `url` containing the provided
    /// key/value `entries`. Entries **must** already be sorted by key.
    ///
    /// Layout (sequential):
    /// ```
    /// [data blocks][block index][bloom filter][footer]
    /// ```
    ///
    /// - Each data block is at most `blockSize` bytes and contains a series of
    ///   length‑prefixed key/value pairs.
    /// - The block index stores the first key for every block together with the
    ///   file offset and length of the block, enabling binary search on read.
    /// - A simple Bloom filter is built while writing blocks and persisted after
    ///   the block index for fast negative lookups.
    public static func create(at url: URL, entries: [(TableKey, TableValue)]) async throws -> SSTableHandle {
        // Ensure the output file exists and open a handle for writing.
        _ = FileManager.default.createFile(atPath: url.path, contents: nil)
        let fh = try FileHandle(forWritingTo: url)
        defer { try? fh.close() }

        // Configuration.
        let blockSize = 4 * 1024 // 4KB blocks.

        // Index entries: (firstKey, offset, length)
        var blockIndex: [(Data, UInt64, UInt64)] = []

        // Optional bloom filter - size heuristically chosen.
        let bitCount = max(64, entries.count * 10)
        let hashCount = 3
        var bloom = BloomFilter(bitCount: bitCount, hashes: hashCount)

        // Writing state.
        var currentBlock = Data()
        var currentFirstKey: Data? = nil
        var offset: UInt64 = 0

        func flushCurrentBlock() throws {
            guard !currentBlock.isEmpty, let first = currentFirstKey else { return }
            // Append per-block CRC32 (little-endian)
            let crc = Self.crc32(currentBlock)
            var blockWithCRC = currentBlock
            var crcLE = crc.littleEndian
            blockWithCRC.append(Data(bytes: &crcLE, count: 4))
            try fh.write(contentsOf: blockWithCRC)
            blockIndex.append((first, offset, UInt64(blockWithCRC.count)))
            offset += UInt64(blockWithCRC.count)
            currentBlock.removeAll(keepingCapacity: true)
            currentFirstKey = nil
        }

        // Serialize entries into fixed size blocks.
        for (key, value) in entries {
            let keyData = key.raw
            let valueData = value.raw

            // Bloom filter insert while iterating.
            bloom.insert(keyData)

            // Encode entry (length‑prefixed key and value).
            var entry = Data()
            var klen = UInt32(keyData.count).littleEndian
            var vlen = UInt32(valueData.count).littleEndian
            entry.append(Data(bytes: &klen, count: 4))
            entry.append(keyData)
            entry.append(Data(bytes: &vlen, count: 4))
            entry.append(valueData)

            if currentFirstKey == nil { currentFirstKey = keyData }

            // If the block would overflow, flush first.
            if currentBlock.count + entry.count > blockSize && !currentBlock.isEmpty {
                try flushCurrentBlock()
                currentFirstKey = keyData
            }

            currentBlock.append(entry)
        }

        // Flush the last block if needed.
        try flushCurrentBlock()

        // Write block index.
        let indexOffset = offset
        var indexData = Data()
        var blockCount = UInt32(blockIndex.count).littleEndian
        indexData.append(Data(bytes: &blockCount, count: 4))
        for (firstKey, blkOffset, blkSize) in blockIndex {
            var klen = UInt32(firstKey.count).littleEndian
            indexData.append(Data(bytes: &klen, count: 4))
            indexData.append(firstKey)
            var o = UInt64(blkOffset).littleEndian
            var s = UInt64(blkSize).littleEndian
            indexData.append(Data(bytes: &o, count: 8))
            indexData.append(Data(bytes: &s, count: 8))
        }
        try fh.write(contentsOf: indexData)
        let indexSize = UInt64(indexData.count)
        offset += indexSize

        // Serialize bloom filter.
        let bloomOffset = offset
        let bloomData = bloom.serialize()
        try fh.write(contentsOf: bloomData)
        let bloomSize = UInt64(bloomData.count)
        offset += bloomSize

        // Footer with offsets/sizes.
        var footer = Data()
        var iOff = indexOffset.littleEndian
        var iSize = indexSize.littleEndian
        var bOff = bloomOffset.littleEndian
        var bSize = bloomSize.littleEndian
        footer.append(Data(bytes: &iOff, count: 8))
        footer.append(Data(bytes: &iSize, count: 8))
        footer.append(Data(bytes: &bOff, count: 8))
        footer.append(Data(bytes: &bSize, count: 8))
        try fh.write(contentsOf: footer)

        return SSTableHandle(id: UUID(), path: url)
    }

    /// Read all key/value pairs from an SSTable by iterating the block index
    /// and validating each block's CRC.
    public static func scan(_ handle: SSTableHandle) throws -> [(TableKey, TableValue)] {
        let fh = try FileHandle(forReadingFrom: handle.path)
        defer { try? fh.close() }
        // Read footer
        let fileSize = try fh.seekToEnd()
        guard fileSize >= 32 else { return [] }
        try fh.seek(toOffset: fileSize - 32)
        guard let footer = try fh.read(upToCount: 32), footer.count == 32 else { throw SSTableError.corrupt }
        let iOff = footer[0..<8].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
        let iSize = footer[8..<16].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
        // Load index
        try fh.seek(toOffset: iOff)
        guard let iData = try fh.read(upToCount: Int(iSize)), iData.count == iSize else { throw SSTableError.corrupt }
        var cursor = iData.startIndex
        guard iData.count - cursor >= 4 else { throw SSTableError.corrupt }
        let blockCount = Int(iData[cursor..<(cursor+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian)
        cursor += 4
        var blocks: [(Data, UInt64, UInt64)] = []
        blocks.reserveCapacity(blockCount)
        for _ in 0..<blockCount {
            guard iData.count - cursor >= 4 else { throw SSTableError.corrupt }
            let klen = Int(iData[cursor..<(cursor+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian)
            cursor += 4
            guard iData.count - cursor >= klen + 16 else { throw SSTableError.corrupt }
            let firstKey = iData[cursor..<(cursor+klen)]
            cursor += klen
            let blkOff = iData[cursor..<(cursor+8)].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
            cursor += 8
            let blkSize = iData[cursor..<(cursor+8)].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
            cursor += 8
            blocks.append((Data(firstKey), blkOff, blkSize))
        }
        var res: [(TableKey, TableValue)] = []
        for (_, blkOff, blkSize) in blocks {
            try fh.seek(toOffset: blkOff)
            guard let blockData = try fh.read(upToCount: Int(blkSize)), blockData.count == blkSize else { throw SSTableError.corrupt }
            guard blockData.count >= 4 else { throw SSTableError.corrupt }
            let payload = blockData[..<(blockData.count - 4)]
            let stored = blockData[(blockData.count - 4)..<blockData.count].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian
            let crc = Self.crc32(Data(payload))
            if crc != stored { throw SSTableError.corrupt }
            var p = payload.startIndex
            while p < payload.endIndex {
                guard payload.count - p >= 4 else { break }
                let klen = Int(payload[p..<(p+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian)
                p += 4
                guard payload.count - p >= klen else { break }
                let kdata = payload[p..<(p+klen)]
                p += klen
                guard payload.count - p >= 4 else { break }
                let vlen = Int(payload[p..<(p+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian)
                p += 4
                guard payload.count - p >= vlen else { break }
                let vdata = payload[p..<(p+vlen)]
                p += vlen
                res.append((TableKey(raw: Data(kdata)), TableValue(raw: Data(vdata))))
            }
        }
        return res
    }
    public static func get(_ handle: SSTableHandle, key: TableKey) async throws -> TableValue? {
        let fh = try FileHandle(forReadingFrom: handle.path)
        defer { try? fh.close() }

        // Read footer to locate index and bloom filter.
        let fileSize = try fh.seekToEnd()
        guard fileSize >= 32 else { throw SSTableError.corrupt }
        try fh.seek(toOffset: fileSize - 32)
        guard let footer = try fh.read(upToCount: 32), footer.count == 32 else {
            throw SSTableError.corrupt
        }
        let iOff = footer[0..<8].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
        let iSize = footer[8..<16].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
        let bOff = footer[16..<24].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
        let bSize = footer[24..<32].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian

        // Load bloom filter and quickly reject missing keys.
        try fh.seek(toOffset: bOff)
        guard let bData = try fh.read(upToCount: Int(bSize)), bData.count == bSize else { throw SSTableError.corrupt }
        if bData.count >= 16 {
            let bloom = try BloomFilter.deserialize(bData)
            if !bloom.mayContain(key.raw) { return nil }
        }

        // Read block index into memory.
        try fh.seek(toOffset: iOff)
        guard let iData = try fh.read(upToCount: Int(iSize)), iData.count == iSize else {
            throw SSTableError.corrupt
        }
        var cursor = iData.startIndex
        guard iData.count - cursor >= 4 else { throw SSTableError.corrupt }
        let blockCount = Int(iData[cursor..<(cursor+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian)
        cursor += 4
        var blocks: [(Data, UInt64, UInt64)] = []
        blocks.reserveCapacity(blockCount)
        for _ in 0..<blockCount {
            guard iData.count - cursor >= 4 else { throw SSTableError.corrupt }
            let klen = Int(iData[cursor..<(cursor+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian)
            cursor += 4
            guard iData.count - cursor >= klen + 16 else { throw SSTableError.corrupt }
            let firstKey = iData[cursor..<(cursor+klen)]
            cursor += klen
            let blkOff = iData[cursor..<(cursor+8)].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
            cursor += 8
            let blkSize = iData[cursor..<(cursor+8)].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
            cursor += 8
            blocks.append((Data(firstKey), blkOff, blkSize))
        }

        // Binary search block index for candidate block.
        guard !blocks.isEmpty else { return nil }
        let target = key.raw
        var lo = 0
        var hi = blocks.count
        while lo < hi {
            let mid = (lo + hi) / 2
            if blocks[mid].0.lexicographicallyPrecedes(target) {
                lo = mid + 1
            } else {
                hi = mid
            }
        }
        var idx = lo
        if idx == blocks.count { idx = blocks.count - 1 }
        else if blocks[idx].0 != target { if idx == 0 { return nil } else { idx -= 1 } }
        let (blkKey, blkOff, blkSize) = blocks[idx]
        // If first key of block is greater than target, no match.
        if blkKey.lexicographicallyPrecedes(target) == false && blkKey != target && idx == 0 {
            return nil
        }

        // Read block and scan entries.
        try fh.seek(toOffset: blkOff)
        guard let blockData = try fh.read(upToCount: Int(blkSize)), blockData.count == blkSize else { throw SSTableError.corrupt }
        guard blockData.count >= 4 else { throw SSTableError.corrupt }
        let payload = blockData[..<(blockData.count - 4)]
        let stored = blockData[(blockData.count - 4)..<blockData.count].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian
        let crc = Self.crc32(Data(payload))
        if crc != stored { throw SSTableError.corrupt }
        var p = payload.startIndex
        while p < payload.endIndex {
            guard blockData.count - p >= 4 else { break }
            let klen = Int(payload[p..<(p+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian)
            p += 4
            guard payload.count - p >= klen else { break }
            let kdata = payload[p..<(p+klen)]
            p += klen
            guard blockData.count - p >= 4 else { break }
            let vlen = Int(payload[p..<(p+4)].withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }.littleEndian)
            p += 4
            guard payload.count - p >= vlen else { break }
            let vdata = payload[p..<(p+vlen)]
            p += vlen
            if Data(kdata) == target { return TableValue(raw: Data(vdata)) }
        }
        return nil
    }
}
