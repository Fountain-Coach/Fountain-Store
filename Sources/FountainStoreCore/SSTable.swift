
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
        FileManager.default.createFile(atPath: url.path, contents: nil)
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
            try fh.write(contentsOf: currentBlock)
            blockIndex.append((first, offset, UInt64(currentBlock.count)))
            offset += UInt64(currentBlock.count)
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
        var bloomData = Data()
        do {
            // Extract internal representation via reflection.
            let mirror = Mirror(reflecting: bloom)
            var bits: [UInt64] = []
            var kValue: Int = hashCount
            for child in mirror.children {
                if child.label == "bits" { bits = child.value as? [UInt64] ?? [] }
                if child.label == "k" { kValue = child.value as? Int ?? hashCount }
            }
            var kLE = UInt64(kValue).littleEndian
            var bitCntLE = UInt64(bitCount).littleEndian
            bloomData.append(Data(bytes: &kLE, count: 8))
            bloomData.append(Data(bytes: &bitCntLE, count: 8))
            for var b in bits { var le = b.littleEndian; bloomData.append(Data(bytes: &le, count: 8)) }
            try fh.write(contentsOf: bloomData)
        }
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
        let iOff = footer[0..<8].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
        let iSize = footer[8..<16].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
        let bOff = footer[16..<24].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
        let bSize = footer[24..<32].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian

        // Load bloom filter and quickly reject missing keys.
        try fh.seek(toOffset: bOff)
        guard let bData = try fh.read(upToCount: Int(bSize)), bData.count == bSize else {
            throw SSTableError.corrupt
        }
        if bData.count >= 16 { // deserialize bloom filter
            let k = Int(bData[0..<8].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian)
            let bitCnt = Int(bData[8..<16].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian)
            let wordCnt = max(0, (bData.count - 16) / 8)
            var bits: [UInt64] = []
            bits.reserveCapacity(wordCnt)
            for i in 0..<wordCnt {
                let start = 16 + i*8
                let val = bData[start..<start+8].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
                bits.append(val)
            }
            _ = bitCnt // currently unused but reserved for integrity checks
            let bitCount = bits.count * 64
            func idx(_ d: Data, _ i: Int) -> Int {
                var h: UInt64 = 1469598103934665603 &+ UInt64(i)
                for b in d { h = (h ^ UInt64(b)) &* 1099511628211 }
                return Int(h % UInt64(bitCount))
            }
            func test(_ data: Data) -> Bool {
                func get(_ bit: Int) -> Bool {
                    (bits[bit/64] & (1 << UInt64(bit%64))) != 0
                }
                for i in 0..<k { if !get(idx(data, i)) { return false } }
                return true
            }
            if !test(key.raw) { return nil }
        }

        // Read block index into memory.
        try fh.seek(toOffset: iOff)
        guard let iData = try fh.read(upToCount: Int(iSize)), iData.count == iSize else {
            throw SSTableError.corrupt
        }
        var cursor = iData.startIndex
        guard iData.count - cursor >= 4 else { throw SSTableError.corrupt }
        let blockCount = Int(iData[cursor..<(cursor+4)].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
        cursor += 4
        var blocks: [(Data, UInt64, UInt64)] = []
        blocks.reserveCapacity(blockCount)
        for _ in 0..<blockCount {
            guard iData.count - cursor >= 4 else { throw SSTableError.corrupt }
            let klen = Int(iData[cursor..<(cursor+4)].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
            cursor += 4
            guard iData.count - cursor >= klen + 16 else { throw SSTableError.corrupt }
            let firstKey = iData[cursor..<(cursor+klen)]
            cursor += klen
            let blkOff = iData[cursor..<(cursor+8)].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
            cursor += 8
            let blkSize = iData[cursor..<(cursor+8)].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
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
        guard let blockData = try fh.read(upToCount: Int(blkSize)), blockData.count == blkSize else {
            throw SSTableError.corrupt
        }
        var p = blockData.startIndex
        while p < blockData.endIndex {
            guard blockData.count - p >= 4 else { break }
            let klen = Int(blockData[p..<(p+4)].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
            p += 4
            guard blockData.count - p >= klen else { break }
            let kdata = blockData[p..<(p+klen)]
            p += klen
            guard blockData.count - p >= 4 else { break }
            let vlen = Int(blockData[p..<(p+4)].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
            p += 4
            guard blockData.count - p >= vlen else { break }
            let vdata = blockData[p..<(p+vlen)]
            p += vlen
            if Data(kdata) == target { return TableValue(raw: Data(vdata)) }
        }
        return nil
    }
}
