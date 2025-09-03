
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

        // MARK: Read footer
        let fileSize = try fh.seekToEnd()
        guard fileSize >= 32 else { throw SSTableError.corrupt }
        try fh.seek(toOffset: fileSize - 32)
        guard let footerData = try fh.read(upToCount: 32), footerData.count == 32 else {
            throw SSTableError.corrupt
        }
        func readUInt64(_ data: Data, _ start: Int) -> UInt64 {
            data.withUnsafeBytes { $0.load(fromByteOffset: start, as: UInt64.self) }.littleEndian
        }
        let indexOffset = readUInt64(footerData, 0)
        let indexSize = readUInt64(footerData, 8)
        let bloomOffset = readUInt64(footerData, 16)
        let bloomSize = readUInt64(footerData, 24)

        // MARK: - Bloom filter precheck (optional)
        if bloomSize > 0 {
            try fh.seek(toOffset: bloomOffset)
            guard let bloomData = try fh.read(upToCount: Int(bloomSize)), bloomData.count == Int(bloomSize) else {
                throw SSTableError.corrupt
            }
            var cursor = bloomData.startIndex
            func readNextUInt64() -> UInt64? {
                guard cursor + 8 <= bloomData.endIndex else { return nil }
                defer { cursor += 8 }
                return bloomData[cursor..<cursor+8].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
            }
            guard let kVal = readNextUInt64(), let _ = readNextUInt64() else {
                throw SSTableError.corrupt
            }
            var bits: [UInt64] = []
            while cursor < bloomData.endIndex {
                guard let v = readNextUInt64() else { throw SSTableError.corrupt }
                bits.append(v)
            }
            let k = Int(kVal)
            // Local bloom filter check mirroring BloomFilter.mayContain
            func hashIdx(_ data: Data, _ i: Int) -> Int {
                var h: UInt64 = 1469598103934665603 &+ UInt64(i)
                for b in data { h = (h ^ UInt64(b)) &* 1099511628211 }
                return Int(h % UInt64(bits.count * 64))
            }
            func bitSet(_ bit: Int) -> Bool {
                (bits[bit/64] & (1 << UInt64(bit%64))) != 0
            }
            var mayContain = true
            for i in 0..<k { if !bitSet(hashIdx(key.raw, i)) { mayContain = false; break } }
            if !mayContain { return nil }
        }

        // MARK: - Read block index
        try fh.seek(toOffset: indexOffset)
        guard let idxData = try fh.read(upToCount: Int(indexSize)), idxData.count == Int(indexSize) else {
            throw SSTableError.corrupt
        }
        var i = 0
        guard idxData.count >= 4 else { throw SSTableError.corrupt }
        let blockCount = Int(idxData[i..<i+4].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
        i += 4
        var blocks: [(Data, UInt64, UInt64)] = []
        blocks.reserveCapacity(blockCount)
        for _ in 0..<blockCount {
            guard i + 4 <= idxData.count else { throw SSTableError.corrupt }
            let klen = Int(idxData[i..<i+4].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
            i += 4
            guard i + klen <= idxData.count else { throw SSTableError.corrupt }
            let firstKey = Data(idxData[i..<i+klen])
            i += klen
            guard i + 16 <= idxData.count else { throw SSTableError.corrupt }
            let off = idxData[i..<i+8].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
            i += 8
            let len = idxData[i..<i+8].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian
            i += 8
            blocks.append((firstKey, off, len))
        }

        // MARK: - Binary search block index
        guard !blocks.isEmpty else { return nil }
        var lo = 0
        var hi = blocks.count - 1
        var candidate = -1
        while lo <= hi {
            let mid = (lo + hi) / 2
            let midKey = blocks[mid].0
            if midKey.lexicographicallyPrecedes(key.raw) || midKey == key.raw {
                candidate = mid
                lo = mid + 1
            } else {
                hi = mid - 1
            }
        }
        if candidate < 0 { return nil }
        let blockMeta = blocks[candidate]

        // MARK: - Load target block
        try fh.seek(toOffset: blockMeta.1)
        guard let blockData = try fh.read(upToCount: Int(blockMeta.2)), blockData.count == Int(blockMeta.2) else {
            throw SSTableError.corrupt
        }

        // MARK: - Search within block
        var pos = 0
        let target = key.raw
        while pos < blockData.count {
            guard pos + 4 <= blockData.count else { throw SSTableError.corrupt }
            let klen = Int(blockData[pos..<pos+4].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
            pos += 4
            guard pos + klen <= blockData.count else { throw SSTableError.corrupt }
            let kData = blockData[pos..<pos+klen]
            pos += klen
            guard pos + 4 <= blockData.count else { throw SSTableError.corrupt }
            let vlen = Int(blockData[pos..<pos+4].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
            pos += 4
            guard pos + vlen <= blockData.count else { throw SSTableError.corrupt }
            let vData = Data(blockData[pos..<pos+vlen])
            pos += vlen
            if kData.elementsEqual(target) {
                return TableValue(raw: vData)
            } else if target.lexicographicallyPrecedes(Data(kData)) {
                break // keys are sorted; early exit
            }
        }
        return nil
    }
}
