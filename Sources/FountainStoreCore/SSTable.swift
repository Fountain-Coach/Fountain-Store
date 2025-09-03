
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
        // TODO: binary search via block index; bloom precheck.
        return nil
    }
}
