
//
//  BloomFilter.swift
//  FountainStoreCore
//
//  Simple Bloom filter for fast negative lookups.
//

import Foundation

public struct BloomFilter: Sendable {
    private var bits: [UInt64]
    private let k: Int
    public init(bitCount: Int, hashes: Int) {
        self.bits = Array(repeating: 0, count: max(1, bitCount / 64))
        self.k = max(1, hashes)
    }
    public init(bits: [UInt64], hashes: Int) {
        self.bits = bits
        self.k = max(1, hashes)
    }
    public mutating func insert(_ data: Data) {
        for i in 0..<k { set(bit: idx(data, i)) }
    }
    public func mayContain(_ data: Data) -> Bool {
        for i in 0..<k { if !get(bit: idx(data, i)) { return false } }
        return true
    }
    public func serialize() -> Data {
        var out = Data()
        var kLE = UInt64(k).littleEndian
        var bitCntLE = UInt64(bits.count * 64).littleEndian
        out.append(Data(bytes: &kLE, count: 8))
        out.append(Data(bytes: &bitCntLE, count: 8))
        for b in bits { var le = b.littleEndian; out.append(Data(bytes: &le, count: 8)) }
        return out
    }
    public static func deserialize(_ data: Data) throws -> BloomFilter {
        guard data.count >= 16 else { return BloomFilter(bitCount: 64, hashes: 1) }
        let k = Int(data[0..<8].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian)
        let bitCnt = Int(data[8..<16].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian)
        let wordCnt = max(0, (data.count - 16) / 8)
        var bits: [UInt64] = []
        bits.reserveCapacity(wordCnt)
        for i in 0..<wordCnt {
            let start = 16 + i*8
            let val = data[start..<start+8].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian
            bits.append(val)
        }
        // bitCnt currently unused but reserved for integrity checks / shape verification
        _ = bitCnt
        return BloomFilter(bits: bits, hashes: k)
    }
    // MARK: - Internals (toy hash; replace later)
    private func idx(_ d: Data, _ i: Int) -> Int {
        var h: UInt64 = 1469598103934665603 &+ UInt64(i)
        for b in d { h = (h ^ UInt64(b)) &* 1099511628211 }
        return Int(h % UInt64(bits.count * 64))
    }
    private mutating func set(bit: Int) { bits[bit/64] |= (1 << UInt64(bit%64)) }
    private func get(bit: Int) -> Bool { (bits[bit/64] & (1 << UInt64(bit%64))) != 0 }
}
