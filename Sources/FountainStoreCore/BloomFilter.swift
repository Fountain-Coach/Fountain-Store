
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
    public mutating func insert(_ data: Data) {
        for i in 0..<k { set(bit: idx(data, i)) }
    }
    public func mayContain(_ data: Data) -> Bool {
        for i in 0..<k { if !get(bit: idx(data, i)) { return false } }
        return true
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
