
//
//  HNSW.swift
//  FountainVector
//
//  Simple vector index with linear scan search.
//  Placeholder for pure‑Swift HNSW (to be added in M5).

import Foundation

public struct HNSWIndex: Sendable, Hashable {
    private var vectors: [String: [Double]] = [:]

    public init() {}

    /// Inserts or replaces a vector for the given identifier.
    public mutating func add(id: String, vector: [Double]) {
        vectors[id] = vector
    }

    /// Removes a vector from the index.
    public mutating func remove(id: String) {
        vectors[id] = nil
    }

    /// Returns the `k` nearest identifiers to the query using L2 distance.
    public func search(_ query: [Double], k: Int) -> [String] {
        var scored: [(String, Double)] = []
        scored.reserveCapacity(vectors.count)
        for (id, vec) in vectors {
            guard vec.count == query.count else { continue }
            let dist = l2(query, vec)
            scored.append((id, dist))
        }
        scored.sort { $0.1 < $1.1 }
        return scored.prefix(k).map { $0.0 }
    }

    private func l2(_ a: [Double], _ b: [Double]) -> Double {
        var sum = 0.0
        for i in 0..<a.count {
            let d = a[i] - b[i]
            sum += d * d
        }
        return sum
    }
}
