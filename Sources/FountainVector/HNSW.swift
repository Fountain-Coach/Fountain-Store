
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

    public enum DistanceMetric: Sendable {
        case l2
        case cosine
    }

    /// Returns the `k` nearest identifiers to the query using the specified metric.
    /// Defaults to L2 distance when no metric is provided.
    public func search(_ query: [Double], k: Int, metric: DistanceMetric = .l2) -> [String] {
        var scored: [(String, Double)] = []
        scored.reserveCapacity(vectors.count)
        for (id, vec) in vectors {
            guard vec.count == query.count else { continue }
            let dist = distance(query, vec, metric: metric)
            scored.append((id, dist))
        }
        scored.sort { $0.1 < $1.1 }
        return scored.prefix(k).map { $0.0 }
    }

    private func distance(_ a: [Double], _ b: [Double], metric: DistanceMetric) -> Double {
        switch metric {
        case .l2:
            var sum = 0.0
            for i in 0..<a.count {
                let d = a[i] - b[i]
                sum += d * d
            }
            return sum
        case .cosine:
            return 1.0 - cosine(a, b)
        }
    }

    private func cosine(_ a: [Double], _ b: [Double]) -> Double {
        var dot = 0.0
        var na = 0.0
        var nb = 0.0
        for i in 0..<a.count {
            dot += a[i] * b[i]
            na += a[i] * a[i]
            nb += b[i] * b[i]
        }
        let denom = (na.squareRoot() * nb.squareRoot())
        if denom == 0 { return 0 }
        return dot / denom
    }
}
