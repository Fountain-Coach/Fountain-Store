//
//  HNSW.swift
//  FountainVector
//
//  Baseline pure‑Swift implementation of a hierarchical
//  navigable small world (HNSW) index supporting L2 and
//  cosine distance metrics. This is a simple variant that
//  uses deterministic levels and global neighbor selection
//  to keep the structure deterministic for tests.

import Foundation

/// A lightweight HNSW index for approximate nearest neighbour search.
/// Each vector is assigned a deterministic level based on its identifier.
/// Neighbor links are bidirectional and truncated to a small fixed fanout.
public struct HNSWIndex: Sendable, Hashable {
    private struct Node: Sendable, Hashable {
        var vector: [Double]
        var neighbors: [[String]] // per level
    }

    private var nodes: [String: Node] = [:]
    private var entry: String?
    private var maxLevel: Int = 0
    private let maxNeighbors = 4

    public init() {}

    /// Inserts or replaces a vector for the given identifier.
    /// Links are created to the closest existing nodes on each level.
    public mutating func add(id: String, vector: [Double]) {
        let level = levelForID(id)
        var node = Node(vector: vector, neighbors: Array(repeating: [], count: level + 1))
        if nodes.isEmpty {
            nodes[id] = node
            entry = id
            maxLevel = level
            return
        }

        for l in 0...level {
            var candidates: [(String, Double)] = []
            candidates.reserveCapacity(nodes.count)
            for (oid, onode) in nodes where onode.neighbors.count > l {
                let dist = distance(vector, onode.vector, metric: .l2)
                candidates.append((oid, dist))
            }
            candidates.sort { $0.1 < $1.1 }
            let neigh = candidates.prefix(maxNeighbors).map { $0.0 }
            node.neighbors[l] = Array(neigh)
            for nid in neigh {
                // grow neighbor levels if needed
                if nodes[nid]!.neighbors.count <= l {
                    nodes[nid]!.neighbors.append([])
                }
                nodes[nid]!.neighbors[l].append(id)
                if nodes[nid]!.neighbors[l].count > maxNeighbors {
                    var neigh = nodes[nid]!.neighbors[l]
                    let base = nodes[nid]!.vector
                    neigh.sort {
                        distance(nodes[$0]!.vector, base, metric: .l2) <
                        distance(nodes[$1]!.vector, base, metric: .l2)
                    }
                    neigh.removeLast()
                    nodes[nid]!.neighbors[l] = neigh
                }
            }
        }

        nodes[id] = node
        if level > maxLevel {
            maxLevel = level
            entry = id
        }
    }

    /// Removes a vector from the index and cleans up neighbor links.
    public mutating func remove(id: String) {
        guard let node = nodes.removeValue(forKey: id) else { return }
        for (level, neigh) in node.neighbors.enumerated() {
            for n in neigh {
                nodes[n]?.neighbors[level].removeAll { $0 == id }
            }
        }
        if entry == id {
            entry = nodes.keys.first
        }
        maxLevel = nodes.values.map { $0.neighbors.count - 1 }.max() ?? 0
    }

    public enum DistanceMetric: Sendable {
        case l2
        case cosine
    }

    /// Returns the `k` nearest identifiers to the query using the specified metric.
    /// Search starts from the top entry point and descends to the base layer.
    public func search(_ query: [Double], k: Int, metric: DistanceMetric = .l2) -> [String] {
        guard let entry = entry else { return [] }
        var current = entry
        var currentDist = distance(query, nodes[current]!.vector, metric: metric)
        if maxLevel > 0 {
            for l in stride(from: maxLevel, through: 1, by: -1) {
                var changed = true
                while changed {
                    changed = false
                    for n in nodes[current]?.neighbors[l] ?? [] {
                        let d = distance(query, nodes[n]!.vector, metric: metric)
                        if d < currentDist {
                            currentDist = d
                            current = n
                            changed = true
                        }
                    }
                }
            }
        }

        // Explore level 0 graph via BFS and rank by distance
        var visited: Set<String> = [current]
        var queue: [String] = [current]
        var scored: [(String, Double)] = [(current, currentDist)]
        var idx = 0
        while idx < queue.count {
            let v = queue[idx]; idx += 1
            for n in nodes[v]?.neighbors[0] ?? [] {
                if !visited.insert(n).inserted { continue }
                let d = distance(query, nodes[n]!.vector, metric: metric)
                scored.append((n, d))
                queue.append(n)
            }
        }
        scored.sort { $0.1 < $1.1 }
        return scored.prefix(k).map { $0.0 }
    }

    // MARK: - Helpers

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

    /// Deterministic pseudo‑random level generator based on the identifier.
    private func levelForID(_ id: String) -> Int {
        let seed = id.utf8.reduce(0) { ($0 &* 31) &+ Int($1) }
        var lvl = 0
        var x = seed
        while x & 1 == 1 {
            lvl += 1
            x >>= 1
        }
        return lvl
    }

    public static func == (lhs: HNSWIndex, rhs: HNSWIndex) -> Bool {
        lhs.nodes == rhs.nodes && lhs.entry == rhs.entry && lhs.maxLevel == rhs.maxLevel
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(entry)
        hasher.combine(maxLevel)
        for (id, node) in nodes.sorted(by: { $0.key < $1.key }) {
            hasher.combine(id)
            for v in node.vector { hasher.combine(v) }
            for neigh in node.neighbors {
                hasher.combine(neigh.count)
                for n in neigh.sorted() { hasher.combine(n) }
            }
        }
    }
}

