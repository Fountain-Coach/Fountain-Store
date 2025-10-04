
//
//  Compactor.swift
//  FountainStoreCore
//
//  Background compactor merging overlapping SSTables.
//

import Foundation

/// Very simple background compactor.  It scans all known SSTables from the
/// manifest, finds overlapping key ranges and merges them into a new SSTable.
/// The compactor itself is an actor so concurrent invocations are serialized;
/// an additional flag prevents re‑entrant work to make `tick` safe when called
/// concurrently from multiple places.
public actor Compactor {
    private let directory: URL
    private let manifest: ManifestStore
    private var running = false
    private var lastPending: Int = 0

    public init(directory: URL, manifest: ManifestStore) {
        self.directory = directory
        self.manifest = manifest
    }

    /// Trigger a single compaction cycle.
    ///
    /// 1. Enumerates current SSTables from the manifest.
    /// 2. Determines overlapping key ranges.
    /// 3. Merges overlapping tables using `SSTable.create`.
    /// 4. Updates the manifest and removes obsolete files.
    ///
    /// The operation is intentionally coarse grained; it merges any group of
    /// overlapping tables into a single SSTable. This simple approach omits
    /// leveled compaction and throttling.
    public func tick() async {
        if running { return } // prevent overlapping invocations
        running = true
        defer { running = false }

        do {
            var m = try await manifest.load()
            let fm = FileManager.default
            let handles = m.tables.map { SSTableHandle(id: $0.key, path: $0.value) }
            guard handles.count > 1 else { return }

            // Compute levels by size (same heuristic as status)
            let base: Int64 = 256 * 1024
            var levelsMap: [UUID: Int] = [:]
            for (id, url) in m.tables {
                let sz = (try? fm.attributesOfItem(atPath: url.path)[.size] as? NSNumber)?.int64Value ?? 0
                let lvl: Int
                if sz <= 0 { lvl = 0 } else { var t = max(Int64(1), sz / base); var l = 0; while t > 1 { t >>= 1; l += 1 }; lvl = l }
                levelsMap[id] = lvl
            }

            // Determine key ranges for every table.
            var ranges: [(SSTableHandle, Data, Data)] = []
            for h in handles {
                if let r = try keyRange(of: h) { ranges.append((h, r.0, r.1)) }
            }
            guard !ranges.isEmpty else { return }

            // Sort by lower bound and group overlapping ranges.
            ranges.sort { $0.1.lexicographicallyPrecedes($1.1) }
            var groups: [[(SSTableHandle, Data, Data)]] = []
            var current: [(SSTableHandle, Data, Data)] = []
            var currentEnd: Data? = nil
            func maxData(_ a: Data, _ b: Data) -> Data { a.lexicographicallyPrecedes(b) ? b : a }
            for r in ranges {
                if current.isEmpty { current = [r]; currentEnd = r.2; continue }
                if let end = currentEnd, !end.lexicographicallyPrecedes(r.1) { current.append(r); currentEnd = maxData(end, r.2) }
                else { groups.append(current); current = [r]; currentEnd = r.2 }
            }
            if !current.isEmpty { groups.append(current) }

            // Prefer L0-only groups when L0 count is high, limit merges per tick.
            let maxMerges = 2
            let l0Groups = groups.filter { grp in grp.allSatisfy { levelsMap[$0.0.id] == 0 } && grp.count > 1 }
            let l0Count = levelsMap.values.filter { $0 == 0 }.count
            var worklist: [[(SSTableHandle, Data, Data)]] = []
            if l0Count > 4, !l0Groups.isEmpty {
                // Pick up to maxMerges L0 groups with largest sizes (approx by group count)
                worklist = Array(l0Groups.sorted { $0.count > $1.count }.prefix(maxMerges))
            } else {
                worklist = Array(groups.filter { $0.count > 1 }.prefix(maxMerges))
            }

            var pending = 0
            for g in worklist where g.count > 1 {
                pending += (g.count - 1)
                var allEntries: [(TableKey, TableValue)] = []
                for (h, _, _) in g { allEntries.append(contentsOf: try readEntries(h)) }
                allEntries.sort { $0.0.raw.lexicographicallyPrecedes($1.0.raw) }
                var merged: [(TableKey, TableValue)] = []
                var lastKey: Data? = nil
                for e in allEntries {
                    if let lk = lastKey, lk == e.0.raw { merged[merged.count - 1] = e }
                    else { merged.append(e); lastKey = e.0.raw }
                }
                let outURL = directory.appendingPathComponent(UUID().uuidString + ".sst")
                let newHandle = try await SSTable.create(at: outURL, entries: merged)
                for (h, _, _) in g {
                    m.tables.removeValue(forKey: h.id)
                    try? FileManager.default.removeItem(at: h.path)
                }
                m.tables[newHandle.id] = newHandle.path
                try await manifest.save(m)
            }
            lastPending = pending
        } catch {
            // Ignore errors for now – compaction is best effort.
        }
    }

    // MARK: - Status
    public struct LevelStatus: Sendable, Hashable, Codable {
        public let level: Int
        public let tables: Int
        public let sizeBytes: Int64
        public init(level: Int, tables: Int, sizeBytes: Int64) {
            self.level = level; self.tables = tables; self.sizeBytes = sizeBytes
        }
    }
    public struct Status: Sendable, Hashable, Codable {
        public let running: Bool
        public let pendingTables: Int
        public let levels: [LevelStatus]
        public let debtBytes: Int64
        public init(running: Bool, pendingTables: Int, levels: [LevelStatus], debtBytes: Int64) {
            self.running = running; self.pendingTables = pendingTables; self.levels = levels; self.debtBytes = debtBytes
        }
    }

    public func status() async throws -> Status {
        let m = try await manifest.load()
        let fm = FileManager.default
        // Compute virtual levels based on file size buckets.
        // Base size 256KB; level = floor(log2(size/base)) clipped at 0.
        let base: Int64 = 256 * 1024
        var byLevel: [Int: (count: Int, bytes: Int64)] = [:]
        for (_, url) in m.tables {
            let attrs = try? fm.attributesOfItem(atPath: url.path)
            let sz = (attrs?[.size] as? NSNumber)?.int64Value ?? 0
            let lvl: Int
            if sz <= 0 { lvl = 0 }
            else {
                var t = max(Int64(1), sz / base)
                var l = 0
                while t > 1 { t >>= 1; l += 1 }
                lvl = l
            }
            var entry = byLevel[lvl] ?? (0, 0)
            entry.count += 1
            entry.bytes += sz
            byLevel[lvl] = entry
        }
        let levels = byLevel.keys.sorted().map { k in
            LevelStatus(level: k, tables: byLevel[k]!.count, sizeBytes: byLevel[k]!.bytes)
        }
        // Simple debt heuristic: allow up to 4 tables at L0; if more, debt is total bytes beyond the first 4 smallest.
        var debt: Int64 = 0
        if let l0 = byLevel[0], l0.count > 4 {
            // accumulate all L0 sizes and estimate debt
            var sizes: [Int64] = []
            for (_, url) in m.tables {
                let attrs = try? fm.attributesOfItem(atPath: url.path)
                let sz = (attrs?[.size] as? NSNumber)?.int64Value ?? 0
                // recompute lvl to filter L0
                let lvl = (sz <= 0) ? 0 : {
                    var t = max(Int64(1), sz / base); var l = 0; while t > 1 { t >>= 1; l += 1 }; return l
                }()
                if lvl == 0 { sizes.append(sz) }
            }
            sizes.sort()
            for s in sizes.dropFirst(4) { debt += s }
        }
        return Status(running: running, pendingTables: lastPending, levels: levels, debtBytes: debt)
    }

    // MARK: - Helpers
    private func keyRange(of handle: SSTableHandle) throws -> (Data, Data)? {
        let entries = try readEntries(handle)
        guard let first = entries.first, let last = entries.last else { return nil }
        return (first.0.raw, last.0.raw)
    }

    private func readEntries(_ handle: SSTableHandle) throws -> [(TableKey, TableValue)] {
        try SSTable.scan(handle)
    }
}
