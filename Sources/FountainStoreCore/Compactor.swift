
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
            let handles = m.tables.map { SSTableHandle(id: $0.key, path: $0.value) }
            guard handles.count > 1 else { return }

            // Determine key ranges for every table.
            var ranges: [(SSTableHandle, Data, Data)] = []
            for h in handles {
                if let r = try keyRange(of: h) {
                    ranges.append((h, r.0, r.1))
                }
            }
            guard !ranges.isEmpty else { return }

            // Sort by lower bound and group overlapping ranges.
            ranges.sort { $0.1.lexicographicallyPrecedes($1.1) }
            var groups: [[(SSTableHandle, Data, Data)]] = []
            var current: [(SSTableHandle, Data, Data)] = []
            var currentEnd: Data? = nil
            func maxData(_ a: Data, _ b: Data) -> Data {
                return a.lexicographicallyPrecedes(b) ? b : a
            }
            for r in ranges {
                if current.isEmpty {
                    current = [r]
                    currentEnd = r.2
                    continue
                }
                if let end = currentEnd, !end.lexicographicallyPrecedes(r.1) {
                    current.append(r)
                    currentEnd = maxData(end, r.2)
                } else {
                    groups.append(current)
                    current = [r]
                    currentEnd = r.2
                }
            }
            if !current.isEmpty { groups.append(current) }

            // Merge each overlapping group into a new SSTable.
            for g in groups where g.count > 1 {
                var allEntries: [(TableKey, TableValue)] = []
                for (h, _, _) in g {
                    let entries = try readEntries(h)
                    allEntries.append(contentsOf: entries)
                }

                // Merge and deduplicate by key (newer wins).
                allEntries.sort { $0.0.raw.lexicographicallyPrecedes($1.0.raw) }
                var merged: [(TableKey, TableValue)] = []
                var lastKey: Data? = nil
                for e in allEntries {
                    if let lk = lastKey, lk == e.0.raw {
                        merged[merged.count - 1] = e
                    } else {
                        merged.append(e)
                        lastKey = e.0.raw
                    }
                }

                // Write merged table.
                let outURL = directory.appendingPathComponent(UUID().uuidString + ".sst")
                let newHandle = try await SSTable.create(at: outURL, entries: merged)

                // Update manifest and remove old files.
                for (h, _, _) in g {
                    m.tables.removeValue(forKey: h.id)
                    try? FileManager.default.removeItem(at: h.path)
                }
                m.tables[newHandle.id] = newHandle.path
                try await manifest.save(m)
            }
        } catch {
            // Ignore errors for now – compaction is best effort.
        }
    }

    // MARK: - Helpers
    private func keyRange(of handle: SSTableHandle) throws -> (Data, Data)? {
        let entries = try readEntries(handle)
        guard let first = entries.first, let last = entries.last else { return nil }
        return (first.0.raw, last.0.raw)
    }

    private func readEntries(_ handle: SSTableHandle) throws -> [(TableKey, TableValue)] {
        let data = try Data(contentsOf: handle.path)
        guard data.count >= 32 else { return [] }
        let footerStart = data.count - 32
        let indexOffset = Int(data[footerStart..<(footerStart + 8)].withUnsafeBytes { $0.load(as: UInt64.self) }.littleEndian)
        let blockData = data[..<indexOffset]
        var offset = 0
        var res: [(TableKey, TableValue)] = []
        while offset < blockData.count {
            if offset + 4 > blockData.count { break }
            let klen = Int(blockData[offset..<(offset + 4)].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
            offset += 4
            if offset + klen > blockData.count { break }
            let key = Data(blockData[offset..<(offset + klen)])
            offset += klen
            if offset + 4 > blockData.count { break }
            let vlen = Int(blockData[offset..<(offset + 4)].withUnsafeBytes { $0.load(as: UInt32.self) }.littleEndian)
            offset += 4
            if offset + vlen > blockData.count { break }
            let value = Data(blockData[offset..<(offset + vlen)])
            offset += vlen
            res.append((TableKey(raw: key), TableValue(raw: value)))
        }
        return res
    }
}
