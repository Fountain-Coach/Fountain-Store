
# Roadmap (current)

- [x] M1: KV core
- [x] M2: Compaction & snapshots
- [x] M3: Transactions & indexes
- [x] M4: Observability & Tuning – metrics counters, structured logs, configuration knobs.
- [x] M5: Optional Modules – baseline FTS (BM25) with analyzers and vector search (HNSW with cosine/L2).
- [x] M6: Persistence – WAL integration, crash recovery, SSTable read path

vNext

- [x] M7: Persistent MVCC across restarts (sequence-bearing SSTable keys)
- [x] M8: Transactional WAL frames + store-level batch (replay-time support; API exposed)
- [x] M9: Index persistence catalog + dynamic index rebuild for HTTPDoc; typed auto-rebuild pending
- [x] M10: SSTable integrity (per-block CRC) and bloom serialization
- [~] M11: Leveled compaction preference + backpressure (initial MVP; full leveling TBD)
- [x] M12: Read caching honoring `cacheBytes`
- [x] M13: Optional HTTP surface (admin + observability subset of OpenAPI) with pagination and metadata
- [x] M14: Backups/restore (manifest snapshotting and restore)
- [x] M15: WAL segmentation/rotation and GC
- [x] M16: Index expressiveness (arrays/closures) and improved scans

Notes
- Full leveled compaction and scheduling remain as follow-ups.
- Typed indexes do not auto-rebuild at open; dynamic HTTPDoc indexes do.
