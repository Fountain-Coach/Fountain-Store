
# Roadmap

- [x] M1: KV core
- [x] M2: Compaction & snapshots
- [x] M3: Transactions & indexes
- [x] M4: Observability & Tuning – metrics counters, structured logs, configuration knobs.
- [x] M5: Optional Modules – baseline FTS (BM25) with analyzers and vector search (HNSW with cosine/L2).
- [x] M6: Persistence – WAL integration, crash recovery, SSTable read path

vNext

- [ ] M7: Persistent MVCC history across restarts (on-disk sequences, tombstones, retention)
- [ ] M8: Transactional WAL frames + store-level multi-collection batch
- [ ] M9: Index persistence and background rebuild (FTS/Vector optional snapshots)
- [ ] M10: SSTable integrity (per-block CRC) and explicit bloom serialization
- [ ] M11: Leveled compaction with status/metrics and backpressure
- [ ] M12: Read caching honoring `cacheBytes` with hit/miss metrics
- [ ] M13: Optional HTTP surface (admin + observability subset of OpenAPI)
- [ ] M14: Backups/restore (manifest snapshotting and restore tool)
- [ ] M15: WAL segmentation/rotation and GC
- [ ] M16: Index expressiveness (arrays/closures) and improved scans
