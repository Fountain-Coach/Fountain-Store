
# FountainStore Architecture

- **Engine**: Log‑structured merge (WAL → Memtable → SSTables). Compaction merges sorted tables.
- **Durability**: WAL with CRC + `fsync` on commit; manifest tracking live tables.
- **Isolation**: MVCC snapshots keyed by sequence numbers.
- **Indexes**: Maintained atomically with base writes; unique and multi‑value.
- **Optional**: FTS (inverted index) and Vector (HNSW).
- **Metrics**: Operation counters exposed via `metricsSnapshot()` for observability.
- **Logs**: Structured operation events delivered via `StoreOptions.logger`.

See `agent.md` for implementation steps.
