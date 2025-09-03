
# FountainStore Architecture

- **Engine**: Log‑structured merge (WAL → Memtable → SSTables). Compaction merges sorted tables.
- **Durability**: WAL with CRC + `fsync` on commit; manifest tracking live tables.
- **Isolation**: MVCC snapshots keyed by sequence numbers.
- **Indexes**: Maintained atomically with base writes; unique and multi‑value with equality and prefix scans.
- **Optional**: FTS (inverted index with pluggable analyzers) and Vector (HNSW).
- **Metrics**: Operation counters (puts, gets, deletes, scans, index lookups, batches, histories) exposed via `metricsSnapshot()` and reset with `resetMetrics()` for observability.
- **Logs**: Structured operation events delivered via `StoreOptions.logger`.
- **Configuration**: Tunable defaults such as `StoreOptions.defaultScanLimit` for range and index scans.

See `agent.md` for implementation steps.
