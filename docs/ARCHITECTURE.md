
# FountainStore Architecture

- **Engine**: Log‑structured merge (WAL → Memtable → SSTables). Compaction merges sorted tables.
- **Durability**: WAL with CRC + `fsync` on commit; manifest tracking live tables; SSTables loaded on startup for crash recovery.
- **Isolation**: MVCC snapshots keyed by sequence numbers.
- **Indexes**: Maintained atomically with base writes; unique and multi‑value with equality and prefix scans.
- **Optional**: FTS (inverted index with pluggable analyzers) and Vector (HNSW with cosine/L2 metrics), currently stubbed.
- **FTS Analyzer**: Collections can supply custom analyzers when defining indexes using `.fts(\\Type.field, analyzer: ...)`.
- **FTS Search**: BM25-ranked results with an optional result limit via `searchText`.
- **Metrics**: Operation counters (puts, gets, deletes, scans, index lookups, batches, histories) exposed via `metricsSnapshot()` and reset with `resetMetrics()` for observability.
- **Logs**: Structured operation events delivered via `StoreOptions.logger`.
- **Configuration**: Tunable defaults such as `StoreOptions.defaultScanLimit` for range and index scans.
- **Benchmarks**: `FountainStoreBenchmarks` executable measures baseline put/get throughput and emits JSON metrics.

See `agent.md` for implementation steps.
