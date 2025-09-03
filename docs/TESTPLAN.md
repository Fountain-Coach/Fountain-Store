
# Test Plan

## Core
- Put/Get/Delete round trips
- MVCC history retrieval
- Range/prefix scans
- WAL append and replay on startup
- SSTable read path after memtable flush and restart
- Crash recovery matrix (kill at WAL append, after append before fsync, after fsync before memtable apply, etc.)
- Manifest integrity

## Transactions
- Multi‑collection atomicity
- Unique index enforcement
- Index scans (prefix)
- Snapshot repeatability

## Observability
- Operation metrics counters and history logging

## Optional Modules
- FTS search with BM25 ranking and custom analyzers
- Vector search via HNSW using L2 and cosine metrics

## Performance (later)
- Write amplification tracking
- Bloom false‑positive sampling
