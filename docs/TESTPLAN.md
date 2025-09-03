
# Test Plan

## Core
- Put/Get/Delete round trips
- MVCC history retrieval
- Range/prefix scans
- Crash recovery matrix (kill at WAL append, after append before fsync, after fsync before memtable apply, etc.)
- Manifest integrity

## Transactions
- Multi‑collection atomicity
- Unique index enforcement
- Index scans (prefix)
- Snapshot repeatability

## Observability
- Operation metrics counters

## Performance (later)
- Write amplification tracking
- Bloom false‑positive sampling
