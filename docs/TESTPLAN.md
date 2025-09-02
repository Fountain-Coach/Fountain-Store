
# Test Plan

## Core
- Put/Get/Delete round trips
- Range/prefix scans
- Crash recovery matrix (kill at WAL append, after append before fsync, after fsync before memtable apply, etc.)
- Manifest integrity

## Transactions
- Multi‑collection atomicity
- Unique index enforcement
- Snapshot repeatability

## Performance (later)
- Write amplification tracking
- Bloom false‑positive sampling
