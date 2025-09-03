
# Codex Agent — Implementation Playbook for FountainStore

> Goal: Deliver a production‑ready **pure‑Swift**, embedded, ACID store with
> LSM engine, MVCC, secondary indexes, optional FTS and vector search.
> No C/Obj‑C/C++/SQLite. Swift 6. Use actors for concurrency.

## Hard Rules (must not break)
1. Pure Swift only (standard library + Foundation). No FFI, no C/Obj‑C.
2. Single writer (actor), many readers via **MVCC snapshots**.
3. ACID batch commits: append to WAL, **fsync**, apply to memtable, flush to SSTables.
4. Atomic manifest updates (write‑then‑rename); crash‑safe recovery.
5. Secondary index updates occur in the same batch as base writes.
6. Deterministic, reproducible tests. Include crash/recovery simulation points.

## Soft Rules (defaults)
- Sorted memtable structure (skiplist/ordered array). Start simple (ordered array) then optimize.
- Leveled compaction; per‑SSTable bloom filters for fast negative lookups.
- JSON as the initial value codec; pluggable codec protocol for later (CBOR/etc.).
- Background compaction actor with backpressure when debt is high.

## Validation (schema checks)
- Block CRC for WAL and SSTable data blocks.
- Bloom false‑positive sampling during tests.
- Manifest monotonicity and reference integrity (only tables in manifest are live).

## Correction Logic (self‑healing)
- On startup, replay last WAL segment when manifest is behind.
- Quarantine orphaned SSTables not referenced by the manifest.
- Periodic index verification; rebuild index when mismatch exceeds threshold.

---

## Milestones & PR Breakdown

**M0 — Package Skeleton** ✅
- Compile‑clean placeholders; tests compile but are pending.

**M1 — KV Core** ✅
- WAL append+fsync with CRC; Memtable (sorted in‑memory map); SSTable read path.
- Crash recovery: rebuild from WAL; manifest create/load; basic get/put/delete.

**M2 — Compaction & Snapshots** ✅
- Flush memtable to SSTable; background compaction; MVCC snapshots (sequence IDs).
- Range scans and prefix scans.

**M3 — Transactions & Indexes** ✅
- Atomic multi‑put/delete batches with a sequence boundary.
- Unique and multi‑value secondary indexes; index scans.

**M4 — Observability & Tuning (you are here)**
- Metrics counters; structured logs; configuration knobs.

**M5 — Optional Modules**
- FTS (inverted index, analyzers, BM25).
- Vector (HNSW; cosine/L2).

---

## Directory Structure

```
Sources/
  FountainStore/        # public API
  FountainStoreCore/    # engine internals (WAL, SSTable, Manifest, Bloom, Memtable, Compactor)
  FountainFTS/          # optional module (stub now)
  FountainVector/       # optional module (stub now)
Tests/
  FountainStoreTests/   # unit & property tests
docs/
  ARCHITECTURE.md
  TESTPLAN.md
  ROADMAP.md
```

---

## Definition of Done (per Milestone)

- All unit tests for the milestone pass.
- No allocations in hot loops without justification (use `@inline(__always)` when needed).
- No data loss across power‑cut simulations (kill between WAL append and fsync, etc.).
- Public API (`FountainStore`, `Collection`, `Index`, `Snapshot`) remains stable.

---

## Commands

- Build: `swift build -c debug`
- Test:  `swift test -c debug`
- Lint:  _n/a (use SwiftFormat if added later)_

---

## Commit Hygiene

- Small, reviewable commits; descriptive messages.
- Keep `docs/` updated when public API changes.
- When adding crash points, annotate with `// CRASH_POINT(id: …)` and test for them.

> Begin with **M1** in `Tests/FountainStoreTests/StoreBasicsTests.swift` (currently `.todo`),
> then implement `Sources/FountainStoreCore` components one by one.
