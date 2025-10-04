# FountainStore Delivery Plan (vNext)

This plan takes FountainStore from the current status quo to full promise fulfillment as outlined in README, VISION, ARCHITECTURE, ROADMAP, and the OpenAPI spec. Work is delivered in small, reviewable increments with semantic commits to `main` and green CI at every step.

Guiding principles

- Preserve API stability: keep `FountainStore`, `Collection`, `Index`, and `Snapshot` stable; add capabilities compatibly.
- Crash-safety first: WAL/fsync boundaries and manifest atomicity must remain correct during all transitions.
- Incremental migration: new on-disk formats ship behind migration/feature flags where feasible; data remains readable.
- Test-driven: each milestone lands with focused unit/property tests and CI coverage.
- Measurable: expose metrics to validate improvements (compaction debt, bloom FPP, cache hit rate).

Versioning

- Current: `0.1.x`
- Breaking on-disk changes will bump minor: `0.2.0`, `0.3.0`, … (still 0.x), with clear MIGRATION notes.

Milestones

M7 — Persistent MVCC history (0.2.0)

- Goal: Retain version history across restarts; snapshot reads reflect pre-restart state.
- Design:
  - Extend key layout in WAL/SSTable to include sequence: `key = collection\0 idJSON \0 seqBE`.
  - Store tombstones as zero-length values; compaction retains latest ≤ snapshot watermark.
  - Bootstrap on startup loads versions with their original sequences (no manifest-sequence collapsing).
- Tasks:
  - feat(core): extend WAL payload schema to carry sequence; keep JSON codec.
  - feat(core): flush memtable entries as composite keys with sequence.
  - feat(core): implement `get(id:snapshot:)` to seek latest seq ≤ snapshot.
  - test: add restart history tests (versions visible across restarts).
  - docs: MIGRATION for v0.2.0; enable read-compat to load pre-0.2 data as “latest only”.
- Risks: disk usage growth; compaction policy needs snapshot-retention; start with “keep last N versions” (config: `historyRetention = .unbounded | .latestOnly | .count(N)`).

M8 — Transactional WAL + multi-collection transactions (0.3.0)

- Goal: Batch all-or-nothing across crash; support atomic ops across multiple collections.
- Design:
  - Add WAL frames: `BEGIN(txid)`, `OP`, `COMMIT(txid)`; replay ignores uncommitted chunks.
  - New store-level `FountainStore.batch(_ ops: [Operation], requireSequenceAtLeast: ...)` that routes to collections.
- Tasks:
  - feat(core): WAL begin/commit markers; idempotent replay. (DELIVERED: replay supports BEGIN/OP/COMMIT and ignores uncommitted ops.)
  - feat(api): store-level batch; keep collection-level batch for BC.
  - test: power-cut simulations at each crash point to assert atomic visibility.
  - docs: transaction semantics and crash-matrix.
- Risks: performance due to extra frames; mitigate by grouping fsyncs per batch.

M9 — Index persistence and rebuild (0.3.x)

- Goal: Persist index definitions; guarantee index availability after restart; optional on-disk FTS/Vector.
- Design:
  - Persist index definitions in manifest; reload on startup and lazy-rebuild in background.
  - Phase 1: rebuild FTS/Vector from base data; Phase 2 (optional): serialize on-disk snapshots.
- Tasks:
  - feat(api): persist index schemas; background rebuild queues per collection.
  - test: restart with indexes defined and queries succeeding without manual re-define.
  - docs: manifest schema additions.

M10 — SSTable integrity + bloom format (0.4.0)

- Goal: Detect data corruption; remove reflection from bloom persistence.
- Design:
  - Add per-block CRC32; validate on read; record counts in footer.
  - Define explicit bloom serialization (k, bitCount, bitset bytes) without reflection.
- Tasks:
  - feat(core): write block CRCs; read-path validation and error surfacing.
  - refactor(core): bloom serializer/deserializer.
  - test: corruption property tests (flip bytes → detect).

M11 — Compaction v2 + status/metrics (0.4.x)

- Goal: Leveled compaction, backpressure, and visibility into debt/status.
- Design:
  - L0-Ln leveled scheme; size-tiered at L0, leveled beyond; configurable thresholds.
  - Track `debtBytes`, per-level table counts/sizes; expose `CompactionStatus`.
- Tasks:
  - feat(core): leveled planner; non-blocking ticks; throttle under high debt.
  - feat(metrics): export compaction metrics; integrate with optional HTTP.
  - test: compaction planning tests and invariants.

M12 — Read caching (0.5.0)

- Goal: Make `cacheBytes` effective; reduce disk IO.
- Design:
  - Block cache (clock/LRU) for SSTable data/index blocks; admission via bloom hits.
  - Expose cache hit/miss counters.
- Tasks:
  - feat(core): cache layer keyed by (fileID, offset, length).
  - test: cache behavior under repeated gets and scans.
  - docs: tuning guidance.

M13 — Optional HTTP surface (0.5.x)

- Goal: Implement a small subset of OpenAPI for admin/observability.
- Scope (phase 1): `/health`, `/status`, `/metrics`, `/compaction/status`, `/compaction/run`, `/collections` (list/create), basic `/records` get/put/delete.
- Tasks:
  - feat(http): lightweight SwiftNIO or URLSession-based HTTP server (pure Swift).
  - test: endpoint contract tests; JSON schema spot checks.
  - docs: enable flag; security note.

M14 — Backups/restore (0.6.0)

- Goal: Snapshot manifests and copy immutable SSTables; safe restore.
- Design:
  - Quiesce writes; fsync; copy manifest + WAL rotation; record backup metadata; restore by replacing directory.
- Tasks:
  - feat(core): backup coordinator; restore tooling.
  - test: backup/restore property tests.

M15 — WAL segmentation + rotation (0.6.x)

- Goal: Bounded WAL growth; quick startup.
- Design:
  - Size-based segments with index; rollover after flush or size; GC segments referenced ≤ manifest sequence.
- Tasks:
  - feat(core): segment manager; replay across segments; GC.
  - test: rollover and GC tests.

M16 — Index expressiveness (0.7.0)

- Goal: Multi-value extraction (arrays), simple path DSL.
- Design:
  - Extend `Index.Kind.multi` to accept `KeyPath<C, [String]>` and closure extractors.
- Tasks:
  - feat(api): new overloads; backward compatible with existing `String` key paths.
  - test: array-based indexes and scans.

Testing and CI

- Expand property tests: crash recovery, compaction invariants, corruption detection.
- Track coverage and benchmark trends; fail CI if regressions exceed thresholds (to be defined once metrics stabilize).

Semantic commit examples

- feat(core): persist sequence in SSTable keys for MVCC
- feat(api): add store-level batch with transactional WAL frames
- perf(io): add SSTable block cache honoring cacheBytes
- fix(compaction): prevent overlapping range regression at L0
- refactor(bloom): replace reflection-based serializer with explicit format
- test(recovery): add randomized crash-matrix for BEGIN/COMMIT frames
- docs(roadmap): add vNext milestones and migration notes for 0.2.0

Acceptance criteria per milestone

- All new tests pass, no regressions in existing suite.
- Benchmarks show non-regression or documented tradeoffs.
- MIGRATION updated when on-disk formats change.
