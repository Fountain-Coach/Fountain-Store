# Project Status (Updated 2025-10-04)

OpenAPI is the source of truth for the HTTP surface:
- Spec: openapi-fountainstore.yaml

This document is the single development‑tracking reference. All other prior planning/vision docs are archived under docs/LEGACY for historical context.

## Scope Summary
- Engine: WAL + manifest + persistent MVCC (sequence in SSTable keys), memtable/SSTables with per‑block CRC and Bloom, background compaction, block cache.
- Indexes: unique, multi, FTS (BM25), vector (HNSW). HTTP‑defined dynamic indexes persisted and rebuilt on startup.
- Transactions: atomic batch with BEGIN/OP/COMMIT replay, sequence guard.
- Backups: list/create/restore.
- HTTP: Admin/observability subset implemented with pagination and RFC7807 errors; record responses include metadata.

## Notable Features (0.2.0‑beta)
- Record responses carry sequence/deleted fields; PUT returns 201/200 with Location on first write.
- Pagination tokens are opaque (HMAC‑signed) when an API key is configured.
- Prometheus metrics via GET /metrics?format=prometheus.
- Optional API key auth via SecretStore (Keychain/Secret Service/FileKeystore) or env.
- Dynamic unique enforcement for HTTP‑defined indexes; dynamic index rebuild on startup.
- Compaction prefers L0 groups with limited merges per tick; write backpressure scales with compaction debt.

## Known Limitations
- Typed indexes are not auto‑rebuilt at open (dynamic HTTPDoc indexes are).
- Compaction is an MVP; full leveling/scheduling TBD.
- HTTP auth is optional; recommended to configure FS_API_KEY (or SecretStore) in production.

## Next Steps (PX)
- P2: Full leveled compaction + scheduling; stress tests (WAL rotation under load, compaction under writes, large scans, backups under traffic).
- P2: Extend opaque tokens everywhere they apply (complete parity achieved for lists and queries).
- P3: Observability improvements (latency histograms; fuller Prometheus endpoint).

## Release Notes
- Latest: v0.2.0‑beta.1 — https://github.com/Fountain-Coach/Fountain-Store/releases/tag/v0.2.0-beta.1
- See the repository releases/tags for the full CHANGELOG. (Historical docs moved under docs/LEGACY.)
