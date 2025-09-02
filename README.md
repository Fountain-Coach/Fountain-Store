
# FountainStore (Pure‑Swift Embedded Store)

**Status:** scaffold / bootstrap — ready for Codex-driven implementation.

FountainStore is a **pure‑Swift**, embedded, ACID persistence engine for FountainAI.
It follows an LSM-style architecture (WAL → Memtable → SSTables) with MVCC snapshots,
secondary indexes, optional FTS and Vector modules, and zero non‑Swift dependencies.

See `agent.md` for Codex instructions and `docs/` for the full blueprint.
