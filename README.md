
# FountainStore

**Status:** Initial release v0.1.0 – includes optional FTS and vector modules with disk persistence, crash recovery, and SSTable read path.

FountainStore is a **pure‑Swift**, embedded, ACID persistence engine for FountainAI. The engine persists data to disk via a WAL and SSTables and reloads state on startup. It follows an LSM-style architecture (WAL → Memtable → SSTables) with MVCC snapshots,
secondary indexes, and optional FTS and Vector modules, all with zero non‑Swift dependencies.

See `agent.md` for Codex instructions and `docs/` for the full blueprint.

Benchmarks for put/get throughput live in `FountainStoreBenchmarks` and run in CI with JSON results uploaded as artifacts.

## Installation

FountainStore is distributed as a Swift Package. To add it to your project, include the following dependency in your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/FountainAI/Fountain-Store.git", from: "0.1.0")
]
```

---

# Vision (For Everyone)

FountainStore is like a **digital filing cabinet** designed especially for FountainAI.

## Why It Matters
- **Safe**: Nothing gets lost, even if the system crashes mid‑save.
- **Fast**: Finds what you need instantly, even among millions of entries.
- **Private**: Runs locally inside FountainAI, with no outside servers involved.
- **Evolves**: Can adapt to new data shapes as FountainAI grows.

## Everyday Metaphors
- **Notebook**: Every change is first jotted down quickly (our write‑ahead log).
- **Whiteboard**: Recent items stay handy for quick access (our in‑memory store).
- **Binders**: Older notes are neatly archived in order (our on‑disk tables).
- **Librarian**: Keeps binders tidy, merging them for faster lookups (compaction).

## What People Can Do With It
- Save new records instantly and safely.
- Retrieve records by ID, tag, or keyword.
- Look back at how records looked in the past.
- Trust that data stays consistent and private.

## The Ambition
We’re not just building another database. We’re giving FountainAI its own **memory core** —
Swift‑native, reliable, and tuned to support everything from coaching interactions to planning
and narrative building.

---

For more detail, see [`docs/VISION.md`](docs/VISION.md).


---

# Visual Overview

![FountainStore Diagram](docs/diagram.png)

This diagram shows the flow of data in FountainStore:

- **Notebook (WAL Log)** → quick jots of every change.
- **Whiteboard (Memtable)** → recent notes kept handy.
- **Binders (SSTables)** → long-term, sorted archive.
- **Librarian (Compaction)** → keeps binders tidy and efficient.
