Implementation audit for Fountain‑Store

Introduction

This audit compares the promises laid out in the project’s playbook ([agent.md](https://agent.md)) with the current implementation.  The playbook’s goal is to deliver a production‑ready embedded ACID store in pure Swift using an LSM architecture with MVCC snapshots, secondary indexes and optional full‑text and vector search modules ￼.  It imposes hard rules (pure Swift, single writer actor, ACID batch commits, crash‑safe manifest updates, atomic index updates and deterministic crash simulation points) and outlines milestones M0–M6 ￼.  This audit examines whether those promises are met and notes omissions or simplifications.

Core engine (M1‑M2)

Write‑ahead log (WAL) & durability
	•	Implementation: WAL.swift implements a write‑ahead log with CRC32 checksums and fsync boundaries ￼.  Records consist of a sequence number, payload and CRC; appending and replay verify the checksum and call synchronize() to persist the file.  This meets the requirement for durable writes.
	•	Crash recovery: On opening a store, [FountainStore.open](https://FountainStore.open) loads the manifest, sets the current sequence and replays WAL records newer than the manifest sequence ￼.  Tests inject crashes between WAL append and fsync (CrashPoints) and verify that partial WAL records or pre‑fsync writes are ignored ￼.  Randomized property tests further exercise recovery ￼.
	•	Limitations: The WAL encodes payloads as JSON via JSONEncoder; there is no pluggable codec yet.  There is no pre‑allocation or segment rollover, so logs can grow unbounded.

Memtable and SSTables
	•	Memtable: A simple Memtable actor holds entries in an ordered array and flushes when a size limit is exceeded ￼.  Flush callbacks feed drained entries into higher‑level structures.
	•	SSTables: SSTable.create writes sorted key/value pairs into a file using fixed‑size blocks, builds an in‑memory block index and a bloom filter, and writes a footer with offsets and sizes ￼.  Reads use the bloom filter and block index to locate blocks and search keys ￼.  The creation path includes CRC checks for blocks and footers (fulfilling the validation rule).
	•	Compaction: A Compactor actor merges overlapping SSTables.  It scans the manifest, groups tables with overlapping key ranges, reads their entries, sorts and deduplicates them (newer values win) and writes a new SSTable ￼.  Old SSTables are removed and the manifest is updated.  This implements background compaction but lacks leveled compaction, throttling or backpressure; the compactor merges any overlapping group and can generate large output tables.
	•	Manifest: ManifestStore writes a manifest file atomically by writing to a temporary file then renaming ￼.  The manifest tracks live SSTables and the last persisted sequence number, fulfilling the atomic update promise ￼.
	•	Limitations:  The memtable flush is triggered only when an in‑memory limit is reached; there is no tuning based on disk size or throughput.  Compaction is coarse‑grained and does not implement leveled compaction or backpressure ￼.  The bloom filter uses a simple FNV‑based hash and a fixed number of hashes ￼; there is no false‑positive sampling or dynamic sizing as suggested in the playbook.

Crash recovery & persistence
	•	Manifest & WAL replay: On startup the store loads the manifest, seeds the sequence counter and replays any WAL records after the manifest sequence ￼.  Crash tests simulate partial WAL records and verify that corrupted data is ignored and that values written before the crash remain visible after restart ￼.  A property test randomly writes documents, flushes the memtable, restarts and verifies that the final state matches expectations ￼.
	•	Snapshot isolation: Snapshots capture the current sequence number and are used to query historical versions of a document or index ￼.  Tests confirm that data deleted after a snapshot remains visible when read with the snapshot and that range scans honour snapshots ￼.  However, sequence numbers of all entries are reset to the manifest sequence when SSTables are loaded on restart ￼, which causes the store to lose fine‑grained version history after a flush and restart.  Thus MVCC is only fully realised in memory; persistent MVCC across restarts is not yet implemented.

Transactions & secondary indexes (M3)
	•	Batch operations: The Collection actor provides a batch method that atomically executes a sequence of put/delete operations.  It pre‑allocates sequence numbers, writes each WAL entry, flushes the memtable if needed and then applies updates to in‑memory structures.  A transaction guard allows callers to require that the store’s sequence is at least a given value and will throw if it isn’t ￼.  Tests ensure that batch operations are atomic and that sequence guards work as intended ￼ ￼.
	•	Secondary indexes: Collection.define registers unique, multi, FTS or vector indexes.  Unique indexes enforce that a given key maps to at most one document; multi indexes map keys to arrays of document IDs.  Indexes are updated atomically during put and delete operations and support equality and prefix scans ￼ ￼.  Tests verify unique constraint violations, index lookups and prefix scans on both unique and multi indexes【419451925117233†L107-L165】.
	•	Limitations: Index data lives entirely in memory; it is not persisted to disk.  After a restart, callers must re‑define indexes to rebuild them from existing documents ￼.  There is no multi‑collection transaction scope; batches only affect a single collection.  Unique and multi indexes store values as strings and support only equality and prefix scans; there is no support for range queries on numeric types.

Observability & tuning (M4)
	•	Metrics: FountainStore maintains counters for puts, gets, deletes, scans, index lookups, batches and histories.  These can be read via metricsSnapshot() and reset via resetMetrics() ￼.  Tests confirm that counters are incremented correctly and that resetting returns the previous snapshot ￼.
	•	Logging: A logger closure in StoreOptions receives structured LogEvent values for each operation ￼.  LoggingTests verifies that events are emitted in the correct order for puts, gets, index lookups, scans, histories, deletes and batches ￼.  Log events are Codable for potential persistence or analysis.
	•	Configuration: StoreOptions includes tunables such as defaultScanLimit and cacheBytes.  The default scan limit is honoured in range scans and index scans ￼, and there is a test verifying custom limits ￼.  However, cacheBytes is currently unused; there is no block or buffer cache.

Optional modules (M5)

Full‑text search (FTS)
	•	Inverted index: FountainFTS/FTS.swift implements a basic inverted index.  Each document is tokenized using a default analyzer (lowercases and splits on non‑alphanumeric characters) and a frequency map is stored per token and document ￼.  Searching intersects the posting sets for query tokens and ranks results using BM25 ￼.  Custom analyzers can be supplied when defining the index; the package includes a stopword analyzer factory ￼.
	•	Integration: The Collection actor exposes searchText for BM25‑ranked queries and uses an optional limit parameter ￼.  Optional‑module integration tests confirm that FTS indexes can be created, used, limited and re‑built after restart ￼.

Vector search
	•	HNSW index: FountainVector/HNSW.swift implements a simplified Hierarchical Navigable Small World index.  Each vector gets a deterministic level based on its ID; neighbours are selected globally and truncated to a fixed fan‑out.  Search starts from the top entry, descends through levels and performs a breadth‑first search on the base layer to find the k nearest neighbours ￼.  Both L2 and cosine distance metrics are supported ￼.
	•	Integration: Vector indexes can be defined on [Double] properties; vectorSearch returns the nearest documents for a query vector ￼.  Tests verify insertion, removal and both L2 and cosine searches ￼, and integration tests ensure that vector indexes survive restarts and return expected results ￼.
	•	Limitations: The HNSW implementation is intentionally simple.  It uses a deterministic level generator and does not support dynamic neighbour heuristics or multi‑threaded construction.  It does not persist index structures; like other indexes, it must be rebuilt from scratch after a restart.  There is no concurrency control around vector index updates beyond the single writer actor.

Documentation & test coverage
	•	Architecture & test plan: The docs/ARCHITECTURE.md file summarises the engine design, durability, MVCC snapshots, indexes, optional modules, metrics, logs and configuration ￼.  The docs/TESTPLAN.md enumerates core tests, transaction tests, observability tests and optional module tests ￼.  All areas mentioned in the test plan have corresponding tests.
	•	Roadmap: docs/ROADMAP.md marks milestones M1–M6 as completed ￼, and the repository contains unit/property tests for each milestone.  Crash points are annotated and used in tests.

Unfulfilled promises & gaps

Although the implementation meets most milestone requirements, several promised or implied features remain absent or simplified:
	1.	Leveled compaction & backpressure: Compaction merges all overlapping SSTables into a single new table with no consideration for levels or write amplification.  There is no backpressure or throttling as suggested by the soft rules ￼.  As the dataset grows, this naive compaction could lead to very large tables and high write amplification.
	2.	Persistent MVCC history: MVCC snapshots work only in memory; when SSTables are loaded after a restart, all entries are assigned the manifest sequence, discarding individual sequence numbers and prior versions ￼.  Thus historical versions are not preserved across restarts, contrary to the goal of MVCC snapshots. ￼ ensures snapshot isolation only within a single process lifetime.
	3.	Cache and tuning: The cacheBytes option is unused, and there is no caching of SSTable blocks or memtable entries.  Without a block cache, repeated reads scan entire SSTables and reload bloom filters and indexes from disk, impacting performance.
	4.	Index persistence and verification: Secondary, FTS and vector indexes are not persisted and must be redefined after each restart.  The playbook mentions periodic index verification and rebuilding mismatched indexes ￼, but this is not implemented.  There is also no quarantine of orphaned SSTables ￼.
	5.	Bloom filter enhancements: The bloom filter is extremely simple and uses a single FNV‑based hash; there is no dynamic sizing, false‑positive sampling or pluggable hash functions ￼.
	6.	Transaction scope: Batch operations apply only to a single collection.  Multi‑collection transactions (e.g., updating multiple collections atomically) are not supported, even though the test plan lists “multi‑collection atomicity” ￼.
	7.	Write‑ahead log management: There is no log segmentation, compaction or pruning of old WAL entries.  Over time the WAL can grow indefinitely.
	8.	Observability improvements: Metrics counters are limited to operation counts; there is no latency, memory usage or compaction metrics.  The logger is simple and cannot be filtered or redirected to structured back‑ends.
	9.	Pluggable codecs: JSON encoding/decoding is hard‑coded for values and keys; there is no protocol to plug in CBOR or other formats ￼.

Conclusion

Fountain‑Store delivers a functional, pure‑Swift embedded key‑value store that implements the basic LSM pipeline, supports MVCC snapshots (in‑memory), provides unique and multi secondary indexes, and adds optional full‑text and vector search modules.  The implementation adheres to the hard rules of the playbook: it uses actors for concurrency, writes to a WAL and fsyncs before applying writes, updates indexes atomically and uses deterministic crash injection points for tests.  Comprehensive unit and property tests cover the core engine, transactions, observability and optional modules.  Documentation describes the architecture and test plan and marks milestones as completed.

However, several promises are only partially fulfilled.  The compaction strategy is simplistic and lacks levelling or throttling; MVCC snapshots do not survive restarts; indexes are in‑memory only and require manual rebuilding; caching and tuning hooks exist but are unused; and advanced validation and correction logic (index verification, orphan table quarantine) are not present.  These gaps mean that while the project functions correctly for small workloads and demonstrates the intended API surface, it is not yet production‑ready for large or long‑running deployments.  Addressing the highlighted areas would bring the implementation closer to the ambitious goals set in the playbook.