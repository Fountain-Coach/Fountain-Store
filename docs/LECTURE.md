# FountainStore — A Lecture on Memory, Simplicity, and Speed

There is a particular joy in systems that do one thing with uncommon care. FountainStore is such a system. It is the memory core for FountainAI: a compact, pure‑Swift engine that takes durable persistence seriously without surrendering the elegance of Swift’s actor model. This lecture explores what FountainStore is, the problems it solves, the choices it makes, where it is already unique, and where it is deliberately headed.

The most honest way to meet FountainStore is through its contract. The OpenAPI specification (docs/openapi-fountainstore.yaml) is our single source of truth for the HTTP surface. Every endpoint—health, status, collections, indexes, records, queries, transactions, snapshots, backups—has an explicit shape. Pagination is precise. Errors speak RFC 7807. If you prefer reading code to prose, start there. If you enjoy a story about why an engine is the way it is, read on.

FountainStore’s core is a log‑structured merge architecture built for today’s Swift. Every mutation is recorded in a write‑ahead log with CRC protection and hardened fsync boundaries; those records are then folded into an in‑memory memtable and, when appropriate, flushed into immutable SSTables. This is a familiar lineage—LSM trees are the workhorse of modern storage—but the choices here are intentional. We keep the data path legible. We pin the critical invariants in tests. We favor a simple background compactor over a maze of levels, but we still think hard about when to merge, how much to merge, and how to pace the writers when the store owes itself some housekeeping.

There is a second, quieter choice: FountainStore treats sequence numbers as first‑class citizens. Snapshots are real. We embed the sequence into SSTable keys so that the historical shape of the store survives a restart. It is one thing to replay a log into memory; it is another to recognize that a consistent read view is a promise users rely on. This is why record responses carry the sequence alongside your data, and why you can take a snapshot, delete a document, and still read the past with confidence.

Indexes are not an aftermarket bolt‑on here; they are native. Unique and multi‑value indexes move in step with puts and deletes and are reconstructed in memory on open. We offer full‑text search and vector search as optional, pure‑Swift modules because a modern assistant does not just key off equality and prefixes. Uniqueness is enforced strictly for typed indexes and—when you define them dynamically over JSON via HTTP—enforced at the boundary with care. The HTTP layer will happily derive multiple values from a JSON array path and guarantee that no two records claim the same key.

What about the day‑to‑day ergonomics? The HTTP surface is deliberately small, crisp, and testable. If you PUT a record for the first time, you get a 201 with a Location to the resource; if you update it, you get a 200 and the same record including useful metadata. If you list or query, you get pages with an opaque token that can be handed back as‑is—no leaky cursors, and, when you configure an API key, tokens are HMAC‑signed so you can trust them across hops. If you speak Prometheus, ask for metrics in that dialect; if you prefer JSON, the same counters are there. If you want immutability for a while, take a snapshot; if you want to housekeep, ask the compactor to tick. It’s all in the spec, and it’s all covered by end‑to‑end tests.

Under the hood, we choose clarity over ceremony. WAL segmentation is small by default so you can rotate logs often; Bloom filters and per‑block CRCs live in the SSTables because a little bit of local foresight eliminates a lot of IO. The compactor prefers taming a noisy Level‑0 before anything else and will not greedily merge the world in one sitting. Writers apply gentle backpressure when the compactor owes work; readers get predictable latency because the layout is simple and observable.

FountainStore is not a database that aspires to be everything for everyone. It is the right memory for a conversational system that cares about its past and responds to its future. That is why we integrate with SecretStore to resolve API keys in real deployments, why tokens are signed when secrets are present, and why the contract is expressed as OpenAPI and enforced in tests. The philosophy is that durability, correctness, and empathy for the caller beat cleverness every time.

There is one more piece of the story: FountainKit. FountainStore is the embedded engine, but a modern assistant is an orchestra. FountainKit is the modular SwiftPM workspace that composes services—gateway, planner, function caller, awareness, and more—around clear runtime primitives. If FountainStore is the memory, FountainKit is the nervous system: it routes prompts, curates OpenAPI truth, generates clients, supervises tools, and exposes the end‑to‑end experience. To see how callers use FountainStore in practice, clone FountainKit (https://github.com/Fountain-Coach/FountainKit) and browse the Packages folder. You will find the gateway server, the client generator, the curator, and examples that stitch the memory core into a living assistant. Reading across both repos is the best way to appreciate why the store’s contract is the way it is—and how a clean persistence engine accelerates the rest of the system.

Where does FountainStore go from here? We will continue to harden the compaction story, refine backpressure, and expose more observability at the points that matter (latency histograms and richer Prometheus output). Typed indexes already have strong semantics; we will extend rebuild ergonomics where it makes sense. Pagination is robust; we will keep tokens stable and non‑surprising. The HTTP surface will remain tight and well‑documented as the system grows. The principle remains the same: do the simple thing so well that it becomes an asset, not a liability, to the rest of FountainAI.

If you take away one idea, let it be this: FountainStore is a small, carefully‑made machine that refuses to trade clarity for magic. It leaves you with a store you can trust, a contract you can read, and a model that maps cleanly into Swift’s concurrency.

---

## Appendix — Terminæ Technicæ Legend

Log‑structured merge (LSM): A storage architecture that accepts writes into an append‑only log and periodically merges them into immutable tables. In FountainStore, this is the WAL → memtable → SSTables path. It optimizes writes and defers compaction to a background actor.

Write‑ahead log (WAL): The durable, append‑only journal of changes. Each record carries a CRC to detect corruption. FountainStore fsyncs at the right boundaries and rotates segments so recovery is predictable and resource‑bounded.

SSTable: The immutable, sorted run of key/value pairs written during flush or compaction. FountainStore’s SSTables include per‑block CRCs, an index for fast seeks, and a Bloom filter for quick, negative checks.

Snapshot (MVCC): A consistent read view at a specific sequence number. FountainStore embeds sequences in SSTable keys so snapshots survive restarts and reads can reconstruct the visible version history.

Compaction: The process of merging overlapping SSTables into fewer, larger tables so reads remain efficient. FountainStore prefers merging the noisiest level first and limits work per tick to keep latency smooth.

Bloom filter: A probabilistic set representation that can say “definitely not present” cheaply. FountainStore stores Bloom filters alongside the SSTable to avoid unnecessary disk reads during lookups.

Backpressure: A feedback mechanism that slows writers when the system owes background work. FountainStore scales its sleep proportionally to compaction debt, smoothing throughput under pressure.

Index (unique/multi): A secondary mapping from computed keys to record identifiers. Unique indexes enforce a one‑to‑one mapping; multi‑value indexes allow many records per key. Dynamic HTTP indexes extract keys from JSON paths; typed indexes use Swift KeyPaths.

Full‑text search (FTS): An inverted index that maps terms to documents with BM25‑style ranking. Optional in FountainStore, pure‑Swift, useful for conversational search.

Vector search (HNSW): An approximate nearest neighbor index for high‑dimensional vectors (e.g., embeddings). Also optional in FountainStore; helpful for semantic retrieval.

RFC 7807 (problem+json): A standardized error payload that carries `type`, `title`, `status`, and `detail`. FountainStore returns problem documents on HTTP errors so clients can reason about failures uniformly.

OpenAPI: The machine‑readable contract for the HTTP surface. In this project, the spec is the source of truth for clients and servers alike. FountainStore’s tests treat the spec as a living constraint.

SecretStore: A cross‑platform abstraction for secure secret management. In FountainStore, it resolves the API key from Keychain, Secret Service, or an encrypted file keystore when configured.

FountainKit: The modular SwiftPM workspace that composes services around FountainStore and other runtime kits. A recommended cross‑repo read to see how the memory core participates in the assistant’s wider loop.
