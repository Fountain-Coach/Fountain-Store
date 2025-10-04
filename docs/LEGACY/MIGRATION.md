# Migration Notes

This document describes steps required to migrate between FountainStore versions.

## 0.1.0
- Initial release. No previous versions; existing data stores require no migration.
- Future releases will describe necessary data migrations here.

## 0.2.0
- Persistent MVCC sequences in SSTables.
  - New SSTables append the 8-byte big-endian sequence to each key (format: `collection\0idJSON\0seq`).
  - WAL format remains unchanged; replay continues to be forward-compatible.
  - Backward compatibility: existing 0.1.x SSTables (without appended sequence) remain readable; on load, their entries are assigned the manifest sequence (latest-only history), matching 0.1.x behavior.
  - Forward behavior: MVCC history across restarts is preserved for data flushed with 0.2.0+.
  - No user action is required; history across restart will become available gradually as memtables flush under 0.2.0+.
