import Foundation
@testable import FountainStore

/// Create a new temporary directory and open a store at that path.
func makeTempStore() async throws -> (FountainStore, URL) {
    let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    let store = try await FountainStore.open(.init(path: dir))
    return (store, dir)
}

/// Re-open a store at the given directory.
func reopenStore(at url: URL) async throws -> FountainStore {
    try await FountainStore.open(.init(path: url))
}

/// Force the current memtable to flush by writing enough records to exceed its limit.
func triggerMemtableFlush(_ store: FountainStore) async throws {
    struct Dummy: Codable, Identifiable { var id: Int }
    let coll = await store.collection("_flush", of: Dummy.self)
    let limit = await store.memtable.limit
    for i in 0...limit {
        try await coll.put(.init(id: i))
    }
    try await store.flushMemtableIfNeeded()
}
