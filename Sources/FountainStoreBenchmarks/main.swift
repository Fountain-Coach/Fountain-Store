import Foundation
import FountainStore

struct BenchDoc: Codable, Identifiable {
    let id: Int
    let value: String
}

struct BenchmarkResult: Codable {
    let putsPerSecond: Double
    let getsPerSecond: Double
    let metrics: Metrics
}

@main
struct Benchmarks {
    static func main() async throws {
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let store = try await FountainStore.open(StoreOptions(path: tmp))
        let coll = await store.collection("bench", of: BenchDoc.self)
        let ops = 1_000

        var start = Date()
        for i in 0..<ops {
            try await coll.put(BenchDoc(id: i, value: "val"))
        }
        var elapsed = Date().timeIntervalSince(start)
        let putRate = Double(ops) / elapsed

        start = Date()
        for i in 0..<ops {
            _ = try await coll.get(id: i)
        }
        elapsed = Date().timeIntervalSince(start)
        let getRate = Double(ops) / elapsed

        let metrics = await store.metricsSnapshot()
        let result = BenchmarkResult(putsPerSecond: putRate, getsPerSecond: getRate, metrics: metrics)
        let data = try JSONEncoder().encode(result)
        try data.write(to: URL(fileURLWithPath: "benchmark.json"))
        if let out = String(data: data, encoding: .utf8) {
            print(out)
        }
    }
}
