import Foundation

public actor BlockCache {
    private let capacityBytes: Int
    private var usedBytes: Int = 0
    private var map: [String: Data] = [:]
    private var order: [String] = [] // LRU order: oldest at index 0
    private var hits: UInt64 = 0
    private var misses: UInt64 = 0

    public init(capacityBytes: Int) {
        self.capacityBytes = max(0, capacityBytes)
    }

    public func get(_ key: String) -> Data? {
        if let data = map[key] {
            hits &+= 1
            if let idx = order.firstIndex(of: key) { order.remove(at: idx); order.append(key) }
            return data
        }
        misses &+= 1
        return nil
    }

    public func put(_ key: String, data: Data) {
        guard capacityBytes > 0 else { return }
        if let existing = map[key] {
            usedBytes -= existing.count
            if let idx = order.firstIndex(of: key) { order.remove(at: idx) }
        }
        map[key] = data
        order.append(key)
        usedBytes += data.count
        evictIfNeeded()
    }

    private func evictIfNeeded() {
        while usedBytes > capacityBytes && !order.isEmpty {
            let key = order.removeFirst()
            if let data = map.removeValue(forKey: key) { usedBytes -= data.count }
        }
    }

    public func stats() -> (hits: UInt64, misses: UInt64, items: Int, bytes: Int) {
        (hits, misses, map.count, usedBytes)
    }
}

