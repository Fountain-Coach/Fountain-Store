import XCTest
import Foundation
@testable import FountainStoreCore

final class SSTableIntegrityTests: XCTestCase {
    func test_block_crc_detection() async throws {
        // Build a small table
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("t.sst")
        let entries: [(TableKey, TableValue)] = [
            (TableKey(raw: Data("k1".utf8)), TableValue(raw: Data("v1".utf8))),
            (TableKey(raw: Data("k2".utf8)), TableValue(raw: Data("v2".utf8))),
        ]
        let handle = try await SSTable.create(at: url, entries: entries)

        // Locate first block payload and flip a bit (not CRC region!)
        var data = try Data(contentsOf: url)
        // Footer is 32 bytes; read index offset from footer
        let footerStart = data.count - 32
        let iOff = Int(data[footerStart..<(footerStart + 8)].withUnsafeBytes { $0.loadUnaligned(as: UInt64.self) }.littleEndian)
        // Corrupt a byte in the first block (before CRC). Assume block starts at 0.
        if iOff > 10 {
            data[0] ^= 0xFF
        }
        try data.write(to: url)

        // Access should detect corruption when reading block.
        do {
            _ = try await SSTable.get(handle, key: TableKey(raw: Data("k1".utf8)))
            XCTFail("expected corruption error")
        } catch {
            // expected
        }
    }
}
