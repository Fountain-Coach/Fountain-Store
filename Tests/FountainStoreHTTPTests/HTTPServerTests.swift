import XCTest
import Foundation

final class HTTPServerTests: XCTestCase {
    private func launchServer(port: Int, dir: URL) throws -> Process {
        let proc = Process()
        #if os(macOS)
        proc.executableURL = URL(fileURLWithPath: ".build/debug/FountainStoreHTTPServer")
        #else
        proc.executableURL = URL(fileURLWithPath: ".build/debug/FountainStoreHTTPServer")
        #endif
        proc.environment = [
            "PORT": String(port),
            "FS_PATH": dir.path
        ]
        let outPipe = Pipe()
        proc.standardOutput = outPipe
        proc.standardError = outPipe
        try proc.run()
        return proc
    }

    private func request(_ method: String, _ url: URL, json body: Any? = nil) async throws -> (Int, Data) {
        var req = URLRequest(url: url)
        req.httpMethod = method
        if let b = body {
            req.httpBody = try JSONSerialization.data(withJSONObject: b)
            req.addValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        let (data, resp) = try await URLSession.shared.data(for: req)
        return ((resp as? HTTPURLResponse)?.statusCode ?? -1, data)
    }

    func test_health_and_collections_crud() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 18080..<(18080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // Wait for server readiness by polling /health
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        var ready = false
        for _ in 0..<50 { // up to ~5s
            do {
                let (code, _) = try await request("GET", healthURL)
                if code == 200 { ready = true; break }
            } catch {
                // ignore until ready
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        XCTAssertTrue(ready, "server did not become ready")

        // GET /health
        let (hStatus, hData) = try await request("GET", healthURL)
        XCTAssertEqual(hStatus, 200)
        if let obj = try? JSONSerialization.jsonObject(with: hData) as? [String: Any] {
            XCTAssertEqual(obj["status"] as? String, "ok")
            XCTAssertNotNil((obj["engine"] as? [String: Any])?["sequence"])
        } else { XCTFail("invalid json") }

        // POST /collections
        let name = "e2e_docs"
        let (cStatus, _) = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])
        XCTAssertEqual(cStatus, 201)

        // GET /collections
        let (listStatus, listData) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/collections")!)
        XCTAssertEqual(listStatus, 200)
        if let obj = try? JSONSerialization.jsonObject(with: listData) as? [String: Any], let items = obj["items"] as? [[String: Any]] {
            XCTAssertTrue(items.contains(where: { ($0["name"] as? String) == name }))
        } else { XCTFail("invalid json") }

        // DELETE /collections/{name}
        let (delStatus, _) = try await request("DELETE", URL(string: "http://127.0.0.1:\(port)/collections/\(name)")!)
        XCTAssertEqual(delStatus, 204)
        // GET again should be 404
        let (getStatus, _) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/collections/\(name)")!)
        XCTAssertEqual(getStatus, 404)
    }

    func test_put_records_201_vs_200_and_get_delete() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 19080..<(19080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // Wait for readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        // Create collection
        let name = "e2e_put"
        _ = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])

        // First PUT => 201
        let recURL = URL(string: "http://127.0.0.1:\(port)/collections/\(name)/records/abc")!
        let (s1, d1) = try await request("PUT", recURL, json: ["data": ["x": 1]])
        XCTAssertEqual(s1, 201, String(data: d1, encoding: .utf8) ?? "")

        // Second PUT => 200
        let (s2, _) = try await request("PUT", recURL, json: ["data": ["x": 2]])
        XCTAssertEqual(s2, 200)

        // GET => 200
        let (s3, gData) = try await request("GET", recURL)
        XCTAssertEqual(s3, 200)
        if let obj = try? JSONSerialization.jsonObject(with: gData) as? [String: Any] {
            XCTAssertEqual(obj["id"] as? String, "abc")
        } else { XCTFail("invalid json") }

        // DELETE => 204
        let (s4, _) = try await request("DELETE", recURL)
        XCTAssertEqual(s4, 204)

        // GET after delete => 404
        let (s5, _) = try await request("GET", recURL)
        XCTAssertEqual(s5, 404)
    }

    func test_indexes_and_query_pagination() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 20080..<(20080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // Wait for readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        let name = "e2e_idx"
        _ = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])

        // Define index on .tag
        let (istatus, _) = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections/\(name)/indexes")!, json: [
            "name": "byTag",
            "kind": "multi",
            "keyPath": ".tag"
        ])
        XCTAssertEqual(istatus, 201)

        // List indexes
        let (liStatus, liData) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/collections/\(name)/indexes")!)
        XCTAssertEqual(liStatus, 200)
        if let obj = try? JSONSerialization.jsonObject(with: liData) as? [String: Any], let items = obj["items"] as? [[String: Any]] {
            XCTAssertTrue(items.contains(where: { ($0["name"] as? String) == "byTag" }))
        } else { XCTFail("invalid json") }

        // Insert records sharing tag = "x"
        for i in 1...5 {
            let rid = String(format: "a%02d", i)
            _ = try await request("PUT", URL(string: "http://127.0.0.1:\(port)/collections/\(name)/records/\(rid)")!, json: ["data": ["tag": "x"]])
        }

        // Query by index with pageSize = 2
        let qURL = URL(string: "http://127.0.0.1:\(port)/collections/\(name)/query")!
        var (q1s, q1d) = try await request("POST", qURL, json: [
            "type": "indexEquals",
            "index": "byTag",
            "key": "x",
            "pageSize": 2
        ])
        XCTAssertEqual(q1s, 200)
        var token: String? = nil
        if let obj = try? JSONSerialization.jsonObject(with: q1d) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            token = obj["nextPageToken"] as? String
            XCTAssertNotNil(token)
        } else { XCTFail("invalid json") }

        // Second page
        (q1s, q1d) = try await request("POST", qURL, json: [
            "type": "indexEquals",
            "index": "byTag",
            "key": "x",
            "pageSize": 2,
            "pageToken": token as Any
        ])
        XCTAssertEqual(q1s, 200)
        if let obj = try? JSONSerialization.jsonObject(with: q1d) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            token = obj["nextPageToken"] as? String
            XCTAssertNotNil(token)
        } else { XCTFail("invalid json") }

        // Third (last) page
        (q1s, q1d) = try await request("POST", qURL, json: [
            "type": "indexEquals",
            "index": "byTag",
            "key": "x",
            "pageSize": 2,
            "pageToken": token as Any
        ])
        XCTAssertEqual(q1s, 200)
        if let obj = try? JSONSerialization.jsonObject(with: q1d) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 1)
            XCTAssertNil(obj["nextPageToken"] as? String)
        } else { XCTFail("invalid json") }
    }

    func test_scan_pagination_and_error_shapes() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 21080..<(21080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // Wait for readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        let name = "e2e_scan"
        _ = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])

        for id in ["a1","a2","a3","a4"] {
            _ = try await request("PUT", URL(string: "http://127.0.0.1:\(port)/collections/\(name)/records/\(id)")!, json: ["data": [:]])
        }

        let qURL = URL(string: "http://127.0.0.1:\(port)/collections/\(name)/query")!
        var (qs, qd) = try await request("POST", qURL, json: [
            "type": "scan",
            "limit": 2
        ])
        XCTAssertEqual(qs, 200)
        var token: String? = nil
        if let obj = try? JSONSerialization.jsonObject(with: qd) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            token = obj["nextPageToken"] as? String
            XCTAssertNotNil(token)
        } else { XCTFail("invalid json") }

        (qs, qd) = try await request("POST", qURL, json: [
            "type": "scan",
            "startAfter": token as Any,
            "limit": 2
        ])
        XCTAssertEqual(qs, 200)
        if let obj = try? JSONSerialization.jsonObject(with: qd) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            XCTAssertNil(obj["nextPageToken"] as? String)
        } else { XCTFail("invalid json") }

        // Error shape: unknown collection indexes list
        let (st404, prob) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/collections/nope/indexes")!)
        XCTAssertEqual(st404, 404)
        if let obj = try? JSONSerialization.jsonObject(with: prob) as? [String: Any] {
            XCTAssertEqual(obj["status"] as? Int, 404)
            XCTAssertEqual(obj["title"] as? String, "not found")
        } else { XCTFail("invalid problem json") }
    }

    func test_transactions_and_snapshots() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 22080..<(22080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // Wait for readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        let name = "e2e_tx"
        _ = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])

        // Commit a transaction: 2 puts
        let txURL = URL(string: "http://127.0.0.1:\(port)/transactions")!
        let (txs, txd) = try await request("POST", txURL, json: [
            "operations": [
                ["op": "put", "collection": name, "record": ["id": "t1", "data": [:]]],
                ["op": "put", "collection": name, "record": ["id": "t2", "data": [:]]]
            ]
        ])
        XCTAssertEqual(txs, 200)
        if let obj = try? JSONSerialization.jsonObject(with: txd) as? [String: Any] {
            XCTAssertNotNil(obj["committedSequence"])
            let res = (obj["results"] as? [[String: Any]]) ?? []
            XCTAssertEqual(res.count, 2)
            XCTAssertTrue(res.allSatisfy { ($0["status"] as? String) == "ok" })
        } else { XCTFail("invalid tx json") }

        // Conflict via requireSequenceAtLeast larger than current => RFC7807 in results
        // Read current sequence from /status to set a too-high guard.
        let (stStatus, stData) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/status")!)
        XCTAssertEqual(stStatus, 200)
        let curSeq: Int
        if let obj = try? JSONSerialization.jsonObject(with: stData) as? [String: Any] {
            curSeq = (obj["sequence"] as? Int) ?? 0
        } else { XCTFail("invalid status json"); return }
        let (txs2, txd2) = try await request("POST", txURL, json: [
            "operations": [ ["op": "put", "collection": name, "record": ["id": "t3", "data": [:]]] ],
            "requireSequenceAtLeast": curSeq + 999_999
        ])
        XCTAssertEqual(txs2, 200)
        if let obj = try? JSONSerialization.jsonObject(with: txd2) as? [String: Any],
           let res = (obj["results"] as? [[String: Any]])?.first {
            XCTAssertEqual(res["status"] as? String, "error")
            let prob = res["error"] as? [String: Any]
            XCTAssertEqual(prob?["status"] as? Int, 409)
            XCTAssertEqual(prob?["title"] as? String, "conflict")
        } else { XCTFail("invalid tx error json") }

        // Snapshot create/read-after-delete behavior
        // Create a record, then snapshot, then delete. Snapshot read should still see the old value.
        let recURL = URL(string: "http://127.0.0.1:\(port)/collections/\(name)/records/snap1")!
        _ = try await request("PUT", recURL, json: ["data": ["y": 1]])
        let (ps, pd) = try await request("POST", URL(string: "http://127.0.0.1:\(port)/snapshots")!)
        XCTAssertEqual(ps, 201)
        let snapId: String
        if let obj = try? JSONSerialization.jsonObject(with: pd) as? [String: Any] {
            snapId = (obj["id"] as? String) ?? ""
            XCTAssertFalse(snapId.isEmpty)
        } else { XCTFail("invalid snapshot json"); return }
        _ = try await request("DELETE", recURL)

        // Read with snapshot should still see it
        let (gs, gd) = try await request("GET", URL(string: recURL.absoluteString + "?snapshot=\(snapId)")!)
        XCTAssertEqual(gs, 200)
        if let obj = try? JSONSerialization.jsonObject(with: gd) as? [String: Any] {
            XCTAssertEqual(obj["id"] as? String, "snap1")
        } else { XCTFail("invalid json") }

        // Read without snapshot should be gone
        let (gs2, _) = try await request("GET", recURL)
        XCTAssertEqual(gs2, 404)
    }


    func test_create_collection_conflict_and_invalid_name() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 23080..<(23080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        // Create once
        let name = "dupe"
        let (s1, _) = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])
        XCTAssertEqual(s1, 201)
        // Create again => 409
        let (s2, d2) = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])
        XCTAssertEqual(s2, 409, String(data: d2, encoding: .utf8) ?? "")

        // Invalid name => 400
        let (s3, _) = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": "bad name with spaces"]) 
        XCTAssertEqual(s3, 400)
    }

    func test_index_define_404_and_409() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 24080..<(24080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        // POST index on missing collection => 404
        let (s404, _) = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections/nope/indexes")!, json: ["name": "byTag", "kind": "multi", "keyPath": ".tag"]) 
        XCTAssertEqual(s404, 404)

        // Create collection
        let name = "idxdupe"
        _ = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])
        // Define index twice => second 409
        let idxURL = URL(string: "http://127.0.0.1:\(port)/collections/\(name)/indexes")!
        let (s1, _) = try await request("POST", idxURL, json: ["name": "byTag", "kind": "multi", "keyPath": ".tag"]) 
        XCTAssertEqual(s1, 201)
        let (s2, _) = try await request("POST", idxURL, json: ["name": "byTag", "kind": "multi", "keyPath": ".tag"]) 
        XCTAssertEqual(s2, 409)
    }

    func test_record_id_mismatch_and_delete_404() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 25080..<(25080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        let name = "recerrs"
        _ = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])

        // Mismatch id => 400
        let url = URL(string: "http://127.0.0.1:\(port)/collections/\(name)/records/pathid")!
        let (s1, _) = try await request("PUT", url, json: ["id": "bodyid", "data": [:]])
        XCTAssertEqual(s1, 400)

        // DELETE missing => 404
        let (s2, _) = try await request("DELETE", URL(string: "http://127.0.0.1:\(port)/collections/\(name)/records/nope")!)
        XCTAssertEqual(s2, 404)
    }

    func test_compaction_run_invalid_body_and_tick() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 26080..<(26080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        let runURL = URL(string: "http://127.0.0.1:\(port)/compaction/run")!
        // Missing mode => 400
        let (s1, _) = try await request("POST", runURL, json: [:])
        XCTAssertEqual(s1, 400)
        // Invalid mode => 400
        let (s2, _) = try await request("POST", runURL, json: ["mode": "weird"]) 
        XCTAssertEqual(s2, 400)
        // tick => 202
        let (s3, _) = try await request("POST", runURL, json: ["mode": "tick"]) 
        XCTAssertEqual(s3, 202)
    }

    func test_collections_pagination_and_status_compaction() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 27080..<(27080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        // Create 5 collections
        for n in ["a","b","c","d","e"] { _ = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": n]) }

        // Page 1
        var (s1, d1) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/collections?pageSize=2")!)
        XCTAssertEqual(s1, 200)
        var next: String? = nil
        if let obj = try? JSONSerialization.jsonObject(with: d1) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            next = obj["nextPageToken"] as? String
            XCTAssertNotNil(next)
        } else { XCTFail("invalid json") }

        // Page 2
        (s1, d1) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/collections?pageSize=2&pageToken=\(next!)")!)
        XCTAssertEqual(s1, 200)
        if let obj = try? JSONSerialization.jsonObject(with: d1) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            next = obj["nextPageToken"] as? String
            XCTAssertNotNil(next)
        } else { XCTFail("invalid json") }

        // Page 3 (last)
        (s1, d1) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/collections?pageSize=2&pageToken=\(next!)")!)
        XCTAssertEqual(s1, 200)
        if let obj = try? JSONSerialization.jsonObject(with: d1) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 1)
            XCTAssertNil(obj["nextPageToken"])
        } else { XCTFail("invalid json") }

        // Status includes compaction
        let (st, sd) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/status")!)
        XCTAssertEqual(st, 200)
        if let obj = try? JSONSerialization.jsonObject(with: sd) as? [String: Any] {
            _ = obj["compaction"] as? [String: Any] // optional presence is fine
        } else { XCTFail("invalid json") }
        // Compaction status endpoint
        let (cs, cd) = try await request("GET", URL(string: "http://127.0.0.1:\(port)/compaction/status")!)
        XCTAssertEqual(cs, 200)
        if let _ = try? JSONSerialization.jsonObject(with: cd) as? [String: Any] {
            // shape checked loosely
        } else { XCTFail("invalid compaction json") }
    }

    func test_indexes_pagination_and_backups() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let port = Int.random(in: 28080..<(28080+1000))
        let p = try launchServer(port: port, dir: dir)
        defer { p.terminate(); try? p.waitUntilExit(); try? FileManager.default.removeItem(at: dir) }

        // readiness
        let healthURL = URL(string: "http://127.0.0.1:\(port)/health")!
        for _ in 0..<50 { if (try? await request("GET", healthURL).0) == 200 { break }; try await Task.sleep(nanoseconds: 100_000_000) }

        // Create collection
        let name = "pidx"
        _ = try await request("POST", URL(string: "http://127.0.0.1:\(port)/collections")!, json: ["name": name])
        let idxURL = URL(string: "http://127.0.0.1:\(port)/collections/\(name)/indexes")!
        for i in 1...5 { _ = try await request("POST", idxURL, json: ["name": "i\(i)", "kind": "multi", "keyPath": ".tag"]) }

        // Page indexes
        var (s1, d1) = try await request("GET", URL(string: idxURL.absoluteString + "?pageSize=2")!)
        XCTAssertEqual(s1, 200)
        var next: String? = nil
        if let obj = try? JSONSerialization.jsonObject(with: d1) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            next = obj["nextPageToken"] as? String
            XCTAssertNotNil(next)
        } else { XCTFail("invalid json") }

        (s1, d1) = try await request("GET", URL(string: idxURL.absoluteString + "?pageSize=2&pageToken=\(next!)")!)
        XCTAssertEqual(s1, 200)
        if let obj = try? JSONSerialization.jsonObject(with: d1) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            next = obj["nextPageToken"] as? String
            XCTAssertNotNil(next)
        } else { XCTFail("invalid json") }

        (s1, d1) = try await request("GET", URL(string: idxURL.absoluteString + "?pageSize=2&pageToken=\(next!)")!)
        XCTAssertEqual(s1, 200)
        if let obj = try? JSONSerialization.jsonObject(with: d1) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 1)
            XCTAssertNil(obj["nextPageToken"])
        } else { XCTFail("invalid json") }

        // Backups list/create/restore + pagination
        let listURL = URL(string: "http://127.0.0.1:\(port)/backups")!
        // initial empty list
        var (lb, lbd) = try await request("GET", listURL)
        XCTAssertEqual(lb, 200)
        if let obj = try? JSONSerialization.jsonObject(with: lbd) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            // may contain from prior runs in same dir, but we created fresh dir so expect 0
            XCTAssertEqual(items.count, 0)
        } else { XCTFail("invalid json") }

        // create 3 backups
        var ids: [String] = []
        for i in 1...3 {
            let (cs, cd) = try await request("POST", listURL, json: ["note": "n\(i)"])
            XCTAssertEqual(cs, 201)
            if let obj = try? JSONSerialization.jsonObject(with: cd) as? [String: Any] {
                if let id = obj["id"] as? String { ids.append(id) }
            }
        }
        XCTAssertEqual(ids.count, 3)

        (lb, lbd) = try await request("GET", URL(string: listURL.absoluteString + "?pageSize=2")!)
        XCTAssertEqual(lb, 200)
        var nextBk: String? = nil
        if let obj = try? JSONSerialization.jsonObject(with: lbd) as? [String: Any] {
            let items = (obj["items"] as? [[String: Any]]) ?? []
            XCTAssertEqual(items.count, 2)
            nextBk = obj["nextPageToken"] as? String
            XCTAssertNotNil(nextBk)
        } else { XCTFail("invalid json") }

        let (rb, _) = try await request("POST", URL(string: "http://127.0.0.1:\(port)/backups/\(ids[0])/restore")!)
        XCTAssertEqual(rb, 202)
    }
}
