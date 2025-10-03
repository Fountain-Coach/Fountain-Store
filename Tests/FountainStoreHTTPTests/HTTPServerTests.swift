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
}
