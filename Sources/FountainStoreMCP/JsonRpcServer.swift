import Foundation

public struct JsonRpcMessage {
    public let id: Any?
    public let method: String
    public let params: Any?

    public init?(from object: [String: Any]) {
        guard let method = object["method"] as? String else { return nil }
        self.id = object["id"]
        self.method = method
        self.params = object["params"]
    }
}

public struct JsonRpcReply {
    public let response: [String: Any]?
    public let shouldExit: Bool

    public static func none() -> JsonRpcReply {
        JsonRpcReply(response: nil, shouldExit: false)
    }

    public static func exit() -> JsonRpcReply {
        JsonRpcReply(response: nil, shouldExit: true)
    }

    public static func result(id: Any, value: Any) -> JsonRpcReply {
        let payload: [String: Any] = [
            "jsonrpc": "2.0",
            "id": id,
            "result": value
        ]
        return JsonRpcReply(response: payload, shouldExit: false)
    }

    public static func error(id: Any?, code: Int, message: String, data: Any? = nil) -> JsonRpcReply {
        var err: [String: Any] = [
            "code": code,
            "message": message
        ]
        if let data { err["data"] = data }
        let payload: [String: Any] = [
            "jsonrpc": "2.0",
            "id": id ?? NSNull(),
            "error": err
        ]
        return JsonRpcReply(response: payload, shouldExit: false)
    }
}

@MainActor
public final class StdioJsonRpcServer {
    private let stdin = FileHandle.standardInput
    private let stdout = FileHandle.standardOutput
    private var buffer = Data()

    public init() {}

    public func run(handler: @MainActor @escaping (JsonRpcMessage) async -> JsonRpcReply) async {
        while true {
            let data = stdin.readData(ofLength: 4096)
            if data.isEmpty { break }
            buffer.append(data)
            while let payload = nextPayload() {
                guard let obj = try? JSONSerialization.jsonObject(with: payload) as? [String: Any],
                      let msg = JsonRpcMessage(from: obj) else {
                    continue
                }
                let reply = await handler(msg)
                if let response = reply.response {
                    send(response)
                }
                if reply.shouldExit {
                    return
                }
            }
        }
    }

    private func nextPayload() -> Data? {
        let delimiter = Data("\r\n\r\n".utf8)
        guard let headerRange = buffer.range(of: delimiter) else { return nil }
        let headerData = buffer.subdata(in: 0..<headerRange.lowerBound)
        let headerText = String(data: headerData, encoding: .utf8) ?? ""
        var contentLength: Int?
        for line in headerText.split(whereSeparator: \.isNewline) {
            let parts = line.split(separator: ":", maxSplits: 1)
            guard parts.count == 2 else { continue }
            if parts[0].trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "content-length" {
                contentLength = Int(parts[1].trimmingCharacters(in: .whitespacesAndNewlines))
                break
            }
        }
        let bodyStart = headerRange.upperBound
        guard let length = contentLength else {
            buffer.removeSubrange(0..<bodyStart)
            return nil
        }
        guard buffer.count >= bodyStart + length else { return nil }
        let body = buffer.subdata(in: bodyStart..<(bodyStart + length))
        buffer.removeSubrange(0..<(bodyStart + length))
        return body
    }

    private func send(_ payload: [String: Any]) {
        guard let data = try? JSONSerialization.data(withJSONObject: payload, options: []) else { return }
        let header = "Content-Length: \(data.count)\r\n\r\n"
        if let headerData = header.data(using: .utf8) {
            stdout.write(headerData)
        }
        stdout.write(data)
    }
}
