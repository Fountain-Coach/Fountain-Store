import Foundation
import FountainStore
import FountainStoreHTTP

@main
struct FountainStoreMCP {
    static func main() async {
        let env = ProcessInfo.processInfo.environment
        let storePath = env["STORE_PATH"] ?? ".fountainstore-local"
        let cacheBytes = intEnv(env, "STORE_CACHE_BYTES")
        let defaultScanLimit = intEnv(env, "STORE_DEFAULT_SCAN_LIMIT")
        let walSegmentBytes = intEnv(env, "STORE_WAL_SEGMENT_BYTES")

        let options = StoreOptions(
            path: URL(fileURLWithPath: storePath),
            cacheBytes: cacheBytes ?? (64 << 20),
            logger: nil,
            defaultScanLimit: defaultScanLimit ?? 100,
            walSegmentBytes: walSegmentBytes ?? (4 << 20)
        )

        let store: FountainStore
        do {
            store = try await FountainStore.open(options)
        } catch {
            fputs("[mcp] failed to open store at \(storePath): \(error)\n", stderr)
            return
        }
        let admin = AdminService(store: store)

        let server = StdioJsonRpcServer()
        await server.run { msg in
            switch msg.method {
            case "initialize":
                let result: [String: Any] = [
                    "protocolVersion": "2024-11-05",
                    "serverInfo": ["name": "FountainStoreMCP", "version": "0.2.1"],
                    "capabilities": [
                        "tools": ["listChanged": false],
                        "resources": ["listChanged": false],
                        "prompts": ["listChanged": false]
                    ]
                ]
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: result)
            case "initialized":
                return JsonRpcReply.none()
            case "shutdown":
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: NSNull())
            case "exit":
                return JsonRpcReply.exit()
            case "tools/list":
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["tools": toolSpecs()])
            case "tools/call":
                return await callTool(msg: msg, admin: admin)
            case "resources/list":
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["resources": []])
            case "resources/read":
                return JsonRpcReply.error(id: msg.id, code: -32000, message: "resources/read not supported")
            case "prompts/list":
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["prompts": []])
            case "prompts/get":
                return JsonRpcReply.error(id: msg.id, code: -32000, message: "prompts/get not supported")
            case "logging/setLevel":
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: NSNull())
            default:
                return JsonRpcReply.error(id: msg.id, code: -32601, message: "Method not found")
            }
        }
    }

    private static func toolSpecs() -> [[String: Any]] {
        [
            tool(name: "fountainstore.health", description: "Store health + sequence.", properties: [:], required: []),
            tool(name: "fountainstore.status", description: "Store status summary.", properties: [:], required: []),
            tool(name: "fountainstore.metrics", description: "Metrics snapshot.", properties: [:], required: []),

            tool(name: "fountainstore.collections.list", description: "List collections.", properties: [:], required: []),
            tool(
                name: "fountainstore.collections.create",
                description: "Create a collection.",
                properties: ["name": ["type": "string"]],
                required: ["name"]
            ),
            tool(
                name: "fountainstore.collections.drop",
                description: "Drop a collection (deletes all records).",
                properties: ["name": ["type": "string"]],
                required: ["name"]
            ),

            tool(
                name: "fountainstore.indexes.list",
                description: "List index names for a collection.",
                properties: ["collection": ["type": "string"]],
                required: ["collection"]
            ),
            tool(
                name: "fountainstore.indexes.definitions",
                description: "List index definitions for a collection.",
                properties: ["collection": ["type": "string"]],
                required: ["collection"]
            ),
            tool(
                name: "fountainstore.indexes.define",
                description: "Define a dynamic index for a collection.",
                properties: [
                    "collection": ["type": "string"],
                    "definition": [
                        "type": "object",
                        "properties": [
                            "name": ["type": "string"],
                            "kind": ["type": "string"],
                            "keyPath": ["type": "string"],
                            "options": ["type": "object"]
                        ],
                        "required": ["name", "kind", "keyPath"]
                    ]
                ],
                required: ["collection", "definition"]
            ),
            tool(
                name: "fountainstore.indexes.rebuild",
                description: "Rebuild dynamic indexes from persisted definitions.",
                properties: [:],
                required: []
            ),

            tool(
                name: "fountainstore.record.put",
                description: "Put a record into a collection.",
                properties: [
                    "collection": ["type": "string"],
                    "id": ["type": "string"],
                    "data": ["type": ["object", "array", "string", "number", "boolean", "null"]]
                ],
                required: ["collection", "id", "data"]
            ),
            tool(
                name: "fountainstore.record.get",
                description: "Get a record from a collection.",
                properties: [
                    "collection": ["type": "string"],
                    "id": ["type": "string"],
                    "snapshotId": ["type": "string"]
                ],
                required: ["collection", "id"]
            ),
            tool(
                name: "fountainstore.record.getMeta",
                description: "Get a record with metadata.",
                properties: [
                    "collection": ["type": "string"],
                    "id": ["type": "string"],
                    "snapshotId": ["type": "string"]
                ],
                required: ["collection", "id"]
            ),
            tool(
                name: "fountainstore.record.delete",
                description: "Delete a record from a collection.",
                properties: [
                    "collection": ["type": "string"],
                    "id": ["type": "string"]
                ],
                required: ["collection", "id"]
            ),

            tool(
                name: "fountainstore.query",
                description: "Run a query against a collection.",
                properties: [
                    "collection": ["type": "string"],
                    "query": ["type": "object"],
                    "snapshotId": ["type": "string"]
                ],
                required: ["collection", "query"]
            ),

            tool(
                name: "fountainstore.transaction.commit",
                description: "Commit a transactional batch.",
                properties: [
                    "transaction": ["type": "object"]
                ],
                required: ["transaction"]
            ),

            tool(name: "fountainstore.snapshot.create", description: "Create a snapshot.", properties: [:], required: []),
            tool(
                name: "fountainstore.snapshot.release",
                description: "Release a snapshot by id.",
                properties: ["snapshotId": ["type": "string"]],
                required: ["snapshotId"]
            ),

            tool(name: "fountainstore.compaction.status", description: "Compaction status.", properties: [:], required: []),
            tool(name: "fountainstore.compaction.tick", description: "Trigger a compaction tick.", properties: [:], required: [])
        ]
    }

    private static func tool(name: String, description: String, properties: [String: Any], required: [String]) -> [String: Any] {
        [
            "name": name,
            "description": description,
            "inputSchema": [
                "type": "object",
                "properties": properties,
                "required": required
            ]
        ]
    }

    @MainActor
    private static func callTool(msg: JsonRpcMessage, admin: AdminService) async -> JsonRpcReply {
        guard let params = msg.params as? [String: Any],
              let name = params["name"] as? String else {
            return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing tool name")
        }
        let args = params["arguments"] as? [String: Any] ?? [:]

        do {
            switch name {
            case "fountainstore.health":
                let res = await admin.health()
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))
            case "fountainstore.status":
                let res = await admin.status()
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))
            case "fountainstore.metrics":
                let res = await admin.metrics()
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))

            case "fountainstore.collections.list":
                let res = await admin.listCollections()
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["items": res])
            case "fountainstore.collections.create":
                guard let name = args["name"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing name")
                }
                let res = await admin.createCollection(name)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["name": res])
            case "fountainstore.collections.drop":
                guard let name = args["name"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing name")
                }
                try await admin.dropCollection(name)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["status": "ok"])

            case "fountainstore.indexes.list":
                guard let collection = args["collection"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing collection")
                }
                let res = await admin.listIndexNames(collection)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["items": res])
            case "fountainstore.indexes.definitions":
                guard let collection = args["collection"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing collection")
                }
                let res = await admin.listIndexDefinitions(collection)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))
            case "fountainstore.indexes.define":
                guard let collection = args["collection"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing collection")
                }
                guard let defValue = args["definition"],
                      let def = decode(defValue, as: AdminService.IndexDefinition.self) else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Invalid definition")
                }
                let res = try await admin.defineIndex(collection: collection, def: def)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))
            case "fountainstore.indexes.rebuild":
                await admin.rebuildDynamicIndexes()
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["status": "ok"])

            case "fountainstore.record.put":
                guard let collection = args["collection"] as? String,
                      let id = args["id"] as? String,
                      let dataValue = args["data"],
                      let data = decode(dataValue, as: AnyJSON.self) else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing or invalid data")
                }
                let doc = try await admin.putRecord(collection: collection, id: id, data: data)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(doc))
            case "fountainstore.record.get":
                guard let collection = args["collection"] as? String,
                      let id = args["id"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing collection or id")
                }
                let snapshotId = args["snapshotId"] as? String
                let doc = try await admin.getRecord(collection: collection, id: id, snapshotId: snapshotId)
                if let doc {
                    return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(doc))
                }
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: NSNull())
            case "fountainstore.record.getMeta":
                guard let collection = args["collection"] as? String,
                      let id = args["id"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing collection or id")
                }
                let snapshotId = args["snapshotId"] as? String
                let doc = try await admin.getRecordWithMeta(collection: collection, id: id, snapshotId: snapshotId)
                if let doc {
                    return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(doc))
                }
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: NSNull())
            case "fountainstore.record.delete":
                guard let collection = args["collection"] as? String,
                      let id = args["id"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing collection or id")
                }
                try await admin.deleteRecord(collection: collection, id: id)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["status": "ok"])

            case "fountainstore.query":
                guard let collection = args["collection"] as? String,
                      let queryValue = args["query"],
                      let query = decode(queryValue, as: AdminService.Query.self) else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing or invalid query")
                }
                let snapshotId = args["snapshotId"] as? String
                let res = try await admin.query(collection: collection, query: query, snapshotId: snapshotId)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))

            case "fountainstore.transaction.commit":
                guard let txValue = args["transaction"],
                      let tx = decode(txValue, as: AdminService.Transaction.self) else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing or invalid transaction")
                }
                let res = await admin.commitTransaction(tx)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))

            case "fountainstore.snapshot.create":
                let res = await admin.createSnapshot()
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))
            case "fountainstore.snapshot.release":
                guard let snapshotId = args["snapshotId"] as? String else {
                    return JsonRpcReply.error(id: msg.id, code: -32602, message: "Missing snapshotId")
                }
                let ok = await admin.releaseSnapshot(snapshotId)
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["released": ok])

            case "fountainstore.compaction.status":
                let res = try await admin.compactionStatus()
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: encode(res))
            case "fountainstore.compaction.tick":
                await admin.compactionTick()
                return JsonRpcReply.result(id: msg.id ?? NSNull(), value: ["status": "ok"])

            default:
                return JsonRpcReply.error(id: msg.id, code: -32601, message: "Unknown tool \(name)")
            }
        } catch {
            return JsonRpcReply.error(id: msg.id, code: -32000, message: "FountainStore error", data: "\(error)")
        }
    }

    private static func intEnv(_ env: [String: String], _ key: String) -> Int? {
        guard let raw = env[key], !raw.isEmpty else { return nil }
        return Int(raw)
    }

    private static func encode<T: Encodable>(_ value: T) -> Any {
        let encoder = JSONEncoder()
        guard let data = try? encoder.encode(value),
              let obj = try? JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed]) else {
            return NSNull()
        }
        return obj
    }

    private struct DecodeWrapper<T: Decodable>: Decodable { let value: T }

    private static func decode<T: Decodable>(_ value: Any, as type: T.Type) -> T? {
        let wrapper: [String: Any] = ["value": value]
        guard JSONSerialization.isValidJSONObject(wrapper),
              let data = try? JSONSerialization.data(withJSONObject: wrapper, options: []) else {
            return nil
        }
        return try? JSONDecoder().decode(DecodeWrapper<T>.self, from: data).value
    }
}
