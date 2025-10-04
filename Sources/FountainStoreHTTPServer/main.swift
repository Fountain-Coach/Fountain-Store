import Foundation
@preconcurrency import NIO
@preconcurrency import NIOHTTP1
import FountainStoreHTTP
import SecretStore
import FountainStore

final class HTTPHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart
    private let admin: AdminService
    private let apiKey: String?
    private var bodyBuffer: ByteBuffer?
    private var lastHead: HTTPRequestHead?

    init(admin: AdminService, apiKey: String?) { self.admin = admin; self.apiKey = apiKey }

    struct Response { let status: HTTPResponseStatus; let headers: [(String,String)]; let body: Data }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch self.unwrapInboundIn(data) {
        case .head(let head):
            lastHead = head
            bodyBuffer = context.channel.allocator.buffer(capacity: 0)
        case .body(var buf):
            if bodyBuffer == nil { bodyBuffer = context.channel.allocator.buffer(capacity: 0) }
            bodyBuffer?.writeBuffer(&buf)
        case .end:
            let head = lastHead
            let reqBody = bodyBuffer?.getString(at: 0, length: bodyBuffer?.readableBytes ?? 0) ?? ""
            Task { [admin, apiKey] in
                let resp = await HTTPHandler.route(admin: admin, apiKey: apiKey, head: head, rawBody: reqBody)
                context.eventLoop.execute { self.writeResponse(resp, context: context) }
            }
        }
    }

    func writeResponse(_ resp: Response, context: ChannelHandlerContext) {
        var head = HTTPResponseHead(version: .http1_1, status: resp.status)
        head.headers.add(name: "Content-Length", value: String(resp.body.count))
        for (k,v) in resp.headers { head.headers.add(name: k, value: v) }
        context.write(self.wrapOutboundOut(.head(head)), promise: nil)
        var buf = context.channel.allocator.buffer(capacity: resp.body.count)
        buf.writeBytes(resp.body)
        context.write(self.wrapOutboundOut(.body(.byteBuffer(buf))), promise: nil)
        context.writeAndFlush(self.wrapOutboundOut(.end(nil)), promise: nil)
    }

    static func json<T: Encodable>(_ value: T, status: HTTPResponseStatus = .ok) -> Response {
        let data = (try? JSONEncoder().encode(value)) ?? Data("{}".utf8)
        return Response(status: status, headers: [("content-type", "application/json")], body: data)
    }
    static func problem(_ p: AdminService.Problem) -> Response {
        let data = (try? JSONEncoder().encode(p)) ?? Data("{}".utf8)
        return Response(status: HTTPResponseStatus(statusCode: p.status), headers: [("content-type", "application/problem+json")], body: data)
    }

    static func route(admin: AdminService, apiKey: String?, head: HTTPRequestHead?, rawBody: String) async -> Response {
        guard let head = head else { return Response(status: .badRequest, headers: [("content-type","text/plain")], body: Data("bad request".utf8)) }
        let method = head.method
        let uri = head.uri
        // API key authentication if configured
        if let required = apiKey, !required.isEmpty {
            let hdrs = head.headers
            let provided = hdrs.first(name: "x-api-key") ?? {
                if let auth = hdrs.first(name: "authorization"), auth.lowercased().hasPrefix("bearer ") { return String(auth.dropFirst(7)) }
                return nil
            }()
            if provided != required {
                return problem(.init(title: "unauthorized", status: 401, detail: nil, instance: nil))
            }
        }
        let dec = JSONDecoder()
        let enc = JSONEncoder()
        enc.outputFormatting = []
        func components(_ path: String) -> [String] {
            var urlPath = path
            if let q = urlPath.firstIndex(of: "?") { urlPath = String(urlPath[..<q]) }
            return urlPath.split(separator: "/").map(String.init)
        }
        func queryItem(_ name: String) -> String? {
            if var comps = URLComponents(string: uri) { return comps.queryItems?.first(where: { $0.name == name })?.value }
            return nil
        }
        let parts = components(uri)

        // Health
        if parts == ["health"], method == .GET {
            let h = await admin.health()
            struct Engine: Codable { let sequence: UInt64; let writable: Bool; let lastFlushMs: Int }
            struct Health: Codable { let status: String; let engine: Engine }
            return json(Health(status: "ok", engine: Engine(sequence: h.sequence, writable: true, lastFlushMs: 0)))
        }
        // Status
        if parts == ["status"], method == .GET {
            let s = await admin.status()
            let comp = try? await admin.compactionStatus()
            struct Status: Codable {
                let sequence: UInt64
                let collectionsCount: Int
                let collections: [AdminService.CollectionRef]
                let compaction: FountainStore.CompactionStatus?
            }
            return json(Status(sequence: s.sequence, collectionsCount: s.collectionsCount, collections: s.collections, compaction: comp))
        }
        // Metrics
        if parts == ["metrics"], method == .GET {
            let m = await admin.metrics()
            return json(m)
        }
        // Collections list/create
        if parts == ["collections"], method == .GET {
            struct RespItem: Codable { let name: String; let recordsApprox: Int }
            struct Resp: Codable { let items: [RespItem]; let nextPageToken: String? }
            let st = await admin.status()
            var items = st.collections.map { RespItem(name: $0.name, recordsApprox: $0.recordsApprox) }.sorted { $0.name < $1.name }
            let pageSize = Int(queryItem("pageSize") ?? "0") ?? 0
            let token = queryItem("pageToken")
            if let t = token { items = Array(items.drop(while: { $0.name <= t })) }
            let limit = pageSize > 0 ? pageSize : items.count
            let page = Array(items.prefix(limit))
            let next = (limit < items.count) ? page.last?.name : nil
            return json(Resp(items: page, nextPageToken: next))
        }
        if parts == ["collections"], method == .POST {
            struct CreateReq: Codable { let name: String; let version: String? }
            guard let data = rawBody.data(using: .utf8), let req = try? dec.decode(CreateReq.self, from: data), !req.name.isEmpty else {
                return problem(.init(title: "invalid body", status: 400, detail: nil, instance: nil))
            }
            // Validate name against OpenAPI pattern
            let pattern = "^[A-Za-z0-9._-]{1,128}$"
            if req.name.range(of: pattern, options: .regularExpression) == nil {
                return problem(.init(title: "invalid collection name", status: 400, detail: "must match ^[A-Za-z0-9._-]{1,128}$", instance: nil))
            }
            let existing = await admin.listCollections()
            if existing.contains(req.name) {
                return problem(.init(title: "conflict", status: 409, detail: "collection exists", instance: nil))
            }
            let name = await admin.createCollection(req.name)
            struct Resp: Codable { let name: String; let recordsApprox: Int }
            return json(Resp(name: name, recordsApprox: 0), status: .created)
        }
        // Collection details
        if parts.count == 2, parts[0] == "collections", method == .GET {
            let c = parts[1]
            struct Resp: Codable { let name: String; let version: String?; let indexes: [AdminService.IndexDefinition] }
            let names = await admin.listCollections()
            guard names.contains(c) else { return problem(.init(title: "not found", status: 404, detail: nil, instance: nil)) }
            let idx = await admin.listIndexDefinitions(c)
            return json(Resp(name: c, version: nil, indexes: idx))
        }
        // Drop collection
        if parts.count == 2, parts[0] == "collections", method == .DELETE {
            let c = parts[1]
            let names = await admin.listCollections()
            guard names.contains(c) else { return problem(.init(title: "not found", status: 404, detail: nil, instance: nil)) }
            do { try await admin.dropCollection(c) } catch {
                return problem(.init(title: "drop failed", status: 500, detail: nil, instance: nil))
            }
            return Response(status: .noContent, headers: [], body: Data())
        }
        // Indexes list/define
        if parts.count == 3, parts[0] == "collections", parts[2] == "indexes", method == .GET {
            let c = parts[1]
            struct Resp: Codable { let items: [AdminService.IndexDefinition]; let nextPageToken: String? }
            let names = await admin.listCollections()
            guard names.contains(c) else { return problem(.init(title: "not found", status: 404, detail: nil, instance: nil)) }
            var idx = await admin.listIndexDefinitions(c)
            idx.sort { $0.name < $1.name }
            let pageSize = Int(queryItem("pageSize") ?? "0") ?? 0
            let token = queryItem("pageToken")
            if let t = token { idx = Array(idx.drop(while: { $0.name <= t })) }
            let limit = pageSize > 0 ? pageSize : idx.count
            let page = Array(idx.prefix(limit))
            let next = (limit < idx.count) ? page.last?.name : nil
            return json(Resp(items: page, nextPageToken: next))
        }
        if parts.count == 3, parts[0] == "collections", parts[2] == "indexes", method == .POST {
            let c = parts[1]
            guard let data = rawBody.data(using: .utf8), let def = try? dec.decode(AdminService.IndexDefinition.self, from: data) else {
                return problem(.init(title: "invalid body", status: 400, detail: nil, instance: nil))
            }
            let names = await admin.listCollections()
            guard names.contains(c) else { return problem(.init(title: "not found", status: 404, detail: nil, instance: nil)) }
            let existingIdx = await admin.listIndexNames(c)
            if existingIdx.contains(def.name) {
                return problem(.init(title: "conflict", status: 409, detail: "index exists", instance: nil))
            }
            do {
                let created = try await admin.defineIndex(collection: c, def: def)
                return json(created, status: .created)
            } catch let e as CollectionError {
                switch e {
                case .uniqueConstraintViolation(let index, let key):
                    return problem(.init(title: "unique constraint", status: 409, detail: "\(index) key=\(key)", instance: nil))
                }
            } catch {
                return problem(.init(title: "index define", status: 500, detail: "failed to define index", instance: nil))
            }
        }
        // Records
        if parts.count == 4, parts[0] == "collections", parts[2] == "records" {
            let c = parts[1], id = parts[3]
            switch method {
            case .GET:
                do {
                    let snap = queryItem("snapshot")
                    if let v = try await admin.getRecordWithMeta(collection: c, id: id, snapshotId: snap) {
                        return json(v)
                    } else {
                        return problem(.init(title: "not found", status: 404, detail: nil, instance: nil))
                    }
                } catch {
                    return problem(.init(title: "get failed", status: 500, detail: nil, instance: nil))
                }
            case .PUT:
                struct Body: Codable { let id: String?; let data: AnyJSON; let version: String? }
                guard let data = rawBody.data(using: .utf8), let req = try? dec.decode(Body.self, from: data) else {
                    return problem(.init(title: "invalid body", status: 400, detail: nil, instance: nil))
                }
                // Enforce path/body id consistency if provided
                if let bid = req.id, bid != id {
                    return problem(.init(title: "invalid body", status: 400, detail: "id in body must match path", instance: nil))
                }
                do {
                    let rid = req.id ?? id
                    let existed = try await admin.getRecord(collection: c, id: rid, snapshotId: nil) != nil
                    let v = try await admin.putRecord(collection: c, id: rid, data: req.data)
                    if existed {
                        let out = try await admin.getRecordWithMeta(collection: c, id: rid, snapshotId: nil) ?? HTTPDocOut(id: v.id, data: v.data, version: v.version, sequence: nil, deleted: false)
                        return json(out, status: .ok)
                    } else {
                        // Encode body manually to attach Location header
                        let out = try await admin.getRecordWithMeta(collection: c, id: rid, snapshotId: nil) ?? HTTPDocOut(id: v.id, data: v.data, version: v.version, sequence: nil, deleted: false)
                        let data = (try? JSONEncoder().encode(out)) ?? Data("{}".utf8)
                        let loc = "/collections/\(c)/records/\(rid)"
                        return Response(status: .created, headers: [("content-type", "application/json"), ("Location", loc)], body: data)
                    }
                } catch let e as CollectionError {
                    switch e {
                    case .uniqueConstraintViolation(let index, let key):
                        return problem(.init(title: "unique constraint", status: 409, detail: "\(index) key=\(key)", instance: nil))
                    }
                } catch {
                    return problem(.init(title: "put failed", status: 500, detail: nil, instance: nil))
                }
            case .DELETE:
                do {
                    // Return 404 if record does not exist
                    if (try await admin.getRecord(collection: c, id: id)) == nil {
                        return problem(.init(title: "not found", status: 404, detail: nil, instance: nil))
                    }
                    try await admin.deleteRecord(collection: c, id: id)
                    return Response(status: .noContent, headers: [], body: Data())
                } catch {
                    return problem(.init(title: "delete failed", status: 500, detail: nil, instance: nil))
                }
            default:
                break
            }
        }
        // Query
        if parts.count == 3, parts[0] == "collections", parts[2] == "query", method == .POST {
            let c = parts[1]
            let snap = queryItem("snapshot")
            guard let data = rawBody.data(using: .utf8), let q = try? dec.decode(AdminService.Query.self, from: data) else {
                return problem(.init(title: "invalid body", status: 400, detail: nil, instance: nil))
            }
            do {
                let resp = try await admin.query(collection: c, query: q, snapshotId: snap)
                // Map items to include metadata
                let coll = c
                var outItems: [HTTPDocOut] = []
                for item in resp.items {
                    if let withMeta = try? await admin.getRecordWithMeta(collection: coll, id: item.id, snapshotId: snap) {
                        outItems.append(withMeta)
                    } else {
                        outItems.append(HTTPDocOut(id: item.id, data: item.data, version: item.version, sequence: nil, deleted: false))
                    }
                }
                struct Out: Codable { let items: [HTTPDocOut]; let nextPageToken: String? }
                return json(Out(items: outItems, nextPageToken: resp.nextPageToken))
            } catch {
                return problem(.init(title: "query failed", status: 500, detail: nil, instance: nil))
            }
        }
        // Backups
        if parts == ["backups"], method == .GET {
            var refs = await admin.underlyingStore().listBackups()
            // Sort newest first by createdAt, but use id for token (stable order with tie-breaker)
            refs.sort { (a, b) in
                if let ad = ISO8601DateFormatter().date(from: a.createdAt), let bd = ISO8601DateFormatter().date(from: b.createdAt), ad != bd {
                    return ad > bd
                }
                return a.id > b.id
            }
            let pageSize = Int(queryItem("pageSize") ?? "0") ?? 0
            let token = queryItem("pageToken")
            if let t = token, let pos = refs.firstIndex(where: { $0.id == t }) { refs = Array(refs.dropFirst(pos+1)) }
            let limit = pageSize > 0 ? pageSize : refs.count
            let page = Array(refs.prefix(limit))
            let next = (limit < refs.count) ? page.last?.id : nil
            struct Resp: Codable { let items: [FountainStore.BackupRef]; let nextPageToken: String? }
            return json(Resp(items: page, nextPageToken: next))
        }
        if parts == ["backups"], method == .POST {
            struct Req: Codable { let note: String? }
            let note: String?
            if let data = rawBody.data(using: .utf8), let req = try? dec.decode(Req.self, from: data) { note = req.note } else { note = nil }
            do {
                let ref = try await admin.underlyingStore().createBackup(note: note)
                return json(ref, status: .created)
            } catch {
                return problem(.init(title: "backup failed", status: 500, detail: nil, instance: nil))
            }
        }
        if parts.count == 3, parts[0] == "backups", parts[2] == "restore", method == .POST {
            let id = parts[1]
            do {
                try await admin.underlyingStore().restoreBackup(id: id)
                return Response(status: .accepted, headers: [], body: Data())
            } catch {
                return problem(.init(title: "restore failed", status: 500, detail: nil, instance: nil))
            }
        }
        // Snapshots
        if parts == ["snapshots"], method == .POST {
            let s = await admin.createSnapshot()
            return json(s, status: .created)
        }
        if parts.count == 2, parts[0] == "snapshots", method == .DELETE {
            let id = parts[1]
            let ok = await admin.releaseSnapshot(id)
            return ok ? Response(status: .noContent, headers: [], body: Data()) : problem(.init(title: "not found", status: 404, detail: nil, instance: nil))
        }
        // Compaction
        if parts == ["compaction", "status"], method == .GET {
            do { return json(try await admin.compactionStatus()) } catch { return problem(.init(title: "status failed", status: 500, detail: nil, instance: nil)) }
        }
        if parts == ["compaction", "run"], method == .POST {
            // Accept body: { mode: tick|full } and require a body for stricter adherence
            guard let data = rawBody.data(using: .utf8),
                  let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let mode = obj["mode"] as? String else {
                return problem(.init(title: "invalid body", status: 400, detail: "missing mode", instance: nil))
            }
            if mode == "full" {
                // No explicit full compaction entrypoint; best-effort schedule multiple ticks.
                for _ in 0..<3 { await admin.compactionTick() }
            } else if mode == "tick" {
                await admin.compactionTick()
            } else {
                return problem(.init(title: "invalid body", status: 400, detail: "mode must be tick or full", instance: nil))
            }
            return Response(status: .accepted, headers: [], body: Data())
        }
        // Transactions
        if parts == ["transactions"], method == .POST {
            guard let data = rawBody.data(using: .utf8), let tx = try? dec.decode(AdminService.Transaction.self, from: data) else {
                return problem(.init(title: "invalid body", status: 400, detail: nil, instance: nil))
            }
            let res = await admin.commitTransaction(tx)
            return json(res)
        }

        return Response(status: .notFound, headers: [("content-type","text/plain")], body: Data("not found".utf8))
    }
}

// Non-Sendable by nature (NIO contexts); assert safety manually for Task captures.
extension HTTPHandler: @unchecked Sendable {}

@main
struct ServerMain {
    static func main() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        let path = URL(fileURLWithPath: ProcessInfo.processInfo.environment["FS_PATH"] ?? FileManager.default.temporaryDirectory.appendingPathComponent("fs").path)
        let store = try await FountainStore.open(.init(path: path))
        let admin = AdminService(store: store)
        // Rebuild dynamic (HTTP-defined) indexes after startup for HTTPDoc collections.
        await admin.rebuildDynamicIndexes()

        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .childChannelInitializer { channel in
                channel.pipeline.configureHTTPServerPipeline().flatMap {
                    let key = ProcessInfo.processInfo.environment["FS_API_KEY"]
                    return channel.pipeline.addHandler(HTTPHandler(admin: admin, apiKey: key), name: "HTTPHandler")
                }
            }
            .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.maxMessagesPerRead, value: 16)
            .childChannelOption(ChannelOptions.recvAllocator, value: AdaptiveRecvByteBufferAllocator())

        // Resolve API key via SecretStore if available, else env.
        var apiKeySource: String? = ProcessInfo.processInfo.environment["FS_API_KEY"]
        if apiKeySource == nil {
            #if canImport(Security)
            let store = KeychainStore(service: "com.fountain.store.http")
            if let data = try? store.retrieveSecret(for: "FS_API_KEY"), let s = String(data: data, encoding: .utf8), !s.isEmpty {
                apiKeySource = s
            }
            #else
            if let pathStr = ProcessInfo.processInfo.environment["FS_SECRETSTORE_PATH"],
               let password = ProcessInfo.processInfo.environment["FS_SECRETSTORE_PASSWORD"] {
                if let url = URL(string: pathStr), let keystore = try? FileKeystore(storeURL: url, password: password, iterations: 100_000),
                   let data = try? keystore.retrieveSecret(for: "FS_API_KEY"), let s = String(data: data, encoding: .utf8), !s.isEmpty {
                    apiKeySource = s
                }
            }
            #endif
        }

        let port = Int(ProcessInfo.processInfo.environment["PORT"] ?? "8080") ?? 8080
        let channel = try await bootstrap.bind(host: "0.0.0.0", port: port).get()
        print("FountainStoreHTTPServer listening on \(channel.localAddress!) path=\(path.path) auth=\(apiKeySource != nil ? "on" : "off")")
        try await channel.closeFuture.get()
        // Shutdown on a background thread to avoid blocking async context.
        DispatchQueue.global().async { try? group.syncShutdownGracefully() }
    }
}
