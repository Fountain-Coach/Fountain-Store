import Foundation
@preconcurrency import NIO
@preconcurrency import NIOHTTP1
import FountainStoreHTTP
import FountainStore

final class HTTPHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart
    private let admin: AdminService
    private var bodyBuffer: ByteBuffer?
    private var lastHead: HTTPRequestHead?

    init(admin: AdminService) { self.admin = admin }

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
            Task { [admin] in
                let resp = await HTTPHandler.route(admin: admin, head: head, rawBody: reqBody)
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

    static func route(admin: AdminService, head: HTTPRequestHead?, rawBody: String) async -> Response {
        guard let head = head else { return Response(status: .badRequest, headers: [("content-type","text/plain")], body: Data("bad request".utf8)) }
        let method = head.method
        let uri = head.uri
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
            return json(h)
        }
        // Status
        if parts == ["status"], method == .GET {
            let s = await admin.status()
            return json(s)
        }
        // Metrics
        if parts == ["metrics"], method == .GET {
            let m = await admin.metrics()
            return json(m)
        }
        // Collections list/create
        if parts == ["collections"], method == .GET {
            struct RespItem: Codable { let name: String; let recordsApprox: Int }
            struct Resp: Codable { let items: [RespItem] }
            let st = await admin.status()
            let items = st.collections.map { RespItem(name: $0.name, recordsApprox: $0.recordsApprox) }
            return json(Resp(items: items))
        }
        if parts == ["collections"], method == .POST {
            struct CreateReq: Codable { let name: String; let version: String? }
            guard let data = rawBody.data(using: .utf8), let req = try? dec.decode(CreateReq.self, from: data), !req.name.isEmpty else {
                return problem(.init(title: "invalid body", status: 400, detail: nil, instance: nil))
            }
            let name = await admin.createCollection(req.name)
            struct Resp: Codable { let name: String; let recordsApprox: Int }
            return json(Resp(name: name, recordsApprox: 0), status: .created)
        }
        // Collection details
        if parts.count == 2, parts[0] == "collections", method == .GET {
            let c = parts[1]
            struct Resp: Codable { let name: String; let version: String?; let indexes: [AdminService.IndexDefinition] }
            let idx = await admin.listIndexDefinitions(c)
            return json(Resp(name: c, version: nil, indexes: idx))
        }
        // Indexes list/define
        if parts.count == 3, parts[0] == "collections", parts[2] == "indexes", method == .GET {
            let c = parts[1]
            struct Resp: Codable { let items: [AdminService.IndexDefinition] }
            let idx = await admin.listIndexDefinitions(c)
            return json(Resp(items: idx))
        }
        if parts.count == 3, parts[0] == "collections", parts[2] == "indexes", method == .POST {
            let c = parts[1]
            guard let data = rawBody.data(using: .utf8), let def = try? dec.decode(AdminService.IndexDefinition.self, from: data) else {
                return problem(.init(title: "invalid body", status: 400, detail: nil, instance: nil))
            }
            do {
                let created = try await admin.defineIndex(collection: c, def: def)
                return json(created, status: .created)
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
                    if let v = try await admin.getRecord(collection: c, id: id, snapshotId: snap) {
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
                do {
                    let v = try await admin.putRecord(collection: c, id: req.id ?? id, data: req.data)
                    // Use 200 for update, 201 for new is indistinguishable cheaply; return 200
                    return json(v)
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
                return json(resp)
            } catch {
                return problem(.init(title: "query failed", status: 500, detail: nil, instance: nil))
            }
        }
        // Backups
        if parts == ["backups"], method == .GET {
            let refs = await admin.underlyingStore().listBackups()
            struct Resp: Codable { let items: [FountainStore.BackupRef] }
            return json(Resp(items: refs))
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
            await admin.compactionTick()
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

        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .childChannelInitializer { channel in
                channel.pipeline.configureHTTPServerPipeline().flatMap {
                    channel.pipeline.addHandler(HTTPHandler(admin: admin), name: "HTTPHandler")
                }
            }
            .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.maxMessagesPerRead, value: 16)
            .childChannelOption(ChannelOptions.recvAllocator, value: AdaptiveRecvByteBufferAllocator())

        let port = Int(ProcessInfo.processInfo.environment["PORT"] ?? "8080") ?? 8080
        let channel = try await bootstrap.bind(host: "0.0.0.0", port: port).get()
        print("FountainStoreHTTPServer listening on \(channel.localAddress!) path=\(path.path)")
        try await channel.closeFuture.get()
        // Shutdown on a background thread to avoid blocking async context.
        DispatchQueue.global().async { try? group.syncShutdownGracefully() }
    }
}
