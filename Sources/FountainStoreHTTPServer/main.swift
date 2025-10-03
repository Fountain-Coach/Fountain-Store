import Foundation
import NIO
import NIOHTTP1
import FountainStoreHTTP
import FountainStore

final class HTTPHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart
    private let admin: AdminService
    private var bodyBuffer: ByteBuffer?
    var lastHead: HTTPRequestHead?

    init(admin: AdminService) { self.admin = admin }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch self.unwrapInboundIn(data) {
        case .head(let head):
            lastHead = head
            bodyBuffer = context.channel.allocator.buffer(capacity: 0)
        case .body(var buf):
            if bodyBuffer == nil { bodyBuffer = context.channel.allocator.buffer(capacity: 0) }
            bodyBuffer?.writeBuffer(&buf)
        case .end:
            let reqBody = bodyBuffer?.getString(at: 0, length: bodyBuffer?.readableBytes ?? 0) ?? ""
            let response = handleRequest(body: reqBody, context: context)
            writeResponse(response, context: context)
        }
    }

    struct Response { let status: HTTPResponseStatus; let headers: [(String,String)]; let body: Data }

    func jsonResponse<T: Encodable>(_ value: T, status: HTTPResponseStatus = .ok) -> Response {
        let data = (try? JSONEncoder().encode(value)) ?? Data("{}".utf8)
        return Response(status: status, headers: [("content-type", "application/json")], body: data)
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

    func handleRequest(body: String, context: ChannelHandlerContext) -> Response {
        let uri = lastHead?.uri ?? "/"
        let method = lastHead?.method ?? .GET
        // Minimal endpoints: /health, /status, /metrics
        if uri.hasPrefix("/health") && method == .GET {
            struct Health: Codable { let status: String; let engine: [String:Int] }
            return jsonResponse(Health(status: "ok", engine: ["sequence": 0]))
        } else if uri.hasPrefix("/status") && method == .GET {
            struct CollectionRef: Codable { let name: String; let recordsApprox: Int }
            struct Status: Codable { let sequence: Int; let collectionsCount: Int; let collections: [CollectionRef] }
            return jsonResponse(Status(sequence: 0, collectionsCount: 0, collections: []))
        } else if uri.hasPrefix("/metrics") && method == .GET {
            return jsonResponse(["puts":0, "gets":0, "deletes":0])
        }
        return Response(status: .notFound, headers: [("content-type","text/plain")], body: Data("not found".utf8))
    }
}

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
