
// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "FountainStore",
    platforms: [
        .macOS(.v13), .iOS(.v16), .tvOS(.v16), .watchOS(.v9)
    ],
    products: [
        .library(name: "FountainStore", targets: ["FountainStore"]),
        .library(name: "FountainFTS", targets: ["FountainFTS"]),
        .library(name: "FountainVector", targets: ["FountainVector"]),
        .library(name: "FountainStoreHTTP", targets: ["FountainStoreHTTP"]),
        .executable(name: "FountainStoreHTTPServer", targets: ["FountainStoreHTTPServer"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.59.0"),
        .package(url: "https://github.com/Fountain-Coach/swift-secretstore.git", from: "0.1.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "3.0.0")
    ],
    targets: [
        .target(name: "FountainStore", dependencies: ["FountainStoreCore", "FountainFTS", "FountainVector"]),
        .target(name: "FountainStoreCore"),
        .target(name: "FountainFTS", dependencies: ["FountainStoreCore"]),
        .target(name: "FountainVector", dependencies: ["FountainStoreCore"]),
        .target(name: "FountainStoreHTTP", dependencies: ["FountainStore"]),
        .executableTarget(name: "FountainStoreHTTPServer", dependencies: [
            "FountainStoreHTTP",
            .product(name: "NIO", package: "swift-nio"),
            .product(name: "NIOHTTP1", package: "swift-nio"),
            .product(name: "SecretStore", package: "swift-secretstore"),
            .product(name: "Crypto", package: "swift-crypto")
        ]),
        .testTarget(name: "FountainStoreTests", dependencies: ["FountainStore", "FountainFTS", "FountainVector"]),
        .testTarget(name: "FountainStoreHTTPTests", dependencies: ["FountainStoreHTTP"]),
        .executableTarget(name: "FountainStoreBenchmarks", dependencies: ["FountainStore"]),
    ]
)
