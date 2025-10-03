
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
    ],
    targets: [
        .target(name: "FountainStore", dependencies: ["FountainStoreCore", "FountainFTS", "FountainVector"]),
        .target(name: "FountainStoreCore"),
        .target(name: "FountainFTS", dependencies: ["FountainStoreCore"]),
        .target(name: "FountainVector", dependencies: ["FountainStoreCore"]),
        .target(name: "FountainStoreHTTP", dependencies: ["FountainStore"]),
        .testTarget(name: "FountainStoreTests", dependencies: ["FountainStore", "FountainFTS", "FountainVector"]),
        .testTarget(name: "FountainStoreHTTPTests", dependencies: ["FountainStoreHTTP"]),
        .executableTarget(name: "FountainStoreBenchmarks", dependencies: ["FountainStore"]),
    ]
)
