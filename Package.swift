// swift-tools-version:5.5
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "sebbu-mq",
    platforms: [.macOS(.v12)],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "SebbuMQ",
            targets: ["SebbuMQ"]),
    ],
    dependencies: [
        .package(url: "https://github.com/MarSe32m/sebbu-bitstream.git", .branch("main")),
        .package(url: "https://github.com/MarSe32m/sebbu-cryptography.git", .branch("main")),
        .package(url: "https://github.com/apple/swift-collections.git", .branch("main")),
        .package(url: "https://github.com/apple/swift-nio.git", .branch("main")),
        .package(url: "https://github.com/apple/swift-nio-extras.git", .branch("main")),
        .package(url: "https://github.com/apple/swift-atomics.git", .branch("main"))
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .executableTarget(name: "SebbuMQDevelopment",
                          dependencies:
                            [.product(name: "NIO", package: "swift-nio"),
                             .product(name: "NIOExtras", package: "swift-nio-extras"),
                             .product(name: "DequeModule", package: "swift-collections"),
                             "SebbuMQ",
                             .product(name: "Atomics", package: "swift-atomics")
                            ]),
        .target(
            name: "SebbuMQ",
            dependencies: [.product(name: "SebbuBitStream", package: "sebbu-bitstream"),
                           .product(name: "SebbuCrypto", package: "sebbu-cryptography"),
                           .product(name: "NIO", package: "swift-nio"),
                           .product(name: "_NIOConcurrency", package: "swift-nio"),
                           .product(name: "NIOExtras", package: "swift-nio-extras"),
                           .product(name: "DequeModule", package: "swift-collections"),
                           .product(name: "Atomics", package: "swift-atomics")]),
        .testTarget(name: "SebbuMQTests", dependencies: ["SebbuMQ", .product(name: "NIO", package: "swift-nio")]),
    ]
)
