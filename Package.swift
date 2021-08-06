// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "sebbu-mq",
    platforms: [.macOS(.v11)],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "SebbuMQ",
            targets: ["SebbuMQ"]),
    ],
    dependencies: [
        .package(url: "https://github.com/MarSe32m/SebbuKit.git", .branch("main")),
        .package(url: "https://github.com/apple/swift-collections.git", .branch("main"))
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "SebbuMQ",
            dependencies: [.product(name: "SebbuKit", package: "SebbuKit"),
                           .product(name: "DequeModule", package: "swift-collections")],
            //TODO: Remove!!!
            swiftSettings: [.unsafeFlags(["-Xfrontend", "-enable-experimental-concurrency"]),
                            .unsafeFlags(["-Xfrontend", "-disable-availability-checking"])]),
        .testTarget(
            name: "SebbuMQTests",
            dependencies: ["SebbuMQ"],
            //TODO: Remove!!!
            swiftSettings: [.unsafeFlags(["-Xfrontend", "-disable-availability-checking"])]),
    ]
)
