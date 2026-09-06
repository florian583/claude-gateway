// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "GatewayMenu",
    platforms: [.macOS(.v13)],
    products: [.executable(name: "GatewayMenu", targets: ["GatewayMenu"])],
    targets: [
        .target(name: "GatewayMenuCore"),
        .executableTarget(name: "GatewayMenu", dependencies: ["GatewayMenuCore"]),
        .testTarget(name: "GatewayMenuCoreTests", dependencies: ["GatewayMenuCore"])
    ]
)
