// swift-tools-version: 6.2
import PackageDescription

// The TheirCore tests reach internal state machines through DEBUG-only seams.
let debugSeams: [SwiftSetting] = [
    .define("DEBUG", .when(configuration: .debug))
]

let package = Package(
    name: "TheirCore",
    platforms: [
        .iOS(.v15),
        .macOS(.v12),
        .tvOS(.v15),
        .visionOS(.v1),
        .watchOS(.v8)
    ],
    products: [
        .library(name: "TheirCore", targets: ["TheirCore"]),
        .library(name: "TheirCoreTesting", targets: ["TheirCoreTesting"])
    ],
    targets: [
        .target(
            name: "TheirCore",
            swiftSettings: debugSeams
        ),
        .target(
            name: "TheirCoreTesting",
            dependencies: ["TheirCore"],
            swiftSettings: debugSeams
        ),
        .testTarget(
            name: "TheirCorePublicAPITests",
            dependencies: ["TheirCore", "TheirCoreTesting"]
        ),
        .testTarget(
            name: "TheirCoreTestingTests",
            dependencies: ["TheirCore", "TheirCoreTesting"],
            swiftSettings: debugSeams
        ),
        .testTarget(
            name: "TheirCoreTests",
            dependencies: ["TheirCore", "TheirCoreTesting"],
            swiftSettings: debugSeams
        )
    ],
    swiftLanguageModes: [.v6]
)
