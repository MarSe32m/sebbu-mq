import XCTest
import SebbuMQ
import NIO

final class SebbuMQTests: XCTestCase {
    func testCorrectMessages() async throws {
        let server = try! MessageQueueServer(username: "username", password: "password1", numberOfThreads: 1)
        try await server.startIPv4(port: 25564)
        let mtelg = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let client = MessageQueueClient(eventLoopGroup: mtelg)
        try await client.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25564)
        var pushData: [UInt8] = [1]
        for i in 0..<1000 {
            client.push(queue: "test_queue", pushData)
            let popData = try await client.pop(queue: "test_queue", timeout: 1)
            XCTAssertEqual(pushData, popData)
            pushData.append(UInt8(i % 255))
        }
        
        pushData = [0]
        
        for _ in 0..<1000 {
            client.push(queue: "test_queue", pushData)
            let popData = try await client.pop(queue: "test_queue", timeout: 1)
            XCTAssertEqual(pushData, popData)
            pushData = (0..<1000).map {_ in UInt8.random(in: .min ... .max)}
        }
        
        try await client.disconnect()
        try await Task.sleep(nanoseconds: 1_000_000_000)
        try await server.shutdown()
    }
    
    func test100Pushes100Pops() async throws {
        let server = try! MessageQueueServer(username: "username", password: "password1", numberOfThreads: 1)
        try await server.startIPv4(port: 25565)
        let mtelg = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let client1 = MessageQueueClient(eventLoopGroup: mtelg)
        let client2 = MessageQueueClient(eventLoopGroup: mtelg)
        try await client1.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
        try await client2.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
        for _ in 0..<100 {
            XCTAssertNoThrow {
                try await client1.reliablePush(queue: "test_queue", [1,2,3,4,5,6,7,8,9,10])
            }
        }
        for _ in 0..<100 {
            XCTAssertNoThrow {
                try await client2.pop(queue: "test_queue", timeout: 1)
            }
        }
        try await client1.disconnect()
        try await client2.disconnect()
        try await Task.sleep(nanoseconds: 1_000_000_000)
        try await server.shutdown()
    }
    
    func testMaxTotalBytes() async throws {
        let server = try! MessageQueueServer(username: "username", password: "password1", numberOfThreads: 1)
        try await server.startIPv4(port: 25566)
        let mtelg = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let client = MessageQueueClient(eventLoopGroup: mtelg)
        try await client.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25566)
        server.totalMaximumBytes = 100
        for _ in 0..<19 {
            try await client.reliablePush(queue: "test_queue_", [1,2,3,4,5])
        }
        XCTAssertNoThrow {
            try await client.reliablePush(queue: "test_queue_", [1,2,3,4,5])
        }
        
        do {
            try await client.reliablePush(queue: "test_queue_", [1,2,2,3,3])
        } catch let error {
            guard let error = error as? MessageQueueClient.PushError else {
                XCTFail("Unknown error type?")
                fatalError()
                
            }
            guard case .queueFull = error else {
                XCTFail("Error type was wrong. Should be a queueFull error")
                fatalError()
            }
        }
        
        try await client.disconnect()
        try await Task.sleep(nanoseconds: 1_000_000_000)
        try await server.shutdown()
    }
    
    func testPopTimeout() async throws {
        let server = try! MessageQueueServer(username: "username", password: "password1", numberOfThreads: 1)
        try await server.startIPv4(port: 25567)
        let mtelg = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let client = MessageQueueClient(eventLoopGroup: mtelg)
        try await client.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25567)
        let value = try? await client.pop(queue: "abcdefg", timeout: 5)
        XCTAssertNil(value)
        
        try await client.disconnect()
        try await Task.sleep(nanoseconds: 1_000_000_000)
        try await server.shutdown()
    }
}
