    import XCTest
    import SebbuMQ

    final class sebbu_mqTests: XCTestCase {
        func testExample() async throws {
            let server = try await MessageQueueServer(username: "username", password: "password1", numberOfThreads: 1)
            try server.startIPv4(port: 25565)
            let client = MessageQueueClient()
            try await client.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
            let sendData: [UInt8] = [1,2,3,4,5,6,8,9,4]
            for _ in 0..<10 {
                let task1 = Task.detached {
                    await withTaskGroup(of: Void.self, body: { group in
                        for i in 0..<1_000 {
                            group.addTask {
                                print(i, "sent")
                                let data = await client.pop(queue: "Hello \(i % 10)", timeout: nil)
                                print(i, "received")
                                print(data == sendData)
                            }
                        }
                        await group.waitForAll()
                    })
                    
                }
                let task2 = Task.detached {
                    await Task.sleep(1_000_000)
                    for i in 0..<1_000 {
                        //await Task.sleep(.random(in: 1_000_000...10_000_000))
                        client.push(queue: "Hello \(i % 10)", sendData)
                        print(i, "pushed")
                    }
                }
                
                await task1.value
                await task2.value
                print()
                print()
            }
            try client.disconnect()
            await Task.sleep(1_000_000_000)
            print("Hello")
            try await server.shutdown()
        }
    }
