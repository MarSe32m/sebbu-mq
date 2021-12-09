//
//  main.swift
//  
//
//  Created by Sebastian Toivonen on 8.8.2021.
//

import Foundation
import SebbuMQ
import NIO

let server = try! MessageQueueServer(username: "username", password: "password1")
let mtelg1 = MultiThreadedEventLoopGroup(numberOfThreads: 1)
let mtelg2 = MultiThreadedEventLoopGroup(numberOfThreads: 6)
let client1 = MessageQueueClient(eventLoopGroup: mtelg1)

func doFunc(_ client: MessageQueueClient, count: Int, workers: Int) async {
    for _ in 0..<count / workers {
        let data = await client.pop(queue: "Hello", timeout: nil)
        if data == nil {
            fatalError("We got nil data...")
        }
    }
}

@main
public struct Application {
    public static func main() async throws {
        try server.startIPv4(port: 25565)
        try await client1.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
        
        let workers = 12
        let popClients = (0..<workers).map {_ in
            MessageQueueClient(eventLoopGroup: mtelg2)
        }
        for client in popClients {
            try await client.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
        }
        
        guard await client1.pop(queue: "random_queue", timeout: 5.0) == nil else {
            fatalError("Timeout failed...")
        }
        
        let sendData: [UInt8] = [UInt8].random(count: 128)
        let count = 2_000_000
        
        let start = Date()
        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                for i in 0..<count {
                    client1.push(queue: "Hello", sendData)
                    if count > 1_000_000 && i % 100_000 == 0 {
                        print(i, "messages pushed")
                    }
                }
            }
            for i in 0..<workers {
                group.addTask {
                    await doFunc(popClients[i], count: count, workers: workers)
                }
            }
            await group.waitForAll()
        }
        
        let end = Date()
        let delta = start.distance(to: end)
        print(delta)
        print("Per second: \(Double(count) / delta)")
        
        for _ in 0..<10 {
            guard await client1.reliablePush(queue: "reliable_push", [1,2,3,4,5,7,8], 1) else {
                fatalError("Failed to push reliably...")
            }
        }
        
        print()
        for _ in 0..<10 {
            guard let _ = await client1.pop(queue: "reliable_push", timeout: 5) else {
                fatalError("Failed to pop reliably pushed payloads...")
            }
        }
        
        client1.disconnect()
        popClients.forEach { $0.disconnect() }
        await Task.sleep(10_000_000_000)
        try server.shutdown()
    }
}
