//
//  Application.swift
//  
//
//  Created by Sebastian Toivonen on 8.8.2021.
//

import Foundation
import SebbuMQ
import NIO

let server = try! MessageQueueServer(username: "username", password: "password1", numberOfThreads: 6)
let mtelg1 = MultiThreadedEventLoopGroup(numberOfThreads: 1)
let mtelg2 = MultiThreadedEventLoopGroup(numberOfThreads: 6)
let client1 = MessageQueueClient(eventLoopGroup: mtelg1)

func doFunc(_ client: MessageQueueClient, count: Int, workers: Int) async throws {
    try await client.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
    for _ in 0..<count / workers {
        guard (try? await client.pop(queue: "Hello", timeout: nil)) != nil else {
            fatalError("Failed to pop data...")
        }
    }
}

@main
public struct Application {
    public static func main() async throws {
        //server.totalMaximumBytes = 16_000_000
        try await server.startIPv4(port: 25565)
        try await client1.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
        
        let workers = 12
        let popClients = (0..<workers).map {_ in
            MessageQueueClient(eventLoopGroup: mtelg2)
        }
        
        let popValue = try? await client1.pop(queue: "random_queue", timeout: 1.0)
        guard popValue == nil else {
            fatalError("Timeout failed...")
        }
        
        let sendData: [UInt8] = [UInt8].random(count: 128)
        let count = 2_000_000
        
        let start = Date()
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for i in 0..<count {
                    do {
                        try await client1.push(queue: "Hello", sendData)
                    } catch let error as MessageQueueClient.PushError {
                        switch error {
                        case .disconnected:
                            print("Failed to push due to being disconnected...")
                        case .queueFull:
                            print("Queue is full!!")
                        case .timedOut:
                            print("Push timed out....")
                        case .unknown:
                            print("Unknown push error...")
                        }
                    }
                    if count > 1_000_000 && i % 100_000 == 0 {
                        print(i, "messages pushed")
                    }
                }
            }
            for i in 0..<workers {
                group.addTask {
                    try await doFunc(popClients[i], count: count, workers: workers)
                }
            }
            try await group.waitForAll()
        }
        
        let end = Date()
        let delta = start.distance(to: end)
        print(delta)
        print("Per second: \(Double(count) / delta)")
        
        for _ in 0..<10 {
            try await client1.reliablePush(queue: "reliable_push", [1,2,3,4,5,7,8], 1)
        }
        
        print()
        for _ in 0..<10 {
            let _ = try await client1.pop(queue: "reliable_push", timeout: 5)
        }
        
        try await client1.disconnect()
        for client in popClients {
            try await client.disconnect()
        }
        try await Task.sleep(nanoseconds: 5_000_000_000)
        try await server.shutdown()
    }
}
