//
//  main.swift
//  
//
//  Created by Sebastian Toivonen on 8.8.2021.
//

import Foundation
import SebbuMQ
import NIO

let server = try MessageQueueServer(username: "username", password: "password1")
try server.startIPv4(port: 25565)
let mtelg1 = MultiThreadedEventLoopGroup(numberOfThreads: 1)
let mtelg2 = MultiThreadedEventLoopGroup(numberOfThreads: 5)
let client1 = MessageQueueClient(eventLoopGroup: mtelg1)

func doFunc(_ client: MessageQueueClient, count: Int, workers: Int) async {
    for _ in 0..<count / workers {
        let data = await client.pop(queue: "Hello", timeout: nil)
        if data == nil {
            fatalError("We got nil data...")
        }
    }
}

_runAsyncMain {
    try await client1.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
    
    let workers = 5
    let popClients = (0..<workers).map {_ in
        MessageQueueClient(eventLoopGroup: mtelg2)
    }
    for client in popClients {
        try await client.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
    }
    
    let sendData: [UInt8] = [UInt8].random(count: 1024 * 2)
    let count = 2_000_000
    
    let start = Date()
    await withTaskGroup(of: Void.self) { group in
        group.addTask {
            for i in 0..<count {
                client1.push(queue: "Hello", sendData)
                if count > 1_000_000 && i % 100_000 == 0 {
                    await Task.sleep(1_000_000_000)
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
    client1.disconnect()
    popClients.forEach { $0.disconnect() }
    await Task.sleep(1_000_000_000)
    try server.shutdown()
}
