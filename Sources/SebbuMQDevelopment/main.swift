//
//  main.swift
//  
//
//  Created by Sebastian Toivonen on 8.8.2021.
//

import Foundation
import SebbuMQ
import NIO


_runAsyncMain {
    let server = try MessageQueueServer(username: "username", password: "password1")
    try server.startIPv4(port: 25565)
    let mtelg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let client1 = MessageQueueClient(eventLoopGroup: mtelg)
    let client2 = MessageQueueClient(eventLoopGroup: mtelg)
    try await client1.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
    try await client2.connect(username: "username", password: "password1", host: "127.0.0.1", port: 25565)
    let sendData: [UInt8] = [UInt8].random(count: 128)
    
    let count = 1_000_00
    for i in 0..<count {
        //await Task.sleep(.random(in: 1_000_000...10_000_000))
        client2.push(queue: "Hello \(i % 10)", sendData)
        //print(i, "pushed")
    }
    let start = Date()
    for i in 0..<count {
        let data = await client1.pop(queue: "Hello \(i % 10)", timeout: 5.0)
        if data?.count ?? 0 == 2 {
            print("Hello")
        }
    }
    let end = Date()
    let delta = start.distance(to: end)
    print(delta)
    print("Per second: \(Double(count) / delta)")
    try client1.disconnect()
    try client2.disconnect()
    await Task.sleep(1_000_000_000)
    try server.shutdown()
}
