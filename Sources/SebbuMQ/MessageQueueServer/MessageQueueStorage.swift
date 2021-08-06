//
//  MessageQueueStorage.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import Foundation

@MainActor
final class MessageQueueStorage {
    private var queues: [String : MessageQueue] = [:]
    
    public init() {}
    
    public final func startLoop() {
        Task.detached { [weak self] in
            while let self = self {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                await self.removeTimedOutClients()
            }
        }
    }
    
    public final func push(queue: String, value: [UInt8]) {
        if let queue = queues[queue] {
            queue.push(value)
        } else {
            let newQueue = MessageQueue(name: queue)
            queues[queue] = newQueue
            newQueue.push(value)
        }
    }
    
    @inlinable
    public final func pop(queue: String, id: UUID, client: MessageQueueServerClient, timeout: Double?) {
        if let queue = queues[queue] {
            queue.pop(for: client, id: id, timeout: timeout)
        } else {
            let newQueue = MessageQueue(name: queue)
            queues[queue] = newQueue
            newQueue.pop(for: client, id: id, timeout: timeout)
        }
    }
    
    @inlinable
    public final func remove(client: MessageQueueServerClient) {
        for queue in queues.values {
            queue.remove(client: client)
        }
        var keysToRemove = [String]()
        for (key, value) in queues {
            if value.waitingClients.isEmpty {
                keysToRemove.append(key)
            }
        }
        for key in keysToRemove {
            queues[key] = nil
        }
    }
    
    private final func removeTimedOutClients() async {
        for queue in queues.values {
            queue.removeTimedOutClients()
        }
    }
}
