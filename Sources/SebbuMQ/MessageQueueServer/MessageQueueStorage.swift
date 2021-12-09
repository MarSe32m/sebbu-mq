//
//  MessageQueueStorage.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import Foundation
import NIO

final class MessageQueueStorage {
    var queues: [String : MessageQueue] = [:]
    
    public init() {}
    
    public final func push(queue: String, value: [UInt8]) {
        if let queue = queues[queue] {
            queue.push(value)
        } else {
            let newQueue = MessageQueue(name: queue)
            queues[queue] = newQueue
            newQueue.push(value)
        }
    }
    
    //TODO: Persistent push to disk?
    
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
    
    final func startRemoveLoop(eventLoop: EventLoop) {
        eventLoop.scheduleRepeatedTask(initialDelay: .seconds(1), delay: .seconds(1), notifying: nil) {[weak self] task in
            guard let self = self else {
                task.cancel(promise: nil)
                return
            }
            self.removeTimedOutClients()
        }
    }
    
    private final func removeTimedOutClients() {
        for queue in queues.values {
            queue.removeTimedOutClients()
        }
    }
}
