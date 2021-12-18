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
    
    var count: Int = 0
    
    var totalMaxBytes: Int = Int.max
    
    public init() {}
    
    
    //TODO: Persistent push to disk?
    
    @discardableResult
    public final func push(queue: String, value: [UInt8]) -> Bool {
        if let queue = queues[queue] {
            return queue.push(value)
        } else {
            let newQueue = MessageQueue(name: queue, self)
            queues[queue] = newQueue
            return newQueue.push(value)
        }
    }
    
    @inlinable
    public final func pop(queue: String, id: UInt64, client: MessageQueueServerClient, timeout: Double?) {
        if let queue = queues[queue] {
            queue.pop(for: client, id: id, timeout: timeout)
        } else {
            let newQueue = MessageQueue(name: queue, self)
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
