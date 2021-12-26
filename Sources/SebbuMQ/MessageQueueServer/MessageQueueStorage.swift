//
//  MessageQueueStorage.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import Foundation
import NIO
import SebbuTSDS
import Atomics

final class MessageQueueStorage {
    var queues: LockedDictionary<String, MessageQueue> = LockedDictionary()
    //var queues: [String : MessageQueue] = [:]
    
    var count = ManagedAtomic<Int>(0)
    
    var totalMaxBytes: Int = Int.max
    
    public init() {}
    
    
    //TODO: Persistent push to disk?
    
    @discardableResult
    public final func push(queue: String, value: [UInt8]) -> Bool {
        if let queue = queues[queue] {
            return queue.push(value)
        } else {
            let newQueue = MessageQueue(name: queue, self)
            if queues.setIfNotExist(queue, value: newQueue) {
                return newQueue.push(value)
            }
        }
        return push(queue: queue, value: value)
    }
    
    @inlinable
    public final func pop(queue: String, id: UInt64, client: MessageQueueServerClient, timeout: Double?) {
        if let queue = queues[queue] {
            queue.pop(for: client, id: id, timeout: timeout)
        } else {
            let newQueue = MessageQueue(name: queue, self)
            if queues.setIfNotExist(queue, value: newQueue) {
                newQueue.pop(for: client, id: id, timeout: timeout)
            } else {
                pop(queue: queue, id: id, client: client, timeout: timeout)
            }
        }
    }
    
    @inlinable
    public final func remove(client: MessageQueueServerClient) {
        for queue in queues.values {
            queue.remove(client: client)
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
