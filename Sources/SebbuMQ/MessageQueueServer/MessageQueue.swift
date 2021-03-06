//
//  MessageQueue.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import DequeModule
import Foundation
import Atomics
import SebbuTSDS

final class MessageQueue {
    let name: String
    
    var _waitingClients: LockedQueue<(client: MessageQueueServerClient, id: UInt64, expirationDate: Date?)> = LockedQueue(size: 4, resizeAutomatically: true)
    var _messages: LockedQueue<[UInt8]> = LockedQueue(size: 2_000_100, resizeAutomatically: false)
    
    
    public private(set) var count = ManagedAtomic<Int>(0)
    public var maxSize = Int.max
    
    unowned let messageQueueStorage: MessageQueueStorage
    
    public init(name: String, _ messageQueueStorage: MessageQueueStorage) {
        self.name = name
        self.messageQueueStorage = messageQueueStorage
    }
    
    /// When a client pushes new data to the queue
    @discardableResult
    public final func push(_ value: [UInt8]) -> Bool {
        let count = self.count.wrappingIncrementThenLoad(by: value.count, ordering: .relaxed)
        let storageCount = messageQueueStorage.count.wrappingIncrementThenLoad(by: value.count, ordering: .relaxed)
        if count  > maxSize || storageCount > messageQueueStorage.totalMaxBytes {
            self.count.wrappingDecrement(by: value.count, ordering: .relaxed)
            messageQueueStorage.count.wrappingDecrement(by: value.count, ordering: .relaxed)
            return false
        }
        // If there is clients waiting for a response, then it means that the queue is empty, so we just send the payload straight to the first waiting client
        while let item = _waitingClients.dequeue() {
            if let expirationDate = item.expirationDate, expirationDate < Date() {
                item.client.expire(queue: name, id: item.id)
                continue
            }
            self.count.wrappingDecrement(by: value.count, ordering: .relaxed)
            messageQueueStorage.count.wrappingDecrement(by: value.count, ordering: .relaxed)
            item.client.send(.popResponse(PopResponsePacket(queue: name, id: item.id, payload: value)))
            return true
        }
        _messages.enqueue(value)
        return true
    }
    
    /// When a client asks for a message
    public final func pop(for client: MessageQueueServerClient, id: UInt64, timeout: Double?) {
        //if let message = messages.popFirst() {
        if let message = _messages.dequeue() {
            count.wrappingDecrement(by: message.count, ordering: .relaxed)
            messageQueueStorage.count.wrappingDecrement(by: message.count, ordering: .relaxed)
            client.send(.popResponse(PopResponsePacket(queue: name, id: id, payload: message)))
        } else {
            _waitingClients.enqueue((client: client, id: id, expirationDate: timeout != nil ? Date().addingTimeInterval(timeout!) : nil))
        }
    }
    
    internal final func removeTimedOutClients() {
        _waitingClients.removeAll { (client, id, expirationDate) in
            if expirationDate == nil ? false : expirationDate! < Date() {
                client.expire(queue: name, id: id)
                return true
            }
            return false
        }
    }
    
    internal final func remove(client: MessageQueueServerClient) {
        client.channel.close(mode: .all, promise: nil)
        _waitingClients.removeAll { $0.client == client }
    }
}
