//
//  MessageQueue.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import DequeModule
import Foundation

final class MessageQueue {
    let name: String
    
    var waitingClients: Deque<(client: MessageQueueServerClient, id: UInt64, expirationDate: Date?)> = Deque()
    var messages: Deque<[UInt8]> = Deque()
    
    public private(set) var count = 0
    public var maxSize = Int.max
    
    unowned let messageQueueStorage: MessageQueueStorage
    
    public init(name: String, _ messageQueueStorage: MessageQueueStorage) {
        self.name = name
        self.messageQueueStorage = messageQueueStorage
    }
    
    /// When a client pushes new data to the queue
    @discardableResult
    public final func push(_ value: [UInt8]) -> Bool {
        if count + value.count > maxSize || messageQueueStorage.count + value.count > messageQueueStorage.totalMaxBytes {
            return false
        }
        // If there is clients waiting for a response, then it means that the queue is empty, so we just send the payload straight to the first waiting client
        while let item = waitingClients.popFirst() {
            if let expirationDate = item.expirationDate, expirationDate < Date() {
                item.client.expire(queue: name, id: item.id)
                continue
            }
            item.client.send(.popResponse(PopResponsePacket(queue: name, id: item.id, payload: value)))
            return true
        }
        // No clients were waiting for a pop so we just add the data to the queue
        count += value.count
        messageQueueStorage.count += value.count
        messages.append(value)
        return true
    }
    
    /// When a client asks for a message
    public final func pop(for client: MessageQueueServerClient, id: UInt64, timeout: Double?) {
        if let message = messages.popFirst() {
            count -= message.count
            messageQueueStorage.count -= message.count
            client.send(.popResponse(PopResponsePacket(queue: name, id: id, payload: message)))
        } else {
            waitingClients.append((client: client, id: id, expirationDate: timeout != nil ? Date().addingTimeInterval(timeout!) : nil))
        }
    }
    
    internal final func removeTimedOutClients() {
        waitingClients.removeAll { (client, id, expirationDate) in
            if expirationDate == nil ? false : expirationDate! < Date() {
                client.expire(queue: name, id: id)
                return true
            }
            return false
        }
    }
    
    internal final func remove(client: MessageQueueServerClient) {
        client.channel.close(mode: .all, promise: nil)
        waitingClients.removeAll { $0.client == client }
    }
}
