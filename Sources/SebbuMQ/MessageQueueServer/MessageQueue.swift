//
//  MessageQueue.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import DequeModule
import Foundation

final class MessageQueue {
    var waitingClients: Deque<(client: MessageQueueServerClient, id: UUID, expirationDate: Date?)> = Deque()
    var messages: Deque<[UInt8]> = Deque()
    
    let name: String
    
    public init(name: String) {
        self.name = name
    }
    
    /// When a client pushes new data to the queue
    public final func push(_ value: [UInt8]) {
        while let item = waitingClients.popFirst() {
            if let expirationDate = item.expirationDate, expirationDate < Date() {
                item.client.expire(queue: name, id: item.id)
                continue
            }
            item.client.send(.popResponse(PopResponsePacket(queue: name, id: item.id, payload: value)))
            return
        }
        messages.append(value)
    }
    
    /// When a client asks for a message
    public final func pop(for client: MessageQueueServerClient, id: UUID, timeout: Double?) {
        if let message = messages.popFirst() {
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
        waitingClients.removeAll { $0.client === client }
    }
}
