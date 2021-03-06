//
//  MessageQueueServerClient.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import NIO
import Foundation
import SebbuBitStream

final class MessageQueueServerClient {
    public weak var messageQueueServer: MessageQueueServer?
    public let channel: Channel
    
    public var isAuthenticated: Bool = false
    
    public let id = UUID()
    
    public init(server: MessageQueueServer, channel: Channel) {
        self.messageQueueServer = server
        self.channel = channel
    }
    
    public func send(_ packet: MessageQueuePacket) {
        var writeStream: WritableBitStream
        if case let .popResponse(popResponse) = packet {
            writeStream = WritableBitStream(size: popResponse.payload.count + 16 + popResponse.queue.count + 1 + 4)
        } else {
            writeStream = WritableBitStream(size: 32)
        }
        writeStream.appendObject(packet)
        let bytes = writeStream.packBytes()
        let buffer = channel.allocator.buffer(bytes: bytes)
        channel.writeAndFlush(buffer, promise: nil)
    }
    
    public func expire(queue: String, id: UInt64) {
        send(.popExpired(PopExpirationPacket(queue: queue, id: id)))
    }
    
    public func received(_ data: [UInt8]) {
        var readStream = ReadableBitStream(bytes: data)
        guard let packet = try? MessageQueuePacket(from: &readStream) else {
            return
        }
        messageQueueServer?.received(packet: packet, from: self)
    }
    
    public func disconnected() {
        messageQueueServer?.disconnect(messageQueueClient: self)
    }
}

extension MessageQueueServerClient: Equatable {
    public static func ==(lhs: MessageQueueServerClient, rhs: MessageQueueServerClient) -> Bool {
        lhs.id == rhs.id
    }
}
