//
//  MessageQueueServerClient.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import SebbuKit
import WebSocketKit

final class MessageQueueServerClient {
    public unowned let messageQueueServer: MessageQueueServer
    public let webSocket: WebSocket
    
    public init(server: MessageQueueServer, webSocket: WebSocket) {
        self.messageQueueServer = server
        self.webSocket = webSocket
        webSocket.onBinary { [weak self] _, buffer in
            guard let bytes = buffer.getBytes(at: 0, length: buffer.readableBytes) else {
                return
            }
            self?.received(bytes)
        }
        _ = webSocket.onClose.always { [weak self] _ in
            self?.disconnected()
        }
    }
    
    func send(_ packet: MessageQueuePacket) {
        var writeStream = WritableBitStream(size: 128)
        writeStream.appendObject(packet)
        let data = writeStream.packBytes()
        webSocket.send(data, promise: nil)
    }
    
    func expire(queue: String, id: UUID) {
        send(.popExpired(PopExpirationPacket(queue: queue, id: id)))
    }
    
    public func received(_ data: [UInt8]) {
        var readStream = ReadableBitStream(bytes: data)
        guard let packet = try? MessageQueuePacket(from: &readStream) else {
            print("Got a faulty packet...")
            return
        }
        Task { await messageQueueServer.received(packet: packet, from: self) }
    }
    
    public func disconnected() {
        Task { await messageQueueServer.disconnect(messageQueueClient: self) }
    }
}
