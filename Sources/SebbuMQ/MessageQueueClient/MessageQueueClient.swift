//
//  MessageQueueClient.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import Foundation
import SebbuKit
import NIO

@globalActor
actor MessageQueueClientActor: GlobalActor {
    static var shared: MessageQueueClientActor = MessageQueueClientActor()
}

public final class MessageQueueClient {
    private var webSocket: WebSocket!
    private var webSocketClient: WebSocketClient
    
    @MessageQueueClientActor
    private var popRequests = [(id: UUID, queue: String, continuation: UnsafeContinuation<[UInt8]?, Never>)]()
    
    private var connectionContinuation: UnsafeContinuation<Void, Error>?
    private var isDisconnected = false
    
    private var evlg: EventLoopGroup?
    
    public init(eventLoopGroup: EventLoopGroup? = nil) {
        self.evlg = eventLoopGroup
        self.webSocketClient = WebSocketClient.init(eventLoopGroupProvider: evlg == nil ? .createNew : .shared(evlg!))
    }
    
    public final func connect(username: String, password: String, host: String, port: Int) async throws {
        webSocket = try webSocketClient.connect(scheme: "ws", host: host, port: port)
        webSocket.onBinary { _, buffer in
            guard let bytes = buffer.getBytes(at: 0, length: buffer.readableBytes) else {
                return
            }
            self.received(bytes)
        }
        _ = webSocket.onClose.always { _ in
            self.disconnected()
        }
        send(.connect(ConnectionPacket(username: username, password: password)))
        try await withUnsafeThrowingContinuation { continuation in
            connectionContinuation = continuation
        }
    }
    
    public final func disconnect() throws {
        send(.disconnect)
        Task {
            try await Task.sleep(nanoseconds: 10_000_000)
            webSocket.close(code: .normalClosure, promise: nil)
            isDisconnected = true
        }
    }
    
    @discardableResult
    public final func push(queue: String, _ data: [UInt8]) -> Bool {
        if isDisconnected { return false }
        send(.push(PushPacket(queue: queue, payload: data)))
        return true
    }
    
    public final func pop(queue: String, timeout: Double?) async -> [UInt8]? {
        if isDisconnected { return nil }
        let popRequestPacket = PopRequestPacket(queue: queue, timeout: timeout)
        send(.popRequest(popRequestPacket))
        return await withUnsafeContinuation({ continuation in
            Task {await add(popRequestPacket, continuation: continuation)}
        })
    }
    
    @MessageQueueClientActor
    private final func add(_ popRequestPacket: PopRequestPacket, continuation: UnsafeContinuation<[UInt8]?, Never>) {
        popRequests.append((popRequestPacket.id, popRequestPacket.queue, continuation))
    }
    
    @MessageQueueClientActor
    private final func resumeAll() {
        popRequests.forEach { (_, _, continuation) in
            continuation.resume(returning: nil)
        }
        popRequests.removeAll()
    }
    
    @MessageQueueClientActor
    private final func handlePopResponse(_ responsePacket: PopResponsePacket) {
        let continuations = popRequests.filter { $0.id == responsePacket.id && $0.queue == responsePacket.queue }
                                       .map { $0.continuation }
        popRequests.removeAll(where: {$0.id == responsePacket.id && $0.queue == responsePacket.queue})
        continuations.forEach {$0.resume(returning: responsePacket.payload)}
    }
    
    @MessageQueueClientActor
    private final func handleExpiration(_ expirationPacket: PopExpirationPacket) {
        let continuations = popRequests.filter { $0.id == expirationPacket.id && $0.queue == expirationPacket.queue }
                                        .map { $0.continuation }
        popRequests.removeAll(where: {$0.id == expirationPacket.id && $0.queue == expirationPacket.queue})
        continuations.forEach {$0.resume(returning: nil)}
    }
    
    private final func send(_ packet: MessageQueuePacket) {
        var writeStream = WritableBitStream(size: 128)
        writeStream.appendObject(packet)
        let data = writeStream.packBytes()
        webSocket.send(data, promise: nil)
    }
}

extension MessageQueueClient {
    public func received(_ data: [UInt8]) {
        var readStream = ReadableBitStream(bytes: data)
        guard let packet = try? MessageQueuePacket(from: &readStream) else {
            print("Got a faulty packet...")
            return
        }
        
        switch packet {
        case .popRequest(_), .push(_), .connect(_):
            break
        case .connectionAccepted:
            connectionContinuation?.resume()
            connectionContinuation = nil
        case .connectionDeclined(let error):
            connectionContinuation?.resume(throwing: error)
            connectionContinuation = nil
        case .disconnect:
            Task { await resumeAll() }
        case .popResponse(let responsePacket):
            Task { await handlePopResponse(responsePacket) }
        case .popExpired(let expirationPacket):
            Task { await handleExpiration(expirationPacket) }
        }
    }
    
    public func disconnected() {
        connectionContinuation?.resume(throwing: MessageQueueClientConnectionError.unknownError)
        connectionContinuation = nil
        isDisconnected = true
    }
}
