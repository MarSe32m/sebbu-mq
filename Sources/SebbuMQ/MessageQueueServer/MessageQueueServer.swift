//
//  MessageQueueServer.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import SebbuKit
import NIOHTTP1

@MainActor
public final class MessageQueueServer {
    private let storage = MessageQueueStorage()
    
    @usableFromInline
    internal let webSocketServer: WebSocketServer
    
    private var clients = [MessageQueueServerClient]()
    
    public let username: String
    public let password: String
    
    public init(username: String, password: String, eventLoopGroup: EventLoopGroup) throws {
        self.username = try BCrypt.hash(username)
        self.password = try BCrypt.hash(password)
        //TODO: Add tls option
        webSocketServer = try WebSocketServer(tls: nil, eventLoopGroup: eventLoopGroup)
        webSocketServer.delegate = self
    }
    
    public init(username: String, password: String, numberOfThreads: Int) throws {
        self.username = try BCrypt.hash(username)
        self.password = try BCrypt.hash(password)
        //TODO: Add tls option
        webSocketServer = try WebSocketServer(tls: nil, numberOfThreads: numberOfThreads)
        webSocketServer.delegate = self
    }
    
    @inlinable
    public nonisolated final func startIPv4(port: Int) throws {
        try webSocketServer.startIPv4(port: port)
    }
    
    @inlinable
    public nonisolated final func startIPv6(port: Int) throws {
        try webSocketServer.startIPv6(port: port)
    }
    
    @inlinable
    public final func shutdown() throws {
        try webSocketServer.shutdown()
    }
    
    final func received(packet: MessageQueuePacket, from: MessageQueueServerClient) {
        switch packet {
        case .connect(let connectionPacket):
            do {
                if try BCrypt.verify(connectionPacket.username, created: username)
                && (try BCrypt.verify(connectionPacket.password, created: password)) {
                    from.send(.connectionAccepted)
                    return
                } else {
                    from.send(.connectionDeclined(.wrongCredentials))
                    Task {
                        try await Task.sleep(nanoseconds: 1_000_000)
                        from.webSocket.close(code: .policyViolation, promise: nil)
                    }
                    return
                }
            } catch {
                from.send(.connectionDeclined(.unknownError))
                Task {
                    try await Task.sleep(nanoseconds: 1_000_000)
                    from.webSocket.close(code: .policyViolation, promise: nil)
                }
            }
            
        case .disconnect:
            disconnect(messageQueueClient: from)
        case .push(let pushPacket):
            storage.push(queue: pushPacket.queue, value: pushPacket.payload)
        case .popRequest(let popRequestPacket):
            storage.pop(queue: popRequestPacket.queue, id: popRequestPacket.id, client: from, timeout: popRequestPacket.timeout)
        case .connectionAccepted, .connectionDeclined(_), .popResponse(_), .popExpired(_):
            break
        }
    }
    
    final func disconnect(messageQueueClient: MessageQueueServerClient) {	
        clients.removeAll { $0 === messageQueueClient }
        storage.remove(client: messageQueueClient)
    }
    
    private final func _connected(_ client: WebSocket) {
        clients.append(MessageQueueServerClient(server: self, webSocket: client))
    }
}

extension MessageQueueServer: WebSocketServerProtocol {
    nonisolated public func onConnection(requestHead: HTTPRequestHead, webSocket: WebSocket, channel: Channel) {
        Task { await self._connected(webSocket) }
    }
}
