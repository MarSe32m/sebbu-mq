//
//  MessageQueueServer.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import SebbuCrypto

public final class MessageQueueServer {
    private let storage = MessageQueueStorage()
    
    @usableFromInline
    internal var networkServer: NetworkServer!
    
    private var clients = [MessageQueueServerClient]()
    
    private let username: String
    private let password: String
    
    public init(username: String, password: String) throws {
        self.username = try BCrypt.hash(username)
        self.password = try BCrypt.hash(password)
        //TODO: Add tls option
        networkServer = NetworkServer(messageQueueServer: self)
    }
    
    @inlinable
    public nonisolated final func startIPv4(port: Int) throws {
        try networkServer.startIPv4Blocking(port: port)
    }
    
    @inlinable
    public nonisolated final func startIPv6(port: Int) throws {
        try networkServer.startIPv6Blocking(port: port)
    }
    
    @inlinable
    public nonisolated final func shutdown() throws {
        networkServer.shutdown()
    }
    
    final func received(packet: MessageQueuePacket, from: MessageQueueServerClient) {
        switch packet {
        case .connect(let connectionPacket):
            if from.isAuthenticated {
                from.send(.connectionAccepted)
                return
            }
            Task.detached {
                do {
                    if try BCrypt.verify(connectionPacket.username, created: self.username)
                        && (try BCrypt.verify(connectionPacket.password, created: self.password)) {
                        from.send(.connectionAccepted)
                        from.isAuthenticated = true
                        return
                    } else {
                        from.send(.connectionDeclined(.wrongCredentials))
                        Task {
                            try await Task.sleep(nanoseconds: 1_000_000)
                            from.channel.close(mode: .all, promise: nil)
                        }
                        return
                    }
                } catch {
                    from.send(.connectionDeclined(.unknownError))
                    Task {
                        try await Task.sleep(nanoseconds: 1_000_000)
                        from.channel.close(mode: .all, promise: nil)
                    }
                }
            }
            
        case .disconnect:
            disconnect(messageQueueClient: from)
        case .push(let pushPacket):
            if !from.isAuthenticated { return }
            storage.push(queue: pushPacket.queue, value: pushPacket.payload)
        case .popRequest(let popRequestPacket):
            if !from.isAuthenticated {
                from.send(.popResponse(PopResponsePacket(queue: popRequestPacket.queue, id: popRequestPacket.id, payload: [], failed: true)))
                return
            }
            storage.pop(queue: popRequestPacket.queue, id: popRequestPacket.id, client: from, timeout: popRequestPacket.timeout)
        case .connectionAccepted, .connectionDeclined(_), .popResponse(_), .popExpired(_):
            break
        }
    }
    
    final func disconnect(messageQueueClient: MessageQueueServerClient) {	
        clients.removeAll { $0 === messageQueueClient }
        storage.remove(client: messageQueueClient)
    }
    
    final func connected(_ messageQueueServerClient: MessageQueueServerClient) {
        clients.append(messageQueueServerClient)
    }
}
