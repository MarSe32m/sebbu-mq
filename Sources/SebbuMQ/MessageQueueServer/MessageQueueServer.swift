//
//  MessageQueueServer.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import SebbuCrypto
import Foundation
import SebbuTSDS

public final class MessageQueueServer {
    private let storage = MessageQueueStorage()
    
    @usableFromInline
    internal var networkServer: NetworkServer!
    
    //private var clients = [MessageQueueServerClient]()
    private var clients = LockedArray<MessageQueueServerClient>()
    
    private let username: String
    private let password: String
    
    public var totalMaximumBytes: Int {
        get { storage.totalMaxBytes }
        set { storage.totalMaxBytes = newValue }
    }
    
    public init(username: String, password: String, numberOfThreads: Int = 1) throws {
        self.username = try BCrypt.hash(username)
        self.password = try BCrypt.hash(password)
        //TODO: Add tls option
        //TODO: Make this thing multithreaded...
        networkServer = NetworkServer(messageQueueServer: self, numberOfThreads: numberOfThreads)
        storage.startRemoveLoop(eventLoop: networkServer.eventLoopGroup.any())
    }
    
    public nonisolated final func startIPv4(port: Int) throws {
        try networkServer.startIPv4Blocking(port: port)
    }
    
    public nonisolated final func startIPv6(port: Int) throws {
        try networkServer.startIPv6Blocking(port: port)
    }
    
    public nonisolated final func startIPv4(port: Int) async throws {
        try await networkServer.startIPv4(port: port)
    }
    
    public nonisolated final func startIPv6(port: Int) async throws {
        try await networkServer.startIPv6(port: port)
    }
    
    public nonisolated final func shutdown() {
        networkServer.shutdown()
    }
    
    public nonisolated final func shutdown() async throws {
        try await networkServer.shutdown()
    }
    
    final func received(packet: MessageQueuePacket, from: MessageQueueServerClient) {
        switch packet {
        case .connect(let connectionPacket):
            if from.isAuthenticated {
                from.send(.connectionAccepted)
                return
            }
            Task.detached { [username, password] in
                do {
                    async let isCorrectUsername = try BCrypt.verify(connectionPacket.username, created: username)
                    async let isCorrectPassword = try BCrypt.verify(connectionPacket.password, created: password)
                    let correctUsername = try await isCorrectUsername
                    let correctPassword = try await isCorrectPassword
                    
                    let correctCredentials = correctUsername && correctPassword
                    if correctCredentials {
                        from.send(.connectionAccepted)
                        from.isAuthenticated = true
                        self.networkServer.eventLoopGroup.next().execute {
                            self.connected(from)
                        }
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
        case .reliablePush(let reliablePushPacket):
            if !from.isAuthenticated { return }
            var pushError: ReliablePushError? = nil
            if !storage.push(queue: reliablePushPacket.pushPacket.queue, value: reliablePushPacket.pushPacket.payload) {
                pushError = .queueFull
            }
            from.send(.pushConfirmation(PushConfimarionPacket(id: reliablePushPacket.id, error: pushError)))
        case .popRequest(let popRequestPacket):
            if !from.isAuthenticated {
                from.send(.popResponse(PopResponsePacket(queue: popRequestPacket.queue, id: popRequestPacket.id, payload: [], failed: true)))
                return
            }
            storage.pop(queue: popRequestPacket.queue, id: popRequestPacket.id, client: from, timeout: popRequestPacket.timeout)
        case .connectionAccepted, .connectionDeclined(_), .popResponse(_), .popExpired(_), .pushConfirmation(_):
            break
        }
    }
    
    final func disconnect(messageQueueClient: MessageQueueServerClient) {
        clients.removeAll {messageQueueClient == $0}
        storage.remove(client: messageQueueClient)
        messageQueueClient.channel.close(mode: .all, promise: nil)
    }
    
    final func connected(_ messageQueueServerClient: MessageQueueServerClient) {
        clients.append(messageQueueServerClient)
    }
}
