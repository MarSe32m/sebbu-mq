//
//  MessageQueueClient.swift
//  
//
//  Created by Sebastian Toivonen on 5.8.2021.
//

import Foundation
import SebbuCrypto
import SebbuBitStream
import NIO
import _NIOConcurrency

public final class MessageQueueClient {
    private var reliablePushRequests = [UUID : UnsafeContinuation<Bool, Never>]()
    private var popRequests = [UUID : UnsafeContinuation<[UInt8]?, Never>]()
    private var connectionContinuation: UnsafeContinuation<Void, Error>?
    private var isDisconnected = false
    
    private var evlg: EventLoopGroup
    private let networkClient: NetworkClient
    
    public enum PushQuality {
        case unreliable
        case reliable
    }
    
    public enum PushError: Error {
        case failedToVerify
    }
    
    public init(eventLoopGroup: EventLoopGroup? = nil) {
        let _evlg = eventLoopGroup != nil ? eventLoopGroup! : MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.evlg = _evlg
        self.networkClient = NetworkClient(eventLoopGroup: _evlg)
        networkClient.receiveHandler.messageQueueClient = self
    }
    
    public final func connect(username: String, password: String, host: String, port: Int) async throws {
        try await networkClient.connect(host: host, port: port).get()
        try await withUnsafeThrowingContinuation { (continuation: UnsafeContinuation<Void, Error>) in
            networkClient.channel.eventLoop.execute {
                self.connectionContinuation = continuation
                self.send(.connect(ConnectionPacket(username: username, password: password)))
            }
        }
    }
    
    public final func disconnect() {
        send(.disconnect)
        networkClient.channel.eventLoop.scheduleTask(in: .milliseconds(10)) {
            self.networkClient.disconnectBlocking()
            self.isDisconnected = true
        }
    }
    
    @discardableResult
    public final func push(queue: String, _ data: [UInt8])  -> Bool {
        if isDisconnected { return false }
        send(.push(PushPacket(queue: queue, payload: data)))
        return true
    }
    
    @discardableResult
    public final func reliablePush(queue: String, _ data: [UInt8], _ timeout: Int64 = 30) async -> Bool {
        if isDisconnected { return false }
        return await withUnsafeContinuation { (continuation: UnsafeContinuation<Bool, Never>) in
            let id = UUID()
            let pushPacket = PushPacket(queue: queue, payload: data)
            self.reliablePushRequests[id] = continuation
            networkClient.channel.eventLoop.scheduleTask(in: .seconds(timeout)) {
                self.reliablePushRequests.removeValue(forKey: id)?.resume(returning: false)
            }
            send(.reliablePush(ReliablePushPacket(pushPacket, id: id)))
        }
    }
    
    public final func pop(queue: String, timeout: Double?) async -> [UInt8]? {
        if isDisconnected { return nil }
        return await withUnsafeContinuation({ continuation in
            networkClient.channel.eventLoop.execute {
                let popRequestPacket = PopRequestPacket(queue: queue, timeout: timeout)
                self.popRequests[popRequestPacket.id] = continuation
                self.send(.popRequest(popRequestPacket))
            }
        })
    }
    
    private final func resumeAll() {
        for continuation in popRequests.values {
            continuation.resume(returning: nil)
        }
        for continuation in reliablePushRequests.values {
            continuation.resume(returning: false)
        }
        popRequests.removeAll()
    }
    
    private final func handlePushConfirmation(_ responsePacket: PushConfimarionPacket) {
        reliablePushRequests.removeValue(forKey: responsePacket.id)?.resume(returning: true)
    }
    
    private final func handlePopResponse(_ responsePacket: PopResponsePacket) {
        popRequests.removeValue(forKey: responsePacket.id)?.resume(returning: responsePacket.payload)
    }
    
    private final func handleExpiration(_ expirationPacket: PopExpirationPacket) {
        popRequests.removeValue(forKey: expirationPacket.id)?.resume(returning: nil)
    }
    
    private final func send(_ packet: MessageQueuePacket) {
        var writeStream: WritableBitStream
        if case let .push(pushPacket) = packet {
            writeStream = WritableBitStream(size: pushPacket.payload.count + pushPacket.queue.count + 4)
        } else {
            writeStream = WritableBitStream(size: 27)
        }
        writeStream.appendObject(packet)
        let data = writeStream.packBytes()
        networkClient.send(data)
    }
}

extension MessageQueueClient {
    func received(_ data: [UInt8]) {
        var readStream = ReadableBitStream(bytes: data)
        guard let packet = try? MessageQueuePacket(from: &readStream) else {
            return
        }
        
        switch packet {
        case .popRequest(_), .push(_), .reliablePush(_), .connect(_):
            break
        case .connectionAccepted:
            connectionContinuation?.resume()
            connectionContinuation = nil
        case .connectionDeclined(let error):
            connectionContinuation?.resume(throwing: error)
            connectionContinuation = nil
        case .disconnect:
            resumeAll()
        case .popResponse(let responsePacket):
            handlePopResponse(responsePacket)
        case .pushConfirmation(let confirmationPacket):
            handlePushConfirmation(confirmationPacket)
        case .popExpired(let expirationPacket):
            handleExpiration(expirationPacket)
        }
    }
    
    func disconnected() {
        connectionContinuation?.resume(throwing: MessageQueueClientConnectionError.unknownError)
        connectionContinuation = nil
        isDisconnected = true
    }
}
