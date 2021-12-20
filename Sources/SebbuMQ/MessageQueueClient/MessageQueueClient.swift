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
import Atomics

public final class MessageQueueClient {
    private var reliablePushRequests = [UInt64 : UnsafeContinuation<Void, Error>]()
    private var popRequests = [UInt64 : UnsafeContinuation<[UInt8], Error>]()
    private var connectionContinuation: UnsafeContinuation<Void, Error>?
    private var isDisconnected = ManagedAtomic<Bool>(false)
    
    private var evlg: EventLoopGroup
    private let networkClient: NetworkClient
    
    private var currentId = ManagedAtomic<UInt64>(UInt64.random(in: .min ... .max))
    
    public enum PushQuality {
        case unreliable
        case reliable
    }
    
    public enum PushError: Error {
        case timedOut
        case queueFull
        case unknown
        case disconnected
        
        internal init(_ pushError: ReliablePushError) {
            switch pushError {
            case .queueFull:
                self = .queueFull
            case .unknown:
                self = .unknown
            }
        }
    }
    
    public enum PopError: Error {
        case timedOut
        case unknown
        case disconnected
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
    
    public final func disconnect() async throws {
        send(.disconnect)
        isDisconnected.store(true, ordering: .relaxed)
        try? await Task.sleep(nanoseconds: 100_000_000)
        do {
            try await networkClient.disconnect()?.get()
        } catch {
            if let error = error as? ChannelError {
                if case .alreadyClosed = error {
                    return
                }
            }
            throw error
        }
    }
    
    @discardableResult
    public final func push(queue: String, _ data: [UInt8]) -> Bool {
        if isDisconnected.load(ordering: .relaxed) { return false }
        send(.push(PushPacket(queue: queue, payload: data)))
        return true
    }
    
    public final func reliablePush(queue: String, _ data: [UInt8], _ timeout: Int64 = 30) async throws {
        if isDisconnected.load(ordering: .relaxed) { throw PushError.disconnected }
        let id = currentId.loadThenWrappingIncrement(ordering: .relaxed)
        let pushPacket = PushPacket(queue: queue, payload: data)
        return try await withUnsafeThrowingContinuation { (continuation: UnsafeContinuation<Void, Error>) in
            networkClient.channel.eventLoop.execute {
                self.reliablePushRequests[id] = continuation
                self.send(.reliablePush(ReliablePushPacket(pushPacket, id: id)))
                self.networkClient.channel.eventLoop.scheduleTask(in: .seconds(timeout)) {
                    self.reliablePushRequests.removeValue(forKey: id)?.resume(throwing: PushError.timedOut)
                }
            }
        }
    }
    
    public final func pop(queue: String, timeout: Double?) async throws -> [UInt8] {
        if isDisconnected.load(ordering: .relaxed) { throw PopError.disconnected }
        let id = currentId.loadThenWrappingIncrement(ordering: .relaxed)
        let popRequestPacket = PopRequestPacket(queue: queue, timeout: timeout, id: id)
        return try await withUnsafeThrowingContinuation { continuation in
            networkClient.channel.eventLoop.execute {
                self.popRequests[id] = continuation
                self.send(.popRequest(popRequestPacket))
            }
        }
    }
    
    private final func resumeAll() {
        while let continuation = popRequests.popFirst()?.value {
            continuation.resume(throwing: PopError.unknown)
        }
        while let continuation = reliablePushRequests.popFirst()?.value {
            continuation.resume(throwing: PushError.unknown)
        }
    }
    
    private final func handlePushConfirmation(_ responsePacket: PushConfimarionPacket) {
        let continuation = reliablePushRequests.removeValue(forKey: responsePacket.id)
        if let error = responsePacket.error {
            continuation?.resume(throwing: PushError(error))
        } else {
            continuation?.resume()
        }
    }
    
    private final func handlePopResponse(_ responsePacket: PopResponsePacket) {
        popRequests.removeValue(forKey: responsePacket.id)?.resume(returning: responsePacket.payload)
    }
    
    private final func handleExpiration(_ expirationPacket: PopExpirationPacket) {
        popRequests.removeValue(forKey: expirationPacket.id)?.resume(throwing: PopError.timedOut)
    }
    
    private final func send(_ packet: MessageQueuePacket) {
        var writeStream: WritableBitStream
        if case let .push(pushPacket) = packet {
            writeStream = WritableBitStream(size: pushPacket.payload.count + pushPacket.queue.count + 4)
        } else if case let .reliablePush(pushPacket) = packet {
            writeStream = WritableBitStream(size: pushPacket.pushPacket.payload.count + pushPacket.pushPacket.queue.count + 4)
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
        isDisconnected.store(true, ordering: .relaxed)
    }
}
