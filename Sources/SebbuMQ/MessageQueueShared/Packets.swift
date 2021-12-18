//
//  Packets.swift
//  
//
//  Created by Sebastian Toivonen on 6.8.2021.
//

import Foundation
import SebbuBitStream

enum MessageQueuePacket: BitStreamCodable {
    case connect(ConnectionPacket)
    case connectionAccepted
    case connectionDeclined(MessageQueueClientConnectionError)
    case disconnect
    case push(PushPacket)
    case reliablePush(ReliablePushPacket)
    case pushConfirmation(PushConfimarionPacket)
    case popRequest(PopRequestPacket)
    case popResponse(PopResponsePacket)
    case popExpired(PopExpirationPacket)
    
    private enum CodingKey: UInt32, CaseIterable {
        case connect
        case connectionAccepted
        case connectionDeclined
        case disconnect
        case push
        case reliablePush
        case pushConfimation
        case popRequest
        case popResponse
        case popExpiration
    }
    
    init(from bitStream: inout ReadableBitStream) throws {
        let codingKey: CodingKey = try bitStream.read()
        switch codingKey {
        case .connect:
            self = .connect(try ConnectionPacket(from: &bitStream))
        case .connectionAccepted:
            self = .connectionAccepted
        case .connectionDeclined:
            self = .connectionDeclined(try bitStream.read())
        case .disconnect:
            self = .disconnect
        case .push:
            self = .push(try PushPacket(from: &bitStream))
        case .reliablePush:
            self = .reliablePush(try ReliablePushPacket(from: &bitStream))
        case .pushConfimation:
            self = .pushConfirmation(try PushConfimarionPacket(from: &bitStream))
        case .popRequest:
            self = .popRequest(try PopRequestPacket(from: &bitStream))
        case .popResponse:
            self = .popResponse(try PopResponsePacket(from: &bitStream))
        case .popExpiration:
            self = .popExpired(try PopExpirationPacket(from: &bitStream))
        }
    }
    
    func encode(to bitStream: inout WritableBitStream) {
        switch self {
        case .connect(let message):
            bitStream.append(CodingKey.connect)
            bitStream.appendObject(message)
        case .connectionAccepted:
            bitStream.append(CodingKey.connectionAccepted)
        case .connectionDeclined(let message):
            bitStream.append(CodingKey.connectionDeclined)
            bitStream.append(message)
        case .disconnect:
            bitStream.append(CodingKey.disconnect)
        case .push(let message):
            bitStream.append(CodingKey.push)
            bitStream.appendObject(message)
        case .reliablePush(let message):
            bitStream.append(CodingKey.reliablePush)
            bitStream.appendObject(message)
        case .pushConfirmation(let message):
            bitStream.append(CodingKey.pushConfimation)
            bitStream.appendObject(message)
        case .popRequest(let message):
            bitStream.append(CodingKey.popRequest)
            bitStream.appendObject(message)
        case .popResponse(let message):
            bitStream.append(CodingKey.popResponse)
            bitStream.appendObject(message)
        case .popExpired(let message):
            bitStream.append(CodingKey.popExpiration)
            bitStream.appendObject(message)
        }
    }
}

enum MessageQueueClientConnectionError: UInt32, Error, CaseIterable {
    case wrongCredentials
    case unknownError
}

struct ConnectionPacket: BitStreamCodable {
    let username: String
    let password: String
    
    public init(username: String, password: String) {
        self.username = username
        self.password = password
    }
    
    init(from bitStream: inout ReadableBitStream) throws {
        username = try bitStream.read()
        password = try bitStream.read()
    }
    
    func encode(to bitStream: inout WritableBitStream) {
        bitStream.append(username)
        bitStream.append(password)
    }
}

struct PushPacket: BitStreamCodable {
    let queue: String
    let payload: [UInt8]
    
    public init(queue: String, payload: [UInt8]) {
        self.queue = queue
        self.payload = payload
    }
    
    init(from bitStream: inout ReadableBitStream) throws {
        queue = try bitStream.read()
        payload = try bitStream.read()
    }
    
    func encode(to bitStream: inout WritableBitStream) {
        bitStream.append(queue)
        bitStream.append(payload)
    }
}

struct ReliablePushPacket: BitStreamCodable {
    let pushPacket: PushPacket
    let id: UInt64
    
    public init(_ pushPacket: PushPacket, id: UInt64) {
        self.pushPacket = pushPacket
        self.id = id
    }
    
    init(from bitStream: inout ReadableBitStream) throws {
        pushPacket = try PushPacket(from: &bitStream)
        id = try bitStream.read()
    }
    
    func encode(to bitStream: inout WritableBitStream) {
        bitStream.appendObject(pushPacket)
        bitStream.append(id)
    }
}

enum ReliablePushError: UInt32, Error, CaseIterable {
    case queueFull
    case unknown
}

struct PushConfimarionPacket: BitStreamCodable {
    let id: UInt64
    var error: ReliablePushError?
    
    public init(id: UInt64, error: ReliablePushError? = nil) {
        self.id = id
        self.error = error
    }
    
    init(from bitStream: inout ReadableBitStream) throws {
        id = try bitStream.read()
        error = try bitStream.read() as Bool ? try bitStream.read() : nil
    }
    
    func encode(to bitStream: inout WritableBitStream) {
        bitStream.append(id)
        if let error = error {
            bitStream.append(true)
            bitStream.append(error)
        } else {
            bitStream.append(false)
        }
    }
}

struct PopRequestPacket: BitStreamCodable {
    let queue: String
    let timeout: Double?
    let id: UInt64
    
    public init(queue: String, timeout: Double?, id: UInt64) {
        self.queue = queue
        self.timeout = timeout
        self.id = id
    }
    
    init(from bitStream: inout ReadableBitStream) throws {
        queue = try bitStream.read()
        id = try bitStream.read()
        if try bitStream.read() {
            timeout = try bitStream.read()
        } else {
            timeout = nil
        }
    }
    
    func encode(to bitStream: inout WritableBitStream) {
        bitStream.append(queue)
        bitStream.append(id)
        if let timeout = timeout {
            bitStream.append(true)
            bitStream.append(timeout)
        } else {
            bitStream.append(false)
        }
    }
}

struct PopResponsePacket: BitStreamCodable {
    let queue: String
    let id: UInt64
    let payload: [UInt8]
    let failed: Bool
    
    public init(queue: String, id: UInt64, payload: [UInt8], failed: Bool = false) {
        self.queue = queue
        self.id = id
        self.payload = payload
        self.failed = failed
    }
    
    init(from bitStream: inout ReadableBitStream) throws {
        queue = try bitStream.read()
        id = try bitStream.read()
        let isFailedPopRequest: Bool = try bitStream.read()
        failed = isFailedPopRequest
        if !isFailedPopRequest {
            payload = try bitStream.read()
        } else {
            payload = []
        }
    }
    
    func encode(to bitStream: inout WritableBitStream) {
        bitStream.append(queue)
        bitStream.append(id)
        bitStream.append(failed)
        if !failed {
            bitStream.append(payload)
        }
    }
}

struct PopExpirationPacket: BitStreamCodable {
    let queue: String
    let id: UInt64
    
    public init(queue: String, id: UInt64) {
        self.queue = queue
        self.id = id
    }
    
    init(from bitStream: inout ReadableBitStream) throws {
        queue = try bitStream.read()
        id = try bitStream.read()
    }
    
    func encode(to bitStream: inout WritableBitStream) {
        bitStream.append(queue)
        bitStream.append(id)
    }
}
