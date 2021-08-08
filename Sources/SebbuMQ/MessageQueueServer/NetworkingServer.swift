//
//  File.swift
//  
//
//  Created by Sebastian Toivonen on 8.8.2021.
//

import Foundation
import NIO
import NIOExtras

public final class NetworkServer {
    let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    
    var ipv4channel: Channel?
    var ipv6channel: Channel?
    
    unowned var messageQueueServer: MessageQueueServer
    
    public init(messageQueueServer: MessageQueueServer) {
        self.messageQueueServer = messageQueueServer
    }
    
    public final func startIPv4(port: Int) -> EventLoopFuture<Void> {
        bootstrap.bind(host: "0", port: port).flatMap { [unowned self] channel in
            self.ipv4channel = channel
            return eventLoopGroup.next().makeSucceededVoidFuture()
        }
    }
    
    public final func startIPv6(port: Int) -> EventLoopFuture<Void> {
        bootstrap.bind(host: "::", port: port).flatMap { [unowned self] channel in
            self.ipv6channel = channel
            return eventLoopGroup.next().makeSucceededVoidFuture()
        }
    }
    
    public final func startIPv4Blocking(port: Int) throws {
        ipv4channel = try bootstrap.bind(host: "0", port: port).wait()
    }
    
    public final func startIPv6Blocking(port: Int) throws {
        ipv6channel = try bootstrap.bind(host: "::", port: port).wait()
    }
    
    public final func shutdown() {
        ipv4channel?.close(mode: .all, promise: nil)
        ipv6channel?.close(mode: .all, promise: nil)
    }
    
    private var bootstrap: ServerBootstrap {
        ServerBootstrap(group: eventLoopGroup)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.pipeline.addHandler(BackPressureHandler()).flatMap {
                    let messageQueueServerClient = MessageQueueServerClient(server: self.messageQueueServer, channel: channel)
                    return channel.pipeline.addHandlers([ByteToMessageHandler(LengthFieldBasedFrameDecoder(lengthFieldBitLength: .fourBytes)),
                                                  MessageReceiveHandler(messageQueueServerClient: messageQueueServerClient),
                                                  LengthFieldPrepender(lengthFieldBitLength: .fourBytes)])
                }
            }
            .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.socket(IPPROTO_TCP, TCP_NODELAY), value: 1)
            .childChannelOption(ChannelOptions.maxMessagesPerRead, value: 16)
            .childChannelOption(ChannelOptions.recvAllocator, value: AdaptiveRecvByteBufferAllocator())
    }
}

internal final class MessageReceiveHandler: ChannelInboundHandler {
    public typealias InboundIn = ByteBuffer
    public typealias OutboundOut = ByteBuffer

    var messageQueueServerClient: MessageQueueServerClient
    
    init(messageQueueServerClient: MessageQueueServerClient) {
        self.messageQueueServerClient = messageQueueServerClient
    }

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        if let bytes = buffer.getBytes(at: 0, length: buffer.readableBytes) {
            messageQueueServerClient.received(bytes)
        }
    }
    
    func channelUnregistered(context: ChannelHandlerContext) {
        messageQueueServerClient.disconnected()
    }

    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("Error caught in \(#file) \(#line): ", error)
        context.close(promise: nil)
    }
}
