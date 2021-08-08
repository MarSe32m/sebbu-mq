//
//  NetworkClient.swift
//  
//
//  Created by Sebastian Toivonen on 8.8.2021.
//

import NIO
import NIOExtras

internal final class NetworkClient {
    private let eventLoopGroup: EventLoopGroup
    
    private var channel: Channel!
    
    let receiveHandler = ClientReceiveHandler()
    
    public init(eventLoopGroup: EventLoopGroup) {
        self.eventLoopGroup = eventLoopGroup
    }
    
    public final func connect(host: String, port: Int) -> EventLoopFuture<Void> {
        bootstrap.connect(host: host, port: port).flatMap { [unowned self] channel in
            self.channel = channel
            return eventLoopGroup.next().makeSucceededVoidFuture()
        }
    }
    
    public final func connectBlocking(host: String, port: Int) throws {
        if channel != nil { return }
        channel = try bootstrap.connect(host: host, port: port).wait()
    }
    
    public final func disconnect() -> EventLoopFuture<Void>? {
        channel?.close(mode: .all)
    }
    
    public final func disconnectBlocking() {
        channel?.close(mode: .all, promise: nil)
    }
    
    public func send(_ bytes: [UInt8]) {
        let buffer = channel.allocator.buffer(bytes: bytes)
        channel.writeAndFlush(buffer, promise: nil)
    }
    
    private var  bootstrap: ClientBootstrap {
        ClientBootstrap(group: eventLoopGroup)
        .connectTimeout(.seconds(10))
        .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
        .channelOption(ChannelOptions.socket(IPPROTO_TCP, TCP_NODELAY), value: 1)
        .channelInitializer { channel in
            channel.pipeline.addHandlers([ByteToMessageHandler(LengthFieldBasedFrameDecoder(lengthFieldBitLength: .fourBytes)),
                                          self.receiveHandler,
                                          LengthFieldPrepender(lengthFieldBitLength: .fourBytes)])
        }
    }
}

internal final class ClientReceiveHandler: ChannelInboundHandler {
    public typealias InboundIn = ByteBuffer
    public typealias OutboundOut = ByteBuffer

    var messageQueueClient: MessageQueueClient?
    
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        if let bytes = buffer.getBytes(at: 0, length: buffer.readableBytes) {
            messageQueueClient?.received(bytes)
        }
    }
    
    func channelUnregistered(context: ChannelHandlerContext) {
        messageQueueClient?.disconnected()
    }

    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("Error caught in \(#file) \(#line): ", error)
        context.close(promise: nil)
    }
}
