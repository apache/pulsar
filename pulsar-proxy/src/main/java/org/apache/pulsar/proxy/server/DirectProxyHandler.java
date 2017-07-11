/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.proxy.server;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.PulsarLengthFieldFrameDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

public class DirectProxyHandler {

    private Channel inboundChannel;
    Channel outboundChannel;

    private final Authentication authentication;

    public DirectProxyHandler(ProxyService service, ProxyConnection proxyConnection, String targetBrokerUrl) {
        this.authentication = service.getClientAuthentication();
        this.inboundChannel = proxyConnection.ctx().channel();

        // Start the connection attempt.
        Bootstrap b = new Bootstrap();
        // Tie the backend connection on the same thread to avoid context switches when passing data between the 2
        // connections
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.group(inboundChannel.eventLoop()).channel(inboundChannel.getClass()).option(ChannelOption.AUTO_READ, false);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("frameDecoder",
                        new PulsarLengthFieldFrameDecoder(PulsarDecoder.MaxFrameSize, 0, 4, 0, 4));
                ch.pipeline().addLast(new ProxyBackendHandler());
            }
        });

        URI targetBroker;
        try {
            // targetBrokerUrl is coming in the "hostname:6650" form, so we need to extract host and port
            targetBroker = new URI("pulsar://" + targetBrokerUrl);
        } catch (URISyntaxException e) {
            log.warn("[{}] Failed to parse broker url '{}'", inboundChannel, targetBrokerUrl, e);
            inboundChannel.close();
            return;
        }

        ChannelFuture f = b.connect(targetBroker.getHost(), targetBroker.getPort());
        outboundChannel = f.channel();
        f.addListener(future -> {
            if (!future.isSuccess()) {
                // Close the connection if the connection attempt has failed.
                inboundChannel.close();
            }
        });
    }

    enum BackendState {
        Init, HandshakeCompleted
    }

    public class ProxyBackendHandler extends PulsarDecoder implements FutureListener<Void> {

        private BackendState state = BackendState.Init;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            // Send the Connect command to broker
            String authData = "";
            if (authentication.getAuthData().hasDataFromCommand()) {
                authData = authentication.getAuthData().getCommandData();
            }
            outboundChannel
                    .writeAndFlush(Commands.newConnect(authentication.getAuthMethodName(), authData, "Pulsar proxy"));
            outboundChannel.read();
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            switch (state) {
            case Init:
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Received msg on broker connection: {}", inboundChannel, outboundChannel,
                            msg.getClass());
                }

                // Do the regular decoding for the Connected message
                super.channelRead(ctx, msg);
                break;

            case HandshakeCompleted:
                inboundChannel.writeAndFlush(msg).addListener(this);
                break;

            default:
                break;
            }

        }

        @Override
        public void operationComplete(Future<Void> future) throws Exception {
            // This is invoked when the write operation on the paired connection is completed
            if (future.isSuccess()) {
                outboundChannel.read();
            } else {
                log.warn("[{}] [{}] Failed to write on proxy connection. Closing both connections.", inboundChannel,
                        outboundChannel, future.cause());
                inboundChannel.close();
            }
        }

        @Override
        protected void messageReceived() {
            // no-op
        }

        @Override
        protected void handleConnected(CommandConnected connected) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received Connected from broker", inboundChannel, outboundChannel);
            }
            state = BackendState.HandshakeCompleted;

            inboundChannel.writeAndFlush(Commands.newConnected(connected.getProtocolVersion())).addListener(future -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Removing decoder from pipeline", inboundChannel, outboundChannel);
                }
                inboundChannel.pipeline().remove("frameDecoder");
                outboundChannel.pipeline().remove("frameDecoder");

                // Start reading from both connections
                inboundChannel.read();
                outboundChannel.read();
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            inboundChannel.close();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("[{}] [{}] Caught exception: {}", inboundChannel, outboundChannel, cause.getMessage(), cause);
            ctx.close();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DirectProxyHandler.class);
}
