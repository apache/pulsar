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
package org.apache.pulsar.netty.tcp.server;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;

import java.util.Optional;

/**
 * Pulsar Channel Initializer to support different types of decoder and handler
 */
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    private Optional<ChannelInboundHandlerAdapter> decoder;
    private ChannelInboundHandlerAdapter handler;

    public PulsarChannelInitializer(Optional<ChannelInboundHandlerAdapter> decoder,
                                    ChannelInboundHandlerAdapter handler) {
        this.decoder = decoder;
        this.handler = handler;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        if(this.decoder.isPresent()) {
            socketChannel.pipeline().addLast(this.decoder.get());
        } else {
            socketChannel.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE,
                    ClassResolvers.cacheDisabled(null)));
        }

        socketChannel.pipeline().addLast(this.handler);
    }

}
