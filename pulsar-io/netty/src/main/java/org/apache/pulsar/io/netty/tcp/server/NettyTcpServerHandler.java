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
package org.apache.pulsar.io.netty.tcp.server;

import io.netty.channel.*;
import lombok.Data;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.netty.NettyTcpSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;

/**
 * Handles a server-side channel
 */
@ChannelHandler.Sharable
public class NettyTcpServerHandler extends SimpleChannelInboundHandler<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(NettyTcpServerHandler.class);

    private NettyTcpSource nettyTcpSource;

    public NettyTcpServerHandler(NettyTcpSource nettyTcpSource) {
        this.nettyTcpSource = nettyTcpSource;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) throws Exception {
        nettyTcpSource.consume(new NettyTcpRecord(Optional.ofNullable(""), bytes));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Error when processing incoming data", cause);
        ctx.close();
    }

    @Data
    static private class NettyTcpRecord implements Record<byte[]>, Serializable {
        private final Optional<String> key;
        private final byte[] value;
    }

}
