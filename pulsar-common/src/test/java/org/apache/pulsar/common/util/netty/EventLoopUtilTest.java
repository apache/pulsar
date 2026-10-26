/*
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
package org.apache.pulsar.common.util.netty;

import static org.testng.Assert.assertTrue;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetSocketAddress;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class EventLoopUtilTest {
    @Test
    public void testDefaultTransport() throws Exception {
        EventLoopGroup group = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("event-loop-test"));
        verifyTransports(group);
    }

    @Test
    public void testIoUringTransport() throws Exception {
        if (!IoUring.isAvailable()) {
            throw new SkipException("io_uring is unavailable", IoUring.unavailabilityCause());
        }
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, new DefaultThreadFactory("io-uring-test"),
                IoUringIoHandler.newFactory());
        verifyTransports(group);
    }

    private void verifyTransports(EventLoopGroup group) throws Exception {
        Channel server = null;
        Channel client = null;
        Channel datagram = null;
        try {
            server = new ServerBootstrap().group(group)
                    .channel(EventLoopUtil.getServerSocketChannelClass(group))
                    .childHandler(new ChannelInboundHandlerAdapter())
                    .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            client = new Bootstrap().group(group)
                    .channel(EventLoopUtil.getClientSocketChannelClass(group))
                    .handler(new ChannelInboundHandlerAdapter())
                    .connect(server.localAddress()).sync().channel();
            datagram = new Bootstrap().group(group)
                    .channel(EventLoopUtil.getDatagramChannelClass(group))
                    .handler(new ChannelInboundHandlerAdapter())
                    .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            assertTrue(client.isActive());
            assertTrue(datagram.isActive());
        } finally {
            if (client != null) {
                client.close().sync();
            }
            if (server != null) {
                server.close().sync();
            }
            if (datagram != null) {
                datagram.close().sync();
            }
            group.shutdownGracefully().sync();
        }
    }
}
