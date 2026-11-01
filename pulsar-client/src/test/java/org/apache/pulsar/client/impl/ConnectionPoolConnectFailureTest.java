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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.Test;

/**
 * A failed connect completes its connection future through a task on the pool's event loop group. When the client
 * shuts down, the loop that gets that task can have terminated already, and the future must fail anyway.
 */
public class ConnectionPoolConnectFailureTest {

    @Test(timeOut = 30_000)
    public void testConnectFailureAfterEventLoopTerminated() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testConnectFailureAfterEventLoopTerminated"));
        CompletableFuture<Channel> connecting = new CompletableFuture<>();
        CompletableFuture<Channel> connect = new CompletableFuture<>();
        ConnectionPool pool = new ConnectionPool(InstrumentProvider.NOOP, new ClientConfigurationData(), eventLoop,
                null) {
            @Override
            protected CompletableFuture<Channel> connectToPhysicalAddress(Channel channel,
                                                                          InetSocketAddress physicalAddress) {
                connecting.complete(channel);
                return connect;
            }
        };
        try {
            CompletableFuture<ClientCnx> cnxFuture = pool.getConnection(new InetSocketAddress("127.0.0.1", 6650));
            Channel channel = connecting.get(10, TimeUnit.SECONDS);

            // The client shuts down while the connection is being established. Closing the channel fails the
            // connect, but the event loop has terminated by the time the pool handles that failure.
            pool.close();
            channel.close().sync();
            eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
            connect.completeExceptionally(new ClosedChannelException());

            assertThat(cnxFuture).failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(PulsarClientException.class);
        } finally {
            eventLoop.shutdownGracefully();
        }
    }

    /**
     * A connect that fails right away runs the failure handling inside the pool's computeIfAbsent() that is adding
     * the connection, where the connection can't be removed from the pool.
     */
    @Test(timeOut = 30_000)
    public void testImmediateConnectFailureAfterEventLoopTerminated() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testImmediateConnectFailureAfterEventLoopTerminated"));
        ConnectionPool pool = new ConnectionPool(InstrumentProvider.NOOP, new ClientConfigurationData(), eventLoop,
                null) {
            @Override
            CompletableFuture<List<InetSocketAddress>> resolveName(InetSocketAddress unresolvedAddress) {
                return CompletableFuture.failedFuture(new UnknownHostException(unresolvedAddress.getHostString()));
            }
        };
        // A connection asked for after the client has shut down
        pool.close();
        eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();

        CompletableFuture<ClientCnx> cnxFuture =
                pool.getConnection(InetSocketAddress.createUnresolved("broker.invalid", 6650));

        assertThat(cnxFuture).failsWithin(10, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.class);
        assertThat(pool.getPoolSize()).isZero();
    }
}
