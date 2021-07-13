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
package org.apache.pulsar.client.impl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Contains request timeout tests for different request types in ClientCnx
 * that use the requestTimeoutQueue based solution to handle timeouts.
 *
 * This includes ordinary command requests, GetLastMessageId requests,
 * GetTopics requests, GetSchema requests and GetOrCreateSchema requests.
 */
public class ClientCnxRequestTimeoutQueueTest {
    ClientCnx cnx;
    EventLoopGroup eventLoop;
    ByteBuf requestMessage;

    @BeforeTest
    void setupClientCnx() throws Exception {
        eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setKeepAliveIntervalSeconds(0);
        conf.setOperationTimeoutMs(1);
        cnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.writeAndFlush(any())).thenAnswer(args -> mock(ChannelFuture.class));
        when(ctx.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress(1234));
        cnx.channelActive(ctx);

        requestMessage = mock(ByteBuf.class);
    }

    @AfterTest(alwaysRun = true)
    void cleanupClientCnx() {
        if (eventLoop != null) {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void testCommandRequestTimeout() {
        assertFutureTimesOut(cnx.sendRequestWithId(requestMessage, 1L));
    }

    @Test
    void testGetLastMessageIdRequestTimeout() {
        assertFutureTimesOut(cnx.sendGetLastMessageId(requestMessage, 1L));
    }

    @Test
    void testGetTopicsRequestTimeout() {
        assertFutureTimesOut(cnx.newGetTopicsOfNamespace(requestMessage, 1L));
    }

    @Test
    void testNewAckForResponseNoFlushTimeout() {
        assertFutureTimesOut(cnx.newAckForReceipt(requestMessage, 1L));
    }

    @Test
    void testNewAckForResponseFlushTimeout() {
        TimedCompletableFuture<Void> timedCompletableFuture = new TimedCompletableFuture<>();
        cnx.newAckForReceiptWithFuture(requestMessage, 1L, timedCompletableFuture);
        assertFutureTimesOut(timedCompletableFuture);
    }

    @Test
    void testGetSchemaRequestTimeout() {
        assertFutureTimesOut(cnx.sendGetRawSchema(requestMessage, 1L));
    }

    @Test
    void testGetOrCreateSchemaRequestTimeout() {
        assertFutureTimesOut(cnx.sendGetOrCreateSchema(requestMessage, 1L));
    }

    private void assertFutureTimesOut(CompletableFuture<?> future) {
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Future should have timed out.");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
        }
    }
}
