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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Pattern;

import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PulsarClientImpl unit tests.
 */
public class PulsarClientImplTest {
    private PulsarClientImpl clientImpl;
    private EventLoopGroup eventLoopGroup;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws PulsarClientException {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        initializeEventLoopGroup(conf);
        clientImpl = new PulsarClientImpl(conf, eventLoopGroup);
    }

    private void initializeEventLoopGroup(ClientConfigurationData conf) {
        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());
        eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (clientImpl != null) {
            clientImpl.close();
            clientImpl = null;
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully().get();
            eventLoopGroup = null;
        }
    }

    @Test
    public void testIsClosed() throws Exception {
        assertFalse(clientImpl.isClosed());
        clientImpl.close();
        assertTrue(clientImpl.isClosed());
    }

    @Test
    public void testConsumerIsClosed() throws Exception {
        // mock client connection
        LookupService lookup = mock(LookupService.class);
        when(lookup.getTopicsUnderNamespace(
                any(NamespaceName.class),
                any(CommandGetTopicsOfNamespace.Mode.class)))
                .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
        when(lookup.getPartitionedTopicMetadata(any(TopicName.class)))
                .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata()));
        when(lookup.getBroker(any(TopicName.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        Pair.of(mock(InetSocketAddress.class), mock(InetSocketAddress.class))));
        ConnectionPool pool = mock(ConnectionPool.class);
        ClientCnx cnx = mock(ClientCnx.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(mock(SocketAddress.class));
        when(ctx.channel()).thenReturn(channel);
        when(ctx.writeAndFlush(any(), any(ChannelPromise.class))).thenReturn(mock(ChannelFuture.class));
        when(ctx.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(cnx.channel()).thenReturn(channel);
        when(cnx.ctx()).thenReturn(ctx);
        when(cnx.sendRequestWithId(any(ByteBuf.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(mock(ProducerResponse.class)));
        when(pool.getConnection(any(InetSocketAddress.class), any(InetSocketAddress.class), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(cnx));
        Whitebox.setInternalState(clientImpl, "cnxPool", pool);
        Whitebox.setInternalState(clientImpl, "lookup", lookup);

        List<ConsumerBase<byte[]>> consumers = new ArrayList<>();
        /**
         * {@link org.apache.pulsar.client.impl.PulsarClientImpl#patternTopicSubscribeAsync}
         */
        ConsumerConfigurationData<byte[]> consumerConf0 = new ConsumerConfigurationData<>();
        consumerConf0.setSubscriptionName("test-subscription0");
        consumerConf0.setTopicsPattern(Pattern.compile("test-topic"));
        consumers.add((ConsumerBase) clientImpl.subscribeAsync(consumerConf0).get());
        /**
         * {@link org.apache.pulsar.client.impl.PulsarClientImpl#singleTopicSubscribeAsync}
         */
        ConsumerConfigurationData<byte[]> consumerConf1 = new ConsumerConfigurationData<>();
        consumerConf1.setSubscriptionName("test-subscription1");
        consumerConf1.setTopicNames(Collections.singleton("test-topic"));
        consumers.add((ConsumerBase) clientImpl.subscribeAsync(consumerConf1).get());
        /**
         * {@link org.apache.pulsar.client.impl.PulsarClientImpl#multiTopicSubscribeAsync}
         */
        ConsumerConfigurationData<byte[]> consumerConf2 = new ConsumerConfigurationData<>();
        consumerConf2.setSubscriptionName("test-subscription2");
        consumers.add((ConsumerBase) clientImpl.subscribeAsync(consumerConf2).get());

        consumers.forEach(consumer ->
                assertNotSame(consumer.getState(), HandlerState.State.Closed));
        clientImpl.close();
        consumers.forEach(consumer ->
                assertSame(consumer.getState(), HandlerState.State.Closed));
    }

    @Test
    public void testInitializeWithoutTimer() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        PulsarClientImpl client = new PulsarClientImpl(conf);

        HashedWheelTimer timer = mock(HashedWheelTimer.class);
        Field field = client.getClass().getDeclaredField("timer");
        field.setAccessible(true);
        field.set(client, timer);

        client.shutdown();
        verify(timer).stop();
    }

    @Test
    public void testInitializeWithTimer() throws PulsarClientException {
        ClientConfigurationData conf = new ClientConfigurationData();
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("test"));
        ConnectionPool pool = Mockito.spy(new ConnectionPool(conf, eventLoop));
        conf.setServiceUrl("pulsar://localhost:6650");

        HashedWheelTimer timer = new HashedWheelTimer();
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoop, pool, timer);

        client.shutdown();
        client.timer().stop();
    }

    @Test(expectedExceptions = PulsarClientException.class)
    public void testNewTransactionWhenDisable() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setEnableTransaction(false);
        PulsarClientImpl pulsarClient = null;
        try {
            pulsarClient = new PulsarClientImpl(conf);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        pulsarClient.newTransaction();
    }

    @Test
    public void testResourceCleanup() throws PulsarClientException {
        ClientConfigurationData conf = clientImpl.conf;
        conf.setServiceUrl("");
        initializeEventLoopGroup(conf);
        ConnectionPool connectionPool = new ConnectionPool(conf, eventLoopGroup);
        try {
            assertThrows(() -> new PulsarClientImpl(conf, eventLoopGroup, connectionPool));
        } finally {
            // Externally passed eventLoopGroup should not be shutdown.
            assertFalse(eventLoopGroup.isShutdown());
        }
    }

    @Test
    public void testInitializingWithExecutorProviders() throws PulsarClientException {
        ClientConfigurationData conf = clientImpl.conf;
        @Cleanup("shutdownNow")
        ExecutorProvider executorProvider = new ExecutorProvider(2, "shared-executor");
        @Cleanup("shutdownNow")
        ScheduledExecutorProvider scheduledExecutorProvider =
                new ScheduledExecutorProvider(2, "scheduled-executor");
        @Cleanup
        PulsarClientImpl client2 = PulsarClientImpl.builder().conf(conf)
                .internalExecutorProvider(executorProvider)
                .externalExecutorProvider(executorProvider)
                .scheduledExecutorProvider(scheduledExecutorProvider)
                .build();
        @Cleanup
        PulsarClientImpl client3 = PulsarClientImpl.builder().conf(conf)
                .internalExecutorProvider(executorProvider)
                .externalExecutorProvider(executorProvider)
                .scheduledExecutorProvider(scheduledExecutorProvider)
                .build();

        assertEquals(client2.getScheduledExecutorProvider(), scheduledExecutorProvider);
        assertEquals(client3.getScheduledExecutorProvider(), scheduledExecutorProvider);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Both externalExecutorProvider and internalExecutorProvider must be " +
                    "specified or unspecified.")
    public void testBothExecutorProvidersMustBeSpecified() throws PulsarClientException {
        ClientConfigurationData conf = clientImpl.conf;
        @Cleanup("shutdownNow")
        ExecutorProvider executorProvider = new ExecutorProvider(2, "shared-executor");
        @Cleanup
        PulsarClientImpl client2 = PulsarClientImpl.builder().conf(conf)
                .internalExecutorProvider(executorProvider)
                .build();
    }
}
