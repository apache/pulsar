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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
import io.netty.resolver.AddressResolver;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.lang.reflect.Field;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import lombok.Cleanup;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * PulsarClientImpl unit tests.
 */
public class PulsarClientImplTest {
    private EventLoopGroup eventLoopGroup;

    private void initializeEventLoopGroup(ClientConfigurationData conf) {
        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());
        eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully().get();
            eventLoopGroup = null;
        }
    }

    @Test
    public void testIsClosed() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        initializeEventLoopGroup(conf);
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoopGroup);
        assertFalse(client.isClosed());
        client.close();
        assertTrue(client.isClosed());
    }

    @DataProvider
    public Object[][] sharedEventLoop() {
        return new Object[][]{{false}, {true}};
    }

    /**
     * A lookup on a closed client fails without asking for a new connection: once the client's own event loop has
     * terminated, a new connection never completes, and a shared event loop would still open one.
     */
    @Test(dataProvider = "sharedEventLoop", timeOut = 30_000)
    public void testLookupsFailOnClosedClient(boolean sharedEventLoop) throws Exception {
        @Cleanup
        PulsarClientSharedResources sharedResources = sharedEventLoop
                ? PulsarClientSharedResources.builder()
                        .resourceTypes(PulsarClientSharedResources.SharedResource.EventLoopGroup)
                        .build()
                : null;
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .sharedResources(sharedResources)
                .build();
        client.close();

        String topic = "persistent://public/default/test";
        assertFailsAsClosed(client.getSchema(topic));
        assertFailsAsClosed(client.getConnection(topic));
        assertFailsAsClosed(client.getConnectionToServiceUrl());
    }

    private static void assertFailsAsClosed(CompletableFuture<?> future) {
        assertThat(future).failsWithin(Duration.ofSeconds(10))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.AlreadyClosedException.class);
    }

    @DataProvider
    public Object[][] partitions() {
        return new Object[][]{{0}, {2}};
    }

    /**
     * Cancelling the creation future does not stop the creation: the producer still comes up, and the client keeps
     * owning it, so it is closed with the client.
     */
    @Test(dataProvider = "partitions")
    @SuppressWarnings("unchecked")
    public void testCancelledCreationLeavesTheProducerOwnedByTheClient(int partitions) throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        initializeEventLoopGroup(conf);
        LookupService lookup = mock(LookupService.class);
        CompletableFuture<PartitionedTopicMetadata> metadata = new CompletableFuture<>();
        when(lookup.getPartitionedTopicMetadata(any(TopicName.class), anyBoolean(), anyBoolean()))
                .thenReturn(metadata);
        when(lookup.getBroker(any()))
                .thenReturn(CompletableFuture.completedFuture(new LookupTopicResult(
                        mock(InetSocketAddress.class), mock(InetSocketAddress.class), false)));
        ConnectionPool pool = mock(ConnectionPool.class);
        ClientCnx cnx = mock(ClientCnx.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(mock(SocketAddress.class));
        when(channel.eventLoop()).thenReturn(eventLoopGroup.next());
        when(ctx.channel()).thenReturn(channel);
        when(ctx.writeAndFlush(any(), any(ChannelPromise.class))).thenReturn(mock(ChannelFuture.class));
        when(ctx.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(cnx.channel()).thenReturn(channel);
        when(cnx.ctx()).thenReturn(ctx);
        ProducerResponse response = mock(ProducerResponse.class);
        when(response.getProducerName()).thenReturn("cancelled-creation-producer");
        when(cnx.sendRequestWithId(any(ByteBuf.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(response));
        when(pool.getConnection(any(InetSocketAddress.class), any(InetSocketAddress.class), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(cnx));
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoopGroup, pool);
        client.setLookup(lookup);
        try {
            ProducerConfigurationData producerConf = new ProducerConfigurationData();
            producerConf.setTopicName("persistent://public/default/cancelled-creation");
            producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
            CompletableFuture<Producer<byte[]>> creation = client.createProducerAsync(producerConf, Schema.BYTES);

            // Cancelled before the metadata is known, which is before the producer exists.
            creation.cancel(true);
            metadata.complete(new PartitionedTopicMetadata(partitions));

            assertEquals(client.producersCount(), 1, "The client must keep owning the producer");
        } finally {
            client.close();
        }
        assertEquals(client.producersCount(), 0, "Closing the client must close the producer it kept");
    }

    @DataProvider
    public Object[][] endedBeforeRegistration() {
        return new Object[][]{
                {"closed", (Consumer<ProducerImpl<?>>) ProducerImpl::closeAsync},
                {"failed", (Consumer<ProducerImpl<?>>) producer ->
                        producer.connectionFailed(new PulsarClientException.AuthorizationException("not allowed"))},
        };
    }

    /**
     * A producer whose creation fails before the client has registered it, from another thread or from a
     * connection that fails at once, removes itself from the client while it is not there yet. The client must not
     * keep it afterwards.
     */
    @Test(dataProvider = "endedBeforeRegistration")
    @SuppressWarnings("unchecked")
    public void testProducerEndedBeforeItIsRegisteredIsNotKept(String how, Consumer<ProducerImpl<?>> end)
            throws Exception {
        LookupService lookup = mock(LookupService.class);
        when(lookup.getPartitionedTopicMetadata(any(TopicName.class), anyBoolean(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata()));
        when(lookup.getBroker(any())).thenReturn(new CompletableFuture<>());
        ConnectionPool pool = mock(ConnectionPool.class);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoopGroup, pool) {
            @Override
            protected <T> ProducerImpl<T> newProducerImpl(String topic, int partitionIndex,
                                                          ProducerConfigurationData producerConf, Schema<T> schema,
                                                          ProducerInterceptors interceptors,
                                                          CompletableFuture<Producer<T>> producerCreatedFuture,
                                                          Optional<String> overrideProducerName) {
                ProducerImpl<T> producer = super.newProducerImpl(topic, partitionIndex, producerConf, schema,
                        interceptors, producerCreatedFuture, overrideProducerName);
                // Before createProducerAsync() has added the producer.
                end.accept(producer);
                return producer;
            }
        };
        client.setLookup(lookup);
        try {
            ProducerConfigurationData producerConf = new ProducerConfigurationData();
            producerConf.setTopicName("persistent://public/default/closed-before-registered");
            client.createProducerAsync(producerConf, Schema.BYTES);

            assertEquals(client.producersCount(), 0,
                    "A producer " + how + " before it was registered must not be kept");
        } finally {
            client.close();
        }
    }

    @DataProvider
    public Object[][] subscriptionsFailingAtOnce() {
        return new Object[][]{
                {"consumer", 0, "subscribe request"}, {"consumer", 0, "connection"},
                {"reader", 0, "subscribe request"}, {"reader", 0, "connection"},
                {"consumer", 2, "subscribe request"}, {"reader", 2, "subscribe request"},
        };
    }

    /**
     * A subscription whose connection is already open and whose subscribe request has already failed when the
     * consumer attaches its callbacks (the broker's error arrived first), or whose connection fails at once, fails
     * inside the consumer's constructor, before the client has registered the consumer: the consumer removes itself
     * from the client while it is not there yet. The client must not keep it afterwards. On a partitioned topic, the
     * partitions' failures reach the parent consumer through the client's executors, which run tasks at once here.
     */
    @Test(dataProvider = "subscriptionsFailingAtOnce")
    public void testConsumerFailedBeforeItIsRegisteredIsNotKept(String kind, int partitions, String failing)
            throws Exception {
        LookupService lookup = mock(LookupService.class);
        when(lookup.getPartitionedTopicMetadata(any(TopicName.class), anyBoolean(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(partitions)));
        when(lookup.getBroker(any()))
                .thenReturn(CompletableFuture.completedFuture(new LookupTopicResult(
                        mock(InetSocketAddress.class), mock(InetSocketAddress.class), false)));
        ConnectionPool pool = mock(ConnectionPool.class);
        if (failing.equals("subscribe request")) {
            ClientCnx cnx = mock(ClientCnx.class);
            Channel channel = mock(Channel.class);
            when(cnx.channel()).thenReturn(channel);
            ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
            when(ctx.channel()).thenReturn(channel);
            when(cnx.ctx()).thenReturn(ctx);
            when(cnx.sendRequestWithId(any(ByteBuf.class), anyLong())).thenAnswer(invocation -> {
                invocation.getArgument(0, ByteBuf.class).release();
                return CompletableFuture.failedFuture(
                        new PulsarClientException.IncompatibleSchemaException("incompatible schema"));
            });
            when(pool.getConnection(any(InetSocketAddress.class), any(InetSocketAddress.class), anyInt()))
                    .thenReturn(CompletableFuture.completedFuture(cnx));
        } else {
            when(pool.getConnection(any(InetSocketAddress.class), any(InetSocketAddress.class), anyInt()))
                    .thenReturn(CompletableFuture.failedFuture(
                            new PulsarClientException.AuthorizationException("not allowed")));
        }
        ExecutorProvider executors = mock(ExecutorProvider.class);
        when(executors.getExecutor()).thenReturn(ImmediateEventExecutor.INSTANCE);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoopGroup, pool, null, executors, executors, null);
        client.setLookup(lookup);
        try {
            String topic = "persistent://public/default/failed-before-registered";
            CompletableFuture<?> subscription = kind.equals("consumer")
                    ? client.newConsumer().topic(topic).subscriptionName("sub").subscribeAsync()
                    : client.newReader().topic(topic).startMessageId(MessageId.earliest).createAsync();

            assertThat(subscription).isCompletedExceptionally();
            assertEquals(client.consumersCount(), 0, "A " + kind + " on " + partitions + " partitions whose "
                    + failing + " failed before it was registered must not be kept");
        } finally {
            client.close();
        }
    }

    @Test
    public void testConsumerIsClosed() throws Exception {
        // mock client connection
        LookupService lookup = mock(LookupService.class);
        when(lookup.getTopicsUnderNamespace(
                any(NamespaceName.class),
                any(CommandGetTopicsOfNamespace.Mode.class),
                nullable(String.class),
                nullable(String.class),
                nullable(Map.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        new GetTopicsResult(Collections.emptyList(), null, false, true)));
        when(lookup.getPartitionedTopicMetadata(any(TopicName.class), anyBoolean(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata()));
        when(lookup.getBroker(any()))
                .thenReturn(CompletableFuture.completedFuture(new LookupTopicResult(
                        mock(InetSocketAddress.class), mock(InetSocketAddress.class), false)));
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

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoopGroup, pool);
        client.setLookup(lookup);

        List<ConsumerBase<byte[]>> consumers = new ArrayList<>();
        /**
         * {@link org.apache.pulsar.client.impl.PulsarClientImpl#patternTopicSubscribeAsync}
         */
        ConsumerConfigurationData<byte[]> consumerConf0 = new ConsumerConfigurationData<>();
        consumerConf0.setSubscriptionName("test-subscription0");
        consumerConf0.setTopicsPattern(Pattern.compile("test-topic"));
        consumers.add((ConsumerBase<byte[]>) client.subscribeAsync(consumerConf0).get());
        /**
         * {@link org.apache.pulsar.client.impl.PulsarClientImpl#singleTopicSubscribeAsync}
         */
        ConsumerConfigurationData<byte[]> consumerConf1 = new ConsumerConfigurationData<>();
        consumerConf1.setSubscriptionName("test-subscription1");
        consumerConf1.setTopicNames(Collections.singleton("test-topic"));
        consumers.add((ConsumerBase<byte[]>) client.subscribeAsync(consumerConf1).get());
        /**
         * {@link org.apache.pulsar.client.impl.PulsarClientImpl#multiTopicSubscribeAsync}
         */
        ConsumerConfigurationData<byte[]> consumerConf2 = new ConsumerConfigurationData<>();
        consumerConf2.setSubscriptionName("test-subscription2");
        consumers.add((ConsumerBase<byte[]>) client.subscribeAsync(consumerConf2).get());

        consumers.forEach(consumer ->
                assertNotSame(consumer.getState(), HandlerState.State.Closed));
        client.close();
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
    public void testShutdownContinuesWhenAddressResolverCloseFails() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        PulsarClientImpl client = new PulsarClientImpl(conf);

        // Simulate the Netty DNS resolver shutdown race where AddressResolver.close()
        // throws "channel not registered to an event loop".
        @SuppressWarnings("unchecked")
        AddressResolver<InetSocketAddress> failingResolver = mock(AddressResolver.class);
        doThrow(new IllegalStateException("channel not registered to an event loop"))
                .when(failingResolver).close();
        Field addressResolverField = PulsarClientImpl.class.getDeclaredField("addressResolver");
        addressResolverField.setAccessible(true);
        addressResolverField.set(client, failingResolver);

        // Replace the timer with a mock so we can assert that shutdown still reaches the
        // cleanup steps that run after the address resolver is closed.
        HashedWheelTimer timer = mock(HashedWheelTimer.class);
        Field timerField = PulsarClientImpl.class.getDeclaredField("timer");
        timerField.setAccessible(true);
        timerField.set(client, timer);

        // shutdown() still surfaces the original failure, but it must not abort early:
        // the remaining resources have to be released regardless. The key assertion is
        // that timer.stop() (a step that runs after the resolver is closed) is reached.
        assertThrows(IllegalStateException.class, client::shutdown);

        verify(failingResolver).close();
        verify(timer).stop();
    }

    @Test(timeOut = 30_000)
    public void testClosingClientLeavesSharedDnsResolverOpenForOtherClients() throws Exception {
        // A DNS server that never answers: it only shows whether a resolver still sends queries
        @Cleanup
        DatagramSocket dnsServer = new DatagramSocket(0, InetAddress.getLoopbackAddress());
        dnsServer.setSoTimeout(10_000);
        @Cleanup
        PulsarClientSharedResources sharedResources = PulsarClientSharedResources.builder()
                // One event loop, so that both clients get the group's same resolver
                .configureEventLoop(config -> config.numberOfThreads(1))
                .configureDnsResolver(config -> config
                        .serverAddresses(List.of((InetSocketAddress) dnsServer.getLocalSocketAddress()))
                        .searchDomains(List.of())
                        .queryTimeoutMillis(TimeUnit.SECONDS.toMillis(20)))
                .build();
        PulsarClientImpl closedClient = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650").sharedResources(sharedResources).build();
        @Cleanup
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650").sharedResources(sharedResources).build();
        assertSame(client.getAddressResolver(), closedClient.getAddressResolver());

        closedClient.close();

        // A closed resolver would fail the lookup without querying the DNS server
        client.getAddressResolver().resolve(InetSocketAddress.createUnresolved("broker.pulsar.invalid", 6650));
        DatagramPacket query = new DatagramPacket(new byte[512], 512);
        dnsServer.receive(query);
        assertTrue(query.getLength() > 0);
    }

    @Test
    public void testInitializeWithTimer() throws PulsarClientException {
        ClientConfigurationData conf = new ClientConfigurationData();
        @Cleanup("shutdownGracefully")
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("test"));
        ConnectionPool pool = Mockito.spy(new ConnectionPool(InstrumentProvider.NOOP, conf, eventLoop, null));
        conf.setServiceUrl("pulsar://localhost:6650");

        @Cleanup("stop")
        HashedWheelTimer timer = new HashedWheelTimer();
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoop, pool, timer);

        client.shutdown();
    }

    @Test
    public void testInitializeWithDNSServerAddresses() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setDnsServerAddresses(DefaultDnsServerAddressStreamProvider.defaultAddressList());
        conf.setServiceUrl("pulsar://localhost:6650");
        initializeEventLoopGroup(conf);
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoopGroup);
        client.shutdown();
    }

    @Test
    public void testResourceCleanup() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("");
        initializeEventLoopGroup(conf);
        try (ConnectionPool connectionPool = new ConnectionPool(InstrumentProvider.NOOP, conf, eventLoopGroup, null)) {
            assertThrows(() -> new PulsarClientImpl(conf, eventLoopGroup, connectionPool));
        } finally {
            // Externally passed eventLoopGroup should not be shutdown.
            assertFalse(eventLoopGroup.isShutdown());
        }
    }

    @Test
    public void testInitializingWithExecutorProviders() throws PulsarClientException {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        initializeEventLoopGroup(conf);

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
            expectedExceptionsMessageRegExp =
                    "Both externalExecutorProvider and internalExecutorProvider must be specified or unspecified.")
    public void testBothExecutorProvidersMustBeSpecified() throws PulsarClientException {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        initializeEventLoopGroup(conf);
        @Cleanup("shutdownNow")
        ExecutorProvider executorProvider = new ExecutorProvider(2, "shared-executor");
        @Cleanup
        PulsarClientImpl ignore = PulsarClientImpl.builder()
                .conf(conf)
                .internalExecutorProvider(executorProvider)
                .build();
    }
}
