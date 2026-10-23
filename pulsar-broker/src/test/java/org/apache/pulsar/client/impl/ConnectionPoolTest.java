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

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AbstractAddressResolver;
import io.netty.resolver.AddressResolver;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConnectionPoolTest extends MockedPulsarServiceBaseTest {

    String serviceUrl;
    int brokerPort;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
        brokerPort = pulsar.getBrokerListenPort().get();
        serviceUrl = "pulsar://non-existing-dns-name:" + brokerPort;
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSingleIpAddress() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("test"));
        @Cleanup("shutdownNow")
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("test-pulsar-client-scheduled"));
        ConnectionPool pool =
                spyWithClassAndConstructorArgs(ConnectionPool.class, InstrumentProvider.NOOP, conf, eventLoop,
                        scheduledExecutorService);
        conf.setServiceUrl(serviceUrl);
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoop, pool);

        List<InetSocketAddress> result = new ArrayList<>();
        result.add(new InetSocketAddress("127.0.0.1", brokerPort));
        Mockito.when(pool.resolveName(InetSocketAddress.createUnresolved("non-existing-dns-name",
                        brokerPort)))
                .thenReturn(CompletableFuture.completedFuture(result));

        client.newProducer().topic("persistent://public/default/my-topic").create();

        client.close();
        eventLoop.shutdownGracefully();
    }

    @Test
    public void testSelectConnectionForSameProducer() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        final CommandCloseProducer commandCloseProducer = new CommandCloseProducer();
        // 10 connection per broker.
        final PulsarClient clientWith10ConPerBroker = PulsarClient.builder().connectionsPerBroker(10)
                        .serviceUrl(lookupUrl.toString()).build();
        ProducerImpl producer = (ProducerImpl) clientWith10ConPerBroker.newProducer().topic(topicName).create();
        commandCloseProducer.setProducerId(producer.producerId);
        // An error will be reported when the Producer reconnects using a different connection.
        // If no error is reported, the same connection was used when reconnecting.
        for (int i = 0; i < 20; i++) {
            // Trigger reconnect
            ClientCnx cnx = producer.getClientCnx();
            if (cnx != null) {
                cnx.handleCloseProducer(commandCloseProducer);
                Awaitility.await().untilAsserted(() ->
                        Assert.assertEquals(producer.getState().toString(), HandlerState.State.Ready.toString(),
                                "The producer uses a different connection when reconnecting")
                );
            }
        }

        // cleanup.
        producer.close();
        clientWith10ConPerBroker.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testDoubleIpAddress() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        @Cleanup("shutdownNow")
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("test-pulsar-client-scheduled"));
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("test"));
        ConnectionPool pool = spyWithClassAndConstructorArgs(ConnectionPool.class, InstrumentProvider.NOOP, conf,
                eventLoop, scheduledExecutorService);
        conf.setServiceUrl(serviceUrl);
        PulsarClientImpl client = new PulsarClientImpl(conf, eventLoop, pool);

        List<InetSocketAddress> result = new ArrayList<>();

        // Add a non existent IP to the response to check that we're trying the 2nd address as well
        result.add(new InetSocketAddress("127.0.0.99", brokerPort));
        result.add(new InetSocketAddress("127.0.0.1", brokerPort));
        Mockito.when(pool.resolveName(InetSocketAddress.createUnresolved("non-existing-dns-name",
                        brokerPort)))
                .thenReturn(CompletableFuture.completedFuture(result));

        // Create producer should succeed by trying the 2nd IP
        client.newProducer().topic("persistent://public/default/my-topic").create();
        client.close();

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testNoConnectionPool() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setConnectionsPerBroker(0);
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(8, false, new DefaultThreadFactory("test"));
        @Cleanup("shutdownNow")
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("test-pulsar-client-scheduled"));
        ConnectionPool pool =
                spyWithClassAndConstructorArgs(ConnectionPool.class, InstrumentProvider.NOOP, conf, eventLoop,
                        scheduledExecutorService);

        InetSocketAddress brokerAddress =
                InetSocketAddress.createUnresolved("127.0.0.1", brokerPort);
        IntStream.range(1, 5).forEach(i -> {
            pool.getConnection(brokerAddress).thenAccept(cnx -> {
                Assert.assertTrue(cnx.channel().isActive());
                pool.releaseConnection(cnx);
                Assert.assertTrue(cnx.channel().isActive());
            });
        });
        Assert.assertEquals(pool.getPoolSize(), 0);

        pool.closeAllConnections();
        pool.close();
        eventLoop.shutdownGracefully();
    }

    @DataProvider(name = "partitionMetadataFeatureCheck")
    public Object[][] partitionMetadataFeatureCheck() {
        return new Object[][]{
                {0, false, false},
                {1, false, false},
                {0, false, true},
                {0, true, false}
        };
    }

    @Test(dataProvider = "partitionMetadataFeatureCheck", timeOut = 30_000)
    public void testPartitionMetadataFeatureCheckReleasesConnection(int connectionsPerBroker,
            boolean metadataAutoCreationEnabled, boolean useFallback) throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/partition-metadata");
        admin.topics().createPartitionedTopic(topic, 2);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(pulsar.getBrokerServiceUrl());
        conf.setConnectionsPerBroker(connectionsPerBroker);
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("partition-metadata-test"));
        @Cleanup("shutdownNow")
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("partition-metadata-test-scheduled"));
        AtomicReference<ClientCnx> connection = new AtomicReference<>();
        AtomicInteger lookupRequests = new AtomicInteger();
        try (ConnectionPool pool = new ConnectionPool(InstrumentProvider.NOOP, conf, eventLoop, () -> {
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop) {
                @Override
                protected void handleConnected(CommandConnected connected) {
                    // Simulate an old broker's feature flag without bypassing the TCP handshake.
                    connected.getFeatureFlags().setSupportsGetPartitionedMetadataWithoutAutoCreation(false);
                    super.handleConnected(connected);
                }

                @Override
                public CompletableFuture<LookupDataResult> newLookup(ByteBuf request, long requestId) {
                    lookupRequests.incrementAndGet();
                    return super.newLookup(request, requestId);
                }
            };
            connection.set(cnx);
            return cnx;
        }, scheduler);
             PulsarClientImpl client = new PulsarClientImpl(conf, eventLoop, pool)) {
            CompletableFuture<PartitionedTopicMetadata> metadata =
                    client.getPartitionedTopicMetadata(topic, metadataAutoCreationEnabled, useFallback);
            if (!metadataAutoCreationEnabled && !useFallback) {
                ExecutionException error = Assert.expectThrows(ExecutionException.class,
                        () -> metadata.get(5, TimeUnit.SECONDS));
                Throwable cause = FutureUtil.unwrapCompletionException(error);
                Assert.assertTrue(cause instanceof PulsarClientException.FeatureNotSupportedException);
                Assert.assertEquals(
                        ((PulsarClientException.FeatureNotSupportedException) cause).getFailedFeatureCheck(),
                        PulsarClientException.FailedFeatureCheck.SupportsGetPartitionedMetadataWithoutAutoCreation);
                Assert.assertEquals(lookupRequests.get(), 0);
            } else {
                Assert.assertEquals(metadata.get(5, TimeUnit.SECONDS).partitions, 2);
                Assert.assertEquals(lookupRequests.get(), 1);
            }
            ClientCnx cnx = connection.get();
            Assert.assertNotNull(cnx);
            Assert.assertFalse(cnx.isSupportsGetPartitionedMetadataWithoutAutoCreation());
            if (connectionsPerBroker == 0) {
                Assert.assertTrue(cnx.channel().closeFuture().await(5, TimeUnit.SECONDS),
                        "The unpooled lookup connection must be closed even when the feature check rejects it");
            } else {
                cnx.channel().eventLoop().submit(() -> { }).get(5, TimeUnit.SECONDS);
                Assert.assertTrue(cnx.channel().isActive(), "The pooled connection must remain reusable");
                Assert.assertSame(pool.getConnection(client.getLookup().resolveHost())
                        .get(5, TimeUnit.SECONDS), cnx);
            }
        } finally {
            eventLoop.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            admin.topics().deletePartitionedTopic(topic, true);
        }
    }

    @Test
    public void testEnableConnectionPool() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setConnectionsPerBroker(5);
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(8, false, new DefaultThreadFactory("test"));
        @Cleanup("shutdownNow")
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("test-pulsar-client-scheduled"));
        ConnectionPool pool =
                spyWithClassAndConstructorArgs(ConnectionPool.class, InstrumentProvider.NOOP, conf, eventLoop,
                        scheduledExecutorService);

        InetSocketAddress brokerAddress =
                InetSocketAddress.createUnresolved("127.0.0.1", brokerPort);
        IntStream.range(1, 10).forEach(i -> {
            pool.getConnection(brokerAddress).thenAccept(cnx -> {
                Assert.assertTrue(cnx.channel().isActive());
                pool.releaseConnection(cnx);
                Assert.assertTrue(cnx.channel().isActive());
            });
        });
        Assert.assertTrue(pool.getPoolSize() <= 5 && pool.getPoolSize() > 0);

        pool.closeAllConnections();
        pool.close();
        eventLoop.shutdownGracefully();
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testSetProxyToTargetBrokerAddress() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setConnectionsPerBroker(1);
        @Cleanup("shutdownNow")
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("test-pulsar-client-scheduled"));
        EventLoopGroup eventLoop =
                EventLoopUtil.newEventLoopGroup(8, false,
                        new DefaultThreadFactory("test"));

        final AbstractAddressResolver resolver = new AbstractAddressResolver(eventLoop.next()) {
            @Override
            protected boolean doIsResolved(SocketAddress socketAddress) {
                return !((InetSocketAddress) socketAddress).isUnresolved();
            }

            @Override
            protected void doResolve(SocketAddress socketAddress, Promise promise) throws Exception {
                promise.setFailure(new IllegalStateException());
                throw new IllegalStateException();
            }

            @Override
            @SuppressWarnings("unchecked")
            protected void doResolveAll(SocketAddress socketAddress, Promise promise) throws Exception {
                final InetSocketAddress socketAddress1 = (InetSocketAddress) socketAddress;
                String hostName = socketAddress1.getHostName();
                final boolean isProxy = hostName.equals("proxy");
                @SuppressWarnings("unchecked")
                final boolean isBroker = hostName.startsWith("broker");
                if (!isProxy && !isBroker) {
                    promise.setFailure(new IllegalStateException());
                    throw new IllegalStateException();
                }
                List<InetSocketAddress> result = new ArrayList<>();
                // All 127.0.0.0/8 addresses are valid local loopback addresses
                if (isProxy) {
                    result.add(new InetSocketAddress("127.0.0.101", brokerPort));
                    result.add(new InetSocketAddress("127.0.0.102", brokerPort));
                } else if (hostName.equals("broker1")){
                    result.add(new InetSocketAddress("127.0.0.103", brokerPort));
                    result.add(new InetSocketAddress("127.0.0.104", brokerPort));
                } else if (hostName.equals("broker2")){
                    result.add(new InetSocketAddress("127.0.0.105", brokerPort));
                    result.add(new InetSocketAddress("127.0.0.106", brokerPort));
                }
                promise.setSuccess(result);
            }
        };

        ConnectionPool pool =
                spyWithClassAndConstructorArgs(ConnectionPool.class, InstrumentProvider.NOOP, conf, eventLoop,
                        (Supplier<ClientCnx>) () -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop),
                        Optional.<Supplier<AddressResolver<InetSocketAddress>>>of(() -> resolver),
                        scheduledExecutorService);


        ClientCnx cnx = pool.getConnection(
                InetSocketAddress.createUnresolved("proxy", 9999),
                InetSocketAddress.createUnresolved("proxy", 9999),
                pool.genRandomKeyToSelectCon()).get();
        Assert.assertEquals(cnx.remoteHostName, "proxy");
        Assert.assertNull(cnx.proxyToTargetBrokerAddress);

        cnx = pool.getConnection(
                InetSocketAddress.createUnresolved("broker1", 9999),
                InetSocketAddress.createUnresolved("proxy", 9999),
                pool.genRandomKeyToSelectCon()).get();
        Assert.assertEquals(cnx.remoteHostName, "proxy");
        Assert.assertEquals(cnx.proxyToTargetBrokerAddress, "broker1:9999");


        cnx = pool.getConnection(
                InetSocketAddress.createUnresolved("broker2", 9999),
                InetSocketAddress.createUnresolved("broker2", 9999),
                pool.genRandomKeyToSelectCon()).get();
        Assert.assertEquals(cnx.remoteHostName, "broker2");
        Assert.assertNull(cnx.proxyToTargetBrokerAddress);

        pool.closeAllConnections();
        pool.close();
        eventLoop.shutdownGracefully();
    }
}
