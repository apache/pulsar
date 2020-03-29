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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockBookKeeper;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.NoOpShutdownService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.CompactorSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.compaction.CompactedTopic;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 */
public class PersistentTopicTest {
    private PulsarService pulsar;
    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private ConfigurationCacheService configCacheService;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    final String successPartitionTopicName = "persistent://prop/use/ns-abc/successTopic-partition-0";
    final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    final String successSubName = "successSub";
    final String successSubName2 = "successSub2";
    final String successSubName3 = "successSub3";
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicTest.class);

    private OrderedExecutor executor;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).build();
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        pulsar = spy(new PulsarService(svcConfig));
        pulsar.setShutdownService(new NoOpShutdownService());
        doReturn(svcConfig).when(pulsar).getConfiguration();
        doReturn(mock(Compactor.class)).when(pulsar).getCompactor();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        ZooKeeper mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();
        doReturn(createMockBookKeeper(mockZk, ForkJoinPool.commonPool()))
            .when(pulsar).getBookKeeperClient();

        ZooKeeperCache cache = mock(ZooKeeperCache.class);
        doReturn(30).when(cache).getZkOperationTimeoutSeconds();
        doReturn(cache).when(pulsar).getLocalZkCache();

        configCacheService = mock(ConfigurationCacheService.class);
        @SuppressWarnings("unchecked")
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(Optional.empty()).when(zkDataCache).get(any());

        LocalZooKeeperCacheService zkCache = mock(LocalZooKeeperCacheService.class);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache).getAsync(any());
        doReturn(zkDataCache).when(zkCache).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkCache).when(pulsar).getLocalZkCacheService();
        doReturn(executor).when(pulsar).getOrderedExecutor();

        brokerService = spy(new BrokerService(pulsar));
        doReturn(brokerService).when(pulsar).getBrokerService();

        serverCnx = spy(new ServerCnx(pulsar));
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();

        NamespaceService nsSvc = mock(NamespaceService.class);
        doReturn(nsSvc).when(pulsar).getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any());
        doReturn(true).when(nsSvc).isServiceUnitActive(any());

        setupMLAsyncCallbackMocks();
    }

    @AfterMethod
    public void teardown() throws Exception {
        brokerService.getTopics().clear();
        brokerService.close(); //to clear pulsarStats
        try {
            pulsar.close();
        } catch (Exception e) {
            log.warn("Failed to close pulsar service", e);
            throw e;
        }

        executor.shutdownNow();
    }

    @Test
    public void testCreateTopic() throws Exception {
        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        final String topicName = "persistent://prop/use/ns-abc/topic1";
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class),
                any(Supplier.class), any());

        CompletableFuture<Void> future = brokerService.getOrCreateTopic(topicName).thenAccept(topic -> {
            assertTrue(topic.toString().contains(topicName));
        }).exceptionally((t) -> {
            fail("should not fail");
            return null;
        });

        // wait for completion
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Should not fail or time out");
        }
    }

    @Test
    public void testCreateTopicMLFailure() throws Exception {
        final String jinxedTopicName = "persistent://prop/use/ns-abc/topic3";
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                new Thread(() -> {
                    ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                            .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                }).start();

                return null;
            }
        }).when(mlFactoryMock).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class),
                any(Supplier.class), any());

        CompletableFuture<Topic> future = brokerService.getOrCreateTopic(jinxedTopicName);

        // wait for completion
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("should have failed");
        } catch (TimeoutException e) {
            fail("Should not time out");
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testPublishMessage() throws Exception {

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        /*
         * MessageMetadata.Builder messageMetadata = MessageMetadata.newBuilder();
         * messageMetadata.setPublishTime(System.currentTimeMillis()); messageMetadata.setProducerName("producer-name");
         * messageMetadata.setSequenceId(1);
         */
        ByteBuf payload = Unpooled.wrappedBuffer("content".getBytes());

        final CountDownLatch latch = new CountDownLatch(1);

        topic.publishMessage(payload, (exception, ledgerId, entryId) -> {
            latch.countDown();
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testDispatcherMultiConsumerReadFailed() throws Exception {
        PersistentTopic topic = spy(new PersistentTopic(successTopicName, ledgerMock, brokerService));
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("cursor");
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursor, null);
        dispatcher.readEntriesFailed(new ManagedLedgerException.InvalidCursorPositionException("failed"), null);
        verify(topic, atLeast(1)).getBrokerService();
    }

    @Test
    public void testDispatcherSingleConsumerReadFailed() throws Exception {
        PersistentTopic topic = spy(new PersistentTopic(successTopicName, ledgerMock, brokerService));
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("cursor");
        PersistentDispatcherSingleActiveConsumer dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor,
                SubType.Exclusive, 1, topic, null);
        Consumer consumer = mock(Consumer.class);
        dispatcher.readEntriesFailed(new ManagedLedgerException.InvalidCursorPositionException("failed"), consumer);
        verify(topic, atLeast(1)).getBrokerService();
    }

    @Test
    public void testPublishMessageMLFailure() throws Exception {
        final String successTopicName = "persistent://prop/use/ns-abc/successTopic";

        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        MessageMetadata.Builder messageMetadata = MessageMetadata.newBuilder();
        messageMetadata.setPublishTime(System.currentTimeMillis());
        messageMetadata.setProducerName("prod-name");
        messageMetadata.setSequenceId(1);

        ByteBuf payload = Unpooled.wrappedBuffer("content".getBytes());
        final CountDownLatch latch = new CountDownLatch(1);

        // override asyncAddEntry callback to return error
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addFailed(
                        new ManagedLedgerException("Managed ledger failure"), invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), any());

        topic.publishMessage(payload, (exception, ledgerId, entryId) -> {
            if (exception == null) {
                fail("publish should have failed");
            } else {
                latch.countDown();
            }
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testAddRemoveProducer() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        String role = "appid1";
        // 1. simple add producer
        Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, false);
        topic.addProducer(producer);
        assertEquals(topic.getProducers().size(), 1);

        // 2. duplicate add
        try {
            topic.addProducer(producer);
            fail("Should have failed with naming exception because producer 'null' is already connected to the topic");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.NamingException);
        }
        assertEquals(topic.getProducers().size(), 1);

        // 3. add producer for a different topic
        PersistentTopic failTopic = new PersistentTopic(failTopicName, ledgerMock, brokerService);
        Producer failProducer = new Producer(failTopic, serverCnx, 2 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest,0, false);
        try {
            topic.addProducer(failProducer);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // OK
        }

        // 4. simple remove producer
        topic.removeProducer(producer);
        assertEquals(topic.getProducers().size(), 0);

        // 5. duplicate remove
        topic.removeProducer(producer); /* noop */
    }

    @Test
    public void testProducerOverwrite() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        String role = "appid1";
        Producer producer1 = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, true);
        Producer producer2 = new Producer(topic, serverCnx, 2 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, true);
        try {
            topic.addProducer(producer1);
            topic.addProducer(producer2);
            fail("should have failed");
        } catch (BrokerServiceException.NamingException e) {
            // OK
        }

        Assert.assertEquals(topic.getProducers().size(), 1);

        Producer producer3 = new Producer(topic, serverCnx, 2 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 1, false);

        try {
            topic.addProducer(producer3);
            fail("should have failed");
        } catch (BrokerServiceException.NamingException e) {
            // OK
        }

        Assert.assertEquals(topic.getProducers().size(), 1);

        topic.removeProducer(producer1);
        Assert.assertEquals(topic.getProducers().size(), 0);

        Producer producer4 = new Producer(topic, serverCnx, 2 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 2, false);

        topic.addProducer(producer3);
        topic.addProducer(producer4);

        Assert.assertEquals(topic.getProducers().size(), 1);

        topic.getProducers().values().forEach(producer -> Assert.assertEquals(producer.getEpoch(), 2));

        topic.removeProducer(producer4);
        Assert.assertEquals(topic.getProducers().size(), 0);

        Producer producer5 = new Producer(topic, serverCnx, 2 /* producer id */, "pulsar.repl.cluster1",
                role, false, null, SchemaVersion.Latest, 1, false);

        topic.addProducer(producer5);
        Assert.assertEquals(topic.getProducers().size(), 1);

        Producer producer6 = new Producer(topic, serverCnx, 2 /* producer id */, "pulsar.repl.cluster1",
                role, false, null, SchemaVersion.Latest, 2, false);

        topic.addProducer(producer6);
        Assert.assertEquals(topic.getProducers().size(), 1);

        topic.getProducers().values().forEach(producer -> Assert.assertEquals(producer.getEpoch(), 2));

        Producer producer7 = new Producer(topic, serverCnx, 2 /* producer id */, "pulsar.repl.cluster1",
                role, false, null, SchemaVersion.Latest, 3, true);

        topic.addProducer(producer7);
        Assert.assertEquals(topic.getProducers().size(), 1);
        topic.getProducers().values().forEach(producer -> Assert.assertEquals(producer.getEpoch(), 3));
    }

    public void testMaxProducers() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        String role = "appid1";
        // 1. add producer1
        Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name1", role,
                false, null, SchemaVersion.Latest,0, false);
        topic.addProducer(producer);
        assertEquals(topic.getProducers().size(), 1);

        // 2. add producer2
        Producer producer2 = new Producer(topic, serverCnx, 2 /* producer id */, "prod-name2", role,
                false, null, SchemaVersion.Latest,0, false);
        topic.addProducer(producer2);
        assertEquals(topic.getProducers().size(), 2);

        // 3. add producer3 but reached maxProducersPerTopic
        try {
            Producer producer3 = new Producer(topic, serverCnx, 3 /* producer id */, "prod-name3", role,
                    false, null, SchemaVersion.Latest,0, false);
            topic.addProducer(producer3);
            fail("should have failed");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ProducerBusyException);
        }
    }

    @Test
    public void testMaxProducersForBroker() throws Exception {
        // set max clients
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        doReturn(2).when(svcConfig).getMaxProducersPerTopic();
        doReturn(svcConfig).when(pulsar).getConfiguration();
        testMaxProducers();
    }

    @Test
    public void testMaxProducersForNamespace() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        doReturn(svcConfig).when(pulsar).getConfiguration();
        // set max clients
        Policies policies = new Policies();
        policies.max_producers_per_topic = 2;
        when(pulsar.getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, TopicName.get(successTopicName).getNamespace())))
                .thenReturn(Optional.of(policies));
        testMaxProducers();
    }

    @Test
    public void testSubscribeFail() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        // Empty subscription name
        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription("").setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.getDurable(), null, Collections.emptyMap(), cmd.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false, null);
        try {
            f1.get();
            fail("should fail with exception");
        } catch (ExecutionException ee) {
            // Expected
            assertTrue(ee.getCause() instanceof BrokerServiceException.NamingException);
        }
    }

    @Test
    public void testSubscribeUnsubscribe() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        // 1. simple subscribe
        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.getDurable(), null, Collections.emptyMap(), cmd.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/,false, null);
        f1.get();

        // 2. duplicate subscribe
        Future<Consumer> f2 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.getDurable(), null, Collections.emptyMap(), cmd.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/,false, null);
        try {
            f2.get();
            fail("should fail with exception");
        } catch (ExecutionException ee) {
            // Expected
            assertTrue(ee.getCause() instanceof BrokerServiceException.ConsumerBusyException);
        }

        // 3. simple unsubscribe
        Future<Void> f3 = topic.unsubscribe(successSubName);
        f3.get();

        assertNull(topic.getSubscription(successSubName));
    }

    @Test
    public void testChangeSubscriptionType() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "change-sub-type", cursorMock, false);

        Consumer consumer = new Consumer(sub, SubType.Exclusive, topic.getName(), 1, 0, "Cons1", 50000, serverCnx,
                "myrole-1", Collections.emptyMap(), false, InitialPosition.Latest, null);
        sub.addConsumer(consumer);
        consumer.close();

        SubType previousSubType = SubType.Exclusive;
        for (SubType subType : Lists.newArrayList(SubType.Shared, SubType.Failover, SubType.Key_Shared,
                SubType.Exclusive)) {
            Dispatcher previousDispatcher = sub.getDispatcher();

            consumer = new Consumer(sub, subType, topic.getName(), 1, 0, "Cons1", 50000, serverCnx, "myrole-1",
                    Collections.emptyMap(), false, InitialPosition.Latest, null);
            sub.addConsumer(consumer);

            assertTrue(sub.getDispatcher().isConsumerConnected());
            assertFalse(sub.getDispatcher().isClosed());
            assertEquals(sub.getDispatcher().getType(), subType);

            assertFalse(previousDispatcher.isConsumerConnected());
            assertTrue(previousDispatcher.isClosed());
            assertEquals(previousDispatcher.getType(), previousSubType);

            consumer.close();
            previousSubType = subType;
        }
    }

    @Test
    public void testAddRemoveConsumer() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);

        // 1. simple add consumer
        Consumer consumer = new Consumer(sub, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0, "Cons1"/* consumer name */,
                50000, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, InitialPosition.Latest, null);
        sub.addConsumer(consumer);
        assertTrue(sub.getDispatcher().isConsumerConnected());

        // 2. duplicate add consumer
        try {
            sub.addConsumer(consumer);
            fail("Should fail with ConsumerBusyException");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ConsumerBusyException);
        }

        // 3. simple remove consumer
        sub.removeConsumer(consumer);
        assertFalse(sub.getDispatcher().isConsumerConnected());

        // 4. duplicate remove consumer
        try {
            sub.removeConsumer(consumer);
            fail("Should fail with ServerMetadataException");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ServerMetadataException);
        }
    }

    @Test
    public void testAddRemoveConsumerDurableCursor() throws Exception {
        doReturn(false).when(cursorMock).isDurable();

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "non-durable-sub", cursorMock, false);

        Consumer consumer = new Consumer(sub, SubType.Exclusive, topic.getName(), 1, 0, "Cons1", 50000, serverCnx,
                "myrole-1", Collections.emptyMap(), false, InitialPosition.Latest, null);

        sub.addConsumer(consumer);
        assertFalse(sub.getDispatcher().isClosed());
        sub.removeConsumer(consumer);

        // The dispatcher is closed asynchronously
        for (int i = 0; i < 100; i++) {
            if (sub.getDispatcher().isClosed()) {
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(sub.getDispatcher().isClosed());
    }

    public void testMaxConsumersShared() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        PersistentSubscription sub2 = new PersistentSubscription(topic, "sub-2", cursorMock, false);

        // for count consumers on topic
        ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions = new ConcurrentOpenHashMap<>(16, 1);
        subscriptions.put("sub-1", sub);
        subscriptions.put("sub-2", sub2);
        Field field = topic.getClass().getDeclaredField("subscriptions");
        field.setAccessible(true);
        field.set(topic, subscriptions);

        // 1. add consumer1
        Consumer consumer = new Consumer(sub, SubType.Shared, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null);
        sub.addConsumer(consumer);
        assertEquals(sub.getConsumers().size(), 1);

        // 2. add consumer2
        Consumer consumer2 = new Consumer(sub, SubType.Shared, topic.getName(), 2 /* consumer id */, 0,
                "Cons2"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null);
        sub.addConsumer(consumer2);
        assertEquals(sub.getConsumers().size(), 2);

        // 3. add consumer3 but reach maxConsumersPerSubscription
        try {
            Consumer consumer3 = new Consumer(sub, SubType.Shared, topic.getName(), 3 /* consumer id */, 0,
                    "Cons3"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, InitialPosition.Latest, null);
            sub.addConsumer(consumer3);
            fail("should have failed");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ConsumerBusyException);
        }

        // check number of consumers on topic
        assertEquals(topic.getNumberOfConsumers(), 2);

        // 4. add consumer4 to sub2
        Consumer consumer4 = new Consumer(sub2, SubType.Shared, topic.getName(), 4 /* consumer id */, 0,
                "Cons4"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null);
        sub2.addConsumer(consumer4);
        assertEquals(sub2.getConsumers().size(), 1);

        // check number of consumers on topic
        assertEquals(topic.getNumberOfConsumers(), 3);

        // 5. add consumer5 to sub2 but reach maxConsumersPerTopic
        try {
            Consumer consumer5 = new Consumer(sub2, SubType.Shared, topic.getName(), 5 /* consumer id */, 0,
                    "Cons5"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, InitialPosition.Latest, null);
            sub2.addConsumer(consumer5);
            fail("should have failed");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ConsumerBusyException);
        }
    }

    @Test
    public void testMaxConsumersSharedForBroker() throws Exception {
        // set max clients
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        doReturn(2).when(svcConfig).getMaxConsumersPerSubscription();
        doReturn(3).when(svcConfig).getMaxConsumersPerTopic();
        doReturn(svcConfig).when(pulsar).getConfiguration();

        testMaxConsumersShared();
    }

    @Test
    public void testMaxConsumersSharedForNamespace() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        doReturn(svcConfig).when(pulsar).getConfiguration();

        // set max clients
        Policies policies = new Policies();
        policies.max_consumers_per_subscription = 2;
        policies.max_consumers_per_topic = 3;
        when(pulsar.getConfigurationCache().policiesCache()
                .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(successTopicName).getNamespace())))
                .thenReturn(policies);

        testMaxConsumersShared();
    }

    public void testMaxConsumersFailover() throws Exception {

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        PersistentSubscription sub2 = new PersistentSubscription(topic, "sub-2", cursorMock, false);

        // for count consumers on topic
        ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions = new ConcurrentOpenHashMap<>(16, 1);
        subscriptions.put("sub-1", sub);
        subscriptions.put("sub-2", sub2);
        Field field = topic.getClass().getDeclaredField("subscriptions");
        field.setAccessible(true);
        field.set(topic, subscriptions);

        // 1. add consumer1
        Consumer consumer = new Consumer(sub, SubType.Failover, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null);
        sub.addConsumer(consumer);
        assertEquals(sub.getConsumers().size(), 1);

        // 2. add consumer2
        Consumer consumer2 = new Consumer(sub, SubType.Failover, topic.getName(), 2 /* consumer id */, 0,
                "Cons2"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null);
        sub.addConsumer(consumer2);
        assertEquals(sub.getConsumers().size(), 2);

        // 3. add consumer3 but reach maxConsumersPerSubscription
        try {
            Consumer consumer3 = new Consumer(sub, SubType.Failover, topic.getName(), 3 /* consumer id */, 0,
                    "Cons3"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, InitialPosition.Latest, null);
            sub.addConsumer(consumer3);
            fail("should have failed");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ConsumerBusyException);
        }

        // check number of consumers on topic
        assertEquals(topic.getNumberOfConsumers(), 2);

        // 4. add consumer4 to sub2
        Consumer consumer4 = new Consumer(sub2, SubType.Failover, topic.getName(), 4 /* consumer id */, 0,
                "Cons4"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null);
        sub2.addConsumer(consumer4);
        assertEquals(sub2.getConsumers().size(), 1);

        // check number of consumers on topic
        assertEquals(topic.getNumberOfConsumers(), 3);

        // 5. add consumer5 to sub2 but reach maxConsumersPerTopic
        try {
            Consumer consumer5 = new Consumer(sub2, SubType.Failover, topic.getName(), 5 /* consumer id */, 0,
                    "Cons5"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, InitialPosition.Latest, null);
            sub2.addConsumer(consumer5);
            fail("should have failed");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ConsumerBusyException);
        }
    }

    @Test
    public void testMaxConsumersFailoverForBroker() throws Exception {
        // set max clients
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        doReturn(2).when(svcConfig).getMaxConsumersPerSubscription();
        doReturn(3).when(svcConfig).getMaxConsumersPerTopic();
        doReturn(svcConfig).when(pulsar).getConfiguration();

        testMaxConsumersFailover();
    }

    @Test
    public void testMaxConsumersFailoverForNamespace() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        doReturn(svcConfig).when(pulsar).getConfiguration();

        // set max clients
        Policies policies = new Policies();
        policies.max_consumers_per_subscription = 2;
        policies.max_consumers_per_topic = 3;

        when(pulsar.getConfigurationCache().policiesCache()
                .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(successTopicName).getNamespace())))
                .thenReturn(policies);
        testMaxConsumersFailover();
    }

    @Test
    public void testUbsubscribeRaceConditions() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        Consumer consumer1 = new Consumer(sub, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0, "Cons1"/* consumer name */,
                50000, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, InitialPosition.Latest, null);
        sub.addConsumer(consumer1);

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                Thread.sleep(1000);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            sub.doUnsubscribe(consumer1);
            return null;
        }).get();

        try {
            Thread.sleep(10); /* delay to ensure that the ubsubscribe gets executed first */
            new Consumer(sub, SubType.Exclusive, topic.getName(), 2 /* consumer id */, 0, "Cons2"/* consumer name */,
                    50000, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, InitialPosition.Latest, null);
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.SubscriptionFencedException);
        }
    }

    @Test
    public void testDeleteTopic() throws Exception {
        // create topic
        PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();

        String role = "appid1";
        // 1. delete inactive topic
        topic.delete().get();
        assertFalse(brokerService.getTopicReference(successTopicName).isPresent());

        // 2. delete topic with producer
        topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, false);
        topic.addProducer(producer);

        assertTrue(topic.delete().isCompletedExceptionally());
        topic.removeProducer(producer);

        // 3. delete topic with subscriber
        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.getDurable(), null, Collections.emptyMap(), false /* read compacted */, InitialPosition.Latest,
                0 /*avoid reseting cursor*/,false /* replicated */, null);
        f1.get();

        assertTrue(topic.delete().isCompletedExceptionally());
        topic.unsubscribe(successSubName);
    }

    @Test
    public void testDeleteAndUnsubscribeTopic() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.getDurable(), null, Collections.emptyMap(), cmd.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/,false /* replicated */, null);
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    assertFalse(topic.delete().isCompletedExceptionally());
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread unsubscriber = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();

                    // do topic unsubscribe
                    topic.unsubscribe(successSubName);
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        deleter.start();
        unsubscriber.start();

        counter.await();
        assertFalse(gotException.get());
    }

    // @Test
    public void testConcurrentTopicAndSubscriptionDelete() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.getDurable(), null, Collections.emptyMap(), cmd.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/,false /* replicated */, null);
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    // assertTrue(topic.unsubscribe(successSubName).isDone());
                    Thread.sleep(5, 0);
                    log.info("deleter outcome is {}", topic.delete().get());
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread unsubscriber = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    // do subscription delete
                    ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions = topic.getSubscriptions();
                    PersistentSubscription ps = subscriptions.get(successSubName);
                    // Thread.sleep(5,0);
                    log.info("unsubscriber outcome is {}", ps.doUnsubscribe(ps.getConsumers().get(0)).get());
                    // assertFalse(ps.delete().isCompletedExceptionally());
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        deleter.start();
        unsubscriber.start();

        counter.await();
        assertFalse(gotException.get());
    }

    @Test
    public void testDeleteTopicRaceConditions() throws Exception {
        PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();

        // override ledger deletion callback to slow down deletion
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(1000);
                ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), any());

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            topic.delete();
            return null;
        }).get();

        try {
            String role = "appid1";
            Thread.sleep(10); /* delay to ensure that the delete gets executed first */
            Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                    role, false, null, SchemaVersion.Latest, 0, false);
            topic.addProducer(producer);
            fail("Should have failed");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.TopicFencedException);
        }

        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.getDurable(), null, Collections.emptyMap(), cmd.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/,false /* replicated */, null);
        try {
            f.get();
            fail("should have failed");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof BrokerServiceException.TopicFencedException);
            // Expected
        }
    }

    @SuppressWarnings("unchecked")
    void setupMLAsyncCallbackMocks() {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        doReturn(true).when(cursorMock).isDurable();
        // doNothing().when(cursorMock).asyncClose(new CloseCallback() {
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                // return closeFuture.get();
                return closeFuture.complete(null);
            }
        })

                .when(cursorMock).asyncClose(new CloseCallback() {

                    @Override
                    public void closeComplete(Object ctx) {
                        log.info("[{}] Successfully closed cursor ledger", "mockCursor");
                        closeFuture.complete(null);
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        // isFenced.set(false);

                        log.error("Error closing cursor for subscription", exception);
                        closeFuture.completeExceptionally(new BrokerServiceException.PersistenceException(exception));
                    }
                }, null);

        // call openLedgerComplete with ledgerMock on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(1, 1),
                        invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), any());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[3]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(Map.class),
                any(OpenCursorCallback.class), any());

        // call deleteLedgerComplete on ledger asyncDelete
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());

        doAnswer((invokactionOnMock) -> {
                ((MarkDeleteCallback) invokactionOnMock.getArguments()[2])
                    .markDeleteComplete(invokactionOnMock.getArguments()[3]);
                return null;
            }).when(cursorMock).asyncMarkDelete(any(), any(), any(MarkDeleteCallback.class), any());
    }

    @Test
    public void testFailoverSubscription() throws Exception {
        PersistentTopic topic1 = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd1 = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Failover).build();

        // 1. Subscribe with non partition topic
        Future<Consumer> f1 = topic1.subscribe(serverCnx, cmd1.getSubscription(), cmd1.getConsumerId(),
                cmd1.getSubType(), 0, cmd1.getConsumerName(), cmd1.getDurable(), null, Collections.emptyMap(),
                cmd1.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/,false /* replicated */, null);
        f1.get();

        // 2. Subscribe with partition topic
        PersistentTopic topic2 = new PersistentTopic(successPartitionTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd2 = CommandSubscribe.newBuilder().setConsumerId(1).setConsumerName("C1")
                .setTopic(successPartitionTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(SubType.Failover).build();

        Future<Consumer> f2 = topic2.subscribe(serverCnx, cmd2.getSubscription(), cmd2.getConsumerId(),
                cmd2.getSubType(), 0, cmd2.getConsumerName(), cmd2.getDurable(), null, Collections.emptyMap(),
                cmd2.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false /* replicated */, null);
        f2.get();

        // 3. Subscribe and create second consumer
        CommandSubscribe cmd3 = CommandSubscribe.newBuilder().setConsumerId(2).setConsumerName("C2")
                .setTopic(successPartitionTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(SubType.Failover).build();

        Future<Consumer> f3 = topic2.subscribe(serverCnx, cmd3.getSubscription(), cmd3.getConsumerId(),
                cmd3.getSubType(), 0, cmd3.getConsumerName(), cmd3.getDurable(), null, Collections.emptyMap(),
                cmd3.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false /* replicated */, null);
        f3.get();

        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerId(), 1);
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerName(),
                "C1");
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(1).consumerId(), 2);
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(1).consumerName(),
                "C2");

        // 4. Subscribe and create third duplicate consumer
        CommandSubscribe cmd4 = CommandSubscribe.newBuilder().setConsumerId(3).setConsumerName("C1")
                .setTopic(successPartitionTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(SubType.Failover).build();

        Future<Consumer> f4 = topic2.subscribe(serverCnx, cmd4.getSubscription(), cmd4.getConsumerId(),
                cmd4.getSubType(), 0, cmd4.getConsumerName(), cmd4.getDurable(), null, Collections.emptyMap(),
                cmd4.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false /* replicated */, null);
        f4.get();

        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerId(), 1);
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerName(),
                "C1");
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(1).consumerId(), 3);
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(1).consumerName(),
                "C1");
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(2).consumerId(), 2);
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(2).consumerName(),
                "C2");

        // 5. Subscribe on partition topic with existing consumer id and different sub type
        CommandSubscribe cmd5 = CommandSubscribe.newBuilder().setConsumerId(2).setConsumerName("C1")
                .setTopic(successPartitionTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(SubType.Exclusive).build();

        Future<Consumer> f5 = topic2.subscribe(serverCnx, cmd5.getSubscription(), cmd5.getConsumerId(),
                cmd5.getSubType(), 0, cmd5.getConsumerName(), cmd5.getDurable(), null, Collections.emptyMap(),
                cmd5.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/,false /* replicated */, null);
        try {
            f5.get();
            fail("should fail with exception");
        } catch (ExecutionException ee) {
            // Expected
            assertTrue(ee.getCause() instanceof BrokerServiceException.SubscriptionBusyException);
        }

        // 6. Subscribe on partition topic with different sub name, type and different consumer id
        CommandSubscribe cmd6 = CommandSubscribe.newBuilder().setConsumerId(4).setConsumerName("C3")
                .setTopic(successPartitionTopicName).setSubscription(successSubName2).setRequestId(1)
                .setSubType(SubType.Exclusive).build();

        Future<Consumer> f6 = topic2.subscribe(serverCnx, cmd6.getSubscription(), cmd6.getConsumerId(),
                cmd6.getSubType(), 0, cmd6.getConsumerName(), cmd6.getDurable(), null, Collections.emptyMap(),
                cmd6.getReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false /* replicated */, null);
        f6.get();

        // 7. unsubscribe exclusive sub
        Future<Void> f7 = topic2.unsubscribe(successSubName2);
        f7.get();

        assertNull(topic2.getSubscription(successSubName2));

        // 8. unsubscribe active consumer from shared sub.
        PersistentSubscription sub = topic2.getSubscription(successSubName);
        Consumer cons = sub.getDispatcher().getConsumers().get(0);
        sub.removeConsumer(cons);

        // Verify second consumer become active
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerId(), 3);
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerName(),
                "C1");

        // 9. unsubscribe active consumer from shared sub.
        cons = sub.getDispatcher().getConsumers().get(0);
        sub.removeConsumer(cons);

        // Verify second consumer become active
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerId(), 2);
        assertEquals(
                topic2.getSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerName(),
                "C2");

        // 10. unsubscribe shared sub
        Future<Void> f8 = topic2.unsubscribe(successSubName);
        f8.get();

        assertNull(topic2.getSubscription(successSubName));
    }

    /**
     * {@link NonPersistentReplicator.removeReplicator} doesn't remove replicator in atomic way and does in multiple step:
     * 1. disconnect replicator producer
     * <p>
     * 2. close cursor
     * <p>
     * 3. remove from replicator-list.
     * <p>
     *
     * If we try to startReplicationProducer before step-c finish then it should not avoid restarting repl-producer.
     *
     * @throws Exception
     */
    @Test
    public void testAtomicReplicationRemoval() throws Exception {
        final String globalTopicName = "persistent://prop/global/ns-abc/successTopic";
        String localCluster = "local";
        String remoteCluster = "remote";
        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doNothing().when(ledgerMock).asyncDeleteCursor(any(), any(), any());
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        PersistentTopic topic = new PersistentTopic(globalTopicName, ledgerMock, brokerService);
        String remoteReplicatorName = topic.getReplicatorPrefix() + "." + remoteCluster;
        ConcurrentOpenHashMap<String, Replicator> replicatorMap = topic.getReplicators();

        final URL brokerUrl = new URL(
                "http://" + pulsar.getAdvertisedAddress() + ":" + pulsar.getConfiguration().getBrokerServicePort().get());
        PulsarClient client = PulsarClient.builder().serviceUrl(brokerUrl.toString()).build();
        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        doReturn(remoteCluster).when(cursor).getName();
        brokerService.getReplicationClients().put(remoteCluster, client);
        PersistentReplicator replicator = spy(
                new PersistentReplicator(topic, cursor, localCluster, remoteCluster, brokerService));
        replicatorMap.put(remoteReplicatorName, replicator);

        // step-1 remove replicator : it will disconnect the producer but it will wait for callback to be completed
        Method removeMethod = PersistentTopic.class.getDeclaredMethod("removeReplicator", String.class);
        removeMethod.setAccessible(true);
        removeMethod.invoke(topic, remoteReplicatorName);

        // step-2 now, policies doesn't have removed replication cluster so, it should not invoke "startProducer" of the
        // replicator
        when(pulsar.getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, TopicName.get(globalTopicName).getNamespace())))
                        .thenReturn(Optional.of(new Policies()));
        // try to start replicator again
        topic.startReplProducers();
        // verify: replicator.startProducer is not invoked
        verify(replicator, Mockito.times(0)).startProducer();

        // step-3 : complete the callback to remove replicator from the list
        ArgumentCaptor<DeleteCursorCallback> captor = ArgumentCaptor.forClass(DeleteCursorCallback.class);
        Mockito.verify(ledgerMock).asyncDeleteCursor(any(), captor.capture(), any());
        DeleteCursorCallback callback = captor.getValue();
        callback.deleteCursorComplete(null);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testClosingReplicationProducerTwice() throws Exception {
        final String globalTopicName = "persistent://prop/global/ns/testClosingReplicationProducerTwice";
        String localCluster = "local";
        String remoteCluster = "remote";
        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doNothing().when(ledgerMock).asyncDeleteCursor(any(), any(), any());
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        PersistentTopic topic = new PersistentTopic(globalTopicName, ledgerMock, brokerService);

        final URL brokerUrl = new URL(
                "http://" + pulsar.getAdvertisedAddress() + ":" + pulsar.getConfiguration().getBrokerServicePort().get());
        PulsarClient client = spy(PulsarClient.builder().serviceUrl(brokerUrl.toString()).build());
        PulsarClientImpl clientImpl = (PulsarClientImpl) client;
        doReturn(new CompletableFuture<Producer>()).when(clientImpl)
            .createProducerAsync(any(ProducerConfigurationData.class), any(Schema.class));

        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        doReturn(remoteCluster).when(cursor).getName();
        brokerService.getReplicationClients().put(remoteCluster, client);
        PersistentReplicator replicator = new PersistentReplicator(topic, cursor, localCluster, remoteCluster, brokerService);

        // PersistentReplicator constructor calls startProducer()
        verify(clientImpl)
            .createProducerAsync(
                any(ProducerConfigurationData.class),
                any(), eq(null)
            );

        replicator.disconnect(false);
        replicator.disconnect(false);

        replicator.startProducer();

        verify(clientImpl, Mockito.times(2)).createProducerAsync(any(), any(), any());
    }

    @Test
    public void testCompactorSubscription() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        CompactedTopic compactedTopic = mock(CompactedTopic.class);
        PersistentSubscription sub = new CompactorSubscription(topic, compactedTopic,
                                                               Compactor.COMPACTION_SUBSCRIPTION,
                                                               cursorMock);
        PositionImpl position = new PositionImpl(1, 1);
        long ledgerId = 0xc0bfefeL;
        sub.acknowledgeMessage(Collections.singletonList(position), AckType.Cumulative,
                ImmutableMap.of(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, ledgerId));
        verify(compactedTopic, Mockito.times(1)).newCompactedLedger(position, ledgerId);
    }


    @Test
    public void testCompactorSubscriptionUpdatedOnInit() throws Exception {
        long ledgerId = 0xc0bfefeL;
        Map<String, Long> properties = ImmutableMap.of(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, ledgerId);
        PositionImpl position = new PositionImpl(1, 1);

        doAnswer((invokactionOnMock) -> properties).when(cursorMock).getProperties();
        doAnswer((invokactionOnMock) -> position).when(cursorMock).getMarkDeletedPosition();

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        CompactedTopic compactedTopic = mock(CompactedTopic.class);
        new CompactorSubscription(topic, compactedTopic, Compactor.COMPACTION_SUBSCRIPTION, cursorMock);
        verify(compactedTopic, Mockito.times(1)).newCompactedLedger(position, ledgerId);
    }

    @Test
    public void testCompactionTriggeredAfterThresholdFirstInvocation() throws Exception {
        CompletableFuture<Long> compactPromise = new CompletableFuture<>();
        Compactor compactor = pulsar.getCompactor();
        doReturn(compactPromise).when(compactor).compact(anyString());

        Policies policies = new Policies();
        policies.compaction_threshold = 1;
        when(pulsar.getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, TopicName.get(successTopicName).getNamespace())))
                .thenReturn(Optional.of(policies));

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        topic.checkCompaction();

        verify(compactor, times(0)).compact(anyString());

        doReturn(10L).when(ledgerMock).getEstimatedBacklogSize();

        topic.checkCompaction();
        verify(compactor, times(1)).compact(anyString());

        // run a second time, shouldn't run again because already running
        topic.checkCompaction();
        verify(compactor, times(1)).compact(anyString());
    }

    @Test
    public void testCompactionTriggeredAfterThresholdSecondInvocation() throws Exception {
        CompletableFuture<Long> compactPromise = new CompletableFuture<>();
        Compactor compactor = pulsar.getCompactor();
        doReturn(compactPromise).when(compactor).compact(anyString());

        ManagedCursor subCursor = mock(ManagedCursor.class);
        doReturn(Lists.newArrayList(subCursor)).when(ledgerMock).getCursors();
        doReturn(Compactor.COMPACTION_SUBSCRIPTION).when(subCursor).getName();

        Policies policies = new Policies();
        policies.compaction_threshold = 1;
        when(pulsar.getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, TopicName.get(successTopicName).getNamespace())))
                .thenReturn(Optional.of(policies));

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        topic.checkCompaction();

        verify(compactor, times(0)).compact(anyString());

        doReturn(10L).when(subCursor).getEstimatedSizeSinceMarkDeletePosition();

        topic.checkCompaction();
        verify(compactor, times(1)).compact(anyString());

        // run a second time, shouldn't run again because already running
        topic.checkCompaction();
        verify(compactor, times(1)).compact(anyString());
    }

    @Test
    public void testCompactionDisabledWithZeroThreshold() throws Exception {
        CompletableFuture<Long> compactPromise = new CompletableFuture<>();
        Compactor compactor = pulsar.getCompactor();
        doReturn(compactPromise).when(compactor).compact(anyString());

        Policies policies = new Policies();
        policies.compaction_threshold = 0;
        when(pulsar.getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, TopicName.get(successTopicName).getNamespace())))
                .thenReturn(Optional.of(policies));

        doReturn(1000L).when(ledgerMock).getEstimatedBacklogSize();

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.checkCompaction();
        verify(compactor, times(0)).compact(anyString());
    }
}
