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

package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoop;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.TopicExistsInfo;
import org.apache.pulsar.broker.service.persistent.CompactorSubscription;
import org.apache.pulsar.broker.service.persistent.GeoPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.compaction.CompactedTopic;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.CompactorMXBean;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentTopicTest extends MockedBookKeeperTestCase {
    private ServerCnx serverCnx;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    final String successPartitionTopicName = "persistent://prop/use/ns-abc/successTopic-partition-0";
    final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    final String successSubName = "successSub";
    final String successSubName2 = "successSub2";
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicTest.class);

    protected PulsarTestContext pulsarTestContext;

    private BrokerService brokerService;

    private ChannelHandlerContext ctx;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = new ServiceConfiguration();
        svcConfig.setAdvertisedAddress("localhost");
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setMaxUnackedMessagesPerConsumer(50000);
        svcConfig.setClusterName("pulsar-cluster");
        svcConfig.setTopicLevelPoliciesEnabled(false);
        svcConfig.setSystemTopicEnabled(false);
        Compactor compactor = mock(Compactor.class);
        when(compactor.getStats()).thenReturn(mock(CompactorMXBean.class));
        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .spyByDefault()
                .useTestPulsarResources(metadataStore)
                .compactor(compactor)
                .build();
        brokerService = pulsarTestContext.getBrokerService();

        doAnswer(invocationOnMock -> CompletableFuture.completedFuture(null))
                .when(pulsarTestContext.getManagedLedgerFactory()).getManagedLedgerPropertiesAsync(any());
        doAnswer(invocation -> {
            DeleteLedgerCallback deleteLedgerCallback = invocation.getArgument(1);
            deleteLedgerCallback.deleteLedgerComplete(null);
            return null;
        }).when(pulsarTestContext.getManagedLedgerFactory()).asyncDelete(any(), any(), any());
        // Mock serviceCnx.
        serverCnx = spyWithClassAndConstructorArgsRecordingInvocations(ServerCnx.class,
                pulsarTestContext.getPulsarService());
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();
        doReturn(new PulsarCommandSenderImpl(null, serverCnx))
                .when(serverCnx).getCommandSender();
        ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        doReturn(spy(DefaultEventLoop.class)).when(channel).eventLoop();
        doReturn(channel).when(ctx).channel();
        doReturn(ctx).when(serverCnx).ctx();
        doReturn(CompletableFuture.completedFuture(true)).when(serverCnx).checkConnectionLiveness();

        NamespaceService nsSvc = mock(NamespaceService.class);
        NamespaceBundle bundle = mock(NamespaceBundle.class);
        doReturn(CompletableFuture.completedFuture(bundle)).when(nsSvc).getBundleAsync(any());
        doReturn(nsSvc).when(pulsarTestContext.getPulsarService()).getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any());
        doReturn(true).when(nsSvc).isServiceUnitActive(any());
        doReturn(CompletableFuture.completedFuture(true)).when(nsSvc).isServiceUnitActiveAsync(any());
        doReturn(CompletableFuture.completedFuture(mock(NamespaceBundle.class))).when(nsSvc).getBundleAsync(any());
        doReturn(CompletableFuture.completedFuture(true)).when(nsSvc).checkBundleOwnership(any(), any());
        doReturn(CompletableFuture.completedFuture(TopicExistsInfo.newNonPartitionedTopicExists())).when(nsSvc)
                .checkTopicExistsAsync(any());
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        LookupService lookupService = mock(LookupService.class);
        doReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(0))).when(lookupService)
                .getPartitionedTopicMetadata(any(), anyBoolean(), anyBoolean());
        doReturn(lookupService).when(pulsarClient).getLookup();
        doReturn(pulsarClient).when(pulsarTestContext.getPulsarService()).getClient();

        setupMLAsyncCallbackMocks();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }
    }

    @Test
    public void testCreateTopic() {
        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doReturn(new ManagedLedgerConfig()).when(ledgerMock).getConfig();
        doReturn(new ArrayList<>()).when(ledgerMock).getCursors();

        final String topicName = "persistent://prop/use/ns-abc/topic1";
        doAnswer(invocationOnMock -> {
            ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
            return null;
        }).when(pulsarTestContext.getManagedLedgerFactory())
                .asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class),
                        any(Supplier.class), any());

        CompletableFuture<Void> future = brokerService.getOrCreateTopic(topicName).thenAccept(topic ->
                assertTrue(topic.toString().contains(topicName))).exceptionally((t) -> {
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
    public void testCreateTopicMLFailure() {
        final String jinxedTopicName = "persistent://prop/use/ns-abc/topic3";
        doAnswer(invocationOnMock -> {
            new Thread(() -> ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                    .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null)).start();

            return null;
        }).when(pulsarTestContext.getManagedLedgerFactory())
                .asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class),
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

        doAnswer(invocationOnMock -> {
            final ByteBuf payload = (ByteBuf) invocationOnMock.getArguments()[0];
            final AddEntryCallback callback = (AddEntryCallback) invocationOnMock.getArguments()[1];
            final Topic.PublishContext ctx = (Topic.PublishContext) invocationOnMock.getArguments()[2];
            callback.addComplete(PositionImpl.LATEST, payload, ctx);
            return null;
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), any());

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        long lastMaxReadPositionMovedForwardTimestamp = topic.getLastMaxReadPositionMovedForwardTimestamp();

        /*
         * MessageMetadata.Builder messageMetadata = MessageMetadata.newBuilder();
         * messageMetadata.setPublishTime(System.currentTimeMillis()); messageMetadata.setProducerName("producer-name");
         * messageMetadata.setSequenceId(1);
         */
        ByteBuf payload = Unpooled.wrappedBuffer("content".getBytes());

        final CountDownLatch latch = new CountDownLatch(1);

        final Topic.PublishContext publishContext = new Topic.PublishContext() {
            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                assertEquals(ledgerId, PositionImpl.LATEST.getLedgerId());
                assertEquals(entryId, PositionImpl.LATEST.getEntryId());
                latch.countDown();
            }

            @Override
            public void setMetadataFromEntryData(ByteBuf entryData) {
                // This method must be invoked before `completed`
                assertEquals(latch.getCount(), 1);
                assertEquals(entryData.array(), payload.array());
            }
        };
        topic.publishMessage(payload, publishContext);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(topic.getLastMaxReadPositionMovedForwardTimestamp() > lastMaxReadPositionMovedForwardTimestamp);
    }

    @Test
    public void testDispatcherMultiConsumerReadFailed() {
        PersistentTopic topic =
                spyWithClassAndConstructorArgsRecordingInvocations(PersistentTopic.class, successTopicName, ledgerMock,
                        brokerService);
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("cursor");
        Subscription subscription = mock(Subscription.class);
        when(subscription.getName()).thenReturn("sub");
        PersistentDispatcherMultipleConsumers dispatcher =
                new PersistentDispatcherMultipleConsumers(topic, cursor, subscription);
        dispatcher.readEntriesFailed(new ManagedLedgerException.InvalidCursorPositionException("failed"), null);
        verify(topic, atLeast(1)).getBrokerService();
    }

    @Test
    public void testDispatcherSingleConsumerReadFailed() {
        PersistentTopic topic =
                spyWithClassAndConstructorArgsRecordingInvocations(PersistentTopic.class, successTopicName, ledgerMock,
                        brokerService);
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
        doReturn(new ManagedLedgerConfig()).when(ledgerMock).getConfig();
        doReturn(new ArrayList<>()).when(ledgerMock).getCursors();

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        ByteBuf payload = Unpooled.wrappedBuffer("content".getBytes());
        final CountDownLatch latch = new CountDownLatch(1);

        // override asyncAddEntry callback to return error
        doAnswer((Answer<Object>) invocationOnMock -> {
            ((AddEntryCallback) invocationOnMock.getArguments()[1]).addFailed(
                    new ManagedLedgerException("Managed ledger failure"), invocationOnMock.getArguments()[2]);
            return null;
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
    public void testAddRemoveProducer() {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        String role = "appid1";
        // 1. simple add producer
        Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, false,
                ProducerAccessMode.Shared, Optional.empty(), true);
        topic.addProducer(producer, new CompletableFuture<>());
        assertEquals(topic.getProducers().size(), 1);

        // 2. duplicate add
        try {
            topic.addProducer(producer, new CompletableFuture<>()).join();
            fail("Should have failed with naming exception because producer 'null' is already connected to the topic");
        } catch (Exception e) {
            assertEquals(e.getCause().getClass(), BrokerServiceException.NamingException.class);
        }
        assertEquals(topic.getProducers().size(), 1);

        // 3. add producer for a different topic
        PersistentTopic failTopic = new PersistentTopic(failTopicName, ledgerMock, brokerService);
        Producer failProducer = new Producer(failTopic, serverCnx, 2 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, false,
                ProducerAccessMode.Shared, Optional.empty(), true);
        try {
            topic.addProducer(failProducer, new CompletableFuture<>());
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // OK
        }

        // 4. Try to remove with unequal producer
        Producer producerCopy = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, false,
                ProducerAccessMode.Shared, Optional.empty(), true);
        topic.removeProducer(producerCopy);
        // Expect producer to be in map
        assertEquals(topic.getProducers().size(), 1);
        assertSame(topic.getProducers().get(producer.getProducerName()), producer);

        // 5. simple remove producer
        topic.removeProducer(producer);
        assertEquals(topic.getProducers().size(), 0);

        // 6. duplicate remove
        topic.removeProducer(producer); /* noop */
    }

    @Test
    public void testProducerOverwrite() {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        String role = "appid1";
        Producer producer1 = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, true, ProducerAccessMode.Shared, Optional.empty(), true);
        Producer producer2 = new Producer(topic, serverCnx, 2 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, true, ProducerAccessMode.Shared, Optional.empty(), true);
        try {
            topic.addProducer(producer1, new CompletableFuture<>()).join();
            topic.addProducer(producer2, new CompletableFuture<>()).join();
            fail("should have failed");
        } catch (Exception e) {
            // OK
            assertEquals(e.getCause().getClass(), BrokerServiceException.NamingException.class);
        }

        Assert.assertEquals(topic.getProducers().size(), 1);

        Producer producer3 = new Producer(topic, serverCnx, 2 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 1, false, ProducerAccessMode.Shared, Optional.empty(), true);

        try {
            topic.addProducer(producer3, new CompletableFuture<>()).join();
            fail("should have failed");
        } catch (Exception e) {
            // OK
            assertEquals(e.getCause().getClass(), BrokerServiceException.NamingException.class);
        }

        Assert.assertEquals(topic.getProducers().size(), 1);

        topic.removeProducer(producer1);
        Assert.assertEquals(topic.getProducers().size(), 0);

        Producer producer4 = new Producer(topic, serverCnx, 2 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 2, false, ProducerAccessMode.Shared, Optional.empty(), true);

        topic.addProducer(producer3, new CompletableFuture<>());
        topic.addProducer(producer4, new CompletableFuture<>());

        Assert.assertEquals(topic.getProducers().size(), 1);

        topic.getProducers().values().forEach(producer -> Assert.assertEquals(producer.getEpoch(), 2));

        topic.removeProducer(producer4);
        Assert.assertEquals(topic.getProducers().size(), 0);

        Producer producer5 = new Producer(topic, serverCnx, 2 /* producer id */, "pulsar.repl.cluster1",
                role, false, null, SchemaVersion.Latest, 1, false, ProducerAccessMode.Shared, Optional.empty(), true);

        topic.addProducer(producer5, new CompletableFuture<>());
        Assert.assertEquals(topic.getProducers().size(), 1);

        Producer producer6 = new Producer(topic, serverCnx, 2 /* producer id */, "pulsar.repl.cluster1",
                role, false, null, SchemaVersion.Latest, 2, false, ProducerAccessMode.Shared, Optional.empty(), true);

        topic.addProducer(producer6, new CompletableFuture<>());
        Assert.assertEquals(topic.getProducers().size(), 1);

        topic.getProducers().values().forEach(producer -> Assert.assertEquals(producer.getEpoch(), 2));

        Producer producer7 = new Producer(topic, serverCnx, 2 /* producer id */, "pulsar.repl.cluster1",
                role, false, null, SchemaVersion.Latest, 3, true, ProducerAccessMode.Shared, Optional.empty(), true);

        topic.addProducer(producer7, new CompletableFuture<>());
        Assert.assertEquals(topic.getProducers().size(), 1);
        topic.getProducers().values().forEach(producer -> Assert.assertEquals(producer.getEpoch(), 3));
    }

    private Producer getMockedProducerWithSpecificAddress(Topic topic, long producerId, InetAddress address) {
        final String producerNameBase = "producer";
        final String role = "appid1";

        ServerCnx cnx = pulsarTestContext.createServerCnxSpy();
        doReturn(true).when(cnx).isActive();
        doReturn(true).when(cnx).isWritable();
        doReturn(new InetSocketAddress(address, 1234)).when(cnx).clientAddress();
        doReturn(address.getHostAddress()).when(cnx).clientSourceAddress();
        doReturn(new PulsarCommandSenderImpl(null, cnx)).when(cnx).getCommandSender();

        return new Producer(topic, cnx, producerId, producerNameBase + producerId, role, false, null,
                SchemaVersion.Latest, 0, false, ProducerAccessMode.Shared, Optional.empty(), true);
    }

    @Test
    public void testMaxSameAddressProducers() throws Exception {
        // set max clients
        pulsarTestContext.getConfig().setMaxSameAddressProducersPerTopic(2);

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        InetAddress address1 = InetAddress.getByName("127.0.0.1");
        InetAddress address2 = InetAddress.getByName("0.0.0.0");
        String ipAddress1 = address1.getHostAddress();
        String ipAddress2 = address2.getHostAddress();

        // 1. add producer1 with ipAddress1
        Producer producer1 = getMockedProducerWithSpecificAddress(topic, 1, address1);
        topic.addProducer(producer1, new CompletableFuture<>());
        assertEquals(topic.getProducers().size(), 1);
        assertEquals(topic.getNumberOfSameAddressProducers(ipAddress1), 1);

        // 2. add producer2 with ipAddress1
        Producer producer2 = getMockedProducerWithSpecificAddress(topic, 2, address1);
        topic.addProducer(producer2, new CompletableFuture<>());
        assertEquals(topic.getProducers().size(), 2);
        assertEquals(topic.getNumberOfSameAddressProducers(ipAddress1), 2);

        // 3. add producer3 with ipAddress1 but reached maxSameAddressProducersPerTopic
        try {
            Producer producer3 = getMockedProducerWithSpecificAddress(topic, 3, address1);
            topic.addProducer(producer3, new CompletableFuture<>()).join();
            fail("should have failed");
        } catch (Exception e) {
            assertEquals(e.getCause().getClass(), BrokerServiceException.ProducerBusyException.class);
        }
        assertEquals(topic.getProducers().size(), 2);
        assertEquals(topic.getNumberOfSameAddressProducers(ipAddress1), 2);

        // 4. add producer4 with ipAddress2
        Producer producer4 = getMockedProducerWithSpecificAddress(topic, 4, address2);
        topic.addProducer(producer4, new CompletableFuture<>());
        assertEquals(topic.getProducers().size(), 3);
        assertEquals(topic.getNumberOfSameAddressProducers(ipAddress2), 1);

        // 5. add producer5 with ipAddress2
        Producer producer5 = getMockedProducerWithSpecificAddress(topic, 5, address2);
        topic.addProducer(producer5, new CompletableFuture<>());
        assertEquals(topic.getProducers().size(), 4);
        assertEquals(topic.getNumberOfSameAddressProducers(ipAddress2), 2);

        // 6. add producer6 with ipAddress2 but reached maxSameAddressProducersPerTopic
        try {
            Producer producer6 = getMockedProducerWithSpecificAddress(topic, 6, address2);
            topic.addProducer(producer6, new CompletableFuture<>()).join();
            fail("should have failed");
        } catch (Exception e) {
            assertEquals(e.getCause().getClass(), BrokerServiceException.ProducerBusyException.class);
        }
        assertEquals(topic.getProducers().size(), 4);
        assertEquals(topic.getNumberOfSameAddressProducers(ipAddress2), 2);

        // 7. remove producer1
        topic.removeProducer(producer1);
        assertEquals(topic.getProducers().size(), 3);
        assertEquals(topic.getNumberOfSameAddressProducers(ipAddress1), 1);

        // 8. add producer7 with ipAddress1
        Producer producer7 = getMockedProducerWithSpecificAddress(topic, 7, address1);
        topic.addProducer(producer7, new CompletableFuture<>());
        assertEquals(topic.getProducers().size(), 4);
        assertEquals(topic.getNumberOfSameAddressProducers(ipAddress1), 2);
    }

    @Test
    public void testSubscribeFail() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        // Empty subscription name
        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription("")
                .setConsumerName("consumer-name")
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Exclusive);

        SubscriptionOption subscriptionOption = getSubscriptionOption(cmd);

        Future<Consumer> f1 = topic.subscribe(subscriptionOption);
        try {
            f1.get();
            fail("should fail with exception");
        } catch (ExecutionException ee) {
            // Expected
            assertTrue(ee.getCause() instanceof BrokerServiceException.NamingException);
        }
    }

    private SubscriptionOption getSubscriptionOption(CommandSubscribe cmd) {
        return SubscriptionOption.builder().cnx(serverCnx)
                .subscriptionName(cmd.getSubscription()).consumerId(cmd.getConsumerId()).subType(cmd.getSubType())
                .priorityLevel(0).consumerName(cmd.getConsumerName()).isDurable(cmd.isDurable()).startMessageId(null)
                .metadata(Collections.emptyMap()).readCompacted(false)
                .initialPosition(InitialPosition.Latest).subscriptionProperties(Optional.empty())
                .startMessageRollbackDurationSec(0).replicatedSubscriptionStateArg(false).keySharedMeta(null)
                .build();
    }

    @Test
    public void testSubscribeUnsubscribe() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setConsumerName("consumer-name")
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Exclusive);

        // 1. simple subscribe
        Future<Consumer> f1 = topic.subscribe(getSubscriptionOption(cmd));
        f1.get();

        // 2. duplicate subscribe
        CommandSubscribe cmd2 = new CommandSubscribe()
                .setConsumerId(2)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setConsumerName("consumer-name")
                .setReadCompacted(false)
                .setRequestId(2)
                .setSubType(SubType.Exclusive);
        Future<Consumer> f2 = topic.subscribe(getSubscriptionOption(cmd2));
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

        Consumer consumer = new Consumer(sub, SubType.Exclusive, topic.getName(), 1, 0, "Cons1", true, serverCnx,
                "myrole-1", Collections.emptyMap(), false,
                new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT), MessageId.latest,
                DEFAULT_CONSUMER_EPOCH);
        sub.addConsumer(consumer);
        consumer.close();

        SubType previousSubType = SubType.Exclusive;
        for (SubType subType : List.of(SubType.Shared, SubType.Failover, SubType.Key_Shared,
                SubType.Exclusive)) {
            Dispatcher previousDispatcher = sub.getDispatcher();

            consumer = new Consumer(sub, subType, topic.getName(), 1, 0, "Cons1", true, serverCnx, "myrole-1",
                    Collections.emptyMap(), false,
                    new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT), MessageId.latest,
                    DEFAULT_CONSUMER_EPOCH);
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
        Consumer consumer = new Consumer(sub, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */,
                true, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest,
                DEFAULT_CONSUMER_EPOCH);
        sub.addConsumer(consumer);
        assertTrue(sub.getDispatcher().isConsumerConnected());

        // 2. simple remove consumer
        sub.removeConsumer(consumer);
        assertFalse(sub.getDispatcher().isConsumerConnected());

        // 3. duplicate remove consumer
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

        Consumer consumer = new Consumer(sub, SubType.Exclusive, topic.getName(), 1, 0, "Cons1", true, serverCnx,
                "myrole-1", Collections.emptyMap(), false, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);

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

    private void testMaxConsumersShared() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.initialize().join();
        assertEquals((int) topic.getHierarchyTopicPolicies().getMaxConsumerPerTopic().get(), 3);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        PersistentSubscription sub2 = new PersistentSubscription(topic, "sub-2", cursorMock, false);

        Method addConsumerToSubscription = AbstractTopic.class.getDeclaredMethod("addConsumerToSubscription",
                Subscription.class, Consumer.class);
        addConsumerToSubscription.setAccessible(true);

        // for count consumers on topic
        ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions =
                ConcurrentOpenHashMap.<String, PersistentSubscription>newBuilder()
                        .expectedItems(16)
                        .concurrencyLevel(1)
                        .build();
        subscriptions.put("sub-1", sub);
        subscriptions.put("sub-2", sub2);
        Field field = topic.getClass().getDeclaredField("subscriptions");
        field.setAccessible(true);
        field.set(topic, subscriptions);

        // 1. add consumer1
        Consumer consumer = new Consumer(sub, SubType.Shared, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        addConsumerToSubscription.invoke(topic, sub, consumer);
        assertEquals(sub.getConsumers().size(), 1);

        // 2. add consumer2
        Consumer consumer2 = new Consumer(sub, SubType.Shared, topic.getName(), 2 /* consumer id */, 0,
                "Cons2"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        addConsumerToSubscription.invoke(topic, sub, consumer2);
        assertEquals(sub.getConsumers().size(), 2);

        // 3. add consumer3 but reach maxConsumersPerSubscription
        try {
            Consumer consumer3 = new Consumer(sub, SubType.Shared, topic.getName(), 3 /* consumer id */, 0,
                    "Cons3"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
            ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub, consumer3)).get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof BrokerServiceException.ConsumerBusyException);
        }

        // check number of consumers on topic
        assertEquals(topic.getNumberOfConsumers(), 2);

        // 4. add consumer4 to sub2
        Consumer consumer4 = new Consumer(sub2, SubType.Shared, topic.getName(), 4 /* consumer id */, 0,
                "Cons4"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        addConsumerToSubscription.invoke(topic, sub2, consumer4);
        assertEquals(sub2.getConsumers().size(), 1);

        // check number of consumers on topic
        assertEquals(topic.getNumberOfConsumers(), 3);

        // 5. add consumer5 to sub2 but reach maxConsumersPerTopic
        try {
            Consumer consumer5 = new Consumer(sub2, SubType.Shared, topic.getName(), 5 /* consumer id */, 0,
                    "Cons5"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
            ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub2, consumer5)).get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof BrokerServiceException.ConsumerBusyException);
        }
    }

    @Test
    public void testMaxConsumersSharedForBroker() throws Exception {
        // set max clients
        pulsarTestContext.getConfig().setMaxConsumersPerSubscription(2);
        pulsarTestContext.getConfig().setMaxConsumersPerTopic(3);
        testMaxConsumersShared();
    }

    @Test
    public void testMaxConsumersSharedForNamespace() throws Exception {
        // set max clients
        Policies policies = new Policies();
        policies.max_consumers_per_subscription = 2;
        policies.max_consumers_per_topic = 3;

        pulsarTestContext.getPulsarResources().getNamespaceResources()
                .createPolicies(TopicName.get(successTopicName).getNamespaceObject(),
                        policies);

        testMaxConsumersShared();
    }

    private void testMaxConsumersFailover() throws Exception {

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.initialize().join();
        assertEquals((int) topic.getHierarchyTopicPolicies().getMaxConsumerPerTopic().get(), 3);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        PersistentSubscription sub2 = new PersistentSubscription(topic, "sub-2", cursorMock, false);

        Method addConsumerToSubscription = AbstractTopic.class.getDeclaredMethod("addConsumerToSubscription",
                Subscription.class, Consumer.class);
        addConsumerToSubscription.setAccessible(true);

        // for count consumers on topic
        ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions =
                ConcurrentOpenHashMap.<String, PersistentSubscription>newBuilder()
                        .expectedItems(16)
                        .concurrencyLevel(1)
                        .build();
        subscriptions.put("sub-1", sub);
        subscriptions.put("sub-2", sub2);
        Field field = topic.getClass().getDeclaredField("subscriptions");
        field.setAccessible(true);
        field.set(topic, subscriptions);

        // 1. add consumer1
        Consumer consumer = new Consumer(sub, SubType.Failover, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        addConsumerToSubscription.invoke(topic, sub, consumer);
        assertEquals(sub.getConsumers().size(), 1);

        // 2. add consumer2
        Consumer consumer2 = new Consumer(sub, SubType.Failover, topic.getName(), 2 /* consumer id */, 0,
                "Cons2"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        addConsumerToSubscription.invoke(topic, sub, consumer2);
        assertEquals(sub.getConsumers().size(), 2);

        // 3. add consumer3 but reach maxConsumersPerSubscription
        try {
            Consumer consumer3 = new Consumer(sub, SubType.Failover, topic.getName(), 3 /* consumer id */, 0,
                    "Cons3"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
            ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub, consumer3)).get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof BrokerServiceException.ConsumerBusyException);
        }

        // check number of consumers on topic
        assertEquals(topic.getNumberOfConsumers(), 2);

        // 4. add consumer4 to sub2
        Consumer consumer4 = new Consumer(sub2, SubType.Failover, topic.getName(), 4 /* consumer id */, 0,
                "Cons4"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        addConsumerToSubscription.invoke(topic, sub2, consumer4);
        assertEquals(sub2.getConsumers().size(), 1);

        // check number of consumers on topic
        assertEquals(topic.getNumberOfConsumers(), 3);

        // 5. add consumer5 to sub2 but reach maxConsumersPerTopic
        try {
            Consumer consumer5 = new Consumer(sub2, SubType.Failover, topic.getName(), 5 /* consumer id */, 0,
                    "Cons5"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
            ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub2, consumer5)).get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof BrokerServiceException.ConsumerBusyException);
        }
    }

    @Test
    public void testMaxConsumersFailoverForBroker() throws Exception {
        // set max clients
        pulsarTestContext.getConfig().setMaxConsumersPerSubscription(2);
        pulsarTestContext.getConfig().setMaxConsumersPerTopic(3);

        testMaxConsumersFailover();
    }

    @Test
    public void testMaxConsumersFailoverForNamespace() throws Exception {
        // set max clients
        Policies policies = new Policies();
        policies.max_consumers_per_subscription = 2;
        policies.max_consumers_per_topic = 3;

        pulsarTestContext.getPulsarResources().getNamespaceResources()
                .createPolicies(TopicName.get(successTopicName).getNamespaceObject(),
                        policies);

        testMaxConsumersFailover();
    }

    private Consumer getMockedConsumerWithSpecificAddress(Topic topic, Subscription sub, long consumerId,
                                                          InetAddress address) {
        final String consumerNameBase = "consumer";
        final String role = "appid1";

        ServerCnx cnx = pulsarTestContext.createServerCnxSpy();
        doReturn(true).when(cnx).isActive();
        doReturn(true).when(cnx).isWritable();
        doReturn(new InetSocketAddress(address, 1234)).when(cnx).clientAddress();
        doReturn(address.getHostAddress()).when(cnx).clientSourceAddress();
        doReturn(new PulsarCommandSenderImpl(null, cnx)).when(cnx).getCommandSender();

        return new Consumer(sub, SubType.Shared, topic.getName(), consumerId, 0, consumerNameBase + consumerId, true,
                cnx, role, Collections.emptyMap(), false, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
    }

    @Test
    public void testMaxSameAddressConsumers() throws Exception {
        // set max clients
        pulsarTestContext.getConfig().setMaxSameAddressConsumersPerTopic(2);

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub1 = new PersistentSubscription(topic, "sub1", cursorMock, false);
        PersistentSubscription sub2 = new PersistentSubscription(topic, "sub2", cursorMock, false);

        InetAddress address1 = InetAddress.getByName("127.0.0.1");
        InetAddress address2 = InetAddress.getByName("0.0.0.0");
        String ipAddress1 = address1.getHostAddress();
        String ipAddress2 = address2.getHostAddress();

        Method addConsumerToSubscription = AbstractTopic.class.getDeclaredMethod("addConsumerToSubscription",
                Subscription.class, Consumer.class);
        addConsumerToSubscription.setAccessible(true);

        // for count consumers on topic
        ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions =
                ConcurrentOpenHashMap.<String, PersistentSubscription>newBuilder()
                        .expectedItems(16)
                        .concurrencyLevel(1)
                        .build();
        subscriptions.put("sub1", sub1);
        subscriptions.put("sub2", sub2);
        Field field = topic.getClass().getDeclaredField("subscriptions");
        field.setAccessible(true);
        field.set(topic, subscriptions);

        // 1. add consumer1 with ipAddress1 to sub1
        Consumer consumer1 = getMockedConsumerWithSpecificAddress(topic, sub1, 1, address1);
        ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub1, consumer1)).get();
        assertEquals(topic.getNumberOfConsumers(), 1);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress1), 1);
        assertEquals(sub1.getNumberOfSameAddressConsumers(ipAddress1), 1);

        // 2. add consumer2 with ipAddress1 to sub2
        Consumer consumer2 = getMockedConsumerWithSpecificAddress(topic, sub2, 2, address1);
        ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub2, consumer2)).get();
        assertEquals(topic.getNumberOfConsumers(), 2);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress1), 2);
        assertEquals(sub1.getNumberOfSameAddressConsumers(ipAddress1), 1);
        assertEquals(sub2.getNumberOfSameAddressConsumers(ipAddress1), 1);

        // 3. add consumer3 with ipAddress2 to sub1
        Consumer consumer3 = getMockedConsumerWithSpecificAddress(topic, sub1, 3, address2);
        ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub1, consumer3)).get();
        assertEquals(topic.getNumberOfConsumers(), 3);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress1), 2);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress2), 1);
        assertEquals(sub1.getNumberOfSameAddressConsumers(ipAddress1), 1);
        assertEquals(sub1.getNumberOfSameAddressConsumers(ipAddress2), 1);

        // 4. add consumer4 with ipAddress2 to sub2
        Consumer consumer4 = getMockedConsumerWithSpecificAddress(topic, sub2, 4, address2);
        ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub2, consumer4)).get();
        assertEquals(topic.getNumberOfConsumers(), 4);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress1), 2);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress2), 2);
        assertEquals(sub2.getNumberOfSameAddressConsumers(ipAddress1), 1);
        assertEquals(sub2.getNumberOfSameAddressConsumers(ipAddress2), 1);

        // 5. add consumer5 with ipAddress1 to sub1 but reach maxSameAddressConsumersPerTopic
        try {
            Consumer consumer5 = getMockedConsumerWithSpecificAddress(topic, sub1, 5, address1);
            ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub1, consumer5)).get();

            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof BrokerServiceException.ConsumerBusyException);
        }
        assertEquals(topic.getNumberOfConsumers(), 4);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress1), 2);
        assertEquals(sub1.getNumberOfSameAddressConsumers(ipAddress1), 1);

        // 6. add consumer6 with ipAddress2 to sub2 but reach maxSameAddressConsumersPerTopic
        try {
            Consumer consumer6 = getMockedConsumerWithSpecificAddress(topic, sub2, 6, address2);
            ((CompletableFuture<Void>) addConsumerToSubscription.invoke(topic, sub2, consumer6)).get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof BrokerServiceException.ConsumerBusyException);
        }
        assertEquals(topic.getNumberOfConsumers(), 4);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress2), 2);
        assertEquals(sub2.getNumberOfSameAddressConsumers(ipAddress2), 1);

        // 7. remove consumer1 from sub1
        consumer1.close();
        assertEquals(topic.getNumberOfConsumers(), 3);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress1), 1);
        assertEquals(sub1.getNumberOfSameAddressConsumers(ipAddress1), 0);

        // 8. add consumer7 with ipAddress1 to sub1
        Consumer consumer7 = getMockedConsumerWithSpecificAddress(topic, sub1, 7, address1);
        addConsumerToSubscription.invoke(topic, sub1, consumer7);
        assertEquals(topic.getNumberOfConsumers(), 4);
        assertEquals(topic.getNumberOfSameAddressConsumers(ipAddress1), 2);
        assertEquals(sub1.getNumberOfSameAddressConsumers(ipAddress1), 1);
    }

    @Test
    public void testUbsubscribeRaceConditions() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        Consumer consumer1 = new Consumer(sub, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */,
                true, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest,
                DEFAULT_CONSUMER_EPOCH);
        sub.addConsumer(consumer1);

        doAnswer(invocationOnMock -> {
            ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
            Thread.sleep(1000);
            return null;
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            sub.doUnsubscribe(consumer1);
            return null;
        }).get();

        try {
            Thread.sleep(10); /* delay to ensure that the ubsubscribe gets executed first */
            sub.addConsumer(new Consumer(sub, SubType.Exclusive, topic.getName(), 2 /* consumer id */,
                    0, "Cons2"/* consumer name */, true, serverCnx,
                    "myrole-1", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest,
                    DEFAULT_CONSUMER_EPOCH)).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof BrokerServiceException.SubscriptionFencedException);
        }
    }

    @Test
    public void testCloseTopic() throws Exception {
        // create topic
        PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();

        Field isFencedField = AbstractTopic.class.getDeclaredField("isFenced");
        isFencedField.setAccessible(true);
        Field isClosingOrDeletingField = PersistentTopic.class.getDeclaredField("isClosingOrDeleting");
        isClosingOrDeletingField.setAccessible(true);

        assertFalse((boolean) isFencedField.get(topic));
        assertFalse((boolean) isClosingOrDeletingField.get(topic));

        // 1. close topic
        topic.close().get();
        assertFalse(brokerService.getTopicReference(successTopicName).isPresent());
        assertTrue((boolean) isFencedField.get(topic));
        assertTrue((boolean) isClosingOrDeletingField.get(topic));

        // 2. publish message to closed topic
        ByteBuf payload = Unpooled.wrappedBuffer("content".getBytes());
        final CountDownLatch latch = new CountDownLatch(1);
        topic.publishMessage(payload, (exception, ledgerId, entryId) -> {
            assertTrue(exception instanceof BrokerServiceException.TopicFencedException);
            latch.countDown();
        });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue((boolean) isFencedField.get(topic));
        assertTrue((boolean) isClosingOrDeletingField.get(topic));
    }

    @Test
    public void testDeleteTopic() throws Exception {
        doReturn(CompletableFuture.completedFuture(null)).when(ledgerMock).asyncTruncate();

        // create topic
        PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();

        Field isFencedField = AbstractTopic.class.getDeclaredField("isFenced");
        isFencedField.setAccessible(true);
        Field isClosingOrDeletingField = PersistentTopic.class.getDeclaredField("isClosingOrDeleting");
        isClosingOrDeletingField.setAccessible(true);

        assertFalse((boolean) isFencedField.get(topic));
        assertFalse((boolean) isClosingOrDeletingField.get(topic));

        String role = "appid1";
        // 1. delete inactive topic
        topic.delete().get();
        assertFalse(brokerService.getTopicReference(successTopicName).isPresent());
        assertTrue((boolean) isFencedField.get(topic));
        assertTrue((boolean) isClosingOrDeletingField.get(topic));

        // 2. publish message to deleted topic
        ByteBuf payload = Unpooled.wrappedBuffer("content".getBytes());
        final CountDownLatch latch = new CountDownLatch(1);
        topic.publishMessage(payload, (exception, ledgerId, entryId) -> {
            assertTrue(exception instanceof BrokerServiceException.TopicFencedException);
            latch.countDown();
        });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue((boolean) isFencedField.get(topic));
        assertTrue((boolean) isClosingOrDeletingField.get(topic));

        // 3. delete topic with producer
        topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, false, ProducerAccessMode.Shared, Optional.empty(), true);
        topic.addProducer(producer, new CompletableFuture<>()).join();

        CompletableFuture<Void> cf = topic.delete();
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(cf::isDone);
        assertTrue(cf.isCompletedExceptionally());
        assertFalse((boolean) isFencedField.get(topic));
        assertFalse((boolean) isClosingOrDeletingField.get(topic));
        topic.removeProducer(producer);

        // 4. delete topic with subscriber
        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setConsumerName("consumer-name")
                .setRequestId(1)
                .setSubType(SubType.Exclusive);

        Future<Consumer> f1 = topic.subscribe(getSubscriptionOption(cmd));
        f1.get();

        CompletableFuture<Void> cf2 = topic.delete();
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(cf2::isDone);
        assertTrue(cf2.isCompletedExceptionally());
        assertFalse((boolean) isFencedField.get(topic));
        assertFalse((boolean) isClosingOrDeletingField.get(topic));
        topic.unsubscribe(successSubName);
    }

    @Test
    public void testDeleteAndUnsubscribeTopic() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setConsumerName("consumer-name")
                .setRequestId(1)
                .setReadCompacted(false)
                .setSubType(SubType.Exclusive);

        Future<Consumer> f1 = topic.subscribe(getSubscriptionOption(cmd));
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread(() -> {
            try {
                barrier.await();
                assertFalse(topic.delete().isCompletedExceptionally());
            } catch (Exception e) {
                e.printStackTrace();
                gotException.set(true);
            } finally {
                counter.countDown();
            }
        });

        Thread unsubscriber = new Thread(() -> {
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
        });

        deleter.start();
        unsubscriber.start();

        counter.await();
        assertFalse(gotException.get());
    }

    @Test(enabled = false)
    public void testConcurrentTopicAndSubscriptionDelete() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setRequestId(1)
                .setSubType(SubType.Exclusive);

        Future<Consumer> f1 = topic.subscribe(getSubscriptionOption(cmd));

        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread(() -> {
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
        });

        Thread unsubscriber = new Thread(() -> {
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
        });

        deleter.start();
        unsubscriber.start();

        counter.await();
        assertFalse(gotException.get());
    }

    @Test
    public void testDeleteTopicRaceConditions() throws Exception {
        PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();

        // override ledger deletion callback to slow down deletion
        doAnswer(invocationOnMock -> {
            Thread.sleep(1000);
            ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
            return null;
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), any());

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            topic.delete();
            return null;
        }).get();

        try {
            String role = "appid1";
            Thread.sleep(10); /* delay to ensure that the delete gets executed first */
            Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                    role, false, null, SchemaVersion.Latest, 0, false, ProducerAccessMode.Shared, Optional.empty(),
                    true);
            topic.addProducer(producer, new CompletableFuture<>()).join();
            fail("Should have failed");
        } catch (Exception e) {
            assertEquals(e.getCause().getClass(), BrokerServiceException.TopicFencedException.class);
        }

        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setConsumerName("consumer-name")
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Exclusive);

        Future<Consumer> f = topic.subscribe(getSubscriptionOption(cmd));
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
        cursorMock = mock(ManagedCursorImpl.class);
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        doReturn(new ArrayList<>()).when(ledgerMock).getCursors();
        doReturn(new ManagedLedgerConfig()).when(ledgerMock).getConfig();
        doReturn("mockCursor").when(cursorMock).getName();
        doReturn(true).when(cursorMock).isDurable();
        // doNothing().when(cursorMock).asyncClose(new CloseCallback() {
        doAnswer((Answer<Object>) invocationOnMock -> {
            // return closeFuture.get();
            return closeFuture.complete(null);
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
        doAnswer(invocationOnMock -> {
            ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
            return null;
        }).when(pulsarTestContext.getManagedLedgerFactory())
                .asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                        any(OpenLedgerCallback.class), any(Supplier.class), any());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(invocationOnMock -> {
            ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                    .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
            return null;
        }).when(pulsarTestContext.getManagedLedgerFactory())
                .asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                        any(OpenLedgerCallback.class), any(Supplier.class), any());

        // call addComplete on ledger asyncAddEntry
        doAnswer(invocationOnMock -> {
            ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(1, 1),
                    null,
                    invocationOnMock.getArguments()[2]);
            return null;
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), any());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(invocationOnMock -> {
            ((OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
            return null;
        }).when(ledgerMock)
                .asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class),
                        any());

        doAnswer(invocationOnMock -> {
            ((OpenCursorCallback) invocationOnMock.getArguments()[4]).openCursorComplete(cursorMock, null);
            return null;
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(Map.class),
                any(Map.class), any(OpenCursorCallback.class), any());

        doAnswer(invocationOnMock -> {
            ((CloseCallback) invocationOnMock.getArguments()[0]).closeComplete(null);
            return null;
        }).when(ledgerMock).asyncClose(any(CloseCallback.class), any());

        // call deleteLedgerComplete on ledger asyncDelete
        doAnswer(invocationOnMock -> {
            ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
            return null;
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), any());

        doAnswer(invocationOnMock -> {
            ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
            return null;
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());

        doAnswer((invokactionOnMock) -> {
            ((MarkDeleteCallback) invokactionOnMock.getArguments()[2])
                    .markDeleteComplete(invokactionOnMock.getArguments()[3]);
            return null;
        }).when(cursorMock).asyncMarkDelete(any(), any(), any(MarkDeleteCallback.class), any());
    }


    @Test
    public void testDeleteTopicDeleteOnMetadataStoreFailed() throws Exception {

        doReturn(CompletableFuture.completedFuture(null)).when(ledgerMock).asyncTruncate();

        // create topic
        PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();

        Field isFencedField = AbstractTopic.class.getDeclaredField("isFenced");
        isFencedField.setAccessible(true);
        Field isClosingOrDeletingField = PersistentTopic.class.getDeclaredField("isClosingOrDeleting");
        isClosingOrDeletingField.setAccessible(true);

        assertFalse((boolean) isFencedField.get(topic));
        assertFalse((boolean) isClosingOrDeletingField.get(topic));

        metadataStore.failConditional(new MetadataStoreException("injected error"), (op, path) ->
                op == OperationType.GET && path.equals("/admin/policies/prop/use/ns-abc"));
        // Remove cache, and then read data from metadata store.
        brokerService.pulsar().getPulsarResources().getNamespaceResources().getCache()
                .invalidate("/admin/policies/prop/use/ns-abc");
        try {
            topic.delete().get();
            fail();
        } catch (ExecutionException e) {
            final Throwable t = FutureUtil.unwrapCompletionException(e);
            assertTrue(t.getMessage().contains("injected error"));
        }
        assertFalse((boolean) isFencedField.get(topic));
        assertFalse((boolean) isClosingOrDeletingField.get(topic));

    }


    @Test
    public void testFailoverSubscription() throws Exception {
        PersistentTopic topic1 = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd1 = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setConsumerName("consumer-name")
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Failover);

        // 1. Subscribe with non partition topic
        Future<Consumer> f1 = topic1.subscribe(getSubscriptionOption(cmd1));
        f1.get();

        // 2. Subscribe with partition topic
        PersistentTopic topic2 = new PersistentTopic(successPartitionTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd2 = new CommandSubscribe()
                .setConsumerId(1)
                .setConsumerName("C1")
                .setTopic(successPartitionTopicName)
                .setSubscription(successSubName)
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Failover);

        Future<Consumer> f2 = topic2.subscribe(getSubscriptionOption(cmd2));
        f2.get();

        // 3. Subscribe and create second consumer
        CommandSubscribe cmd3 = new CommandSubscribe()
                .setConsumerId(2)
                .setConsumerName("C2")
                .setTopic(successPartitionTopicName)
                .setSubscription(successSubName)
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Failover);

        Future<Consumer> f3 = topic2.subscribe(getSubscriptionOption(cmd3));
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
        CommandSubscribe cmd4 = new CommandSubscribe()
                .setConsumerId(3)
                .setConsumerName("C1")
                .setTopic(successPartitionTopicName)
                .setSubscription(successSubName)
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Failover);

        Future<Consumer> f4 = topic2.subscribe(getSubscriptionOption(cmd4));
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
        CommandSubscribe cmd5 = new CommandSubscribe()
                .setConsumerId(2)
                .setConsumerName("C1")
                .setTopic(successPartitionTopicName)
                .setSubscription(successSubName)
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Exclusive);

        Future<Consumer> f5 = topic2.subscribe(getSubscriptionOption(cmd5));
        try {
            f5.get();
            fail("should fail with exception");
        } catch (ExecutionException ee) {
            // Expected
            assertTrue(ee.getCause() instanceof BrokerServiceException.SubscriptionBusyException);
        }

        // 6. Subscribe on partition topic with different sub name, type and different consumer id
        CommandSubscribe cmd6 = new CommandSubscribe()
                .setConsumerId(4)
                .setConsumerName("C3")
                .setTopic(successPartitionTopicName)
                .setSubscription(successSubName2)
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Exclusive);

        Future<Consumer> f6 = topic2.subscribe(getSubscriptionOption(cmd6));
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

    private PulsarAdmin mockReplicationAdmin() {
        PulsarAdmin admin = mock(PulsarAdmin.class);
        Topics topics = mock(Topics.class);
        doReturn(topics).when(admin).topics();
        doReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(0))).when(topics)
                .getPartitionedTopicMetadataAsync(anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(topics).createNonPartitionedTopicAsync(anyString());
        return admin;
    }

    /**
     * NonPersistentReplicator.removeReplicator doesn't remove replicator in atomic way and does in multiple step:
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
        doReturn(new ManagedLedgerConfig()).when(ledgerMock).getConfig();
        doReturn(new ArrayList<>()).when(ledgerMock).getCursors();

        PersistentTopic topic = new PersistentTopic(globalTopicName, ledgerMock, brokerService);
        topic.initialize().join();
        String remoteReplicatorName = topic.getReplicatorPrefix() + "." + remoteCluster;
        ConcurrentOpenHashMap<String, Replicator> replicatorMap = topic.getReplicators();

        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        doReturn(remoteCluster).when(cursor).getName();
        PulsarClientImpl pulsarClientMock = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(pulsarClientMock.getCnxPool()).thenReturn(connectionPool);
        when(pulsarClientMock.newProducer(any())).thenAnswer(
                invocation -> {
                    ProducerBuilderImpl producerBuilder =
                            new ProducerBuilderImpl(pulsarClientMock, invocation.getArgument(0)) {
                                @Override
                                public CompletableFuture<org.apache.pulsar.client.api.Producer> createAsync() {
                                    return CompletableFuture.completedFuture(mock(org.apache.pulsar.client.api.Producer.class));
                                }
                            };
                    return producerBuilder;
                });
        brokerService.getReplicationClients().put(remoteCluster, pulsarClientMock);

        @Cleanup
        PulsarAdmin admin = mockReplicationAdmin();
        brokerService.getClusterAdmins().put(remoteCluster, admin);
        Optional<ClusterData> clusterData = brokerService.pulsar().getPulsarResources().getClusterResources()
                .getCluster(remoteCluster);
        PersistentReplicator replicator = spy(
                new GeoPersistentReplicator(topic, cursor, localCluster, remoteCluster, brokerService,
                        (PulsarClientImpl) brokerService.getReplicationClient(remoteCluster, clusterData),
                        brokerService.getClusterPulsarAdmin(remoteCluster, clusterData)));
        replicatorMap.put(remoteReplicatorName, replicator);

        // step-1 remove replicator : it will disconnect the producer but it will wait for callback to be completed
        Method removeMethod = PersistentTopic.class.getDeclaredMethod("removeReplicator", String.class);
        removeMethod.setAccessible(true);
        removeMethod.invoke(topic, remoteReplicatorName);

        // step-2 now, policies doesn't have removed replication cluster so, it should not invoke "startProducer" of the
        // replicator
        // try to start replicator again
        topic.startReplProducers().join();
        // verify: replicator.startProducer is not invoked
        verify(replicator, Mockito.times(1)).startProducer();

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
        doReturn(new ArrayList<>()).when(ledgerMock).getCursors();
        doReturn(new ManagedLedgerConfig()).when(ledgerMock).getConfig();

        PersistentTopic topic = new PersistentTopic(globalTopicName, ledgerMock, brokerService);

        PulsarService pulsar = pulsarTestContext.getPulsarService();
        final URL brokerUrl = new URL(
                "http://" + pulsar.getAdvertisedAddress() + ":" + pulsar.getConfiguration().getBrokerServicePort()
                        .get());
        @Cleanup
        PulsarClient client = spy(PulsarClient.builder().serviceUrl(brokerUrl.toString()).build());
        PulsarClientImpl clientImpl = (PulsarClientImpl) client;
        doReturn(new CompletableFuture<Producer>()).when(clientImpl)
                .createProducerAsync(any(ProducerConfigurationData.class), any(Schema.class));

        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        doReturn(remoteCluster).when(cursor).getName();
        brokerService.getReplicationClients().put(remoteCluster, client);

        @Cleanup
        PulsarAdmin admin = mockReplicationAdmin();
        brokerService.getClusterAdmins().put(remoteCluster, admin);
        Optional<ClusterData> clusterData = brokerService.pulsar().getPulsarResources().getClusterResources()
                .getCluster(remoteCluster);
        PersistentReplicator replicator = new GeoPersistentReplicator(topic, cursor, localCluster, remoteCluster,
                brokerService, (PulsarClientImpl) brokerService.getReplicationClient(remoteCluster, clusterData),
                brokerService.getClusterPulsarAdmin(remoteCluster, clusterData));

        // PersistentReplicator constructor calls startProducer()
        verify(clientImpl)
                .createProducerAsync(
                        any(ProducerConfigurationData.class),
                        any(), eq(null)
                );

        replicator.terminate();
        replicator.terminate();

        replicator.startProducer();

        verify(clientImpl, Mockito.times(1)).createProducerAsync(any(), any(), any());
    }

    @Test
    public void testCompactorSubscription() {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        CompactedTopic compactedTopic = mock(CompactedTopic.class);
        when(compactedTopic.newCompactedLedger(any(Position.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(mock(CompactedTopicContext.class)));
        PersistentSubscription sub = new CompactorSubscription(topic, compactedTopic,
                Compactor.COMPACTION_SUBSCRIPTION,
                cursorMock);
        PositionImpl position = new PositionImpl(1, 1);
        long ledgerId = 0xc0bfefeL;
        sub.acknowledgeMessage(Collections.singletonList(position), AckType.Cumulative,
                Map.of(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, ledgerId));
        verify(compactedTopic, Mockito.times(1)).newCompactedLedger(position, ledgerId);
    }


    @Test
    public void testCompactorSubscriptionUpdatedOnInit() {
        long ledgerId = 0xc0bfefeL;
        Map<String, Long> properties = Map.of(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, ledgerId);
        PositionImpl position = new PositionImpl(1, 1);

        doAnswer((invokactionOnMock) -> properties).when(cursorMock).getProperties();
        doAnswer((invokactionOnMock) -> position).when(cursorMock).getMarkDeletedPosition();

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        CompactedTopic compactedTopic = mock(CompactedTopic.class);
        when(compactedTopic.newCompactedLedger(any(Position.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
        new CompactorSubscription(topic, compactedTopic, Compactor.COMPACTION_SUBSCRIPTION, cursorMock);
        verify(compactedTopic, Mockito.times(1)).newCompactedLedger(position, ledgerId);
    }

    @Test
    public void testCompactionTriggeredAfterThresholdFirstInvocation() throws Exception {
        CompletableFuture<Long> compactPromise = new CompletableFuture<>();
        Compactor compactor = pulsarTestContext.getPulsarService().getCompactor();
        doReturn(compactPromise).when(compactor).compact(anyString());

        Policies policies = new Policies();
        policies.compaction_threshold = 1L;

        pulsarTestContext.getPulsarResources().getNamespaceResources()
                .createPolicies(TopicName.get(successTopicName).getNamespaceObject(),
                        policies);

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.initialize().get();

        topic.checkCompaction();

        verify(compactor, times(0)).compact(anyString());

        doReturn(10L).when(ledgerMock).getTotalSize();
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
        Compactor compactor = pulsarTestContext.getPulsarService().getCompactor();
        doReturn(compactPromise).when(compactor).compact(anyString());

        ManagedCursor subCursor = mock(ManagedCursor.class);
        doReturn(List.of(subCursor)).when(ledgerMock).getCursors();
        doReturn(Compactor.COMPACTION_SUBSCRIPTION).when(subCursor).getName();

        Policies policies = new Policies();
        policies.compaction_threshold = 1L;

        pulsarTestContext.getPulsarResources().getNamespaceResources()
                .createPolicies(TopicName.get(successTopicName).getNamespaceObject(),
                        policies);

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.initialize().get();

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
        Compactor compactor = pulsarTestContext.getPulsarService().getCompactor();
        doReturn(compactPromise).when(compactor).compact(anyString());

        Policies policies = new Policies();
        policies.compaction_threshold = 0L;

        pulsarTestContext.getPulsarResources().getNamespaceResources()
                .createPolicies(TopicName.get(successTopicName).getNamespaceObject(),
                        policies);

        doReturn(1000L).when(ledgerMock).getEstimatedBacklogSize();

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.initialize().get();
        topic.checkCompaction();
        verify(compactor, times(0)).compact(anyString());
    }

    @Test
    public void testBacklogCursor() throws Exception {
        int backloggedThreshold = 10;
        pulsarTestContext.getConfig().setManagedLedgerCursorBackloggedThreshold(backloggedThreshold);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cache_backlog_ledger");
        PersistentTopic topic = new PersistentTopic(successTopicName, ledger, brokerService);

        // STEP1: prepare cursors
        // Open cursor1, add it into activeCursor-container and add it into subscription consumer list
        ManagedCursor cursor1 = ledger.openCursor("c1");
        PersistentSubscription sub1 = new PersistentSubscription(topic, "sub-1", cursor1, false);
        Consumer consumer1 = new Consumer(sub1, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */,
                true, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest,
                DEFAULT_CONSUMER_EPOCH);
        topic.getSubscriptions().put(Codec.decode(cursor1.getName()), sub1);
        sub1.addConsumer(consumer1);
        // Open cursor2, add it into activeCursor-container and add it into subscription consumer list
        ManagedCursor cursor2 = ledger.openCursor("c2");
        PersistentSubscription sub2 = new PersistentSubscription(topic, "sub-2", cursor2, false);
        Consumer consumer2 = new Consumer(sub2, SubType.Exclusive, topic.getName(), 2 /* consumer id */, 0,
                "Cons2"/* consumer name */,
                true, serverCnx, "myrole-2", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest,
                DEFAULT_CONSUMER_EPOCH);
        topic.getSubscriptions().put(Codec.decode(cursor2.getName()), sub2);
        sub2.addConsumer(consumer2);
        // Open cursor3, add it into activeCursor-container and do not add it into subscription consumer list
        ManagedCursor cursor3 = ledger.openCursor("c3");
        PersistentSubscription sub3 = new PersistentSubscription(topic, "sub-3", cursor3, false);
        Consumer consumer3 = new Consumer(sub2, SubType.Exclusive, topic.getName(), 3 /* consumer id */, 0,
                "Cons2"/* consumer name */,
                true, serverCnx, "myrole-3", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest,
                DEFAULT_CONSUMER_EPOCH);
        topic.getSubscriptions().put(Codec.decode(cursor3.getName()), sub3);

        // Case1: cursors are active as haven't started deactivateBacklogCursor scan
        assertTrue(cursor1.isActive());
        assertTrue(cursor2.isActive());
        assertTrue(cursor3.isActive());

        // deactivate cursor which consumer list is empty
        topic.checkBackloggedCursors();

        // Case2: cursor3 change to be inactive as it does not include consumer
        assertTrue(cursor1.isActive());
        assertTrue(cursor2.isActive());
        assertFalse(cursor3.isActive());

        // Write messages to ledger
        CountDownLatch latch = new CountDownLatch(backloggedThreshold);
        for (int i = 0; i < backloggedThreshold + 1; i++) {
            String content = "entry"; // 5 bytes
            ByteBuf entry = getMessageWithMetadata(content.getBytes());
            ledger.asyncAddEntry(entry, new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    latch.countDown();
                    entry.release();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                    entry.release();
                }

            }, null);
        }
        latch.await();

        assertTrue(cursor1.isActive());
        assertTrue(cursor2.isActive());
        assertFalse(cursor3.isActive());

        // deactivate backlog cursors
        topic.checkBackloggedCursors();

        // Case3: cursor1 and cursor2 change to be inactive because of the backlog
        assertFalse(cursor1.isActive());
        assertFalse(cursor2.isActive());
        assertFalse(cursor3.isActive());

        // read entries so, cursor1 reaches maxBacklog threshold again to be active again
        List<Entry> entries1 = cursor1.readEntries(50);
        for (Entry entry : entries1) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }
        List<Entry> entries3 = cursor3.readEntries(50);
        for (Entry entry : entries3) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }

        // activate cursors which caught up maxbacklog threshold
        topic.checkBackloggedCursors();

        // Case4:
        // cursor1 has consumed messages so, under maxBacklog threshold => active
        assertTrue(cursor1.isActive());
        // cursor2 has not consumed messages so, above maxBacklog threshold => inactive
        assertFalse(cursor2.isActive());
        // cursor3 has not consumer so do not change to active
        assertFalse(cursor3.isActive());

        // add consumer to sub3 and read entries
        sub3.addConsumer(consumer3);
        entries3 = cursor3.readEntries(50);
        for (Entry entry : entries3) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }

        topic.checkBackloggedCursors();
        // Case5: cursor3 has consumer so change to active
        assertTrue(cursor3.isActive());
    }

    @Test
    public void testCheckInactiveSubscriptions() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions =
                ConcurrentOpenHashMap.<String, PersistentSubscription>newBuilder()
                        .expectedItems(16)
                        .concurrencyLevel(1)
                        .build();
        // This subscription is connected by consumer.
        PersistentSubscription nonDeletableSubscription1 =
                spyWithClassAndConstructorArgsRecordingInvocations(PersistentSubscription.class, topic,
                        "nonDeletableSubscription1", cursorMock, false);
        subscriptions.put(nonDeletableSubscription1.getName(), nonDeletableSubscription1);
        // This subscription is not connected by consumer.
        PersistentSubscription deletableSubscription1 =
                spyWithClassAndConstructorArgsRecordingInvocations(PersistentSubscription.class, topic,
                        "deletableSubscription1", cursorMock, false);
        subscriptions.put(deletableSubscription1.getName(), deletableSubscription1);
        // This subscription is replicated.
        PersistentSubscription nonDeletableSubscription2 =
                spyWithClassAndConstructorArgsRecordingInvocations(PersistentSubscription.class, topic,
                        "nonDeletableSubscription2", cursorMock, true);
        subscriptions.put(nonDeletableSubscription2.getName(), nonDeletableSubscription2);

        Field field = topic.getClass().getDeclaredField("subscriptions");
        field.setAccessible(true);
        field.set(topic, subscriptions);

        Method addConsumerToSubscription = AbstractTopic.class.getDeclaredMethod("addConsumerToSubscription",
                Subscription.class, Consumer.class);
        addConsumerToSubscription.setAccessible(true);

        Consumer consumer = new Consumer(nonDeletableSubscription1, SubType.Shared, topic.getName(), 1, 0, "consumer1",
                true, serverCnx, "app1", Collections.emptyMap(), false, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        addConsumerToSubscription.invoke(topic, nonDeletableSubscription1, consumer);

        pulsarTestContext.getPulsarResources().getNamespaceResources()
                .createPolicies(TopicName.get(successTopicName).getNamespaceObject(),
                        new Policies());

        pulsarTestContext.getConfig().setSubscriptionExpirationTimeMinutes(5);

        doReturn(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6)).when(cursorMock).getLastActive();

        topic.checkInactiveSubscriptions();

        verify(nonDeletableSubscription1, times(0)).delete();
        verify(deletableSubscription1, times(1)).delete();
        verify(nonDeletableSubscription2, times(0)).delete();
    }

    @Test
    public void testTopicFencingTimeout() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        Method fence = PersistentTopic.class.getDeclaredMethod("fence");
        fence.setAccessible(true);
        Method unfence = PersistentTopic.class.getDeclaredMethod("unfence");
        unfence.setAccessible(true);

        Field fencedTopicMonitoringTaskField = PersistentTopic.class.getDeclaredField("fencedTopicMonitoringTask");
        fencedTopicMonitoringTaskField.setAccessible(true);
        Field isFencedField = AbstractTopic.class.getDeclaredField("isFenced");
        isFencedField.setAccessible(true);
        Field isClosingOrDeletingField = PersistentTopic.class.getDeclaredField("isClosingOrDeleting");
        isClosingOrDeletingField.setAccessible(true);

        pulsarTestContext.getConfig().setTopicFencingTimeoutSeconds(10);
        fence.invoke(topic);
        unfence.invoke(topic);
        ScheduledFuture<?> fencedTopicMonitoringTask = (ScheduledFuture<?>) fencedTopicMonitoringTaskField.get(topic);
        assertTrue(fencedTopicMonitoringTask.isDone());
        assertTrue(fencedTopicMonitoringTask.isCancelled());
        assertFalse((boolean) isFencedField.get(topic));
        assertFalse((boolean) isClosingOrDeletingField.get(topic));

        pulsarTestContext.getConfig().setTopicFencingTimeoutSeconds(1);
        fence.invoke(topic);
        Thread.sleep(2000);
        fencedTopicMonitoringTask = (ScheduledFuture<?>) fencedTopicMonitoringTaskField.get(topic);
        assertTrue(fencedTopicMonitoringTask.isDone());
        assertFalse(fencedTopicMonitoringTask.isCancelled());
        assertTrue((boolean) isFencedField.get(topic));
        assertTrue((boolean) isClosingOrDeletingField.get(topic));
    }

    @Test
    public void testTopicCloseFencingTimeout() throws Exception {
        pulsarTestContext.getConfig().setTopicFencingTimeoutSeconds(10);
        Method fence = PersistentTopic.class.getDeclaredMethod("fence");
        fence.setAccessible(true);
        Field fencedTopicMonitoringTaskField = PersistentTopic.class.getDeclaredField("fencedTopicMonitoringTask");
        fencedTopicMonitoringTaskField.setAccessible(true);

        // create topic
        PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();

        // fence topic to init fencedTopicMonitoringTask
        fence.invoke(topic);

        // close topic
        topic.close().get();
        assertFalse(brokerService.getTopicReference(successTopicName).isPresent());
        ScheduledFuture<?> fencedTopicMonitoringTask = (ScheduledFuture<?>) fencedTopicMonitoringTaskField.get(topic);
        assertTrue(fencedTopicMonitoringTask.isDone());
        assertTrue(fencedTopicMonitoringTask.isCancelled());
    }

    @Test
    public void testGetDurableSubscription() throws Exception {
        ManagedLedger mockLedger = mock(ManagedLedger.class);
        doReturn(new ManagedLedgerConfig()).when(mockLedger).getConfig();
        ManagedCursor mockCursor = mock(ManagedCursorImpl.class);
        Position mockPosition = mock(Position.class);
        doReturn("test").when(mockCursor).getName();
        doAnswer((Answer<Object>) invocationOnMock -> {
            ((AsyncCallbacks.FindEntryCallback) invocationOnMock.getArguments()[2]).findEntryComplete(mockPosition,
                    invocationOnMock.getArguments()[3]);
            return null;
        }).when(mockCursor).asyncFindNewestMatching(any(), any(), any(), any());
        doAnswer((Answer<Object>) invocationOnMock -> {
            ((AsyncCallbacks.ResetCursorCallback) invocationOnMock.getArguments()[1]).resetComplete(null);
            return null;
        }).when(mockCursor).asyncResetCursor(any(), anyBoolean(), any());
        doAnswer((Answer<Object>) invocationOnMock -> {
            ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
            return null;
        }).when(mockLedger).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());
        doAnswer((Answer<Object>) invocationOnMock -> {
            ((OpenCursorCallback) invocationOnMock.getArguments()[4]).openCursorComplete(mockCursor, null);
            return null;
        }).when(mockLedger).asyncOpenCursor(any(), any(), any(), any(), any(), any());
        PersistentTopic topic = new PersistentTopic(successTopicName, mockLedger, brokerService);

        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setDurable(true)
                .setStartMessageRollbackDurationSec(60)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setConsumerName("consumer-name")
                .setReadCompacted(false)
                .setRequestId(1)
                .setSubType(SubType.Exclusive);

        Future<Consumer> f1 = topic.subscribe(getSubscriptionOption(cmd));
        f1.get();

        Future<Void> f2 = topic.unsubscribe(successSubName);
        f2.get();
    }

    @Test
    public void testDisconnectProducer() {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        String role = "appid1";
        Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, false,
                ProducerAccessMode.Shared, Optional.empty(), true);
        assertFalse(producer.isDisconnecting());
        // Disconnect the producer multiple times.
        producer.disconnect();
        producer.disconnect();
        verify(serverCnx).execute(any());
    }

    @Test
    public void testKeySharedMetadataExposedToStats() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub1 = new PersistentSubscription(topic, "key-shared-stats1", cursorMock, false);
        PersistentSubscription sub2 = new PersistentSubscription(topic, "key-shared-stats2", cursorMock, false);
        PersistentSubscription sub3 = new PersistentSubscription(topic, "key-shared-stats3", cursorMock, false);

        Consumer consumer1 = new Consumer(sub1, SubType.Key_Shared, topic.getName(), 1, 0, "Cons1", true, serverCnx,
                "myrole-1", Collections.emptyMap(), false,
                new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT).setAllowOutOfOrderDelivery(false),
                MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        sub1.addConsumer(consumer1);
        consumer1.close();

        SubscriptionStatsImpl stats1 = sub1.getStats(false, false, false);
        assertEquals(stats1.keySharedMode, "AUTO_SPLIT");
        assertFalse(stats1.allowOutOfOrderDelivery);

        Consumer consumer2 = new Consumer(sub2, SubType.Key_Shared, topic.getName(), 2, 0, "Cons2", true, serverCnx,
                "myrole-1", Collections.emptyMap(), false,
                new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT).setAllowOutOfOrderDelivery(true),
                MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        sub2.addConsumer(consumer2);
        consumer2.close();

        SubscriptionStatsImpl stats2 = sub2.getStats(false, false, false);
        assertEquals(stats2.keySharedMode, "AUTO_SPLIT");
        assertTrue(stats2.allowOutOfOrderDelivery);

        KeySharedMeta ksm = new KeySharedMeta().setKeySharedMode(KeySharedMode.STICKY)
                .setAllowOutOfOrderDelivery(false);
        ksm.addHashRange().setStart(0).setEnd(65535);
        Consumer consumer3 = new Consumer(sub3, SubType.Key_Shared, topic.getName(), 3, 0, "Cons3", true, serverCnx,
                "myrole-1", Collections.emptyMap(), false, ksm, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        sub3.addConsumer(consumer3);
        consumer3.close();

        SubscriptionStatsImpl stats3 = sub3.getStats(false, false, false);
        assertEquals(stats3.keySharedMode, "STICKY");
        assertFalse(stats3.allowOutOfOrderDelivery);
    }

    private ByteBuf getMessageWithMetadata(byte[] data) {
        MessageMetadata messageData = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        ByteBuf payload = Unpooled.wrappedBuffer(data, 0, data.length);

        int msgMetadataSize = messageData.getSerializedSize();
        int headersSize = 4 + msgMetadataSize;
        ByteBuf headers = PulsarByteBufAllocator.DEFAULT.buffer(headersSize, headersSize);
        headers.writeInt(msgMetadataSize);
        messageData.writeTo(headers);
        return ByteBufPair.coalesce(ByteBufPair.get(headers, payload));
    }

    @Test
    public void testGetReplicationClusters() throws MetadataStoreException {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.initialize().join();
        assertEquals(topic.getHierarchyTopicPolicies().getReplicationClusters().get(), Collections.emptyList());

        Policies policies = new Policies();
        Set<String> namespaceClusters = new HashSet<>();
        namespaceClusters.add("namespace-cluster");
        policies.replication_clusters = namespaceClusters;
        pulsarTestContext.getPulsarResources().getNamespaceResources()
                .createPolicies(TopicName.get(successTopicName).getNamespaceObject(), policies);

        topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.initialize().join();
        assertEquals(topic.getHierarchyTopicPolicies().getReplicationClusters().get(), namespaceClusters);

        TopicPoliciesService topicPoliciesService = mock(TopicPoliciesService.class);
        doReturn(topicPoliciesService).when(pulsarTestContext.getPulsarService()).getTopicPoliciesService();
        CompletableFuture<Optional<TopicPolicies>> topicPoliciesFuture = new CompletableFuture<>();
        TopicPolicies topicPolicies = new TopicPolicies();
        List<String> topicClusters = new ArrayList<>();
        topicClusters.add("topic-cluster");
        topicPolicies.setReplicationClusters(topicClusters);
        Optional<TopicPolicies> optionalTopicPolicies = Optional.of(topicPolicies);
        topicPoliciesFuture.complete(optionalTopicPolicies);
        when(topicPoliciesService.getTopicPoliciesIfExists(any())).thenReturn(topicPolicies);

        topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        topic.initialize().join();
        assertEquals(topic.getHierarchyTopicPolicies().getReplicationClusters().get(), namespaceClusters);
    }

    @Test
    public void testSendProducerTxnPrechecks() throws Exception {
        PersistentTopic topic = mock(PersistentTopic.class);
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            Object msg = invocation.getArgument(0);
            ReferenceCountUtil.safeRelease(msg);
            latch.countDown();
            return mock(ChannelFuture.class);
        }).when(ctx).writeAndFlush(any(), any());
        String role = "appid1";
        Producer producer1 = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name",
                role, false, null, SchemaVersion.Latest, 0, true,
                ProducerAccessMode.Shared, Optional.empty(), true);
        producer1.close(false).get();
        ByteBuf headersAndPayload = Unpooled.wrappedBuffer("test".getBytes());
        producer1.publishTxnMessage(
                new TxnID(1L, 0L),
                1, 1, 1, headersAndPayload, 1, false, false
        );
        verify(topic, times(0)).publishTxnMessage(any(), any(), any());
        // wait for the writeAndFlush to be called so that ByteBuf leak isn't reported
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

}
