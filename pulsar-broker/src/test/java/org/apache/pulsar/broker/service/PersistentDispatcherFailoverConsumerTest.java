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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentDispatcherFailoverConsumerTest {

    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    private ServerCnx serverCnxWithOldVersion;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private MetadataStore store;
    private ChannelHandlerContext channelCtx;
    private LinkedBlockingQueue<CommandActiveConsumerChange> consumerChanges;
    private ZooKeeper mockZk;
    protected PulsarService pulsar;
    final String successTopicName = "persistent://part-perf/global/perf.t1/ptopic";
    final String failTopicName = "persistent://part-perf/global/perf.t1/pfailTopic";

    private OrderedExecutor executor;
    private EventLoopGroup eventLoopGroup;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("persistent-dispatcher-failover-test").build();
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        pulsar = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsar).getConfiguration();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        doReturn(TransactionTestBase.createMockBookKeeper(executor))
                .when(pulsar).getBookKeeperClient();
        eventLoopGroup = new NioEventLoopGroup();

        store = MetadataStoreFactory.create("memory://local", MetadataStoreConfig.builder().build());
        doReturn(store).when(pulsar).getLocalMetadataStore();
        doReturn(store).when(pulsar).getConfigurationMetadataStore();

        PulsarResources pulsarResources = new PulsarResources(store, store);
        doReturn(pulsarResources).when(pulsar).getPulsarResources();

        brokerService = spy(new BrokerService(pulsar, eventLoopGroup));
        doReturn(brokerService).when(pulsar).getBrokerService();

        consumerChanges = new LinkedBlockingQueue<>();
        this.channelCtx = mock(ChannelHandlerContext.class);
        doAnswer(invocationOnMock -> {
            ByteBuf buf = invocationOnMock.getArgument(0);

            ByteBuf cmdBuf = buf.retainedSlice(4, buf.writerIndex() - 4);
            try {
                int cmdSize = (int) cmdBuf.readUnsignedInt();
                int writerIndex = cmdBuf.writerIndex();

                BaseCommand cmd = new BaseCommand();
                cmd.parseFrom(cmdBuf, cmdSize);

                if (cmd.hasActiveConsumerChange()) {
                    consumerChanges.put(cmd.getActiveConsumerChange());
                }
            } finally {
                cmdBuf.release();
            }

            return null;
        }).when(channelCtx).writeAndFlush(any(), any());

        serverCnx = spy(new ServerCnx(pulsar));
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();
        when(serverCnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12.getValue());
        when(serverCnx.ctx()).thenReturn(channelCtx);
        doReturn(new PulsarCommandSenderImpl(null, serverCnx))
                .when(serverCnx).getCommandSender();

        serverCnxWithOldVersion = spy(new ServerCnx(pulsar));
        doReturn(true).when(serverCnxWithOldVersion).isActive();
        doReturn(true).when(serverCnxWithOldVersion).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234))
            .when(serverCnxWithOldVersion).clientAddress();
        when(serverCnxWithOldVersion.getRemoteEndpointProtocolVersion())
            .thenReturn(ProtocolVersion.v11.getValue());
        when(serverCnxWithOldVersion.ctx()).thenReturn(channelCtx);
        doReturn(new PulsarCommandSenderImpl(null, serverCnxWithOldVersion))
                .when(serverCnxWithOldVersion).getCommandSender();

        NamespaceService nsSvc = mock(NamespaceService.class);
        doReturn(nsSvc).when(pulsar).getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(true).when(nsSvc).isServiceUnitActive(any(TopicName.class));
        doReturn(CompletableFuture.completedFuture(true)).when(nsSvc).checkTopicOwnership(any(TopicName.class));

        setupMLAsyncCallbackMocks();

    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() throws Exception {
        if (brokerService != null) {
            brokerService.close();
            brokerService = null;
        }
        if (pulsar != null) {
            pulsar.close();
            pulsar = null;
        }

        executor.shutdown();
        eventLoopGroup.shutdownGracefully().get();
        store.close();
    }

    void setupMLAsyncCallbackMocks() {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursorImpl.class);

        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();

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
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(
                        new PositionImpl(1, 1), null, null);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(byte[].class), any(AddEntryCallback.class), any());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class), any());

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
    }

    private void verifyActiveConsumerChange(CommandActiveConsumerChange change,
                                            long consumerId,
                                            boolean isActive) {
        assertEquals(consumerId, change.getConsumerId());
        assertEquals(isActive, change.isIsActive());
    }

    @Test
    public void testConsumerGroupChangesWithOldNewConsumers() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);

        int partitionIndex = 0;
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic, sub);

        // 1. Verify no consumers connected
        assertFalse(pdfc.isConsumerConnected());

        // 2. Add old consumer
        Consumer consumer1 = new Consumer(sub, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */, 50000, serverCnxWithOldVersion, "myrole-1", Collections.emptyMap(), false, InitialPosition.Latest, null, MessageId.latest);
        pdfc.addConsumer(consumer1);
        List<Consumer> consumers = pdfc.getConsumers();
        assertSame(consumers.get(0).consumerName(), consumer1.consumerName());
        assertEquals(1, consumers.size());
        assertNull(consumerChanges.poll());

        verify(channelCtx, times(0)).write(any());

        // 3. Add new consumer
        Consumer consumer2 = new Consumer(sub, SubType.Exclusive, topic.getName(), 2 /* consumer id */, 0,
                "Cons2"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(), false, InitialPosition.Latest, null, MessageId.latest);
        pdfc.addConsumer(consumer2);
        consumers = pdfc.getConsumers();
        assertSame(consumers.get(0).consumerName(), consumer1.consumerName());
        assertEquals(2, consumers.size());

        CommandActiveConsumerChange change = consumerChanges.take();
        verifyActiveConsumerChange(change, 2, false);

        verify(channelCtx, times(1)).writeAndFlush(any(), any());
    }

    @Test
    public void testAddRemoveConsumer() throws Exception {
        log.info("--- Starting PersistentDispatcherFailoverConsumerTest::testAddConsumer ---");

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);

        int partitionIndex = 4;
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic, sub);

        // 1. Verify no consumers connected
        assertFalse(pdfc.isConsumerConnected());

        // 2. Add consumer
        Consumer consumer1 = spy(new Consumer(sub, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null, MessageId.latest));
        pdfc.addConsumer(consumer1);
        List<Consumer> consumers = pdfc.getConsumers();
        assertSame(consumers.get(0).consumerName(), consumer1.consumerName());
        assertEquals(1, consumers.size());
        CommandActiveConsumerChange change = consumerChanges.take();
        verifyActiveConsumerChange(change, 1, true);
        verify(consumer1, times(1)).notifyActiveConsumerChange(same(consumer1));

        // 3. Add again, duplicate allowed
        pdfc.addConsumer(consumer1);
        consumers = pdfc.getConsumers();
        assertSame(consumers.get(0).consumerName(), consumer1.consumerName());
        assertEquals(2, consumers.size());

        // 4. Verify active consumer
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        // get the notified with who is the leader
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 1, true);
        verify(consumer1, times(2)).notifyActiveConsumerChange(same(consumer1));

        // 5. Add another consumer which does not change active consumer
        Consumer consumer2 = spy(new Consumer(sub, SubType.Exclusive, topic.getName(), 2 /* consumer id */, 0, "Cons2"/* consumer name */,
                50000, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, InitialPosition.Latest, null, MessageId.latest));
        pdfc.addConsumer(consumer2);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        assertEquals(3, consumers.size());
        // get notified with who is the leader
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 2, false);
        verify(consumer1, times(2)).notifyActiveConsumerChange(same(consumer1));
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer1));

        // 6. Add a consumer which changes active consumer
        Consumer consumer0 = spy(new Consumer(sub, SubType.Exclusive, topic.getName(), 0 /* consumer id */, 0,
                "Cons0"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null, MessageId.latest));
        pdfc.addConsumer(consumer0);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer0.consumerName());
        assertEquals(4, consumers.size());

        // all consumers will receive notifications
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 0, true);
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 1, false);
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 1, false);
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 2, false);
        verify(consumer0, times(1)).notifyActiveConsumerChange(same(consumer0));
        verify(consumer1, times(2)).notifyActiveConsumerChange(same(consumer1));
        verify(consumer1, times(2)).notifyActiveConsumerChange(same(consumer0));
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer1));
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer0));

        // 7. Remove last consumer
        pdfc.removeConsumer(consumer2);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        assertEquals(3, consumers.size());
        // not consumer group changes
        assertNull(consumerChanges.poll());

        // 8. Verify if we cannot unsubscribe when more than one consumer is connected
        assertFalse(pdfc.canUnsubscribe(consumer0));

        // 9. Remove active consumer
        pdfc.removeConsumer(consumer0);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        assertEquals(2, consumers.size());

        // the remaining consumers will receive notifications
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 1, true);
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 1, true);

        // 10. Attempt to remove already removed consumer
        String cause = "";
        try {
            pdfc.removeConsumer(consumer0);
        } catch (Exception e) {
            cause = e.getMessage();
        }
        assertEquals(cause, "Consumer was not connected");

        // 11. Remove active consumer
        pdfc.removeConsumer(consumer1);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        assertEquals(1, consumers.size());
        // not consumer group changes
        assertNull(consumerChanges.poll());

        // 11. With only one consumer, unsubscribe is allowed
        assertTrue(pdfc.canUnsubscribe(consumer1));
    }

    @Test
    public void testAddRemoveConsumerNonPartitionedTopic() throws Exception {
        log.info("--- Starting PersistentDispatcherFailoverConsumerTest::testAddConsumer ---");

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);

        // Non partitioned topic.
        int partitionIndex = -1;
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic, sub);

        // 1. Verify no consumers connected
        assertFalse(pdfc.isConsumerConnected());

        // 2. Add a consumer
        Consumer consumer1 = spy(new Consumer(sub, SubType.Failover, topic.getName(), 1 /* consumer id */, 1,
                "Cons1"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null, MessageId.latest));
        pdfc.addConsumer(consumer1);
        List<Consumer> consumers = pdfc.getConsumers();
        assertEquals(1, consumers.size());
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());

        // 3. Add a consumer with same priority level and consumer name is smaller in lexicographic order.
        Consumer consumer2 = spy(new Consumer(sub, SubType.Failover, topic.getName(), 2 /* consumer id */, 1,
                "Cons2"/* consumer name */, 50000, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, InitialPosition.Latest, null, MessageId.latest));
        pdfc.addConsumer(consumer2);

        // 4. Verify active consumer doesn't change
        consumers = pdfc.getConsumers();
        assertEquals(2, consumers.size());
        CommandActiveConsumerChange change = consumerChanges.take();
        verifyActiveConsumerChange(change, 2, false);
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer1));

        // 5. Add another consumer which has higher priority level
        Consumer consumer3 = spy(new Consumer(sub, SubType.Failover, topic.getName(), 3 /* consumer id */, 0, "Cons3"/* consumer name */,
                50000, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, InitialPosition.Latest, null, MessageId.latest));
        pdfc.addConsumer(consumer3);
        consumers = pdfc.getConsumers();
        assertEquals(3, consumers.size());
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 3, false);
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        verify(consumer3, times(1)).notifyActiveConsumerChange(same(consumer1));

        // 7. Remove first consumer and active consumer should change to consumer2 since it's added before consumer3
        // though consumer 3 has higher priority level
        pdfc.removeConsumer(consumer1);
        consumers = pdfc.getConsumers();
        assertEquals(2, consumers.size());
        change = consumerChanges.take();
        verifyActiveConsumerChange(change, 2, true);
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer2.consumerName());
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer2));
        verify(consumer3, times(1)).notifyActiveConsumerChange(same(consumer2));
    }

    @Test
    public void testMultipleDispatcherGetNextConsumerWithDifferentPriorityLevel() throws Exception {

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock, null);
        Consumer consumer1 = createConsumer(0, 2, false, 1);
        Consumer consumer2 = createConsumer(0, 2, false, 2);
        Consumer consumer3 = createConsumer(0, 2, false, 3);
        Consumer consumer4 = createConsumer(1, 2, false, 4);
        Consumer consumer5 = createConsumer(1, 1, false, 5);
        Consumer consumer6 = createConsumer(1, 2, false, 6);
        Consumer consumer7 = createConsumer(2, 1, false, 7);
        Consumer consumer8 = createConsumer(2, 1, false, 8);
        Consumer consumer9 = createConsumer(2, 1, false, 9);
        dispatcher.addConsumer(consumer1);
        dispatcher.addConsumer(consumer2);
        dispatcher.addConsumer(consumer3);
        dispatcher.addConsumer(consumer4);
        dispatcher.addConsumer(consumer5);
        dispatcher.addConsumer(consumer6);
        dispatcher.addConsumer(consumer7);
        dispatcher.addConsumer(consumer8);
        dispatcher.addConsumer(consumer9);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer5);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer6);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer6);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer7);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer8);
        // in between add upper priority consumer with more permits
        Consumer consumer10 = createConsumer(0, 2, false, 10);
        dispatcher.addConsumer(consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer9);

    }

    @Test
    public void testFewBlockedConsumerSamePriority() throws Exception{
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock, null);
        Consumer consumer1 = createConsumer(0, 2, false, 1);
        Consumer consumer2 = createConsumer(0, 2, false, 2);
        Consumer consumer3 = createConsumer(0, 2, false, 3);
        Consumer consumer4 = createConsumer(0, 2, false, 4);
        Consumer consumer5 = createConsumer(0, 1, true, 5);
        Consumer consumer6 = createConsumer(0, 2, true, 6);
        dispatcher.addConsumer(consumer1);
        dispatcher.addConsumer(consumer2);
        dispatcher.addConsumer(consumer3);
        dispatcher.addConsumer(consumer4);
        dispatcher.addConsumer(consumer5);
        dispatcher.addConsumer(consumer6);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        assertNull(getNextConsumer(dispatcher));
    }

    @Test
    public void testFewBlockedConsumerDifferentPriority() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock, null);
        Consumer consumer1 = createConsumer(0, 2, false, 1);
        Consumer consumer2 = createConsumer(0, 2, false, 2);
        Consumer consumer3 = createConsumer(0, 2, false, 3);
        Consumer consumer4 = createConsumer(0, 2, false, 4);
        Consumer consumer5 = createConsumer(0, 1, true, 5);
        Consumer consumer6 = createConsumer(0, 2, true, 6);
        Consumer consumer7 = createConsumer(1, 2, false, 7);
        Consumer consumer8 = createConsumer(1, 10, true, 8);
        Consumer consumer9 = createConsumer(1, 2, false, 9);
        Consumer consumer10 = createConsumer(2, 2, false, 10);
        Consumer consumer11 = createConsumer(2, 10, true, 11);
        Consumer consumer12 = createConsumer(2, 2, false, 12);
        dispatcher.addConsumer(consumer1);
        dispatcher.addConsumer(consumer2);
        dispatcher.addConsumer(consumer3);
        dispatcher.addConsumer(consumer4);
        dispatcher.addConsumer(consumer5);
        dispatcher.addConsumer(consumer6);
        dispatcher.addConsumer(consumer7);
        dispatcher.addConsumer(consumer8);
        dispatcher.addConsumer(consumer9);
        dispatcher.addConsumer(consumer10);
        dispatcher.addConsumer(consumer11);
        dispatcher.addConsumer(consumer12);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer7);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer9);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer7);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer9);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer12);
        // add consumer with lower priority again
        Consumer consumer13 = createConsumer(0, 2, false, 13);
        Consumer consumer14 = createConsumer(0, 2, true, 14);
        dispatcher.addConsumer(consumer13);
        dispatcher.addConsumer(consumer14);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer13);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer13);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer12);
        assertNull(getNextConsumer(dispatcher));
    }

    @Test
    public void testFewBlockedConsumerDifferentPriority2() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock, null);
        Consumer consumer1 = createConsumer(0, 2, true, 1);
        Consumer consumer2 = createConsumer(0, 2, true, 2);
        Consumer consumer3 = createConsumer(0, 2, true, 3);
        Consumer consumer4 = createConsumer(1, 2, false, 4);
        Consumer consumer5 = createConsumer(1, 1, false, 5);
        Consumer consumer6 = createConsumer(2, 1, false, 6);
        Consumer consumer7 = createConsumer(2, 2, true, 7);
        dispatcher.addConsumer(consumer1);
        dispatcher.addConsumer(consumer2);
        dispatcher.addConsumer(consumer3);
        dispatcher.addConsumer(consumer4);
        dispatcher.addConsumer(consumer5);
        dispatcher.addConsumer(consumer6);
        dispatcher.addConsumer(consumer7);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer5);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer6);
        assertNull(getNextConsumer(dispatcher));
    }

    @SuppressWarnings("unchecked")
    private Consumer getNextConsumer(PersistentDispatcherMultipleConsumers dispatcher) throws Exception {

        Consumer consumer = dispatcher.getNextConsumer();

        if (consumer != null) {
            Field field = Consumer.class.getDeclaredField("MESSAGE_PERMITS_UPDATER");
            field.setAccessible(true);
            AtomicIntegerFieldUpdater<Consumer> messagePermits = (AtomicIntegerFieldUpdater<Consumer>) field.get(consumer);
            messagePermits.decrementAndGet(consumer);
            return consumer;
        }
        return null;
    }

    private Consumer createConsumer(int priority, int permit, boolean blocked, int id) throws Exception {
        Consumer consumer =
                new Consumer(null, SubType.Shared, "test-topic", id, priority, ""+id, 5000,
                        serverCnx, "appId", Collections.emptyMap(), false /* read compacted */, InitialPosition.Latest, null, MessageId.latest);
        try {
            consumer.flowPermits(permit);
        } catch (Exception e) {
        }
        // set consumer blocked flag
        Field blockField = Consumer.class.getDeclaredField("blockedConsumerOnUnackedMsgs");
        blockField.setAccessible(true);
        blockField.set(consumer, blocked);
        return consumer;
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherFailoverConsumerTest.class);

}
