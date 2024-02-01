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

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "quarantine")
public class PersistentDispatcherFailoverConsumerTest {

    private ServerCnx serverCnx;
    private ServerCnx serverCnxWithOldVersion;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private ChannelHandlerContext channelCtx;
    private LinkedBlockingQueue<CommandActiveConsumerChange> consumerChanges;

    protected PulsarTestContext pulsarTestContext;

    final String successTopicName = "persistent://part-perf/global/perf.t1/ptopic";
    final String failTopicName = "persistent://part-perf/global/perf.t1/pfailTopic";

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = new ServiceConfiguration();
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("pulsar-cluster");
        svcConfig.setSystemTopicEnabled(false);
        svcConfig.setTopicLevelPoliciesEnabled(false);
        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .spyByDefault()
                .build();

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

        serverCnx = pulsarTestContext.createServerCnxSpy();
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();
        when(serverCnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12.getValue());
        when(serverCnx.ctx()).thenReturn(channelCtx);
        doReturn(new PulsarCommandSenderImpl(null, serverCnx))
                .when(serverCnx).getCommandSender();

        serverCnxWithOldVersion = pulsarTestContext.createServerCnxSpy();
        doReturn(true).when(serverCnxWithOldVersion).isActive();
        doReturn(true).when(serverCnxWithOldVersion).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234))
            .when(serverCnxWithOldVersion).clientAddress();
        when(serverCnxWithOldVersion.getRemoteEndpointProtocolVersion())
            .thenReturn(ProtocolVersion.v11.getValue());
        when(serverCnxWithOldVersion.ctx()).thenReturn(channelCtx);
        doReturn(new PulsarCommandSenderImpl(null, serverCnxWithOldVersion))
                .when(serverCnxWithOldVersion).getCommandSender();

        NamespaceService nsSvc = pulsarTestContext.getPulsarService().getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(true).when(nsSvc).isServiceUnitActive(any(TopicName.class));
        doReturn(CompletableFuture.completedFuture(true)).when(nsSvc).checkTopicOwnership(any(TopicName.class));

        setupMLAsyncCallbackMocks();

    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }
    }

    void setupMLAsyncCallbackMocks() {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursorImpl.class);

        doReturn(new ArrayList<>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();

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
            ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(
                    new PositionImpl(1, 1), null, null);
            return null;
        }).when(ledgerMock).asyncAddEntry(any(byte[].class), any(AddEntryCallback.class), any());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(invocationOnMock -> {
            ((OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
            return null;
        }).when(ledgerMock)
                .asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class),
                        any());

        // call deleteLedgerComplete on ledger asyncDelete
        doAnswer(invocationOnMock -> {
            ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
            return null;
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), any());

        doAnswer(invocationOnMock -> {
            ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
            return null;
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());
    }

    private void verifyActiveConsumerChange(CommandActiveConsumerChange change,
                                            long consumerId,
                                            boolean isActive) {
        assertEquals(consumerId, change.getConsumerId());
        assertEquals(isActive, change.isIsActive());
    }

    @Test(timeOut = 10000)
    public void testAddConsumerWhenClosed() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, 0, topic, sub);
        pdfc.close().get();

        Consumer consumer = mock(Consumer.class);
        pdfc.addConsumer(consumer);
        verify(consumer, times(1)).disconnect();
        assertEquals(0, pdfc.consumers.size());
    }

    @Test
    public void testConsumerGroupChangesWithOldNewConsumers() throws Exception {
        PersistentTopic topic =
                new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);

        int partitionIndex = 0;
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic, sub);

        // 1. Verify no consumers connected
        assertFalse(pdfc.isConsumerConnected());

        // 2. Add old consumer
        Consumer consumer1 = new Consumer(sub, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */, true, serverCnxWithOldVersion, "myrole-1", Collections.emptyMap(), false,
                null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        pdfc.addConsumer(consumer1);
        List<Consumer> consumers = pdfc.getConsumers();
        assertSame(consumers.get(0).consumerName(), consumer1.consumerName());
        assertEquals(1, consumers.size());
        assertNull(consumerChanges.poll());

        verify(channelCtx, times(0)).write(any());

        // 3. Add new consumer
        Consumer consumer2 = new Consumer(sub, SubType.Exclusive, topic.getName(), 2 /* consumer id */, 0,
                "Cons2"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(), false, null,
                MessageId.latest, DEFAULT_CONSUMER_EPOCH);
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

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);

        int partitionIndex = 4;
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic, sub);

        // 1. Verify no consumers connected
        assertFalse(pdfc.isConsumerConnected());

        // 2. Add consumer
        Consumer consumer1 = spy(new Consumer(sub, SubType.Exclusive, topic.getName(), 1 /* consumer id */, 0,
                "Cons1"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH));
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
        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 1, true);
        verify(consumer1, times(2)).notifyActiveConsumerChange(same(consumer1));

        // 5. Add another consumer which does not change active consumer
        Consumer consumer2 = spy(new Consumer(sub, SubType.Exclusive, topic.getName(), 2 /* consumer id */, 0, "Cons2"/* consumer name */,
                true, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH));
        pdfc.addConsumer(consumer2);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        assertEquals(3, consumers.size());
        // get notified with who is the leader
        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 2, false);
        verify(consumer1, times(2)).notifyActiveConsumerChange(same(consumer1));
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer1));

        // 6. Add a consumer which changes active consumer
        Consumer consumer0 = spy(new Consumer(sub, SubType.Exclusive, topic.getName(), 0 /* consumer id */, 0,
                "Cons0"/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH));
        pdfc.addConsumer(consumer0);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer0.consumerName());
        assertEquals(4, consumers.size());

        // all consumers will receive notifications
        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 0, true);
        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 1, false);
        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 1, false);
        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 2, false);
        verify(consumer0, times(1)).notifyActiveConsumerChange(same(consumer0));
        verify(consumer1, times(2)).notifyActiveConsumerChange(same(consumer1));
        verify(consumer1, times(2)).notifyActiveConsumerChange(same(consumer0));
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer1));
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer0));

        // 7. Remove last consumer to make active consumer change.
        pdfc.removeConsumer(consumer2);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        assertEquals(3, consumers.size());

        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 0, false);
        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 1, true);
        change = consumerChanges.poll(10, TimeUnit.SECONDS);
        assertNotNull(change);
        verifyActiveConsumerChange(change, 1, true);

        // 8. Verify if we cannot unsubscribe when more than one consumer is connected
        assertFalse(pdfc.canUnsubscribe(consumer0));

        // 9. Remove inactive consumer
        pdfc.removeConsumer(consumer0);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        assertEquals(2, consumers.size());

        // not consumer group changes
        assertNull(consumerChanges.poll(10, TimeUnit.SECONDS));

        // 10. Attempt to remove already removed consumer
        String cause = "";
        try {
            pdfc.removeConsumer(consumer0);
        } catch (Exception e) {
            cause = e.getMessage();
        }
        assertEquals(cause, "Consumer was not connected");

        // 11. Remove same consumer
        pdfc.removeConsumer(consumer1);
        consumers = pdfc.getConsumers();
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        assertEquals(1, consumers.size());
        // not consumer group changes
        assertNull(consumerChanges.poll(10, TimeUnit.SECONDS));

        // 11. With only one consumer, unsubscribe is allowed
        assertTrue(pdfc.canUnsubscribe(consumer1));
    }

    private String[] sortConsumerNameByHashSelector(String...consumerNames) throws Exception {
        String[] result = new String[consumerNames.length];
        PersistentTopic topic =
                new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        int partitionIndex = -1;
        PersistentDispatcherSingleActiveConsumer dispatcher = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic, sub);
        for (String consumerName : consumerNames){
            Consumer consumer = spy(new Consumer(sub, SubType.Failover, topic.getName(), 999 /* consumer id */, 1,
                    consumerName/* consumer name */, true, serverCnx, "myrole-1", Collections.emptyMap(),
                    false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH));
            dispatcher.addConsumer(consumer);
        }
        for (int i = 0; i < consumerNames.length; i++) {
            result[i] = dispatcher.getActiveConsumer().consumerName();
            dispatcher.removeConsumer(dispatcher.getActiveConsumer());
        }
        consumerChanges.clear();
        return result;
    }

    private CommandActiveConsumerChange waitActiveChangeEvent(int consumerId)
            throws Exception {
        AtomicReference<CommandActiveConsumerChange> res = new AtomicReference<>();
        Awaitility.await().until(() -> {
            while (!consumerChanges.isEmpty()){
                CommandActiveConsumerChange change = consumerChanges.take();
                if (change.getConsumerId() == consumerId){
                    res.set(change);
                    return true;
                }
            }
            return false;
        });
        consumerChanges.clear();
        return res.get();
    }

    @Test
    public void testAddRemoveConsumerNonPartitionedTopic() throws Exception {
        log.info("--- Starting PersistentDispatcherFailoverConsumerTest::testAddRemoveConsumerNonPartitionedTopic ---");
        String[] sortedConsumerNameByHashSelector = sortConsumerNameByHashSelector("Cons1", "Cons2");
        BrokerService spyBrokerService = pulsarTestContext.getBrokerService();
        @Cleanup("shutdownNow")
        final EventLoopGroup singleEventLoopGroup = EventLoopUtil.newEventLoopGroup(1,
                pulsarTestContext.getBrokerService().getPulsar().getConfig().isEnableBusyWait(),
                new DefaultThreadFactory("pulsar-io"));
        doAnswer(invocation -> singleEventLoopGroup).when(spyBrokerService).executor();

        PersistentTopic topic =
                new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);

        // Non partitioned topic.
        int partitionIndex = -1;
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic, sub);

        // 1. Verify no consumers connected
        assertFalse(pdfc.isConsumerConnected());

        // 2. Add a consumer
        Consumer consumer1 = spy(new Consumer(sub, SubType.Failover, topic.getName(), 1 /* consumer id */, 1,
                sortedConsumerNameByHashSelector[0]/* consumer name */,
                true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH));
        pdfc.addConsumer(consumer1);
        List<Consumer> consumers = pdfc.getConsumers();
        assertEquals(1, consumers.size());
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        waitActiveChangeEvent(1);

        // 3. Add a consumer with same priority level and consumer name is smaller in lexicographic order.
        Consumer consumer2 = spy(new Consumer(sub, SubType.Failover, topic.getName(), 2 /* consumer id */, 1,
                sortedConsumerNameByHashSelector[1]/* consumer name */,
                true, serverCnx, "myrole-1", Collections.emptyMap(),
                false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH));
        pdfc.addConsumer(consumer2);

        // 4. Verify active consumer doesn't change
        consumers = pdfc.getConsumers();
        assertEquals(2, consumers.size());
        CommandActiveConsumerChange change = waitActiveChangeEvent(2);
        verifyActiveConsumerChange(change, 2, false);
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer1.consumerName());
        verify(consumer2, times(1)).notifyActiveConsumerChange(same(consumer1));

        // 5. Add another consumer which has higher priority level
        Consumer consumer3 = spy(new Consumer(sub, SubType.Failover, topic.getName(), 3 /* consumer id */, 0, "Cons3"/* consumer name */,
                true, serverCnx, "myrole-1", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH));
        pdfc.addConsumer(consumer3);
        consumers = pdfc.getConsumers();
        assertEquals(3, consumers.size());
        change = waitActiveChangeEvent(3);
        verifyActiveConsumerChange(change, 3, true);
        assertSame(pdfc.getActiveConsumer().consumerName(), consumer3.consumerName());
        verify(consumer3, times(1)).notifyActiveConsumerChange(same(consumer3));
    }

    @Test
    public void testMultipleDispatcherGetNextConsumerWithDifferentPriorityLevel() throws Exception {

        PersistentTopic topic =
                new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock, null);
        Consumer consumer1 = createConsumer(topic, 0, 2, false, 1);
        Consumer consumer2 = createConsumer(topic, 0, 2, false, 2);
        Consumer consumer3 = createConsumer(topic, 0, 2, false, 3);
        Consumer consumer4 = createConsumer(topic, 1, 2, false, 4);
        Consumer consumer5 = createConsumer(topic, 1, 1, false, 5);
        Consumer consumer6 = createConsumer(topic, 1, 2, false, 6);
        Consumer consumer7 = createConsumer(topic, 2, 1, false, 7);
        Consumer consumer8 = createConsumer(topic, 2, 1, false, 8);
        Consumer consumer9 = createConsumer(topic, 2, 1, false, 9);
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
        Consumer consumer10 = createConsumer(topic, 0, 2, false, 10);
        dispatcher.addConsumer(consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer9);

    }

    @Test
    public void testFewBlockedConsumerSamePriority() throws Exception{
        PersistentTopic topic =
                new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock, null);
        Consumer consumer1 = createConsumer(topic, 0, 2, false, 1);
        Consumer consumer2 = createConsumer(topic, 0, 2, false, 2);
        Consumer consumer3 = createConsumer(topic, 0, 2, false, 3);
        Consumer consumer4 = createConsumer(topic, 0, 2, false, 4);
        Consumer consumer5 = createConsumer(topic, 0, 1, true, 5);
        Consumer consumer6 = createConsumer(topic, 0, 2, true, 6);
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
        PersistentTopic topic =
                new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock, null);
        Consumer consumer1 = createConsumer(topic, 0, 2, false, 1);
        Consumer consumer2 = createConsumer(topic, 0, 2, false, 2);
        Consumer consumer3 = createConsumer(topic, 0, 2, false, 3);
        Consumer consumer4 = createConsumer(topic, 0, 2, false, 4);
        Consumer consumer5 = createConsumer(topic, 0, 1, true, 5);
        Consumer consumer6 = createConsumer(topic, 0, 2, true, 6);
        Consumer consumer7 = createConsumer(topic, 1, 2, false, 7);
        Consumer consumer8 = createConsumer(topic, 1, 10, true, 8);
        Consumer consumer9 = createConsumer(topic, 1, 2, false, 9);
        Consumer consumer10 = createConsumer(topic, 2, 2, false, 10);
        Consumer consumer11 = createConsumer(topic, 2, 10, true, 11);
        Consumer consumer12 = createConsumer(topic, 2, 2, false, 12);
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
        Consumer consumer13 = createConsumer(topic, 0, 2, false, 13);
        Consumer consumer14 = createConsumer(topic, 0, 2, true, 14);
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
        PersistentTopic topic =
                new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock, null);
        Consumer consumer1 = createConsumer(topic, 0, 2, true, 1);
        Consumer consumer2 = createConsumer(topic, 0, 2, true, 2);
        Consumer consumer3 = createConsumer(topic, 0, 2, true, 3);
        Consumer consumer4 = createConsumer(topic, 1, 2, false, 4);
        Consumer consumer5 = createConsumer(topic, 1, 1, false, 5);
        Consumer consumer6 = createConsumer(topic, 2, 1, false, 6);
        Consumer consumer7 = createConsumer(topic, 2, 2, true, 7);
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

    private Consumer createConsumer(PersistentTopic topic, int priority, int permit, boolean blocked, int id) throws Exception {
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        Consumer consumer =
                new Consumer(sub, SubType.Shared, "test-topic", id, priority, ""+id, true,
                        serverCnx, "appId", Collections.emptyMap(), false /* read compacted */, null, MessageId.latest,DEFAULT_CONSUMER_EPOCH);
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
