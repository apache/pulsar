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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.var;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AcknowledgementsGroupingTrackerTest {

    private ClientCnx cnx;
    private ConsumerImpl<?> consumer;
    private EventLoopGroup eventLoopGroup;
    private AtomicBoolean returnCnx = new AtomicBoolean(true);

    @BeforeClass
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        eventLoopGroup = new NioEventLoopGroup(1);
        consumer = mock(ConsumerImpl.class);
        consumer.unAckedChunkedMessageIdSequenceMap = new ConcurrentHashMap<>();
        cnx = spy(new ClientCnxTest(new ClientConfigurationData(), eventLoopGroup));
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        doReturn(client).when(consumer).getClient();
        doReturn(new ConsumerStatsRecorderImpl()).when(consumer).getStats();
        doReturn(UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED)
                .when(consumer).getUnAckedMessageTracker();
        ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
        doAnswer(invocation -> returnCnx.get() ? cnx : null).when(consumer).getClientCnx();
        doReturn(ctx).when(cnx).ctx();
    }

    @DataProvider(name = "isNeedReceipt")
    public Object[][] isNeedReceipt() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @AfterClass(alwaysRun = true)
    public void teardown() throws Exception {
        eventLoopGroup.shutdownGracefully().get();
    }

    @Test(dataProvider = "isNeedReceipt")
    public void testAckTracker(boolean isNeedReceipt) throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.SECONDS.toMicros(10));
        conf.setAckReceiptEnabled(isNeedReceipt);
        AcknowledgmentsGroupingTracker tracker;
        tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);
        MessageIdImpl msg3 = new MessageIdImpl(5, 3, 0);
        MessageIdImpl msg4 = new MessageIdImpl(5, 4, 0);
        MessageIdImpl msg5 = new MessageIdImpl(5, 5, 0);
        MessageIdImpl msg6 = new MessageIdImpl(5, 6, 0);

        assertFalse(tracker.isDuplicate(msg1));

        tracker.addAcknowledgment(msg1, AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));

        assertFalse(tracker.isDuplicate(msg2));

        tracker.addAcknowledgment(msg5, AckType.Cumulative, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        // Flush while disconnected. the internal tracking will not change
        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.addAcknowledgment(msg6, AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg6));

        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.close();
    }

    @Test(dataProvider = "isNeedReceipt")
    public void testBatchAckTracker(boolean isNeedReceipt) throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.SECONDS.toMicros(10));
        conf.setAckReceiptEnabled(isNeedReceipt);
        AcknowledgmentsGroupingTracker tracker;
        tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);
        MessageIdImpl msg3 = new MessageIdImpl(5, 3, 0);
        MessageIdImpl msg4 = new MessageIdImpl(5, 4, 0);
        MessageIdImpl msg5 = new MessageIdImpl(5, 5, 0);
        MessageIdImpl msg6 = new MessageIdImpl(5, 6, 0);

        assertFalse(tracker.isDuplicate(msg1));

        tracker.addListAcknowledgment(Collections.singletonList(msg1), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));

        assertFalse(tracker.isDuplicate(msg2));

        tracker.addListAcknowledgment(Collections.singletonList(msg5), AckType.Cumulative, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        // Flush while disconnected. the internal tracking will not change
        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.addListAcknowledgment(Collections.singletonList(msg6), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg6));

        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.close();
    }

    @Test(dataProvider = "isNeedReceipt")
    public void testImmediateAckingTracker(boolean isNeedReceipt) throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(0);
        conf.setAckReceiptEnabled(isNeedReceipt);
        AcknowledgmentsGroupingTracker tracker;
        tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);

        assertFalse(tracker.isDuplicate(msg1));

        returnCnx.set(false);
        try {
            tracker.addAcknowledgment(msg1, AckType.Individual, Collections.emptyMap());
            assertFalse(tracker.isDuplicate(msg1));
        } finally {
            returnCnx.set(true);
        }

        tracker.flush();
        assertFalse(tracker.isDuplicate(msg1));

        tracker.addAcknowledgment(msg2, AckType.Individual, Collections.emptyMap());
        // Since we were connected, the ack went out immediately
        assertFalse(tracker.isDuplicate(msg2));
        tracker.close();
    }

    @Test(dataProvider = "isNeedReceipt")
    public void testImmediateBatchAckingTracker(boolean isNeedReceipt) throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(0);
        conf.setAckReceiptEnabled(isNeedReceipt);
        AcknowledgmentsGroupingTracker tracker;
        tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);

        assertFalse(tracker.isDuplicate(msg1));

        returnCnx.set(false);
        try {
            tracker.addListAcknowledgment(Collections.singletonList(msg1), AckType.Individual, Collections.emptyMap());
            assertTrue(tracker.isDuplicate(msg1));
        } finally {
            returnCnx.set(true);
        }

        tracker.flush();
        assertFalse(tracker.isDuplicate(msg1));

        tracker.addListAcknowledgment(Collections.singletonList(msg2), AckType.Individual, Collections.emptyMap());

        tracker.flush();
        // Since we were connected, the ack went out immediately
        assertFalse(tracker.isDuplicate(msg2));
        tracker.close();
    }

    @Test(dataProvider = "isNeedReceipt")
    public void testAckTrackerMultiAck(boolean isNeedReceipt) {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.SECONDS.toMicros(10));
        conf.setAckReceiptEnabled(isNeedReceipt);
        AcknowledgmentsGroupingTracker tracker;
        tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12_VALUE);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);
        MessageIdImpl msg3 = new MessageIdImpl(5, 3, 0);
        MessageIdImpl msg4 = new MessageIdImpl(5, 4, 0);
        MessageIdImpl msg5 = new MessageIdImpl(5, 5, 0);
        MessageIdImpl msg6 = new MessageIdImpl(5, 6, 0);

        assertFalse(tracker.isDuplicate(msg1));

        tracker.addAcknowledgment(msg1, AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));

        assertFalse(tracker.isDuplicate(msg2));

        tracker.addAcknowledgment(msg5, AckType.Cumulative, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        // Flush while disconnected. the internal tracking will not change
        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.addAcknowledgment(msg6, AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg6));

        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.close();
    }

    @Test(dataProvider = "isNeedReceipt")
    public void testBatchAckTrackerMultiAck(boolean isNeedReceipt) throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.SECONDS.toMicros(10));
        conf.setAckReceiptEnabled(isNeedReceipt);
        AcknowledgmentsGroupingTracker tracker;
        tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12_VALUE);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);
        MessageIdImpl msg3 = new MessageIdImpl(5, 3, 0);
        MessageIdImpl msg4 = new MessageIdImpl(5, 4, 0);
        MessageIdImpl msg5 = new MessageIdImpl(5, 5, 0);
        MessageIdImpl msg6 = new MessageIdImpl(5, 6, 0);

        assertFalse(tracker.isDuplicate(msg1));

        tracker.addListAcknowledgment(Collections.singletonList(msg1), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));

        assertFalse(tracker.isDuplicate(msg2));

        tracker.addListAcknowledgment(Collections.singletonList(msg5), AckType.Cumulative, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        // Flush while disconnected. the internal tracking will not change
        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.addListAcknowledgment(Collections.singletonList(msg6), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg6));

        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.close();
    }

    @Test
    public void testDoIndividualBatchAckAsync() {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        var tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);
        var messageId1 = new BatchMessageIdImpl(5, 1, 0, 3, 10, null);
        BitSet bitSet = new BitSet(20);
        for (int i = 0; i < 20; i++) {
            bitSet.set(i, true);
        }
        var messageId2 = new BatchMessageIdImpl(3, 2, 0, 5, 20, bitSet);
        tracker.doIndividualBatchAckAsync(messageId1);
        tracker.doIndividualBatchAckAsync(messageId2);
        var batchIndexAcks = tracker.pendingIndividualBatchIndexAcks;
        MessageIdImpl position1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl position2 = new MessageIdImpl(3, 2, 0);
        assertTrue(batchIndexAcks.containsKey(position1));
        assertNotNull(batchIndexAcks.get(position1));
        assertEquals(batchIndexAcks.get(position1).cardinality(), 9);
        assertTrue(batchIndexAcks.containsKey(position2));
        assertNotNull(batchIndexAcks.get(position2));
        assertEquals(batchIndexAcks.get(position2).cardinality(), 19);
        tracker.close();
    }

    @Test
    public void testDoIndividualBatchAckNeverAffectIsDuplicate() throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setMaxAcknowledgmentGroupSize(1);
        PersistentAcknowledgmentsGroupingTracker tracker =
                new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        BatchMessageIdImpl batchMessageId0 = new BatchMessageIdImpl(5, 1, 0, 0, 10, null);
        BatchMessageIdImpl batchMessageId1 = new BatchMessageIdImpl(5, 1, 0, 1, 10, null);

        int loops = 10000;
        int addAcknowledgmentThreadCount = 10;
        List<Thread> addAcknowledgmentThreads = new ArrayList<>(addAcknowledgmentThreadCount);
        for (int i = 0; i < addAcknowledgmentThreadCount; i++) {
            Thread addAcknowledgmentThread = new Thread(() -> {
                for (int j = 0; j < loops; j++) {
                    tracker.addAcknowledgment(batchMessageId0, AckType.Individual, Collections.emptyMap());
                }
            }, "doIndividualBatchAck-thread-" + i);
            addAcknowledgmentThread.start();
            addAcknowledgmentThreads.add(addAcknowledgmentThread);
        }

        int isDuplicateThreadCount = 10;
        AtomicBoolean assertResult = new AtomicBoolean();
        List<Thread> isDuplicateThreads = new ArrayList<>(isDuplicateThreadCount);
        for (int i = 0; i < isDuplicateThreadCount; i++) {
            Thread isDuplicateThread = new Thread(() -> {
                for (int j = 0; j < loops; j++) {
                    boolean duplicate = tracker.isDuplicate(batchMessageId1);
                    assertResult.set(assertResult.get() || duplicate);
                }
            }, "isDuplicate-thread-" + i);
            isDuplicateThread.start();
            isDuplicateThreads.add(isDuplicateThread);
        }

        for (Thread addAcknowledgmentThread : addAcknowledgmentThreads) {
            addAcknowledgmentThread.join();
        }

        for (Thread isDuplicateThread : isDuplicateThreads) {
            isDuplicateThread.join();
        }

        assertFalse(assertResult.get());
    }

    public class ClientCnxTest extends ClientCnx {

        public ClientCnxTest(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) {
            super(InstrumentProvider.NOOP, conf, eventLoopGroup);
        }

        @Override
        public CompletableFuture<Void> newAckForReceipt(ByteBuf request, long requestId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void newAckForReceiptWithFuture(ByteBuf request, long requestId,
                                               TimedCompletableFuture<Void> future) {
        }
    }
}
