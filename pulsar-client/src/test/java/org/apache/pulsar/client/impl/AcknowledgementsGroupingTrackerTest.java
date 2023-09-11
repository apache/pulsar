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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.util.collections.ConcurrentBitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AcknowledgementsGroupingTrackerTest {

    private ClientCnx cnx;
    private ConsumerImpl<?> consumer;
    private EventLoopGroup eventLoopGroup;

    @BeforeClass
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        eventLoopGroup = new NioEventLoopGroup(1);
        consumer = mock(ConsumerImpl.class);
        consumer.unAckedChunkedMessageIdSequenceMap =
                ConcurrentOpenHashMap.<MessageIdImpl, MessageIdImpl[]>newBuilder().build();
        cnx = spy(new ClientCnxTest(new ClientConfigurationData(), new NioEventLoopGroup()));
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        doReturn(client).when(consumer).getClient();
        doReturn(cnx).when(consumer).getClientCnx();
        doReturn(new ConsumerStatsRecorderImpl()).when(consumer).getStats();
        doReturn(new UnAckedMessageTracker().UNACKED_MESSAGE_TRACKER_DISABLED)
                .when(consumer).getUnAckedMessageTracker();
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(cnx.ctx()).thenReturn(ctx);
    }

    @DataProvider(name = "isNeedReceipt")
    public Object[][] isNeedReceipt() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        eventLoopGroup.shutdownGracefully();
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

        when(consumer.getClientCnx()).thenReturn(cnx);

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

        when(consumer.getClientCnx()).thenReturn(cnx);

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

        when(consumer.getClientCnx()).thenReturn(null);

        tracker.addAcknowledgment(msg1, AckType.Individual, Collections.emptyMap());
        assertFalse(tracker.isDuplicate(msg1));

        when(consumer.getClientCnx()).thenReturn(cnx);

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

        when(consumer.getClientCnx()).thenReturn(null);

        tracker.addListAcknowledgment(Collections.singletonList(msg1), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));

        when(consumer.getClientCnx()).thenReturn(cnx);

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

        when(consumer.getClientCnx()).thenReturn(cnx);

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

        when(consumer.getClientCnx()).thenReturn(cnx);

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
    public void testDoIndividualBatchAckAsync() throws Exception{
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        AcknowledgmentsGroupingTracker tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);
        MessageId messageId1 = new BatchMessageIdImpl(5, 1, 0, 3, 10, BatchMessageAckerDisabled.INSTANCE);
        BitSet bitSet = new BitSet(20);
        for(int i = 0; i < 20; i ++) {
            bitSet.set(i, true);
        }
        MessageId messageId2 = new BatchMessageIdImpl(3, 2, 0, 5, 20, BatchMessageAcker.newAcker(bitSet));
        Method doIndividualBatchAckAsync = PersistentAcknowledgmentsGroupingTracker.class
                .getDeclaredMethod("doIndividualBatchAckAsync", BatchMessageIdImpl.class);
        doIndividualBatchAckAsync.setAccessible(true);
        doIndividualBatchAckAsync.invoke(tracker, messageId1);
        doIndividualBatchAckAsync.invoke(tracker, messageId2);
        Field pendingIndividualBatchIndexAcks = PersistentAcknowledgmentsGroupingTracker.class.getDeclaredField("pendingIndividualBatchIndexAcks");
        pendingIndividualBatchIndexAcks.setAccessible(true);
        ConcurrentHashMap<MessageIdImpl, ConcurrentBitSetRecyclable> batchIndexAcks =
                (ConcurrentHashMap<MessageIdImpl, ConcurrentBitSetRecyclable>) pendingIndividualBatchIndexAcks.get(tracker);
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

    public class ClientCnxTest extends ClientCnx {

        public ClientCnxTest(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) {
            super(conf, eventLoopGroup);
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
