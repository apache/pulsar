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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AbstractBaseDispatcherTest {

    private AbstractBaseDispatcherTestHelper helper;

    private ServiceConfiguration svcConfig;

    private PersistentSubscription subscriptionMock;

    @BeforeMethod
    public void setup() throws Exception {
        this.svcConfig = mock(ServiceConfiguration.class);
        this.subscriptionMock = mock(PersistentSubscription.class);
        this.helper = new AbstractBaseDispatcherTestHelper(this.subscriptionMock, this.svcConfig);
    }

    @Test
    public void testFilterEntriesForConsumerOfNullElement() {
        List<Entry> entries = new ArrayList<>();
        entries.add(null);

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());

        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, null);
        assertEquals(size, 0);
    }


    @Test
    public void testFilterEntriesForConsumerOfEntryFilter() {
        Topic mockTopic = mock(Topic.class);
        when(this.subscriptionMock.getTopic()).thenReturn(mockTopic);

        BrokerService mockBrokerService = mock(BrokerService.class);
        when(mockTopic.getBrokerService()).thenReturn(mockBrokerService);

        EntryFilterWithClassLoader mockFilter = mock(EntryFilterWithClassLoader.class);
        when(mockFilter.filterEntry(any(Entry.class), any(FilterContext.class))).thenReturn(
                EntryFilter.FilterResult.REJECT);
        ImmutableMap<String, EntryFilterWithClassLoader> entryFilters = ImmutableMap.of("key", mockFilter);
        when(mockBrokerService.getEntryFilters()).thenReturn(entryFilters);

        this.helper = new AbstractBaseDispatcherTestHelper(this.subscriptionMock, this.svcConfig);

        List<Entry> entries = new ArrayList<>();

        entries.add(EntryImpl.create(1, 2, createMessage("message1", 1)));
        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        //
        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, null);
        assertEquals(size, 0);
    }

    @Test
    public void testFilterEntriesForConsumerOfTxnMsgAbort() {
        List<Entry> entries = new ArrayList<>();
        entries.add(EntryImpl.create(1, 1, createTnxAbortMessage("message1", 1)));

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, null);
        assertEquals(size, 0);
    }

    @Test
    public void testFilterEntriesForConsumerOfTxnBufferAbort() {
        PersistentTopic mockTopic = mock(PersistentTopic.class);
        when(this.subscriptionMock.getTopic()).thenReturn(mockTopic);

        when(mockTopic.isTxnAborted(any(TxnID.class))).thenReturn(true);

        List<Entry> entries = new ArrayList<>();
        entries.add(EntryImpl.create(1, 1, createTnxMessage("message1", 1)));

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, null);
        assertEquals(size, 0);
    }

    @Test
    public void testFilterEntriesForConsumerOfServerOnlyMarker() {
        List<Entry> entries = new ArrayList<>();
        ByteBuf markerMessage =
                Markers.newReplicatedSubscriptionsSnapshotRequest("testSnapshotId", "testSourceCluster");
        entries.add(EntryImpl.create(1, 1, markerMessage));

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, null);
        assertEquals(size, 0);
    }

    @Test
    public void testFilterEntriesForConsumerOfDelayedMsg() {
        List<Entry> entries = new ArrayList<>();
        entries.add(EntryImpl.create(1, 1, createDelayedMessage("message1", 1)));

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, null);
        assertEquals(size, 0);
    }

    private ByteBuf createMessage(String message, int sequenceId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }

    private ByteBuf createTnxMessage(String message, int sequenceId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis())
                .setTxnidMostBits(8)
                .setTxnidLeastBits(0);
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }

    private ByteBuf createTnxAbortMessage(String message, int sequenceId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis())
                .setTxnidMostBits(8)
                .setTxnidLeastBits(0)
                .setMarkerType(MarkerType.TXN_ABORT_VALUE);
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }

    private ByteBuf createDelayedMessage(String message, int sequenceId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis())
                .setDeliverAtTime(System.currentTimeMillis() + 5000);
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }

    private static class AbstractBaseDispatcherTestHelper extends AbstractBaseDispatcher {

        protected AbstractBaseDispatcherTestHelper(Subscription subscription,
                                                   ServiceConfiguration serviceConfig) {
            super(subscription, serviceConfig);
        }

        @Override
        protected boolean isConsumersExceededOnSubscription() {
            return false;
        }

        @Override
        public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
            //for test.
            return true;
        }

        @Override
        protected void reScheduleRead() {

        }

        @Override
        public void addConsumer(Consumer consumer) throws BrokerServiceException {

        }

        @Override
        public void removeConsumer(Consumer consumer) throws BrokerServiceException {

        }

        @Override
        public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {

        }

        @Override
        public boolean isConsumerConnected() {
            return false;
        }

        @Override
        public List<Consumer> getConsumers() {
            return null;
        }

        @Override
        public boolean canUnsubscribe(Consumer consumer) {
            return false;
        }

        @Override
        public CompletableFuture<Void> close() {
            return null;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
            return null;
        }

        @Override
        public CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
            return null;
        }

        @Override
        public void reset() {

        }

        @Override
        public CommandSubscribe.SubType getType() {
            return null;
        }

        @Override
        public void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {

        }

        @Override
        public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {

        }

        @Override
        public void addUnAckedMessages(int unAckMessages) {

        }

        @Override
        public RedeliveryTracker getRedeliveryTracker() {
            return null;
        }
    }

}
