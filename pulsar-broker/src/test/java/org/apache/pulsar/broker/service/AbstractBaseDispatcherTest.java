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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
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
        when(svcConfig.isDispatchThrottlingForFilteredEntriesEnabled()).thenReturn(true);
        this.subscriptionMock = mock(PersistentSubscription.class);
        this.helper = new AbstractBaseDispatcherTestHelper(this.subscriptionMock, this.svcConfig, null);
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
    public void testFilterEntriesForConsumerOfEntryFilter() throws Exception {
        Topic mockTopic = mock(Topic.class);
        when(this.subscriptionMock.getTopic()).thenReturn(mockTopic);

        final EntryFilterProvider entryFilterProvider = mock(EntryFilterProvider.class);
        final ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
        when(serviceConfiguration.isAllowOverrideEntryFilters()).thenReturn(true);
        final PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(serviceConfiguration);
        BrokerService mockBrokerService = mock(BrokerService.class);
        when(mockBrokerService.pulsar()).thenReturn(pulsar);
        when(mockBrokerService.getEntryFilterProvider()).thenReturn(entryFilterProvider);
        when(mockTopic.getBrokerService()).thenReturn(mockBrokerService);
        EntryFilter mockFilter = mock(EntryFilter.class);
        when(mockFilter.filterEntry(any(Entry.class), any(FilterContext.class))).thenReturn(
                EntryFilter.FilterResult.REJECT);
        when(mockTopic.getEntryFilters()).thenReturn(List.of(mockFilter));
        DispatchRateLimiter subscriptionDispatchRateLimiter = mock(DispatchRateLimiter.class);

        this.helper = new AbstractBaseDispatcherTestHelper(this.subscriptionMock, this.svcConfig,
                subscriptionDispatchRateLimiter);

        List<Entry> entries = new ArrayList<>();

        ByteBuf message = createMessage("message1", 1);
        Entry e = EntryImpl.create(1, 2, message);
        message.release();
        long expectedBytePermits = e.getLength();
        entries.add(e);
        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());

        ManagedCursor cursor = mock(ManagedCursor.class);

        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, cursor, false, null);
        assertEquals(size, 0);
        verify(subscriptionDispatchRateLimiter).tryDispatchPermit(1, expectedBytePermits);
    }

    @Test
    public void testFilterEntriesForConsumerOfTxnMsgAbort() {
        List<Entry> entries = new ArrayList<>();
        ByteBuf message = createTnxAbortMessage("message1", 1);
        entries.add(EntryImpl.create(1, 1, message));
        message.release();

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, null);
        assertEquals(size, 0);
    }

    @Test
    public void testFilterEntriesForConsumerOfTxnBufferAbort() {
        PersistentTopic mockTopic = mock(PersistentTopic.class);
        when(this.subscriptionMock.getTopic()).thenReturn(mockTopic);

        when(mockTopic.isTxnAborted(any(TxnID.class), any())).thenReturn(true);

        List<Entry> entries = new ArrayList<>();
        ByteBuf message = createTnxMessage("message1", 1);
        entries.add(EntryImpl.create(1, 1, message));
        message.release();

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
        markerMessage.release();

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, null);
        assertEquals(size, 0);
    }

    @Test
    public void testFilterEntriesForConsumerOfDelayedMsg() {
        List<Entry> entries = new ArrayList<>();
        ByteBuf message = createDelayedMessage("message1", 1);
        entries.add(EntryImpl.create(1, 1, message));
        message.release();

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

        private final Optional<DispatchRateLimiter> dispatchRateLimiter;

        protected AbstractBaseDispatcherTestHelper(Subscription subscription,
                                                   ServiceConfiguration serviceConfig,
                                                   DispatchRateLimiter rateLimiter) {
            super(subscription, serviceConfig);
            dispatchRateLimiter = Optional.ofNullable(rateLimiter);
        }

        @Override
        public Optional<DispatchRateLimiter> getRateLimiter() {
            return dispatchRateLimiter;
        }

        @Override
        protected boolean isConsumersExceededOnSubscription() {
            return false;
        }

        @Override
        public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
            return msgMetadata.hasDeliverAtTime();
        }

        @Override
        protected void reScheduleRead() {

        }

        @Override
        public String getName() {
            return "AbstractBaseDispatcherTestHelper for subscription" + subscription.getName();
        }

        @Override
        public CompletableFuture<Void> addConsumer(Consumer consumer) {
            return CompletableFuture.completedFuture(null);
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
