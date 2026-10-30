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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
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

    private static final int MANY_ENTRIES_COUNT = 512;

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
    public void testApplyRateLimitsToMessagesAndBytesToReadWithBatchedMessages() {
        DispatchRateLimiter rateLimiter = mock(DispatchRateLimiter.class);
        when(rateLimiter.getAvailableDispatchRateLimitOnMsg()).thenReturn(50L);
        when(rateLimiter.getAvailableDispatchRateLimitOnByte()).thenReturn(-1L);

        PersistentTopic topicMock = mock(PersistentTopic.class);
        when(topicMock.isSystemTopic()).thenReturn(true);
        when(this.subscriptionMock.getTopic()).thenReturn(topicMock);
        when(topicMock.getBrokerDispatchRateLimiter()).thenReturn(Optional.empty());
        when(topicMock.getDispatchRateLimiter()).thenReturn(Optional.empty());

        AbstractBaseDispatcherTestHelper dispatcher =
                new AbstractBaseDispatcherTestHelper(this.subscriptionMock, this.svcConfig, rateLimiter);

        // 1. Unknown batch size (no consumers / consumers report avgMessagesPerEntry = 0):
        // Before any messages have been dispatched, the batch size is unknown, so limit the first read to 1 entry
        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsUnknownBatch =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsUnknownBatch.getLeft().intValue(), 1);

        Consumer consumerZero = mock(Consumer.class);
        when(consumerZero.getAvgMessagesPerEntry()).thenReturn(0);
        dispatcher.setConsumers(List.of(consumerZero));
        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsZeroAvg =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsZeroAvg.getLeft().intValue(), 1);

        // 2. Unbatched messages (consumer reports avgMessagesPerEntry = 1): 50 permits allow 50 entries
        Consumer consumerUnbatched = mock(Consumer.class);
        when(consumerUnbatched.getAvgMessagesPerEntry()).thenReturn(1);
        dispatcher.setConsumers(List.of(consumerUnbatched));

        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsUnbatched =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsUnbatched.getLeft().intValue(), 50);

        // 3. Batched messages (consumer reports avgMessagesPerEntry = 10):
        // 50 message permits must scale to 50 / 10 = 5 entries
        Consumer consumerBatched = mock(Consumer.class);
        when(consumerBatched.getAvgMessagesPerEntry()).thenReturn(10);
        dispatcher.setConsumers(List.of(consumerBatched));

        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsWithBatching =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsWithBatching.getLeft().intValue(), 5);

        // 4. Multiple consumers with different averages (Key_Shared: avg 2 and avg 10 -> representative avg 6):
        // 50 message permits must scale to 50 / 6 = 8 entries
        Consumer consumer1 = mock(Consumer.class);
        when(consumer1.getAvgMessagesPerEntry()).thenReturn(2);
        Consumer consumer2 = mock(Consumer.class);
        when(consumer2.getAvgMessagesPerEntry()).thenReturn(10);
        dispatcher.setConsumers(List.of(consumer1, consumer2));

        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsMultiConsumer =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsMultiConsumer.getLeft().intValue(), 8);

        // 5. Sub-batch budget (positive permits < avgMessagesPerEntry): allows at least 1 entry for forward progress
        when(rateLimiter.getAvailableDispatchRateLimitOnMsg()).thenReturn(5L);
        dispatcher.setConsumers(List.of(consumerBatched));
        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsSubBatch =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsSubBatch.getLeft().intValue(), 1);

        // 6. Exhausted permits (0 permits): defers reading (-1)
        when(rateLimiter.getAvailableDispatchRateLimitOnMsg()).thenReturn(0L);
        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsExhausted =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsExhausted.getLeft().intValue(), -1);
    }

    @Test
    public void testSingleActiveConsumerRateLimitsWithBatchedMessages() {
        DispatchRateLimiter rateLimiter = mock(DispatchRateLimiter.class);
        when(rateLimiter.getAvailableDispatchRateLimitOnMsg()).thenReturn(50L);
        when(rateLimiter.getAvailableDispatchRateLimitOnByte()).thenReturn(-1L);

        PersistentTopic topicMock = mock(PersistentTopic.class);
        when(topicMock.isSystemTopic()).thenReturn(true);
        when(this.subscriptionMock.getTopic()).thenReturn(topicMock);
        when(topicMock.getBrokerDispatchRateLimiter()).thenReturn(Optional.empty());
        when(topicMock.getDispatchRateLimiter()).thenReturn(Optional.empty());

        Consumer standbyConsumer = mock(Consumer.class);
        when(standbyConsumer.getAvgMessagesPerEntry()).thenReturn(1);

        Consumer activeConsumer = mock(Consumer.class);
        when(activeConsumer.getAvgMessagesPerEntry()).thenReturn(10);

        AbstractDispatcherSingleActiveConsumer singleActiveDispatcher =
                new AbstractDispatcherSingleActiveConsumer(
                        CommandSubscribe.SubType.Failover, 0, "test-topic",
                        this.subscriptionMock, this.svcConfig, null) {
                    private Consumer mockActiveConsumer;

                    public void setActiveConsumer(Consumer consumer) {
                        this.mockActiveConsumer = consumer;
                    }

                    @Override
                    public String getName() {
                        return "test-single-active-dispatcher";
                    }

                    @Override
                    public Consumer getActiveConsumer() {
                        return mockActiveConsumer;
                    }

                    @Override
                    protected void scheduleReadOnActiveConsumer() {}

                    @Override
                    protected void cancelPendingRead() {}

                    @Override
                    public Optional<DispatchRateLimiter> getRateLimiter() {
                        return Optional.of(rateLimiter);
                    }

                    @Override
                    protected boolean isConsumersExceededOnSubscription() {
                        return false;
                    }

                    @Override
                    public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
                        return false;
                    }

                    @Override
                    protected void reScheduleRead() {}

                    @Override
                    public CompletableFuture<Void> addConsumer(Consumer consumer) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void removeConsumer(Consumer consumer) {}

                    @Override
                    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {}

                    @Override
                    public void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {}

                    @Override
                    public void redeliverUnacknowledgedMessages(Consumer consumer, List<Position> positions) {}

                    @Override
                    public void addUnAckedMessages(int unAckMessages) {}

                    @Override
                    public RedeliveryTracker getRedeliveryTracker() {
                        return null;
                    }
                };

        // 1. Initial state (no active consumer / avg = 0): limits to 1 entry
        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsNoActive =
                singleActiveDispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsNoActive.getLeft().intValue(), 1);

        // 2. Active consumer has avg = 10: 50 permits scale to 50 / 10 = 5 entries
        // (standby consumer's avg = 1 is ignored)
        singleActiveDispatcher.getConsumers().add(standbyConsumer);
        try {
            java.lang.reflect.Method setActive =
                    singleActiveDispatcher.getClass().getMethod("setActiveConsumer", Consumer.class);
            setActive.invoke(singleActiveDispatcher, activeConsumer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        org.apache.commons.lang3.tuple.Pair<Integer, Long> limitsWithActive =
                singleActiveDispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(limitsWithActive.getLeft().intValue(), 5);
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
    public void testFilterEntriesForConsumerKeepsNullAckSetAbsentForManyEntries() {
        when(svcConfig.isDispatchThrottlingForFilteredEntriesEnabled()).thenReturn(false);

        EntriesAndExpectedMetadata entries = createEntriesWithVaryingBatchSizes(MANY_ENTRIES_COUNT);

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.entries.size());
        EntryBatchIndexesAcks indexesAcks = EntryBatchIndexesAcks.get(entries.entries.size());
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getDeletedBatchIndexesAsLongArray(any(Position.class))).thenReturn(null);
        // Exercise the compatibility default for cursors that implement only the Position-based method.
        when(cursor.getDeletedBatchIndexesAsLongArray(anyLong(), anyLong())).thenCallRealMethod();
        try {
            int size = this.helper.filterEntriesForConsumer(entries.entries, batchSizes, sendMessageInfo,
                    indexesAcks, cursor, false, null);

            assertEquals(size, MANY_ENTRIES_COUNT);
            assertEquals(sendMessageInfo.getTotalMessages(), entries.expectedMessages);
            assertEquals(sendMessageInfo.getTotalBytes(), entries.expectedBytes);
            for (int i = 0; i < MANY_ENTRIES_COUNT; i++) {
                assertEquals(batchSizes.getBatchSize(i), batchSizeForEntry(i));
                assertNull(indexesAcks.getAckSet(i));
            }
            assertEquals(indexesAcks.getTotalAckedIndexCount(), 0);
        } finally {
            entries.release();
            indexesAcks.recycle();
            batchSizes.recyle();
        }
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

        int size = this.helper.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo,
                null, cursor, false, null);
        assertEquals(size, 0);
        verify(subscriptionDispatchRateLimiter).consumeDispatchQuota(1, expectedBytePermits);
    }

    @Test
    public void testRateLimitProgressionWithFilteredInitialBacklog() {
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

        DispatchRateLimiter rateLimiter = mock(DispatchRateLimiter.class);
        when(rateLimiter.getAvailableDispatchRateLimitOnMsg()).thenReturn(50L);
        when(rateLimiter.getAvailableDispatchRateLimitOnByte()).thenReturn(-1L);

        AbstractBaseDispatcherTestHelper dispatcher =
                new AbstractBaseDispatcherTestHelper(this.subscriptionMock, this.svcConfig, rateLimiter);

        // Consumer starts with unknown average (avgMessagesPerEntry = 0)
        java.util.concurrent.atomic.AtomicInteger consumerAvg = new java.util.concurrent.atomic.AtomicInteger(0);
        Consumer consumer = mock(Consumer.class);
        when(consumer.getAvgMessagesPerEntry()).thenAnswer(inv -> consumerAvg.get());
        doAnswer(inv -> {
            int msgs = inv.getArgument(0);
            int entriesCount = inv.getArgument(1);
            consumerAvg.set(Math.round((float) msgs / entriesCount));
            return null;
        }).when(consumer).updateAvgMessagesPerEntry(anyInt(), anyInt());

        dispatcher.setConsumers(List.of(consumer));

        // 1. Initial read limit before any entries are processed: unknown average limits read to 1 entry
        org.apache.commons.lang3.tuple.Pair<Integer, Long> initialLimits =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(initialLimits.getLeft().intValue(), 1);

        // 2. Broker reads 1 entry with 10 batched messages, but EntryFilter REJECTS it
        List<Entry> entries = new ArrayList<>();
        ByteBuf batchMessage = createBatchMessage("batch-message", 1, 10);
        Entry entry = EntryImpl.create(1, 1, batchMessage);
        batchMessage.release();
        entries.add(entry);

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        ManagedCursor cursor = mock(ManagedCursor.class);

        int acceptedEntries = dispatcher.filterEntriesForConsumer(entries, batchSizes, sendMessageInfo,
                null, cursor, false, consumer);
        assertEquals(acceptedEntries, 0);
        assertEquals(sendMessageInfo.getTotalMessages(), 0);

        // 3. Verify consumer learned batch size (10 msgs / 1 entry = 10) despite all entries being filtered
        assertEquals(consumer.getAvgMessagesPerEntry(), 10);

        // 4. Subsequent fully filtered read with a different batch size does not drift/overwrite established average
        List<Entry> subsequentFilteredEntries = new ArrayList<>();
        ByteBuf largeBatch = createBatchMessage("batch-message", 1, 100);
        Entry largeEntry = EntryImpl.create(1, 2, largeBatch);
        largeBatch.release();
        subsequentFilteredEntries.add(largeEntry);
        int acceptedAgain = dispatcher.filterEntriesForConsumer(subsequentFilteredEntries,
                EntryBatchSizes.get(subsequentFilteredEntries.size()), sendMessageInfo,
                null, cursor, false, consumer);
        assertEquals(acceptedAgain, 0);
        assertEquals(consumer.getAvgMessagesPerEntry(), 10);

        // 5. On subsequent read with 50 permits, rate limit progresses to 50 / 10 = 5 entries
        // instead of staying stuck at 1
        org.apache.commons.lang3.tuple.Pair<Integer, Long> subsequentLimits =
                dispatcher.applyRateLimitsToMessagesAndBytesToRead(100, 1000L);
        assertEquals(subsequentLimits.getLeft().intValue(), 5);
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

    private ByteBuf createBatchMessage(String message, int sequenceId, int batchSize) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis())
                .setNumMessagesInBatch(batchSize);
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }

    private EntriesAndExpectedMetadata createEntriesWithVaryingBatchSizes(int entryCount) {
        EntriesAndExpectedMetadata entries = new EntriesAndExpectedMetadata(entryCount);
        for (int i = 0; i < entryCount; i++) {
            int batchSize = batchSizeForEntry(i);
            ByteBuf message = createBatchMessage(messagePayload(i), i, batchSize);
            Entry entry = EntryImpl.create(1, i, message);
            message.release();
            entries.entries.add(entry);
            entries.expectedMessages += batchSize;
            entries.expectedBytes += entry.getLength();
        }
        return entries;
    }

    private static int batchSizeForEntry(int index) {
        return 1 + (index % 10);
    }

    private static String messagePayload(int index) {
        return "message-" + index + "-" + "x".repeat(1 + (index % 128));
    }

    private static final class EntriesAndExpectedMetadata {
        private final List<Entry> entries;
        private int expectedMessages;
        private long expectedBytes;

        private EntriesAndExpectedMetadata(int entryCount) {
            this.entries = new ArrayList<>(entryCount);
        }

        private void release() {
            entries.forEach(Entry::release);
        }
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

        private List<Consumer> consumers;

        public void setConsumers(List<Consumer> consumers) {
            this.consumers = consumers;
        }

        @Override
        public List<Consumer> getConsumers() {
            return consumers;
        }

        @Override
        public boolean canUnsubscribe(Consumer consumer) {
            return false;
        }

        @Override
        public CompletableFuture<Void> close(boolean disconnectConsumers,
                                             Optional<BrokerLookupData> assignedBrokerLookupData) {
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
        public CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor,
                                                              Optional<BrokerLookupData> assignedBrokerLookupData) {
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
        public void redeliverUnacknowledgedMessages(Consumer consumer, List<Position> positions) {

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
