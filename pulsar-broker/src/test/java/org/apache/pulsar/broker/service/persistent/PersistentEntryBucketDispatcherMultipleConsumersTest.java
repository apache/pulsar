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
package org.apache.pulsar.broker.service.persistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import java.util.List;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryAndMetadata;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.scalable.SegmentOwnerMetrics;
import org.apache.pulsar.broker.service.scalable.SegmentOwnerMetrics.EntryBucketViolation;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentEntryBucketDispatcherMultipleConsumersTest {

    private static PersistentEntryBucketDispatcherMultipleConsumers createDispatcher(SegmentOwnerMetrics metrics) {
        ServiceConfiguration conf = new ServiceConfiguration();
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(conf);
        BrokerService broker = mock(BrokerService.class);
        when(broker.pulsar()).thenReturn(pulsar);
        when(broker.executor()).thenReturn(mock(EventLoopGroup.class));
        when(broker.getTopicOrderedExecutor()).thenReturn(mock(OrderedExecutor.class));
        when(broker.getSegmentOwnerMetrics()).thenReturn(metrics);
        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn("segment://tenant/namespace/topic/0000-ffff-0");
        when(topic.getBrokerService()).thenReturn(broker);
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("subscription");
        return new PersistentEntryBucketDispatcherMultipleConsumers(topic, cursor, mock(Subscription.class), conf,
                ksmWithRanges(new int[][]{{0, 0x3FFF}, {0x4000, 0x7FFF}, {0x8000, 0xBFFF}, {0xC000, 0xFFFF}}));
    }

    private static MessageMetadata metadata() {
        return new MessageMetadata().setProducerName("producer").setSequenceId(1).setPublishTime(1)
                .setPartitionKey("key");
    }

    @DataProvider
    public Object[][] routingMetadata() {
        return new Object[][]{
                {metadata().setEntryHashMin(0).setEntryHashMax(0x3FFF), 1, null},
                {metadata().setEntryHashMin(0x4000).setEntryHashMax(0x7FFF).setNumMessagesInBatch(2), 0x4000, null},
                {metadata().setEntryHashMin(0x3FFF).setEntryHashMax(0x4000), 1, EntryBucketViolation.STRADDLING},
                {metadata().setEntryHashMin(0).setEntryHashMax(0xFFFF).setNumMessagesInBatch(2),
                        1, EntryBucketViolation.STRADDLING},
                {metadata().setNumMessagesInBatch(2), null, EntryBucketViolation.UNSTAMPED_BATCH},
                // A one-message batch still has batch framing and cannot be safely routed without a stamp.
                {metadata().setNumMessagesInBatch(1), null, EntryBucketViolation.UNSTAMPED_BATCH},
                {metadata(), null, null},
                {metadata().setEntryHashMin(0x4567), 0x4000, null},
                {metadata().setEntryHashMax(0xFFFF).setNumMessagesInBatch(2),
                        null, EntryBucketViolation.UNSTAMPED_BATCH},
                {null, null, null}
        };
    }

    @Test(dataProvider = "routingMetadata")
    public void testRoutingViolationsAreRecordedOncePerCachedComputation(MessageMetadata metadata,
            Integer stampedHash, EntryBucketViolation violation) {
        SegmentOwnerMetrics metrics = mock(SegmentOwnerMetrics.class);
        var dispatcher = createDispatcher(metrics);
        EntryAndMetadata entry = EntryAndMetadata.create(mock(Entry.class), metadata);
        int expectedHash = stampedHash == null
                ? dispatcher.getSelector().makeStickyKeyHash(entry.getStickyKey()) : stampedHash;

        assertThat(dispatcher.getStickyKeyHash(entry)).as("initial routing hash").isEqualTo(expectedHash);
        assertThat(dispatcher.getStickyKeyHash(entry)).as("cached routing hash").isEqualTo(expectedHash);
        if (violation == null) {
            verifyNoInteractions(metrics);
        } else {
            verify(metrics).recordEntryBucketViolation(violation);
            verifyNoMoreInteractions(metrics);
        }
    }

    @Test
    public void testRoutingWithMetricsDisabled() {
        var dispatcher = createDispatcher(null);
        EntryAndMetadata straddling = EntryAndMetadata.create(mock(Entry.class),
                metadata().setEntryHashMin(0x3FFF).setEntryHashMax(0x4000));
        assertThat(dispatcher.getStickyKeyHash(straddling)).as("straddling routing remains min-based").isEqualTo(1);
        EntryAndMetadata batch = EntryAndMetadata.create(mock(Entry.class), metadata().setNumMessagesInBatch(2));
        assertThat(dispatcher.getStickyKeyHash(batch)).as("unstamped batch routing remains key-based")
                .isEqualTo(dispatcher.getSelector().makeStickyKeyHash("key".getBytes(UTF_8)));
    }

    @DataProvider
    public Object[][] rawEntryMetadata() {
        return new Object[][]{
                {metadata().setEntryHashMin(0x3FFF).setEntryHashMax(0x4000), EntryBucketViolation.STRADDLING},
                {metadata().setNumMessagesInBatch(2), EntryBucketViolation.UNSTAMPED_BATCH}
        };
    }

    @Test(dataProvider = "rawEntryMetadata")
    public void testRawEntryViolationsPreserveKeyBasedRouting(MessageMetadata metadata,
                                                             EntryBucketViolation violation) {
        SegmentOwnerMetrics metrics = mock(SegmentOwnerMetrics.class);
        var dispatcher = createDispatcher(metrics);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf data;
        try {
            data = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
        EntryImpl entry = EntryImpl.create(1, 1, data);
        data.release();
        try {
            int readerIndex = entry.getDataBuffer().readerIndex();
            int expectedHash = dispatcher.getSelector().makeStickyKeyHash("key".getBytes(UTF_8));
            assertThat(dispatcher.getStickyKeyHash(entry)).as("raw routing remains key-based").isEqualTo(expectedHash);
            assertThat(dispatcher.getStickyKeyHash(entry)).as("raw entries have no routing cache")
                    .isEqualTo(expectedHash);
            assertThat(entry.getDataBuffer().readerIndex()).as("metadata peeking preserves the entry")
                    .isEqualTo(readerIndex);
            verify(metrics, times(2)).recordEntryBucketViolation(violation);
            verifyNoMoreInteractions(metrics);
        } finally {
            entry.release();
        }
    }

    @Test(dataProvider = "rawEntryMetadata")
    public void testRawEntryUsesAvailableMetadata(MessageMetadata metadata, EntryBucketViolation violation) {
        SegmentOwnerMetrics metrics = mock(SegmentOwnerMetrics.class);
        var dispatcher = createDispatcher(metrics);
        Entry entry = mock(Entry.class);
        when(entry.getMessageMetadata()).thenReturn(metadata);
        assertThat(dispatcher.getStickyKeyHash(entry)).as("raw routing uses the available message key")
                .isEqualTo(dispatcher.getSelector().makeStickyKeyHash("key".getBytes(UTF_8)));
        verify(metrics).recordEntryBucketViolation(violation);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    public void testDrainingBucketsCountTracksBucketsRatherThanEntries() {
        var dispatcher = createDispatcher(null);
        var tracker = dispatcher.getDrainingHashesTracker();
        Consumer previousOwner = mock(Consumer.class);
        assertThat(dispatcher.getDrainingBucketsCount()).as("no handoff").isZero();
        tracker.addEntry(previousOwner, 1);
        tracker.addEntry(previousOwner, 1);
        tracker.addEntry(previousOwner, 0x4000);
        assertThat(dispatcher.getDrainingBucketsCount()).as("two buckets with three pending entries").isEqualTo(2);
        tracker.reduceRefCount(previousOwner, 1, false);
        assertThat(dispatcher.getDrainingBucketsCount()).as("partial ack keeps a bucket draining").isEqualTo(2);
        tracker.reduceRefCount(previousOwner, 1, false);
        assertThat(dispatcher.getDrainingBucketsCount()).as("final ack completes one handoff").isEqualTo(1);
        tracker.consumerRemoved(previousOwner);
        assertThat(dispatcher.getDrainingBucketsCount()).as("count does not depend on consumer stats").isEqualTo(1);
        tracker.clear();
        assertThat(dispatcher.getDrainingBucketsCount()).as("cleared tracker").isZero();
    }

    private static KeySharedMeta ksmWithRanges(int[][] ranges) {
        KeySharedMeta ksm = new KeySharedMeta().setKeySharedMode(KeySharedMode.STICKY)
                .setEntryBucketDispatch(true);
        for (int[] range : ranges) {
            ksm.addHashRange().setStart(range[0]).setEnd(range[1]);
        }
        return ksm;
    }

    @Test
    public void testValidBoundaries() {
        List<Range> ranges = PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                ksmWithRanges(new int[][]{{0x0000, 0x3FFF}, {0x4000, 0x7FFF}, {0x8000, 0xBFFF}, {0xC000, 0xFFFF}}));
        assertEquals(ranges, List.of(
                Range.of(0x0000, 0x3FFF),
                Range.of(0x4000, 0x7FFF),
                Range.of(0x8000, 0xBFFF),
                Range.of(0xC000, 0xFFFF)));

        // A single bucket spanning the whole ring is valid.
        assertEquals(PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                ksmWithRanges(new int[][]{{0x0000, 0xFFFF}})), List.of(Range.of(0x0000, 0xFFFF)));
    }

    @Test
    public void testInvalidBoundariesAreRejected() {
        // No boundaries at all.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{})));
        // Not starting at 0.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0001, 0xFFFF}})));
        // Gap between buckets.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x3FFF}, {0x5000, 0xFFFF}})));
        // Overlapping buckets.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x7FFF}, {0x4000, 0xFFFF}})));
        // End before start.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x3FFF}, {0x4000, 0x2000}})));
        // Not tiling the whole 16-bit ring.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x3FFF}, {0x4000, 0x7FFF}})));
        // Bucket 0 too narrow to hold the canonical hash 1.
        assertThrows(IllegalArgumentException.class, () ->
                PersistentEntryBucketDispatcherMultipleConsumers.validateBucketBoundaries(
                        ksmWithRanges(new int[][]{{0x0000, 0x0000}, {0x0001, 0xFFFF}})));
    }
}
