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
package org.apache.pulsar.broker.service.persistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import io.netty.channel.EventLoopGroup;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentStickyKeyDispatcherMultipleConsumersTest {

    private PulsarService pulsarMock;
    private BrokerService brokerMock;
    private ManagedCursorImpl cursorMock;
    private Consumer consumerMock;
    private PersistentTopic topicMock;
    private PersistentSubscription subscriptionMock;
    private ServiceConfiguration configMock;
    private ChannelPromise channelMock;

    private PersistentStickyKeyDispatcherMultipleConsumers persistentDispatcher;

    final String topicName = "persistent://public/default/testTopic";
    final String subscriptionName = "testSubscription";

    @BeforeMethod
    public void setup() throws Exception {
        configMock = mock(ServiceConfiguration.class);
        doReturn(true).when(configMock).isSubscriptionRedeliveryTrackerEnabled();
        doReturn(100).when(configMock).getDispatcherMaxReadBatchSize();
        doReturn(true).when(configMock).isSubscriptionKeySharedUseConsistentHashing();
        doReturn(1).when(configMock).getSubscriptionKeySharedConsistentHashingReplicaPoints();

        pulsarMock = mock(PulsarService.class);
        doReturn(configMock).when(pulsarMock).getConfiguration();

        brokerMock = mock(BrokerService.class);
        doReturn(pulsarMock).when(brokerMock).pulsar();

        HierarchyTopicPolicies topicPolicies = new HierarchyTopicPolicies();
        topicPolicies.getMaxConsumersPerSubscription().updateBrokerValue(0);

        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        doReturn(eventLoopGroup).when(brokerMock).executor();
        doAnswer(invocation -> {
            ((Runnable)invocation.getArguments()[0]).run();
            return null;
        }).when(eventLoopGroup).execute(any(Runnable.class));

        topicMock = mock(PersistentTopic.class);
        doReturn(brokerMock).when(topicMock).getBrokerService();
        doReturn(topicName).when(topicMock).getName();
        doReturn(topicPolicies).when(topicMock).getHierarchyTopicPolicies();

        cursorMock = mock(ManagedCursorImpl.class);
        doReturn(null).when(cursorMock).getLastIndividualDeletedRange();
        doReturn(subscriptionName).when(cursorMock).getName();

        consumerMock = mock(Consumer.class);
        channelMock = mock(ChannelPromise.class);
        doReturn("consumer1").when(consumerMock).consumerName();
        doReturn(1000).when(consumerMock).getAvailablePermits();
        doReturn(true).when(consumerMock).isWritable();
        doReturn(channelMock).when(consumerMock).sendMessages(
                anyList(),
                any(EntryBatchSizes.class),
                any(EntryBatchIndexesAcks.class),
                anyInt(),
                anyLong(),
                anyLong(),
                any(RedeliveryTracker.class)
        );

        subscriptionMock = mock(PersistentSubscription.class);
        try (MockedStatic<DispatchRateLimiter> rateLimiterMockedStatic = mockStatic(DispatchRateLimiter.class);) {
            rateLimiterMockedStatic.when(() -> DispatchRateLimiter.isDispatchRateNeeded(
                            any(BrokerService.class),
                            any(Optional.class),
                            anyString(),
                            any(DispatchRateLimiter.Type.class)))
                    .thenReturn(false);
            persistentDispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(
                    topicMock, cursorMock, subscriptionMock, configMock,
                    new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT));
        }
    }

    @Test(timeOut = 10000)
    public void testAddConsumerWhenClosed() throws Exception {
        persistentDispatcher.close().get();
        Consumer consumer = mock(Consumer.class);
        persistentDispatcher.addConsumer(consumer);
        verify(consumer, times(1)).disconnect();
        assertEquals(0, persistentDispatcher.getConsumers().size());
        assertTrue(persistentDispatcher.getSelector().getConsumerKeyHashRanges().isEmpty());
    }

    @Test
    public void testSendMarkerMessage() {
        try {
            persistentDispatcher.addConsumer(consumerMock);
            persistentDispatcher.consumerFlow(consumerMock, 1000);
        } catch (Exception e) {
            fail("Failed to add mock consumer", e);
        }

        List<Entry> entries = new ArrayList<>();
        ByteBuf markerMessage = Markers.newReplicatedSubscriptionsSnapshotRequest("testSnapshotId", "testSourceCluster");
        entries.add(EntryImpl.create(1, 1, markerMessage));
        entries.add(EntryImpl.create(1, 2, createMessage("message1", 1)));
        entries.add(EntryImpl.create(1, 3, createMessage("message2", 2)));
        entries.add(EntryImpl.create(1, 4, createMessage("message3", 3)));
        entries.add(EntryImpl.create(1, 5, createMessage("message4", 4)));
        entries.add(EntryImpl.create(1, 6, createMessage("message5", 5)));

        try {
            persistentDispatcher.readEntriesComplete(entries, PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Normal);
        } catch (Exception e) {
            fail("Failed to readEntriesComplete.", e);
        }

        Awaitility.await().untilAsserted(() -> {
                    ArgumentCaptor<Integer> totalMessagesCaptor = ArgumentCaptor.forClass(Integer.class);
                    verify(consumerMock, times(1)).sendMessages(
                            anyList(),
                            any(EntryBatchSizes.class),
                            any(EntryBatchIndexesAcks.class),
                            totalMessagesCaptor.capture(),
                            anyLong(),
                            anyLong(),
                            any(RedeliveryTracker.class)
                    );

                    List<Integer> allTotalMessagesCaptor = totalMessagesCaptor.getAllValues();
                    Assert.assertEquals(allTotalMessagesCaptor.get(0).intValue(), 5);
                });
    }

    @Test(timeOut = 10000)
    public void testSendMessage() {
        try (MockedStatic<DispatchRateLimiter> rateLimiterMockedStatic = mockStatic(DispatchRateLimiter.class);) {
            rateLimiterMockedStatic.when(() -> DispatchRateLimiter.isDispatchRateNeeded(
                            any(BrokerService.class),
                            any(Optional.class),
                            anyString(),
                            any(DispatchRateLimiter.Type.class)))
                    .thenReturn(false);
            KeySharedMeta keySharedMeta = new KeySharedMeta().setKeySharedMode(KeySharedMode.STICKY);
            DispatchRateLimiter.isDispatchRateNeeded(brokerMock, Optional.empty(), "hello", DispatchRateLimiter.Type.SUBSCRIPTION);
            PersistentStickyKeyDispatcherMultipleConsumers persistentDispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(
                    topicMock, cursorMock, subscriptionMock, configMock, keySharedMeta);
            try {
                keySharedMeta.addHashRange()
                        .setStart(0)
                        .setEnd(9);

                Consumer consumerMock = mock(Consumer.class);
                doReturn(keySharedMeta).when(consumerMock).getKeySharedMeta();
                persistentDispatcher.addConsumer(consumerMock);
                persistentDispatcher.consumerFlow(consumerMock, 1000);
            } catch (Exception e) {
                fail("Failed to add mock consumer", e);
            }

            List<Entry> entries = new ArrayList<>();
            entries.add(EntryImpl.create(1, 1, createMessage("message1", 1)));
            entries.add(EntryImpl.create(1, 2, createMessage("message2", 2)));

            try {
                //Should success,see issue #8960
                persistentDispatcher.readEntriesComplete(entries, PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Normal);
            } catch (Exception e) {
                fail("Failed to readEntriesComplete.", e);
            }
        }
    }

    @Test
    public void testSkipRedeliverTemporally() {
        final Consumer slowConsumerMock = mock(Consumer.class);
        final ChannelPromise slowChannelMock = mock(ChannelPromise.class);
        // add entries to redeliver and read target
        final List<Entry> redeliverEntries = new ArrayList<>();
        redeliverEntries.add(EntryImpl.create(1, 1, createMessage("message1", 1, "key1")));
        final List<Entry> readEntries = new ArrayList<>();
        readEntries.add(EntryImpl.create(1, 2, createMessage("message2", 2, "key1")));
        readEntries.add(EntryImpl.create(1, 3, createMessage("message3", 3, "key2")));

        try {
            Field totalAvailablePermitsField = PersistentDispatcherMultipleConsumers.class.getDeclaredField("totalAvailablePermits");
            totalAvailablePermitsField.setAccessible(true);
            totalAvailablePermitsField.set(persistentDispatcher, 1000);

            doAnswer(invocationOnMock -> {
                ((PersistentStickyKeyDispatcherMultipleConsumers) invocationOnMock.getArgument(2))
                        .readEntriesComplete(readEntries, PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Normal);
                return null;
            }).when(cursorMock).asyncReadEntriesOrWait(
                    anyInt(), anyLong(), any(PersistentStickyKeyDispatcherMultipleConsumers.class),
                    eq(PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Normal), any());
        } catch (Exception e) {
            fail("Failed to set to field", e);
        }

        // Create 2Consumers
        try {
            doReturn("consumer2").when(slowConsumerMock).consumerName();
            // Change slowConsumer availablePermits to 0 and back to normal
            when(slowConsumerMock.getAvailablePermits())
                    .thenReturn(0)
                    .thenReturn(1);
            doReturn(true).when(slowConsumerMock).isWritable();
            doReturn(slowChannelMock).when(slowConsumerMock).sendMessages(
                    anyList(),
                    any(EntryBatchSizes.class),
                    any(EntryBatchIndexesAcks.class),
                    anyInt(),
                    anyLong(),
                    anyLong(),
                    any(RedeliveryTracker.class)
            );

            persistentDispatcher.addConsumer(consumerMock);
            persistentDispatcher.addConsumer(slowConsumerMock);
        } catch (Exception e) {
            fail("Failed to add mock consumer", e);
        }

        // run PersistentStickyKeyDispatcherMultipleConsumers#sendMessagesToConsumers
        // run readMoreEntries internally (and skip internally)
        // Change slowConsumer availablePermits to 1
        // run PersistentStickyKeyDispatcherMultipleConsumers#sendMessagesToConsumers internally
        // and then stop to dispatch to slowConsumer
        persistentDispatcher.sendMessagesToConsumers(PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Normal, redeliverEntries);

        Awaitility.await().untilAsserted(() -> {
            verify(consumerMock, times(1)).sendMessages(
                    argThat(arg -> {
                        assertEquals(arg.size(), 1);
                        Entry entry = arg.get(0);
                        assertEquals(entry.getLedgerId(), 1);
                        assertEquals(entry.getEntryId(), 3);
                        return true;
                    }),
                    any(EntryBatchSizes.class),
                    any(EntryBatchIndexesAcks.class),
                    anyInt(),
                    anyLong(),
                    anyLong(),
                    any(RedeliveryTracker.class)
            );
        });
        verify(slowConsumerMock, times(0)).sendMessages(
                anyList(),
                any(EntryBatchSizes.class),
                any(EntryBatchIndexesAcks.class),
                anyInt(),
                anyLong(),
                anyLong(),
                any(RedeliveryTracker.class)
        );
    }

    @Test(timeOut = 30000)
    public void testMessageRedelivery() throws Exception {
        final Queue<Position> actualEntriesToConsumer1 = new ConcurrentLinkedQueue<>();
        final Queue<Position> actualEntriesToConsumer2 = new ConcurrentLinkedQueue<>();

        final Queue<Position> expectedEntriesToConsumer1 = new ConcurrentLinkedQueue<>();
        expectedEntriesToConsumer1.add(PositionImpl.get(1, 1));
        final Queue<Position> expectedEntriesToConsumer2 = new ConcurrentLinkedQueue<>();
        expectedEntriesToConsumer2.add(PositionImpl.get(1, 2));
        expectedEntriesToConsumer2.add(PositionImpl.get(1, 3));

        final AtomicInteger remainingEntriesNum = new AtomicInteger(
                expectedEntriesToConsumer1.size() + expectedEntriesToConsumer2.size());

        // Messages with key1 are routed to consumer1 and messages with key2 are routed to consumer2
        final List<Entry> allEntries = new ArrayList<>();
        allEntries.add(EntryImpl.create(1, 1, createMessage("message1", 1, "key2")));
        allEntries.add(EntryImpl.create(1, 2, createMessage("message2", 2, "key1")));
        allEntries.add(EntryImpl.create(1, 3, createMessage("message3", 3, "key1")));
        allEntries.forEach(entry -> ((EntryImpl) entry).retain());

        final List<Entry> redeliverEntries = new ArrayList<>();
        redeliverEntries.add(allEntries.get(0)); // message1
        final List<Entry> readEntries = new ArrayList<>();
        readEntries.add(allEntries.get(2)); // message3

        final Consumer consumer1 = mock(Consumer.class);
        doReturn("consumer1").when(consumer1).consumerName();
        // Change availablePermits of consumer1 to 0 and then back to normal
        when(consumer1.getAvailablePermits()).thenReturn(0).thenReturn(10);
        doReturn(true).when(consumer1).isWritable();
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            List<Entry> entries = (List<Entry>) invocationOnMock.getArgument(0);
            for (Entry entry : entries) {
                remainingEntriesNum.decrementAndGet();
                actualEntriesToConsumer1.add(entry.getPosition());
            }
            return channelMock;
        }).when(consumer1).sendMessages(anyList(), any(EntryBatchSizes.class), any(EntryBatchIndexesAcks.class),
                anyInt(), anyLong(), anyLong(), any(RedeliveryTracker.class));

        final Consumer consumer2 = mock(Consumer.class);
        doReturn("consumer2").when(consumer2).consumerName();
        when(consumer2.getAvailablePermits()).thenReturn(10);
        doReturn(true).when(consumer2).isWritable();
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            List<Entry> entries = (List<Entry>) invocationOnMock.getArgument(0);
            for (Entry entry : entries) {
                remainingEntriesNum.decrementAndGet();
                actualEntriesToConsumer2.add(entry.getPosition());
            }
            return channelMock;
        }).when(consumer2).sendMessages(anyList(), any(EntryBatchSizes.class), any(EntryBatchIndexesAcks.class),
                anyInt(), anyLong(), anyLong(), any(RedeliveryTracker.class));

        persistentDispatcher.addConsumer(consumer1);
        persistentDispatcher.addConsumer(consumer2);

        final Field totalAvailablePermitsField = PersistentDispatcherMultipleConsumers.class
                .getDeclaredField("totalAvailablePermits");
        totalAvailablePermitsField.setAccessible(true);
        totalAvailablePermitsField.set(persistentDispatcher, 1000);

        final Field redeliveryMessagesField = PersistentDispatcherMultipleConsumers.class
                .getDeclaredField("redeliveryMessages");
        redeliveryMessagesField.setAccessible(true);
        MessageRedeliveryController redeliveryMessages = (MessageRedeliveryController) redeliveryMessagesField
                .get(persistentDispatcher);
        redeliveryMessages.add(allEntries.get(0).getLedgerId(), allEntries.get(0).getEntryId(),
                getStickyKeyHash(allEntries.get(0))); // message1
        redeliveryMessages.add(allEntries.get(1).getLedgerId(), allEntries.get(1).getEntryId(),
                getStickyKeyHash(allEntries.get(1))); // message2

        // Mock Cursor#asyncReplayEntries
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Set<Position> positions = (Set<Position>) invocationOnMock.getArgument(0);
            List<Entry> entries = allEntries.stream().filter(entry -> positions.contains(entry.getPosition()))
                    .collect(Collectors.toList());
            if (!entries.isEmpty()) {
                ((PersistentStickyKeyDispatcherMultipleConsumers) invocationOnMock.getArgument(1))
                        .readEntriesComplete(entries, PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Replay);
            }
            return Collections.emptySet();
        }).when(cursorMock).asyncReplayEntries(anySet(), any(PersistentStickyKeyDispatcherMultipleConsumers.class),
                eq(PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Replay), anyBoolean());

        // Mock Cursor#asyncReadEntriesOrWait
        AtomicBoolean asyncReadEntriesOrWaitCalled = new AtomicBoolean();
        doAnswer(invocationOnMock -> {
            if (asyncReadEntriesOrWaitCalled.compareAndSet(false, true)) {
                ((PersistentStickyKeyDispatcherMultipleConsumers) invocationOnMock.getArgument(2))
                        .readEntriesComplete(readEntries, PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Normal);
            } else {
                ((PersistentStickyKeyDispatcherMultipleConsumers) invocationOnMock.getArgument(2))
                        .readEntriesComplete(Collections.emptyList(), PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Normal);
            }
            return null;
        }).when(cursorMock).asyncReadEntriesOrWait(anyInt(), anyLong(),
                any(PersistentStickyKeyDispatcherMultipleConsumers.class),
                eq(PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Normal), any());

        // (1) Run sendMessagesToConsumers
        // (2) Attempts to send message1 to consumer1 but skipped because availablePermits is 0
        // (3) Change availablePermits of consumer1 to 10
        // (4) Run readMoreEntries internally
        // (5) Run sendMessagesToConsumers internally
        // (6) Attempts to send message3 to consumer2 but skipped because redeliveryMessages contains message2
        persistentDispatcher.sendMessagesToConsumers(PersistentStickyKeyDispatcherMultipleConsumers.ReadType.Replay,
                redeliverEntries);
        while (remainingEntriesNum.get() > 0) {
            // (7) Run readMoreEntries and resend message1 to consumer1 and message2-3 to consumer2
            persistentDispatcher.readMoreEntries();
        }

        assertEquals(actualEntriesToConsumer1, expectedEntriesToConsumer1);
        assertEquals(actualEntriesToConsumer2, expectedEntriesToConsumer2);

        allEntries.forEach(entry -> entry.release());
    }

    private ByteBuf createMessage(String message, int sequenceId) {
        return createMessage(message, sequenceId, "testKey");
    }

    private ByteBuf createMessage(String message, int sequenceId, String key) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKey(key)
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }

    private int getStickyKeyHash(Entry entry) {
        byte[] stickyKey = Commands.peekStickyKey(entry.getDataBuffer(), topicName, subscriptionName);
        return StickyKeyConsumerSelector.makeStickyKeyHash(stickyKey);
    }
}
