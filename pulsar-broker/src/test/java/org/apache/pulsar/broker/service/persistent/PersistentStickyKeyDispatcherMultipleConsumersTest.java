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
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentStickyKeyDispatcherMultipleConsumersTest {

    private PulsarService pulsarMock;
    private BrokerService brokerMock;
    private ManagedLedgerImpl ledgerMock;
    private ManagedCursorImpl cursorMock;
    private Consumer consumerMock;
    private PersistentTopic topicMock;
    private PersistentSubscription subscriptionMock;
    private ServiceConfiguration configMock;
    private ChannelPromise channelMock;
    private OrderedExecutor orderedExecutor;

    private PersistentStickyKeyDispatcherMultipleConsumers persistentDispatcher;

    final String topicName = "persistent://public/default/testTopic";
    final String subscriptionName = "testSubscription";
    private AtomicInteger consumerMockAvailablePermits;
    int retryBackoffInitialTimeInMs = 10;
    int retryBackoffMaxTimeInMs = 50;

    @BeforeMethod
    public void setup() throws Exception {
        configMock = mock(ServiceConfiguration.class);
        doReturn(true).when(configMock).isSubscriptionRedeliveryTrackerEnabled();
        doReturn(100).when(configMock).getDispatcherMaxReadBatchSize();
        doReturn(true).when(configMock).isSubscriptionKeySharedUseConsistentHashing();
        doReturn(1).when(configMock).getSubscriptionKeySharedConsistentHashingReplicaPoints();
        doReturn(false).when(configMock).isDispatcherDispatchMessagesInSubscriptionThread();
        doReturn(false).when(configMock).isAllowOverrideEntryFilters();
        doAnswer(invocation -> retryBackoffInitialTimeInMs).when(configMock).getDispatcherRetryBackoffInitialTimeInMs();
        doAnswer(invocation -> retryBackoffMaxTimeInMs).when(configMock).getDispatcherRetryBackoffMaxTimeInMs();
        pulsarMock = mock(PulsarService.class);
        doReturn(configMock).when(pulsarMock).getConfiguration();

        EntryFilterProvider mockEntryFilterProvider = mock(EntryFilterProvider.class);
        when(mockEntryFilterProvider.getBrokerEntryFilters()).thenReturn(Collections.emptyList());

        brokerMock = mock(BrokerService.class);
        doReturn(pulsarMock).when(brokerMock).pulsar();
        when(brokerMock.getEntryFilterProvider()).thenReturn(mockEntryFilterProvider);

        HierarchyTopicPolicies topicPolicies = new HierarchyTopicPolicies();
        topicPolicies.getMaxConsumersPerSubscription().updateBrokerValue(0);

        orderedExecutor = OrderedExecutor.newBuilder().build();
        doReturn(orderedExecutor).when(brokerMock).getTopicOrderedExecutor();

        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        doReturn(eventLoopGroup).when(brokerMock).executor();
        doAnswer(invocation -> {
            orderedExecutor.execute(((Runnable)invocation.getArguments()[0]));
            return null;
        }).when(eventLoopGroup).execute(any(Runnable.class));

        topicMock = mock(PersistentTopic.class);
        doReturn(brokerMock).when(topicMock).getBrokerService();
        doReturn(topicName).when(topicMock).getName();
        doReturn(topicPolicies).when(topicMock).getHierarchyTopicPolicies();

        ledgerMock = mock(ManagedLedgerImpl.class);
        doAnswer((invocationOnMock -> {
            final Position position = invocationOnMock.getArgument(0);
            if (position.getEntryId() > 0) {
                return PositionFactory.create(position.getLedgerId(), position.getEntryId() - 1);
            } else {
                fail("Undefined behavior on mock");
                return PositionFactory.EARLIEST;
            }
        })).when(ledgerMock).getPreviousPosition(any(Position.class));
        doAnswer((invocationOnMock -> {
            final Position position = invocationOnMock.getArgument(0);
            return PositionFactory.create(position.getLedgerId(), position.getEntryId() < 0 ? 0 : position.getEntryId() + 1);
        })).when(ledgerMock).getNextValidPosition(any(Position.class));
        doAnswer((invocationOnMock -> {
            final Range<Position> range = invocationOnMock.getArgument(0);
            Position fromPosition = range.lowerEndpoint();
            boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
            Position toPosition = range.upperEndpoint();
            boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;

            long count = 0;

            if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
                // If the 2 positions are in the same ledger
                count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
                count += fromIncluded ? 1 : 0;
                count += toIncluded ? 1 : 0;
            } else {
                fail("Undefined behavior on mock");
            }
            return count;
        })).when(ledgerMock).getNumberOfEntries(any());

        cursorMock = mock(ManagedCursorImpl.class);
        doReturn(null).when(cursorMock).getLastIndividualDeletedRange();
        doReturn(subscriptionName).when(cursorMock).getName();
        doReturn(ledgerMock).when(cursorMock).getManagedLedger();

        consumerMock = createMockConsumer();
        channelMock = mock(ChannelPromise.class);
        doReturn("consumer1").when(consumerMock).consumerName();
        consumerMockAvailablePermits = new AtomicInteger(1000);
        doAnswer(invocation -> consumerMockAvailablePermits.get()).when(consumerMock).getAvailablePermits();
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
        when(subscriptionMock.getTopic()).thenReturn(topicMock);
        persistentDispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(
                topicMock, cursorMock, subscriptionMock, configMock,
                new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT));
    }

    protected static Consumer createMockConsumer() {
        Consumer consumerMock = mock(Consumer.class);
        TransportCnx transportCnx = mock(TransportCnx.class);
        doReturn(transportCnx).when(consumerMock).cnx();
        doReturn(true).when(transportCnx).isActive();
        return consumerMock;
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (persistentDispatcher != null && !persistentDispatcher.isClosed()) {
            persistentDispatcher.close();
        }
        if (orderedExecutor != null) {
            orderedExecutor.shutdownNow();
            orderedExecutor = null;
        }
    }

    @Test(timeOut = 10000)
    public void testAddConsumerWhenClosed() throws Exception {
        persistentDispatcher.close().get();
        Consumer consumer = createMockConsumer();
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
        KeySharedMeta keySharedMeta = new KeySharedMeta().setKeySharedMode(KeySharedMode.STICKY);
        PersistentStickyKeyDispatcherMultipleConsumers persistentDispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(
                topicMock, cursorMock, subscriptionMock, configMock, keySharedMeta);
        try {
            keySharedMeta.addHashRange()
                    .setStart(0)
                    .setEnd(9);

            Consumer consumerMock = createMockConsumer();
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

    @Test
    public void testSkipRedeliverTemporally() {
        final Consumer slowConsumerMock = createMockConsumer();
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
        persistentDispatcher.readEntriesComplete(redeliverEntries,
                PersistentDispatcherMultipleConsumers.ReadType.Replay);

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
        expectedEntriesToConsumer1.add(PositionFactory.create(1, 1));
        final Queue<Position> expectedEntriesToConsumer2 = new ConcurrentLinkedQueue<>();
        expectedEntriesToConsumer2.add(PositionFactory.create(1, 2));
        expectedEntriesToConsumer2.add(PositionFactory.create(1, 3));

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

        final Consumer consumer1 = createMockConsumer();
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

        final Consumer consumer2 = createMockConsumer();
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
                redeliverEntries, true);
        while (remainingEntriesNum.get() > 0) {
            // (7) Run readMoreEntries and resend message1 to consumer1 and message2-3 to consumer2
            persistentDispatcher.readMoreEntries();
        }

        assertThat(actualEntriesToConsumer1).containsExactlyElementsOf(expectedEntriesToConsumer1);
        assertThat(actualEntriesToConsumer2).containsExactlyElementsOf(expectedEntriesToConsumer2);

        allEntries.forEach(entry -> entry.release());
    }

    @DataProvider(name = "testBackoffDelayWhenNoMessagesDispatched")
    private Object[][] testBackoffDelayWhenNoMessagesDispatchedParams() {
        return new Object[][] { { false, true }, { true, true }, { true, false }, { false, false } };
    }

    @Test(dataProvider = "testBackoffDelayWhenNoMessagesDispatched")
    public void testBackoffDelayWhenNoMessagesDispatched(boolean dispatchMessagesInSubscriptionThread, boolean isKeyShared)
            throws Exception {
        persistentDispatcher.close();

        List<Long> retryDelays = new CopyOnWriteArrayList<>();
        doReturn(dispatchMessagesInSubscriptionThread).when(configMock).isDispatcherDispatchMessagesInSubscriptionThread();

        PersistentDispatcherMultipleConsumers dispatcher;
        if (isKeyShared) {
            dispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(
                    topicMock, cursorMock, subscriptionMock, configMock,
                    new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT)) {
                @Override
                protected void reScheduleReadInMs(long readAfterMs) {
                    retryDelays.add(readAfterMs);
                }
            };
        } else {
            dispatcher = new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock) {
                @Override
                protected void reScheduleReadInMs(long readAfterMs) {
                    retryDelays.add(readAfterMs);
                }
            };
        }

        // add a consumer without permits to trigger the retry behavior
        consumerMockAvailablePermits.set(0);
        dispatcher.addConsumer(consumerMock);

        // call "readEntriesComplete" directly to test the retry behavior
        List<Entry> entries = List.of(EntryImpl.create(1, 1, createMessage("message1", 1)));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        Awaitility.await().untilAsserted(() -> {
                    assertEquals(retryDelays.size(), 1);
                    assertEquals(retryDelays.get(0), 10, "Initial retry delay should be 10ms");
                }
        );
        // test the second retry delay
        entries = List.of(EntryImpl.create(1, 1, createMessage("message1", 1)));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        Awaitility.await().untilAsserted(() -> {
                    assertEquals(retryDelays.size(), 2);
                    double delay = retryDelays.get(1);
                    assertEquals(delay, 20.0, 2.0, "Second retry delay should be 20ms (jitter <-10%)");
                }
        );
        // verify the max retry delay
        for (int i = 0; i < 100; i++) {
            entries = List.of(EntryImpl.create(1, 1, createMessage("message1", 1)));
            dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        }
        Awaitility.await().untilAsserted(() -> {
                    assertEquals(retryDelays.size(), 102);
                    double delay = retryDelays.get(101);
                    assertEquals(delay, 50.0, 5.0, "Max delay should be 50ms (jitter <-10%)");
                }
        );
        // unblock to check that the retry delay is reset
        consumerMockAvailablePermits.set(1000);
        entries = List.of(EntryImpl.create(1, 2, createMessage("message2", 1, "key2")));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        // wait that the possibly async handling has completed
        Awaitility.await().untilAsserted(() -> assertFalse(dispatcher.isSendInProgress()));

        // now block again to check the next retry delay so verify it was reset
        consumerMockAvailablePermits.set(0);
        entries = List.of(EntryImpl.create(1, 3, createMessage("message3", 1, "key3")));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        Awaitility.await().untilAsserted(() -> {
                    assertEquals(retryDelays.size(), 103);
                    assertEquals(retryDelays.get(0), 10, "Resetted retry delay should be 10ms");
                }
        );
    }

    @Test(dataProvider = "testBackoffDelayWhenNoMessagesDispatched")
    public void testBackoffDelayWhenRetryDelayDisabled(boolean dispatchMessagesInSubscriptionThread, boolean isKeyShared)
            throws Exception {
        persistentDispatcher.close();

        // it should be possible to disable the retry delay
        // by setting retryBackoffInitialTimeInMs and retryBackoffMaxTimeInMs to 0
        retryBackoffInitialTimeInMs=0;
        retryBackoffMaxTimeInMs=0;

        List<Long> retryDelays = new CopyOnWriteArrayList<>();
        doReturn(dispatchMessagesInSubscriptionThread).when(configMock)
                .isDispatcherDispatchMessagesInSubscriptionThread();

        PersistentDispatcherMultipleConsumers dispatcher;
        if (isKeyShared) {
            dispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(
                    topicMock, cursorMock, subscriptionMock, configMock,
                    new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT)) {
                @Override
                protected void reScheduleReadInMs(long readAfterMs) {
                    retryDelays.add(readAfterMs);
                }
            };
        } else {
            dispatcher = new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock) {
                @Override
                protected void reScheduleReadInMs(long readAfterMs) {
                    retryDelays.add(readAfterMs);
                }
            };
        }

        // add a consumer without permits to trigger the retry behavior
        consumerMockAvailablePermits.set(0);
        dispatcher.addConsumer(consumerMock);

        // call "readEntriesComplete" directly to test the retry behavior
        List<Entry> entries = List.of(EntryImpl.create(1, 1, createMessage("message1", 1)));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        Awaitility.await().untilAsserted(() -> {
                    assertEquals(retryDelays.size(), 1);
                    assertEquals(retryDelays.get(0), 0, "Initial retry delay should be 0ms");
                }
        );
        // test the second retry delay
        entries = List.of(EntryImpl.create(1, 1, createMessage("message1", 1)));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        Awaitility.await().untilAsserted(() -> {
                    assertEquals(retryDelays.size(), 2);
                    double delay = retryDelays.get(1);
                    assertEquals(delay, 0, 0, "Second retry delay should be 0ms");
                }
        );
        // verify the max retry delay
        for (int i = 0; i < 100; i++) {
            entries = List.of(EntryImpl.create(1, 1, createMessage("message1", 1)));
            dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        }
        Awaitility.await().untilAsserted(() -> {
                    assertEquals(retryDelays.size(), 102);
                    double delay = retryDelays.get(101);
                    assertEquals(delay, 0, 0, "Max delay should be 0ms");
                }
        );
        // unblock to check that the retry delay is reset
        consumerMockAvailablePermits.set(1000);
        entries = List.of(EntryImpl.create(1, 2, createMessage("message2", 1, "key2")));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        // wait that the possibly async handling has completed
        Awaitility.await().untilAsserted(() -> assertFalse(dispatcher.isSendInProgress()));

        // now block again to check the next retry delay so verify it was reset
        consumerMockAvailablePermits.set(0);
        entries = List.of(EntryImpl.create(1, 3, createMessage("message3", 1, "key3")));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        Awaitility.await().untilAsserted(() -> {
                    assertEquals(retryDelays.size(), 103);
                    assertEquals(retryDelays.get(0), 0, "Resetted retry delay should be 0ms");
                }
        );
    }


    @Test(dataProvider = "testBackoffDelayWhenNoMessagesDispatched")
    public void testNoBackoffDelayWhenDelayedMessages(boolean dispatchMessagesInSubscriptionThread, boolean isKeyShared)
            throws Exception {
        persistentDispatcher.close();

        doReturn(dispatchMessagesInSubscriptionThread).when(configMock)
                .isDispatcherDispatchMessagesInSubscriptionThread();

        AtomicInteger readMoreEntriesCalled = new AtomicInteger(0);
        AtomicInteger reScheduleReadInMsCalled = new AtomicInteger(0);
        AtomicBoolean delayAllMessages = new AtomicBoolean(true);

        PersistentDispatcherMultipleConsumers dispatcher;
        if (isKeyShared) {
            dispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(
                    topicMock, cursorMock, subscriptionMock, configMock,
                    new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT)) {
                @Override
                protected void reScheduleReadInMs(long readAfterMs) {
                    reScheduleReadInMsCalled.incrementAndGet();
                }

                @Override
                public synchronized void readMoreEntries() {
                    readMoreEntriesCalled.incrementAndGet();
                }

                @Override
                public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
                    if (delayAllMessages.get()) {
                        // simulate delayed message
                        return true;
                    }
                    return super.trackDelayedDelivery(ledgerId, entryId, msgMetadata);
                }
            };
        } else {
            dispatcher = new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock) {
                @Override
                protected void reScheduleReadInMs(long readAfterMs) {
                    reScheduleReadInMsCalled.incrementAndGet();
                }

                @Override
                public synchronized void readMoreEntries() {
                    readMoreEntriesCalled.incrementAndGet();
                }

                @Override
                public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
                    if (delayAllMessages.get()) {
                        // simulate delayed message
                        return true;
                    }
                    return super.trackDelayedDelivery(ledgerId, entryId, msgMetadata);
                }
            };
        }

        doAnswer(invocationOnMock -> {
            GenericFutureListener<Future<Void>> listener = invocationOnMock.getArgument(0);
            Future<Void> future = mock(Future.class);
            when(future.isDone()).thenReturn(true);
            listener.operationComplete(future);
            return channelMock;
        }).when(channelMock).addListener(any());

        // add a consumer with permits
        consumerMockAvailablePermits.set(1000);
        dispatcher.addConsumer(consumerMock);

        List<Entry> entries = new ArrayList<>(List.of(EntryImpl.create(1, 1, createMessage("message1", 1))));
        dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(reScheduleReadInMsCalled.get(), 0, "reScheduleReadInMs should not be called");
            assertTrue(readMoreEntriesCalled.get() >= 1);
        });
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
