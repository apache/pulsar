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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.policies.data.PolicyHierarchyValue;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PersistentDispatcherMultipleConsumersReplayYieldTest extends SharedPulsarBaseTest {

    private static final long BOUND_MILLIS = 100;
    private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);

    private PersistentTopic topic;
    private ManagedCursorImpl cursor;
    private Subscription subscription;
    private ServiceConfiguration serviceConfig;
    private Consumer consumer;
    private int originalPerConsumerLookAhead;

    @BeforeMethod
    public void setupDispatcher() {
        topic = mock(PersistentTopic.class);
        doReturn(getPulsar().getBrokerService()).when(topic).getBrokerService();
        doReturn("persistent://public/default/replay-yield").when(topic).getName();
        doReturn(false).when(topic).isDelayedDeliveryEnabled();
        doReturn(1000L).when(topic).getDelayedDeliveryTickTimeMillis();
        doReturn(false).when(topic).isDispatcherPauseOnAckStatePersistentEnabled();
        doReturn(PositionFactory.create(1, 10)).when(topic).getMaxReadPosition();
        HierarchyTopicPolicies policies = mock(HierarchyTopicPolicies.class);
        doReturn(new PolicyHierarchyValue<Integer>()).when(policies).getMaxConsumersPerSubscription();
        doReturn(policies).when(topic).getHierarchyTopicPolicies();

        cursor = mock(ManagedCursorImpl.class);
        doReturn("sub").when(cursor).getName();
        doReturn(true).when(cursor).hasMoreEntries();
        doReturn(PositionFactory.create(1, 0)).when(cursor).getReadPosition();
        doReturn(Collections.emptySet()).when(cursor).asyncReplayEntries(anySet(), any(), any());
        doReturn(Collections.emptySet()).when(cursor).asyncReplayEntries(anySet(), any(), any(), anyBoolean());

        subscription = mock(PersistentSubscription.class);
        doReturn("sub").when(subscription).getName();
        doReturn(topic).when(subscription).getTopic();

        serviceConfig = getPulsar().getConfig();
        originalPerConsumerLookAhead = serviceConfig.getKeySharedLookAheadMsgInReplayThresholdPerConsumer();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreConfig() {
        serviceConfig.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(originalPerConsumerLookAhead);
    }

    @Test
    public void testYieldsToNormalReadOnceTheBoundElapses() {
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        dispatcher.addMessageToReplay(1, 1);

        // The first replay round that postpones the normal read starts the clock.
        runPassAt(dispatcher, 0);
        assertThat(dispatcher.havePendingReplayRead).isTrue();
        assertThat(dispatcher.havePendingRead).isFalse();

        runPassAt(dispatcher, 50 * MS);
        assertThat(dispatcher.havePendingReplayRead).as("replays keep priority before the bound").isTrue();
        verify(cursor, never()).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());

        runPassAt(dispatcher, 150 * MS);
        assertThat(dispatcher.havePendingRead).as("the bound yields one round to the normal read").isTrue();
        assertThat(dispatcher.havePendingReplayRead).isFalse();
        assertThat(dispatcher.redeliveryMessages.size()).as("the due replays stay queued").isEqualTo(1);
        verify(cursor).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());
    }

    @Test
    public void testKeepsReplaysWhileANormalReadIsPending() {
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 0);

        // A slow normal read is in flight: replays run alongside it instead of being dropped.
        dispatcher.havePendingRead = true;
        doReturn(150 * MS).when(dispatcher).currentTimeNanos();
        dispatcher.havePendingReplayRead = false;
        dispatcher.internalReadMoreEntries();

        assertThat(dispatcher.havePendingReplayRead).as("replays are still scheduled").isTrue();
        verify(cursor, never()).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());
    }

    @Test
    public void testKeepsReplaysWhileKeySharedLookAheadLimitIsReached() {
        serviceConfig.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(1);
        KeySharedMeta keySharedMeta = new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT);
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(
                new PersistentStickyKeyDispatcherMultipleConsumers(topic, cursor, subscription, serviceConfig,
                        keySharedMeta));
        // The look-ahead queue is at its limit, so the normal read is refused until the replays run.
        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 0);

        runPassAt(dispatcher, 150 * MS);

        assertThat(dispatcher.havePendingReplayRead).as("the replay that lowers the queue must still run").isTrue();
        assertThat(dispatcher.havePendingRead).isFalse();
        verify(cursor, never()).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());
    }

    @Test
    public void testRestartsTheClockWhenTheCursorReachesTheTail() {
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 0);

        // The pending normal read drains the backlog while replays keep running: the old clock
        // must not make the next entry yield straight away.
        doReturn(false).when(cursor).hasMoreEntries();
        runPassAt(dispatcher, 10 * MS);
        doReturn(true).when(cursor).hasMoreEntries();

        runPassAt(dispatcher, 500 * MS);
        assertThat(dispatcher.havePendingRead).as("a fresh entry starts a fresh clock").isFalse();
        assertThat(dispatcher.havePendingReplayRead).isTrue();

        runPassAt(dispatcher, 650 * MS);
        assertThat(dispatcher.havePendingRead).as("the bound elapses from the fresh clock").isTrue();
    }

    @Test
    public void testDoesNotCountPostponementWhileTheMaxReadPositionHoldsTheEntriesBack() {
        doReturn(PositionFactory.create(2, 0)).when(cursor).getReadPosition();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 0);

        runPassAt(dispatcher, 500 * MS);

        assertThat(dispatcher.havePendingRead).as("a normal read would deliver nothing").isFalse();
        assertThat(dispatcher.havePendingReplayRead).isTrue();
    }

    @Test
    public void testKeepsStrictPriorityWhileTheBoundIsDisabled() {
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(
                new PersistentDispatcherMultipleConsumers(topic, cursor, subscription), 0);
        dispatcher.addMessageToReplay(1, 1);

        runPassAt(dispatcher, 0);
        runPassAt(dispatcher, TimeUnit.DAYS.toNanos(1));

        assertThat(dispatcher.havePendingReplayRead).as("replays keep priority indefinitely").isTrue();
        assertThat(dispatcher.havePendingRead).isFalse();
        verify(cursor, never()).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());
    }

    @Test
    public void testRestartsTheClockWhenReplaysDrainWithoutANormalRead() {
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 0);

        // The replays drain while the normal read cannot be scheduled (a read is pending): the
        // postponement by replays still ends, so the next storm gets the full bound again.
        dispatcher.redeliveryMessages.clear();
        dispatcher.havePendingRead = true;
        dispatcher.havePendingReplayRead = false;
        doReturn(10 * MS).when(dispatcher).currentTimeNanos();
        dispatcher.internalReadMoreEntries();

        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 500 * MS);
        assertThat(dispatcher.havePendingRead).as("the next storm starts a fresh clock").isFalse();
        assertThat(dispatcher.havePendingReplayRead).isTrue();

        runPassAt(dispatcher, 650 * MS);
        assertThat(dispatcher.havePendingRead).as("the bound elapses from the fresh clock").isTrue();
    }

    @Test
    public void testReplaysGetPriorityAgainAfterAYieldedNormalRead() {
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 0);
        runPassAt(dispatcher, 150 * MS);
        assertThat(dispatcher.havePendingRead).isTrue();

        // Scheduling the normal read ended the postponement; the replays that stayed queued get
        // priority again for a full bound once it completes.
        runPassAt(dispatcher, 200 * MS);
        assertThat(dispatcher.havePendingReplayRead).as("replays get priority again").isTrue();
        runPassAt(dispatcher, 260 * MS);
        assertThat(dispatcher.havePendingRead).as("for a full bound, not the remainder of the old clock").isFalse();
        runPassAt(dispatcher, 350 * MS);
        assertThat(dispatcher.havePendingRead).isTrue();
    }

    @Test
    public void testRestartsTheClockWhenReplaysDrainWithoutPermits() {
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 0);

        // The replays drain while the consumers hold no permits, so the round returns before the
        // replay selection: the postponement must still end.
        dispatcher.redeliveryMessages.clear();
        doReturn(0).when(consumer).getAvailablePermits();
        doReturn(10 * MS).when(dispatcher).currentTimeNanos();
        dispatcher.internalReadMoreEntries();
        doReturn(100).when(consumer).getAvailablePermits();

        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 500 * MS);
        assertThat(dispatcher.havePendingRead).as("the next storm starts a fresh clock").isFalse();
        assertThat(dispatcher.havePendingReplayRead).isTrue();

        runPassAt(dispatcher, 650 * MS);
        assertThat(dispatcher.havePendingRead).as("the bound elapses from the fresh clock").isTrue();
    }

    @Test
    public void testRunsYieldedReplaysWhenTheAckStatePauseFlipsAfterTheYield() {
        doReturn(true).when(topic).isDispatcherPauseOnAckStatePersistentEnabled();
        doReturn(false).when(cursor).isCursorDataFullyPersistable();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        dispatcher.addMessageToReplay(1, 1);
        runPassAt(dispatcher, 0);

        // The pause flag flips between the yield decision and the normal-read guards: the second
        // guard check of the round sees the dispatcher paused on ack-state persistence.
        AtomicInteger guardChecks = new AtomicInteger();
        doAnswer(invocation -> {
            if (guardChecks.incrementAndGet() == 2) {
                dispatcher.afterAckMessages(null, null);
            }
            return invocation.callRealMethod();
        }).when(dispatcher).doesntHavePendingRead();

        runPassAt(dispatcher, 150 * MS);

        assertThat(dispatcher.havePendingReplayRead).as("the replays the yield gave up run instead").isTrue();
        assertThat(dispatcher.havePendingRead).isFalse();
        verify(cursor, never()).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());
    }

    @Test
    public void testKeepsDueDelayedMessagesQueuedAcrossTheYield() {
        // getMessagesToReplayNow() moves a due delayed message into the replay set before selecting,
        // and the yield only clears this round's selection: the position stays queued for the replay
        // that follows the yielded normal read.
        doReturn(true).when(topic).isDelayedDeliveryEnabled();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcher(new PersistentDispatcherMultipleConsumers(
                topic, cursor, subscription));
        assertThat(dispatcher.trackDelayedDelivery(2, 1,
                new MessageMetadata().setDeliverAtTime(System.currentTimeMillis()))).isTrue();

        // Drive passes until the tracker hands the due message to the replay set; that pass starts
        // the clock at 0.
        Awaitility.await("the due delayed message reaches the replay set").atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    runPassAt(dispatcher, 0);
                    assertThat(dispatcher.redeliveryMessages.size()).isEqualTo(1);
                    assertThat(dispatcher.havePendingReplayRead).isTrue();
                });

        runPassAt(dispatcher, 150 * MS);

        assertThat(dispatcher.havePendingRead).as("the bound yields to the normal read").isTrue();
        assertThat(dispatcher.redeliveryMessages.size()).as("the due delayed message stays queued").isEqualTo(1);
    }

    private PersistentDispatcherMultipleConsumers newDispatcher(PersistentDispatcherMultipleConsumers dispatcher) {
        return newDispatcher(dispatcher, BOUND_MILLIS);
    }

    private PersistentDispatcherMultipleConsumers newDispatcher(PersistentDispatcherMultipleConsumers dispatcher,
                                                               long boundMillis) {
        PersistentDispatcherMultipleConsumers spied = spy(dispatcher);
        doNothing().when(spied).readMoreEntriesAsync();
        spied.setMaxReplayPriorityDurationInMillisForTesting((int) boundMillis);
        consumer = connectableConsumer();
        spied.addConsumer(consumer);
        return spied;
    }

    private void runPassAt(PersistentDispatcherMultipleConsumers dispatcher, long nanos) {
        dispatcher.havePendingRead = false;
        dispatcher.havePendingReplayRead = false;
        doReturn(nanos).when(dispatcher).currentTimeNanos();
        dispatcher.internalReadMoreEntries();
    }

    private Consumer connectableConsumer() {
        Consumer consumer = mock(Consumer.class);
        TransportCnx cnx = mock(TransportCnx.class);
        doReturn(cnx).when(consumer).cnx();
        doReturn(true).when(cnx).isActive();
        doReturn(false).when(consumer).isBlocked();
        doReturn(true).when(consumer).isWritable();
        doReturn(100).when(consumer).getAvailablePermits();
        return consumer;
    }
}
