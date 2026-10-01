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

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Key_Shared;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsumerBlockedPermitAccountingTest {

    private static final String TOPIC = "persistent://public/default/blocked-flow-permit-accounting";
    private static final int MAX_UNACKED_MESSAGES = 10;
    private static final int FLOW_PERMITS = 100;
    private static final long LEDGER_ID = 1;
    private static final long ENTRY_ID = 2;

    private Subscription subscription;
    private TransportCnx cnx;
    private HierarchyTopicPolicies policies;
    private AtomicInteger dispatcherFlowPermits;
    private AtomicInteger positiveDispatcherFlowCalls;
    private AtomicInteger zeroDispatcherFlowCalls;

    enum UnblockPath {
        ACK,
        FULL_REDELIVERY,
        SELECTIVE_REDELIVERY
    }

    enum PausePoint {
        BLOCKED_FLOW,
        MAX_UNACKED_READ,
        BLOCK_PUBLICATION
    }

    @BeforeMethod
    public void setup() {
        ServiceConfiguration configuration = mock(ServiceConfiguration.class);
        PulsarService pulsar = mock(PulsarService.class);
        BrokerService brokerService = mock(BrokerService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        subscription = mock(PersistentSubscription.class);
        cnx = mock(TransportCnx.class);
        policies = new HierarchyTopicPolicies();
        policies.getMaxUnackedMessagesOnConsumer().updateBrokerValue(MAX_UNACKED_MESSAGES);

        when(topic.getHierarchyTopicPolicies()).thenReturn(policies);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(pulsar.getConfiguration()).thenReturn(configuration);
        when(pulsar.getConfig()).thenReturn(configuration);
        when(configuration.isTransactionCoordinatorEnabled()).thenReturn(true);
        when(subscription.getTopic()).thenReturn(topic);
        when(subscription.getName()).thenReturn("sub");
        when(((PersistentSubscription) subscription).transactionIndividualAcknowledge(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        dispatcherFlowPermits = new AtomicInteger();
        positiveDispatcherFlowCalls = new AtomicInteger();
        zeroDispatcherFlowCalls = new AtomicInteger();
        doAnswer(invocation -> {
            Consumer flowedConsumer = invocation.getArgument(0);
            int permits = invocation.getArgument(1);
            assertThat(readRemovalBalanceFromAnotherThread(flowedConsumer)).isEqualTo(dispatcherFlowPermits.get());
            flowedConsumer.completePendingDispatcherFlow(permits);
            dispatcherFlowPermits.addAndGet(permits);
            if (permits > 0) {
                positiveDispatcherFlowCalls.incrementAndGet();
            } else if (permits == 0) {
                zeroDispatcherFlowCalls.incrementAndGet();
            }
            return null;
        }).when(subscription).consumerFlow(any(), anyInt());
    }

    @DataProvider(name = "individualAckUnblockVariants")
    public Object[][] individualAckUnblockVariants() {
        return new Object[][] {
                {Shared, UnblockPath.ACK},
                {Key_Shared, UnblockPath.ACK},
                {Shared, UnblockPath.FULL_REDELIVERY},
                {Key_Shared, UnblockPath.FULL_REDELIVERY},
                {Shared, UnblockPath.SELECTIVE_REDELIVERY},
                {Key_Shared, UnblockPath.SELECTIVE_REDELIVERY}
        };
    }

    @Test(dataProvider = "individualAckUnblockVariants", timeOut = 30_000)
    @SuppressWarnings("unchecked")
    public void testFlowRacingWithUnblockDoesNotStrandPermits(SubType subType, UnblockPath unblockPath)
            throws Exception {
        PausingConsumer consumer = new PausingConsumer(subscription, subType, cnx);
        Consumer ackConsumer = newConsumer(subType, 2);
        AtomicInteger redeliveryCalls = new AtomicInteger();
        setConsumerState(consumer, true, 1, 0);
        if (unblockPath != UnblockPath.ACK) {
            assertThat(consumer.getPendingAcks().addPendingAckIfAllowed(LEDGER_ID, ENTRY_ID, 1, 0)).isTrue();
            doAnswer(invocation -> {
                redeliveryCalls.incrementAndGet();
                assertThat(consumer.isBlocked()).isFalse();
                assertThat(consumer.getAvailablePermits()).isEqualTo(FLOW_PERMITS);
                assertThat(readRemovalBalanceFromAnotherThread(consumer)).isZero();
                assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isZero();
                return null;
            }).when(subscription).redeliverUnacknowledgedMessages(any(), any(List.class));
        }

        consumer.pauseAt(PausePoint.BLOCKED_FLOW);
        runRace(consumer, () -> consumer.flowPermits(FLOW_PERMITS),
                () -> unblock(ackConsumer, consumer, unblockPath));

        assertThat(consumer.isBlocked()).isFalse();
        assertThat(consumer.getAvailablePermits()).isEqualTo(FLOW_PERMITS);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(FLOW_PERMITS);
        assertThat(dispatcherFlowPermits).hasValue(FLOW_PERMITS);
        assertThat(positiveDispatcherFlowCalls).hasValue(1);
        assertThat(redeliveryCalls).hasValue(unblockPath == UnblockPath.ACK ? 0 : 1);

        consumer.flowConsumerBlockedPermits(consumer);
        assertThat(consumer.getAvailablePermits()).isEqualTo(FLOW_PERMITS);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(FLOW_PERMITS);
        assertThat(dispatcherFlowPermits).hasValue(FLOW_PERMITS);
        assertThat(positiveDispatcherFlowCalls).hasValue(1);
        assertThat(zeroDispatcherFlowCalls).hasValue(1);
    }

    @Test(timeOut = 30_000)
    public void testUnackedIncrementDoesNotPublishStaleBlockedState() throws Exception {
        PausingConsumer consumer = new PausingConsumer(subscription, Shared, cnx);
        assertThat(consumer.getPendingAcks()
                .addPendingAckIfAllowed(LEDGER_ID, ENTRY_ID, MAX_UNACKED_MESSAGES, 0)).isTrue();
        consumer.pauseAt(PausePoint.MAX_UNACKED_READ);
        runRace(consumer, () -> consumer.incrementUnackedMessagesForTesting(MAX_UNACKED_MESSAGES),
                () -> consumer.removePendingAcksUpToPositionAndDecrementUnacked(LEDGER_ID, ENTRY_ID));

        assertUnblockedAndCanFlow(consumer, 0);
    }

    @Test(timeOut = 30_000)
    public void testPolicyUpdateWaitsForBlockStatePublication() throws Exception {
        PausingConsumer consumer = new PausingConsumer(subscription, Key_Shared, cnx);
        assertThat(consumer.getPendingAcks()
                .addPendingAckIfAllowed(LEDGER_ID, ENTRY_ID, MAX_UNACKED_MESSAGES, 0)).isTrue();
        consumer.pauseAt(PausePoint.BLOCK_PUBLICATION);
        runRace(consumer, () -> consumer.incrementUnackedMessagesForTesting(MAX_UNACKED_MESSAGES),
                () -> {
                    policies.getMaxUnackedMessagesOnConsumer().updateBrokerValue(0);
                    consumer.reconcileBlockedStateAfterPolicyUpdate();
                });

        assertUnblockedAndCanFlow(consumer, MAX_UNACKED_MESSAGES);
    }

    @Test
    public void testTransactionalBatchIndexAckUnblocksAtHalfLimit() {
        Consumer consumer = newConsumer(Shared, 1);
        setConsumerState(consumer, true, MAX_UNACKED_MESSAGES, 0);
        assertThat(consumer.getPendingAcks()
                .addPendingAckIfAllowed(LEDGER_ID, ENTRY_ID, MAX_UNACKED_MESSAGES, 0)).isTrue();
        consumer.flowPermits(FLOW_PERMITS);

        CommandAck ack = new CommandAck()
                .setConsumerId(1)
                .setAckType(CommandAck.AckType.Individual)
                .setTxnidMostBits(1)
                .setTxnidLeastBits(2);
        ack.addMessageId()
                .setLedgerId(LEDGER_ID)
                .setEntryId(ENTRY_ID)
                .setBatchSize(MAX_UNACKED_MESSAGES)
                .addAckSet(0b1_1111L);
        assertThat(consumer.messageAcked(ack, true)).isCompletedWithValue(null);

        assertThat(consumer.getUnackedMessages()).isEqualTo(MAX_UNACKED_MESSAGES / 2);
        assertThat(consumer.isBlocked()).isFalse();
        assertThat(consumer.getAvailablePermits()).isEqualTo(FLOW_PERMITS);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(FLOW_PERMITS);
        assertThat(dispatcherFlowPermits).hasValue(FLOW_PERMITS);
        assertThat(positiveDispatcherFlowCalls).hasValue(1);
    }

    @Test
    public void testUnblockingEmptyBucketPreservesZeroDispatcherNotification() {
        Consumer consumer = newConsumer(Shared, 1);
        setConsumerState(consumer, true, 0, 0);
        consumer.updateBlockedConsumerOnUnackedMsgs(consumer);
        assertThat(consumer.isBlocked()).isFalse();
        assertThat(consumer.getAvailablePermits()).isZero();
        assertThat(zeroDispatcherFlowCalls).hasValue(1);
        assertThat(positiveDispatcherFlowCalls).hasValue(0);
    }

    @Test
    public void testBlockedPermitTransferPreservesSignedIntWrap() {
        Consumer consumer = newConsumer(Key_Shared, 1);
        setConsumerState(consumer, true, 0, 0);
        consumer.flowPermits(Integer.MAX_VALUE);
        consumer.flowPermits(Integer.MAX_VALUE);
        consumer.updateBlockedConsumerOnUnackedMsgs(consumer);

        assertThat(consumer.isBlocked()).isFalse();
        assertThat(consumer.getAvailablePermits()).isEqualTo(-2);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(-2);
        assertThat(dispatcherFlowPermits).hasValue(-2);
    }

    private Consumer newConsumer(SubType subType, long consumerId) {
        return new Consumer(subscription, subType, TOPIC, consumerId, 0, "consumer-" + consumerId, true,
                cnx, "role", emptyMap(), false, new KeySharedMeta(), MessageId.latest, DEFAULT_CONSUMER_EPOCH);
    }

    private static void setConsumerState(Consumer consumer, boolean blocked, int unackedMessages,
                                         int availablePermits) {
        ConsumerStatsImpl stats = new ConsumerStatsImpl();
        stats.blockedConsumerOnUnackedMsgs = blocked;
        stats.unackedMessages = unackedMessages;
        stats.availablePermits = availablePermits;
        consumer.updateStats(stats);
    }

    private static FutureTask<Void> task(ThrowingRunnable action) {
        return new FutureTask<>(() -> {
            action.run();
            return null;
        });
    }

    private void assertUnblockedAndCanFlow(Consumer consumer, int expectedUnackedMessages) {
        assertThat(consumer.getUnackedMessages()).isEqualTo(expectedUnackedMessages);
        assertThat(consumer.isBlocked()).isFalse();
        consumer.flowPermits(1);
        assertThat(consumer.getAvailablePermits()).isOne();
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isOne();
        assertThat(dispatcherFlowPermits).hasValue(1);
        assertThat(positiveDispatcherFlowCalls).hasValue(1);
    }

    private static void runRace(PausingConsumer consumer, ThrowingRunnable pausedAction,
                                ThrowingRunnable racingAction) throws Exception {
        FutureTask<Void> pausedTask = task(pausedAction);
        Thread pausedThread = new Thread(pausedTask, "paused-consumer-operation");
        pausedThread.start();
        assertThat(consumer.awaitPaused()).isTrue();

        FutureTask<Void> racingTask = task(racingAction);
        Thread racingThread = new Thread(racingTask, "racing-consumer-operation");
        racingThread.start();
        try {
            awaitCompletionOrBlockedOn(racingThread, pausedThread);
        } finally {
            consumer.release();
        }
        pausedTask.get(10, SECONDS);
        racingTask.get(10, SECONDS);
    }

    private void unblock(Consumer ackConsumer, Consumer ackOwnedConsumer, UnblockPath unblockPath) {
        switch (unblockPath) {
            case ACK -> ackConsumer.updateBlockedConsumerOnUnackedMsgs(ackOwnedConsumer);
            case FULL_REDELIVERY -> ackOwnedConsumer.redeliverUnacknowledgedMessages(DEFAULT_CONSUMER_EPOCH);
            case SELECTIVE_REDELIVERY -> {
                MessageIdData messageId = new MessageIdData().setLedgerId(LEDGER_ID).setEntryId(ENTRY_ID);
                ackOwnedConsumer.redeliverUnacknowledgedMessages(List.of(messageId));
            }
        }
    }

    private static void awaitCompletionOrBlockedOn(Thread waiter, Thread lockOwner) {
        Awaitility.await().atMost(10, SECONDS).until(() -> {
            if (!waiter.isAlive()) {
                return true;
            }
            ThreadInfo threadInfo = ManagementFactory.getThreadMXBean().getThreadInfo(waiter.threadId());
            return threadInfo != null
                    && threadInfo.getThreadState() == Thread.State.BLOCKED
                    && threadInfo.getLockOwnerId() == lockOwner.threadId();
        });
    }

    private static int readRemovalBalanceFromAnotherThread(Consumer consumer) throws Exception {
        FutureTask<Integer> removalBalance = new FutureTask<>(consumer::getAvailablePermitsForDispatcherRemoval);
        Thread thread = new Thread(removalBalance, "consumer-removal-balance-reader");
        thread.setDaemon(true);
        thread.start();
        return removalBalance.get(5, SECONDS);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class PausingConsumer extends Consumer {
        private final AtomicBoolean pauseOnce = new AtomicBoolean();
        private final CountDownLatch paused = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile PausePoint pausePoint;

        PausingConsumer(Subscription subscription, SubType subType, TransportCnx cnx) {
            super(subscription, subType, TOPIC, 1, 0, "consumer", true, cnx, "role", emptyMap(), false,
                    new KeySharedMeta(), MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        }

        void pauseAt(PausePoint pausePoint) {
            this.pausePoint = pausePoint;
        }

        @Override
        void beforeAddingBlockedFlowPermits() {
            pauseIf(PausePoint.BLOCKED_FLOW);
        }

        @Override
        public int getMaxUnackedMessages() {
            pauseIf(PausePoint.MAX_UNACKED_READ);
            return super.getMaxUnackedMessages();
        }

        @Override
        void beforeSettingBlockedConsumerOnUnackedMessages() {
            pauseIf(PausePoint.BLOCK_PUBLICATION);
        }

        boolean awaitPaused() throws InterruptedException {
            return paused.await(10, SECONDS);
        }

        void release() {
            release.countDown();
        }

        private void pauseIf(PausePoint expected) {
            if (pausePoint == expected && pauseOnce.compareAndSet(false, true)) {
                paused.countDown();
                Awaitility.await().atMost(10, SECONDS).until(() -> release.getCount() == 0);
            }
        }
    }
}
