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

import static org.assertj.core.api.Assertions.assertThat;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumersClassic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Consumer removal must debit both subscription and broker unacknowledged-message counters exactly once.
 * Each dispatcher variant owns its broker so configuration changes cannot affect the shared test cluster.
 */
@Test(groups = "broker-api")
public class SharedSubscriptionUnackedMessagesAccountingTest extends ProducerConsumerBase {
    private static final String SUBSCRIPTION = "shared-churn-sub";
    private static final int UNACKED_MESSAGES = 10;
    private final boolean classic;

    @Factory
    public static Object[] createTestInstances() {
        return new Object[] {new SharedSubscriptionUnackedMessagesAccountingTest(false),
                new SharedSubscriptionUnackedMessagesAccountingTest(true)};
    }

    public SharedSubscriptionUnackedMessagesAccountingTest(boolean classic) {
        this.classic = classic;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setSubscriptionSharedUseClassicPersistentImplementation(classic);
        conf.setMaxUnackedMessagesPerBroker(UNACKED_MESSAGES);
        conf.setMaxUnackedMessagesPerSubscription(UNACKED_MESSAGES);
        conf.setMaxUnackedMessagesPerSubscriptionOnBrokerBlocked(0.5);
    }

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60_000)
    public void testRemovingSameConsumerTwiceDebitsUnackedMessagesOnce() throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> departing = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName)
                     .subscriptionName(SUBSCRIPTION)
                     .subscriptionType(SubscriptionType.Shared)
                     .consumerName("departing")
                     .receiverQueueSize(5)
                     .subscribe()) {
            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                producer.send("unacked-" + i);
            }
            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                assertThat(departing.receive(2, TimeUnit.SECONDS))
                        .as("delivery %s to leave unacknowledged", i).isNotNull();
            }

            BrokerService brokerService = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) brokerService.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            assertThat(dispatcher).as("configured dispatcher implementation").isInstanceOf(classic
                    ? PersistentDispatcherMultipleConsumersClassic.class : PersistentDispatcherMultipleConsumers.class);
            Consumer brokerConsumer = dispatcher.getConsumers().get(0);

            // Serialize with dispatch so both aggregate credits have completed before checking their values.
            synchronized (dispatcher) {
                assertThat(brokerConsumer.getUnackedMessages()).as("departing consumer balance")
                        .isEqualTo(UNACKED_MESSAGES);
                assertThat(dispatcher.getTotalUnackedMessages()).as("subscription balance before removal")
                        .isEqualTo(UNACKED_MESSAGES);
                assertThat(brokerService.getTotalUnackedMessages()).as("broker balance before removal")
                        .isEqualTo(UNACKED_MESSAGES);

                // With no survivor there is no replay to race with these assertions. Checking the broker as well
                // also rejects a fix that merely resets the subscription counter when the last consumer leaves.
                dispatcher.removeConsumer(brokerConsumer);
                assertUnackedMessagesCleared(dispatcher, brokerService, "first removal");
                dispatcher.removeConsumer(brokerConsumer);
                assertUnackedMessagesCleared(dispatcher, brokerService, "repeated removal");
            }
        }
    }

    @Test(timeOut = 60_000)
    public void testAckCompletionRacingWithRemovalDoesNotDebitTwice() throws Exception {
        String topicName = newTopicName();
        CountDownLatch pendingAckRemoved = new CountDownLatch(1);
        CountDownLatch resumeAck = new CountDownLatch(1);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
            producer.send("leave-unacked");
            producer.send("ack-out-of-order");
            assertThat(client.receive(5, TimeUnit.SECONDS)).isNotNull();
            Message<String> acked = client.receive(5, TimeUnit.SECONDS);
            assertThat(acked).isNotNull();
            BrokerService brokerService = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) brokerService.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            Consumer consumer = dispatcher.getConsumers().get(0);
            // Pause the real ACK completion after removing its pending entry, before settling the counters.
            // Acknowledge out of order so mark-delete cleanup cannot consume the entry first.
            consumer.setPendingAcksRemoveHandler(new PendingAcksMap.PendingAcksRemoveHandler() {
                @Override
                public void handleRemoving(Consumer c, long ledgerId, long entryId, int hash, boolean closing) {
                    pendingAckRemoved.countDown();
                    try {
                        assertThat(resumeAck.await(10, TimeUnit.SECONDS)).as("resume ACK completion").isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }

                @Override
                public void startBatch() { }

                @Override
                public void endBatch() { }
            });
            try {
                CompletableFuture<Void> ack = client.acknowledgeAsync(acked);
                assertThat(pendingAckRemoved.await(10, TimeUnit.SECONDS)).as("ACK reached completion").isTrue();
                synchronized (dispatcher) {
                    assertThat(consumer.getUnackedMessages()).isEqualTo(2);
                    dispatcher.removeConsumer(consumer);
                    assertUnackedMessagesCleared(dispatcher, brokerService, "removal while ACK is paused");
                }
                resumeAck.countDown();
                ack.get(10, TimeUnit.SECONDS);
                assertUnackedMessagesCleared(dispatcher, brokerService, "late ACK completion");
                assertThat(consumer.getUnackedMessages()).as("removed consumer balance after late ACK").isZero();
            } finally {
                resumeAck.countDown();
                consumer.setPendingAcksRemoveHandler(null);
            }
        } finally {
            resumeAck.countDown();
        }
    }

    @Test(timeOut = 60_000)
    public void testBrokerUnackedScanDoesNotBlockAckAndConsumerClose() throws Exception {
        String topicName = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
            List<MessageId> messages = new ArrayList<>();
            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                producer.send("unacked-" + i);
                Message<String> message = client.receive(5, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                messages.add(message.getMessageId());
            }
            BrokerService brokerService = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) brokerService.getTopicReference(topicName).orElseThrow();
            Subscription subscription = topic.getSubscription(SUBSCRIPTION);
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) subscription.getDispatcher();
            Consumer consumer = dispatcher.getConsumers().get(0);
            synchronized (dispatcher) {
                assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(UNACKED_MESSAGES);
                assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs()).isTrue();
            }

            long scannerThreadId;
            synchronized (subscription) {
                // Hold the same monitor as consumer close while the real broker scan reaches getDispatcher().
                // Observe the JVM's lock owner instead of mocking the scan or sleeping to guess its timing.
                brokerService.checkUnAckMessageDispatching();
                assertThat(brokerService.isBrokerDispatchingBlocked()).isTrue();
                long ownerThreadId = Thread.currentThread().getId();
                ThreadInfo scanner = Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                        Arrays.stream(ManagementFactory.getThreadMXBean().dumpAllThreads(false, false))
                                .filter(info -> info.getThreadState() == Thread.State.BLOCKED
                                        && info.getLockOwnerId() == ownerThreadId
                                        && info.getLockInfo().getIdentityHashCode()
                                                == System.identityHashCode(subscription)
                                        && isUnackedScan(info))
                                .findFirst().orElse(null), Objects::nonNull);
                scannerThreadId = scanner.getThreadId();

                // Keep the first entry unacked so mark-delete cleanup does not obscure the ACK completion path.
                // Crossing the broker's dispatcher threshold calls unblockDispatchersOnUnAckMessages.
                // This bounded wait fails on the old lock order and releases the subscription monitor, so the
                // regression cannot strand the test JVM in an unrecoverable three-thread deadlock.
                client.acknowledgeAsync(messages.subList(1, messages.size())).get(5, TimeUnit.SECONDS);
                assertThat(consumer.getUnackedMessages()).isEqualTo(1);
                assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(1);
                assertThat(brokerService.getTotalUnackedMessages()).isEqualTo(1);
                assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs()).isFalse();
                consumer.close();
                assertUnackedMessagesCleared(dispatcher, brokerService, "close while broker scan is waiting");
            }
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                    !isUnackedScan(ManagementFactory.getThreadMXBean().getThreadInfo(scannerThreadId, 100)));
            brokerService.checkUnAckMessageDispatching();
            assertThat(brokerService.isBrokerDispatchingBlocked()).isFalse();
            assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs()).isFalse();
        }
    }

    @Test(timeOut = 60_000)
    public void testBrokerUnblockWaitsForInFlightRegistration() throws Exception {
        conf.setMaxUnackedMessagesPerSubscription(1000);
        String topicName = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
            List<MessageId> messages = new ArrayList<>();
            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                producer.send("unacked-" + i);
                messages.add(client.receive(5, TimeUnit.SECONDS).getMessageId());
            }
            BrokerService broker = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) broker.getTopicReference(topicName).orElseThrow();
            Subscription subscription = topic.getSubscription(SUBSCRIPTION);
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) subscription.getDispatcher();
            synchronized (subscription) {
                broker.checkUnAckMessageDispatching();
                // The scan is waiting for this subscription. ACK through the real completion path first.
                client.acknowledgeAsync(messages.subList(1, messages.size())).get(5, TimeUnit.SECONDS);
                assertThat(broker.getTotalUnackedMessages()).isEqualTo(1);
                Lock registrationLock = broker.getUnackedMessagesLock().readLock();
                AtomicReference<Thread> checker = new AtomicReference<>();
                CompletableFuture<Void> unblock;
                registrationLock.lock();
                try {
                    // Model a scan still inside its read-side critical section. A global unblock must wait
                    // before changing its flag or selecting dispatchers, not just before clearing their flags.
                    unblock = CompletableFuture.runAsync(() -> {
                        checker.set(Thread.currentThread());
                        broker.checkUnAckMessageDispatching();
                    });
                    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
                        Thread thread = checker.get();
                        if (thread == null) {
                            return false;
                        }
                        ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId(), 100);
                        return info != null && info.getThreadState() == Thread.State.WAITING
                                && Arrays.stream(info.getStackTrace()).anyMatch(frame ->
                                        frame.getMethodName().equals("checkUnAckMessageDispatching"));
                    });
                    assertThat(broker.isBrokerDispatchingBlocked())
                            .as("broker state remains blocked until registration finishes").isTrue();
                } finally {
                    registrationLock.unlock();
                }
                unblock.get(5, TimeUnit.SECONDS);
            }
            assertThat(broker.isBrokerDispatchingBlocked()).isFalse();
            assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs()).isFalse();
            client.acknowledgeAsync(messages.get(0)).get(5, TimeUnit.SECONDS);
            assertUnackedMessagesCleared(dispatcher, broker, "global unblock");
            producer.send("after-unblock");
            Message<String> resumed = client.receive(5, TimeUnit.SECONDS);
            assertThat(resumed).isNotNull();
            client.acknowledgeAsync(resumed).get(5, TimeUnit.SECONDS);
            assertUnackedMessagesCleared(dispatcher, broker, "delivery resumed");
        }
    }

    private static boolean isUnackedScan(ThreadInfo info) {
        return info != null && Arrays.stream(info.getStackTrace()).anyMatch(frame ->
                frame.getClassName().equals(BrokerService.class.getName())
                        && frame.getMethodName().equals("blockDispatchersWithLargeUnAckMessages"));
    }

    private void assertUnackedMessagesCleared(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                              BrokerService brokerService, String removal) {
        assertThat(dispatcher.getTotalUnackedMessages()).as("subscription balance after %s", removal).isZero();
        assertThat(brokerService.getTotalUnackedMessages()).as("broker balance after %s", removal).isZero();
    }
}
