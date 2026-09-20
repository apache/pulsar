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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumersClassic;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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
        conf.setTopicFactoryClassName(RegistrationRaceTopicFactory.class.getName());
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

    @DataProvider
    public static Object[][] remainingUnackedMessages() {
        return new Object[][] {{0}, {1}};
    }

    @Test(timeOut = 60_000, dataProvider = "remainingUnackedMessages")
    public void testAckBelowLowWatermarkRacingWithDispatcherBlockRegistration(int remaining) throws Exception {
        conf.setMaxUnackedMessagesPerSubscription(100);
        String targetName = newTopicName() + "-late-block";
        String pressureName = newTopicName();
        CountDownLatch beforeBlock = new CountDownLatch(1);
        CountDownLatch resumeBlock = new CountDownLatch(1);
        AtomicReference<Thread> scanner = new AtomicReference<>();
        try (Producer<String> pressureProducer = pulsarClient.newProducer(Schema.STRING)
                     .topic(pressureName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> pressure = pulsarClient.newConsumer(Schema.STRING)
                     .topic(pressureName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(targetName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(targetName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
            // This independent subscription keeps the broker above its global unblock threshold throughout.
            for (int i = 0; i < 9; i++) {
                pressureProducer.send("pressure-" + i);
                assertThat(pressure.receive(5, TimeUnit.SECONDS)).isNotNull();
            }
            List<MessageId> messages = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                producer.send("target-" + i);
                messages.add(client.receive(5, TimeUnit.SECONDS).getMessageId());
            }
            BrokerService broker = pulsar.getBrokerService();
            RegistrationRaceTopic topic = (RegistrationRaceTopic) broker.getTopicReference(targetName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            assertThat(dispatcher).isInstanceOf(classic
                    ? PersistentDispatcherMultipleConsumersClassic.class : PersistentDispatcherMultipleConsumers.class);
            Awaitility.await().untilAsserted(() -> assertThat(broker.getTotalUnackedMessages()).isEqualTo(15));
            assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs()).isFalse();
            topic.beforeBlock.set(() -> {
                scanner.set(Thread.currentThread());
                beforeBlock.countDown();
                try {
                    assertThat(resumeBlock.await(10, TimeUnit.SECONDS)).as("resume block registration").isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            try {
                broker.checkUnAckMessageDispatching();
                assertThat(beforeBlock.await(10, TimeUnit.SECONDS)).as("scan passed the high-water check").isTrue();
                // The scan can share a Netty event loop with this client's connection. Submit its real ACK
                // command directly so pausing that event loop does not also prevent receipt of the ACK.
                Consumer owner = dispatcher.getConsumers().get(0);
                CommandAck ack = new CommandAck().setConsumerId(owner.consumerId())
                        .setAckType(CommandAck.AckType.Individual);
                for (MessageId id : messages.subList(remaining, messages.size())) {
                    addAckId(ack, id);
                }
                owner.messageAcked(ack, true).get(5, TimeUnit.SECONDS);
                assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(remaining);
                assertThat(broker.getTotalUnackedMessages()).isEqualTo(9 + remaining);
                assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs()).isFalse();
                resumeBlock.countDown();
                // Do not let an assertion pass on the pre-registration false flag while the scan is paused.
                Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !isUnackedScan(
                        ManagementFactory.getThreadMXBean().getThreadInfo(scanner.get().getId(), 100)));
                for (int i = 0; i < 5; i++) {
                    broker.checkUnAckMessageDispatching();
                    assertThat(broker.isBrokerDispatchingBlocked()).isTrue();
                    assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs())
                            .as("ACK below low water must cancel a late block").isFalse();
                }
                producer.send("after-late-block");
                Message<String> resumed = client.receive(5, TimeUnit.SECONDS);
                assertThat(resumed).as("normal delivery resumes while broker stays blocked").isNotNull();
                client.acknowledgeAsync(resumed).get(5, TimeUnit.SECONDS);
                assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(remaining);
                assertThat(broker.isBrokerDispatchingBlocked()).isTrue();
            } finally {
                resumeBlock.countDown();
            }
        } finally {
            resumeBlock.countDown();
        }
    }

    @Test(timeOut = 60_000)
    public void testGroupedWholeEntryAckCountsMessagesInBatches() throws Exception {
        String topicName = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(true).batchingMaxMessages(3)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .subscribe()) {
            List<MessageId> entries = new ArrayList<>();
            for (int batch = 0; batch < 3; batch++) {
                List<CompletableFuture<MessageId>> sends = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    sends.add(producer.sendAsync("batch-" + batch + "-" + i));
                }
                producer.flush();
                for (CompletableFuture<MessageId> send : sends) {
                    send.get(5, TimeUnit.SECONDS);
                }
                for (int i = 0; i < 3; i++) {
                    Message<String> message = client.receive(5, TimeUnit.SECONDS);
                    assertThat(message).isNotNull();
                    if (i == 0) {
                        entries.add(message.getMessageId());
                    }
                }
            }
            BrokerService broker = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) broker.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            Consumer owner = dispatcher.getConsumers().get(0);
            assertThat(owner.getUnackedMessages()).isEqualTo(9);
            for (MessageId entry : entries) {
                MessageIdImpl id = (MessageIdImpl) entry;
                assertThat(owner.getPendingAcks().getRemainingUnacked(id.getLedgerId(), id.getEntryId())).isEqualTo(3);
            }
            // A real whole-entry ACK deliberately omits batch-index ack sets. Keep the first batch pending
            // so mark-delete cleanup cannot consume these entries before the grouped completion runs.
            CommandAck ack = new CommandAck().setConsumerId(owner.consumerId())
                    .setAckType(CommandAck.AckType.Individual);
            addAckId(ack, entries.get(1));
            addAckId(ack, entries.get(2));
            addAckId(ack, entries.get(1));
            owner.messageAcked(ack, true).get(5, TimeUnit.SECONDS);
            assertThat(owner.getUnackedMessages()).isEqualTo(3);
            assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(3);
            assertThat(broker.getTotalUnackedMessages()).isEqualTo(3);
            ack.clear().setConsumerId(owner.consumerId()).setAckType(CommandAck.AckType.Individual);
            addAckId(ack, entries.get(0));
            owner.messageAcked(ack, true).get(5, TimeUnit.SECONDS);
            assertUnackedMessagesCleared(dispatcher, broker, "whole-entry ACK of batched messages");
        }
    }

    @Test(timeOut = 60_000)
    public void testGroupedAckRemovalBeforeAccountingFlush() throws Exception {
        String topicName = newTopicName();
        CountDownLatch secondEntryRemoved = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
            List<MessageId> messages = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                producer.send("message-" + i);
                messages.add(client.receive(5, TimeUnit.SECONDS).getMessageId());
            }
            BrokerService broker = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) broker.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            Consumer owner = dispatcher.getConsumers().get(0);
            AtomicInteger removed = new AtomicInteger();
            owner.setPendingAcksRemoveHandler(new PendingAcksMap.PendingAcksRemoveHandler() {
                @Override
                public void handleRemoving(Consumer c, long ledgerId, long entryId, int hash, boolean closing) {
                    if (removed.incrementAndGet() == 2) {
                        secondEntryRemoved.countDown();
                        try {
                            assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                }

                @Override
                public void startBatch() { }

                @Override
                public void endBatch() { }
            });
            try {
                CompletableFuture<Void> ack = client.acknowledgeAsync(messages.subList(1, messages.size()));
                assertThat(secondEntryRemoved.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(owner.getUnackedMessages()).as("first debit is still command-local").isEqualTo(4);
                dispatcher.removeConsumer(owner);
                resume.countDown();
                ack.get(10, TimeUnit.SECONDS);
                assertThat(owner.getUnackedMessages()).isZero();
                assertUnackedMessagesCleared(dispatcher, broker, "removal before grouped flush");
            } finally {
                resume.countDown();
                owner.setPendingAcksRemoveHandler(null);
            }
        } finally {
            resume.countDown();
        }
    }

    @Test(timeOut = 60_000)
    public void testGroupedKeySharedAckRacingWithOwnerRemoval() throws Exception {
        conf.setSubscriptionKeySharedUseClassicPersistentImplementation(classic);
        conf.setSubscriptionKeySharedUseConsistentHashing(false);
        String topicName = newTopicName();
        CountDownLatch secondEntryRemoved = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Key_Shared)
                     .consumerName("old-owner").subscribe()) {
            BrokerService broker = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) broker.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            StickyKeyConsumerSelector selector = ((StickyKeyDispatcher) dispatcher).getSelector();
            // The range selector gives its lower half to the next consumer. Choose a real key in that half.
            String key = null;
            int hash = 0;
            for (int i = 0; i < 1000; i++) {
                String candidate = "transferred-key-" + i;
                int candidateHash = selector.makeStickyKeyHash(candidate.getBytes(StandardCharsets.UTF_8));
                if (candidateHash <= selector.getKeyHashRange().getEnd() / 2) {
                    key = candidate;
                    hash = candidateHash;
                    break;
                }
            }
            assertThat(key).isNotNull();
            List<MessageId> messages = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                producer.newMessage().key(key).value("message-" + i).send();
                messages.add(client.receive(5, TimeUnit.SECONDS).getMessageId());
            }
            Consumer owner = dispatcher.getConsumers().get(0);
            try (org.apache.pulsar.client.api.Consumer<String> successor = pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Key_Shared)
                    .consumerName("new-owner").acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                    .isAckReceiptEnabled(true).subscribe()) {
                assertThat(selector.select(hash).consumerName()).isEqualTo("new-owner");
                DrainingHashesTracker tracker = classic ? null
                        : ((PersistentStickyKeyDispatcherMultipleConsumers) dispatcher).getDrainingHashesTracker();
                if (tracker != null) {
                    assertThat(tracker.getEntry(hash).getRefCount()).isEqualTo(4);
                }
                // Preserve the actual draining-hash callbacks; only pause after the second removal callback.
                PendingAcksMap.PendingAcksRemoveHandler delegate = owner.getPendingAcksRemoveHandler();
                AtomicInteger removed = new AtomicInteger();
                owner.setPendingAcksRemoveHandler(new PendingAcksMap.PendingAcksRemoveHandler() {
                    @Override
                    public void handleRemoving(Consumer c, long ledgerId, long entryId, int stickyHash,
                                               boolean closing) {
                        if (delegate != null) {
                            delegate.handleRemoving(c, ledgerId, entryId, stickyHash, closing);
                        }
                        if (removed.incrementAndGet() == 2) {
                            secondEntryRemoved.countDown();
                            try {
                                assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(e);
                            }
                        }
                    }

                    @Override
                    public void startBatch() {
                        if (delegate != null) {
                            delegate.startBatch();
                        }
                    }

                    @Override
                    public void endBatch() {
                        if (delegate != null) {
                            delegate.endBatch();
                        }
                    }
                });
                try {
                    CommandAck ack = new CommandAck().setConsumerId(owner.consumerId())
                            .setAckType(CommandAck.AckType.Individual);
                    for (MessageId id : messages.subList(1, messages.size())) {
                        addAckId(ack, id);
                    }
                    CompletableFuture<Void> completion = CompletableFuture.supplyAsync(() ->
                            owner.messageAcked(ack, true)).thenCompose(future -> future);
                    assertThat(secondEntryRemoved.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(owner.getUnackedMessages()).as("grouped debit has not flushed").isEqualTo(4);
                    if (tracker != null) {
                        assertThat(tracker.getEntry(hash).getRefCount()).isEqualTo(2);
                    }
                    CompletableFuture<Void> removal = CompletableFuture.runAsync(() -> {
                        try {
                            owner.close();
                        } catch (BrokerServiceException e) {
                            throw new AssertionError(e);
                        }
                    });
                    Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                            assertThat(owner.getUnackedMessages())
                                    .as("removal settles before late ACK flush").isZero());
                    resume.countDown();
                    completion.get(5, TimeUnit.SECONDS);
                    removal.get(5, TimeUnit.SECONDS);
                    Message<String> replayed = successor.receive(5, TimeUnit.SECONDS);
                    assertThat(replayed).isNotNull();
                    assertThat(replayed.getValue()).isEqualTo("message-0");
                    successor.acknowledgeAsync(replayed).get(5, TimeUnit.SECONDS);
                    producer.newMessage().key(key).value("after-transfer").send();
                    Message<String> next = successor.receive(5, TimeUnit.SECONDS);
                    assertThat(next).isNotNull();
                    assertThat(next.getValue()).isEqualTo("after-transfer");
                    successor.acknowledgeAsync(next).get(5, TimeUnit.SECONDS);
                    assertThat(owner.getUnackedMessages()).isZero();
                    assertUnackedMessagesCleared(dispatcher, broker, "grouped ACK and hash ownership transfer");
                    if (tracker != null) {
                        assertThat(tracker.getEntry(hash)).isNull();
                    }
                } finally {
                    resume.countDown();
                    owner.setPendingAcksRemoveHandler(delegate);
                }
            }
        } finally {
            resume.countDown();
        }
    }

    @Test(timeOut = 60_000)
    public void testGroupedAckByAnotherConsumerResumesDelivery() throws Exception {
        conf.setMaxUnackedMessagesPerConsumer(UNACKED_MESSAGES);
        String topicName = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> owner = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
            List<MessageId> messages = new ArrayList<>();
            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                producer.send("message-" + i);
                messages.add(owner.receive(5, TimeUnit.SECONDS).getMessageId());
            }
            BrokerService broker = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) broker.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            Consumer brokerOwner = dispatcher.getConsumers().get(0);
            assertThat(brokerOwner.isBlocked()).isTrue();
            assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs()).isTrue();
            try (org.apache.pulsar.client.api.Consumer<String> receiver = pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                    .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
                receiver.acknowledgeAsync(messages.subList(1, messages.size())).get(5, TimeUnit.SECONDS);
                // Repeated grouped ACKs must not debit the same pending entries twice.
                receiver.acknowledgeAsync(messages.subList(1, messages.size())).get(5, TimeUnit.SECONDS);
                assertThat(brokerOwner.getUnackedMessages()).isEqualTo(1);
                assertThat(brokerOwner.isBlocked()).isFalse();
                assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(1);
                assertThat(broker.getTotalUnackedMessages()).isEqualTo(1);
                assertThat(dispatcher.isBlockedDispatcherOnUnackedMsgs()).isFalse();
            }
            producer.send("after-grouped-ack");
            Message<String> resumed = owner.receive(5, TimeUnit.SECONDS);
            assertThat(resumed).isNotNull();
            owner.acknowledgeAsync(List.of(messages.get(0), resumed.getMessageId())).get(5, TimeUnit.SECONDS);
            assertUnackedMessagesCleared(dispatcher, broker, "grouped ACK and resumed delivery");
        }
    }

    @Test(timeOut = 60_000)
    public void testGroupedAckWithMixedOwnersAndDuplicateIds() throws Exception {
        String topicName = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> first = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .consumerName("first").receiverQueueSize(0).subscribe()) {
            List<MessageId> firstIds = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                producer.send("first-" + i);
                firstIds.add(first.receiveAsync().get(5, TimeUnit.SECONDS).getMessageId());
            }
            try (org.apache.pulsar.client.api.Consumer<String> second = pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                    .consumerName("second").receiverQueueSize(0).subscribe()) {
                List<MessageId> secondIds = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    producer.send("second-" + i);
                    secondIds.add(second.receiveAsync().get(5, TimeUnit.SECONDS).getMessageId());
                }
                BrokerService broker = pulsar.getBrokerService();
                PersistentTopic topic = (PersistentTopic) broker.getTopicReference(topicName).orElseThrow();
                AbstractPersistentDispatcherMultipleConsumers dispatcher =
                        (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION)
                                .getDispatcher();
                Consumer receiver = dispatcher.getConsumers().stream()
                        .filter(c -> c.consumerName().equals("first")).findFirst().orElseThrow();
                CommandAck ack = new CommandAck().setConsumerId(receiver.consumerId())
                        .setAckType(CommandAck.AckType.Individual);
                // Submit the real broker command to retain duplicate IDs and owner ordering, which the
                // client's ACK grouping may otherwise normalize. Every ID was actually delivered above.
                for (MessageId id : List.of(firstIds.get(1), firstIds.get(2), secondIds.get(1),
                        secondIds.get(2), firstIds.get(1))) {
                    addAckId(ack, id);
                }
                receiver.messageAcked(ack, true).get(5, TimeUnit.SECONDS);
                for (Consumer consumer : dispatcher.getConsumers()) {
                    assertThat(consumer.getUnackedMessages()).isEqualTo(1);
                }
                assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(2);
                assertThat(broker.getTotalUnackedMessages()).isEqualTo(2);
                ack.clear().setConsumerId(receiver.consumerId()).setAckType(CommandAck.AckType.Individual);
                addAckId(ack, firstIds.get(0));
                addAckId(ack, secondIds.get(0));
                receiver.messageAcked(ack, true).get(5, TimeUnit.SECONDS);
                assertUnackedMessagesCleared(dispatcher, broker, "mixed owners and duplicate IDs");
                producer.send("after-mixed-ack");
                assertThat(first.receiveAsync().get(5, TimeUnit.SECONDS)).isNotNull();
            }
        }
    }

    @Test(timeOut = 60_000)
    public void testGroupedAckFlushesCompletedRemovalsOnFailure() throws Exception {
        String topicName = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .subscribe()) {
            List<MessageId> messages = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                producer.send("message-" + i);
                messages.add(client.receive(5, TimeUnit.SECONDS).getMessageId());
            }
            BrokerService broker = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) broker.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            Consumer owner = dispatcher.getConsumers().get(0);
            AtomicInteger removed = new AtomicInteger();
            IllegalStateException failure = new IllegalStateException("injected pending-ack callback failure");
            owner.setPendingAcksRemoveHandler(new PendingAcksMap.PendingAcksRemoveHandler() {
                @Override
                public void handleRemoving(Consumer c, long ledgerId, long entryId, int hash, boolean closing) {
                    if (removed.incrementAndGet() == 3) {
                        throw failure;
                    }
                }

                @Override
                public void startBatch() { }

                @Override
                public void endBatch() { }
            });
            try {
                CommandAck ack = new CommandAck().setConsumerId(owner.consumerId())
                        .setAckType(CommandAck.AckType.Individual);
                for (MessageId id : messages.subList(1, messages.size())) {
                    addAckId(ack, id);
                }
                Throwable error = owner.messageAcked(ack, true).handle((unused, ex) -> ex)
                        .get(5, TimeUnit.SECONDS);
                assertThat(error).hasCause(failure);
                // Two removals returned successfully before the failing callback: neither debit may be lost.
                // The failing removal has not returned a balance; closing settles all remaining accounting.
                assertThat(owner.getUnackedMessages()).isEqualTo(2);
                assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(2);
                assertThat(broker.getTotalUnackedMessages()).isEqualTo(2);
            } finally {
                owner.setPendingAcksRemoveHandler(null);
            }
            owner.close();
            assertUnackedMessagesCleared(dispatcher, broker, "close after completion failure");
        }
    }

    public static class RegistrationRaceTopicFactory implements TopicFactory {
        @Override
        public <T extends Topic> T create(String topic, ManagedLedger ledger, BrokerService broker,
                                         Class<T> topicClass) {
            return topic.endsWith("-late-block") ? topicClass.cast(new RegistrationRaceTopic(topic, ledger, broker))
                    : null;
        }
    }

    private static class RegistrationRaceTopic extends PersistentTopic {
        private final AtomicReference<Runnable> beforeBlock = new AtomicReference<>();

        RegistrationRaceTopic(String topic, ManagedLedger ledger, BrokerService broker) {
            super(topic, ledger, broker);
        }

        @Override
        protected PersistentSubscription createPersistentSubscription(String name, ManagedCursor cursor,
                Boolean replicated, Map<String, String> properties) {
            if (!SUBSCRIPTION.equals(name)) {
                return super.createPersistentSubscription(name, cursor, replicated, properties);
            }
            return new PersistentSubscription(this, name, cursor, replicated) {
                @Override
                protected Dispatcher reuseOrCreateDispatcher(Dispatcher existing, Consumer consumer) {
                    if (existing != null || consumer.subType() != SubType.Shared) {
                        return super.reuseOrCreateDispatcher(existing, consumer);
                    }
                    if (topic.getBrokerService().pulsar().getConfiguration()
                            .isSubscriptionSharedUseClassicPersistentImplementation()) {
                        return new PersistentDispatcherMultipleConsumersClassic(topic, cursor, this) {
                            @Override
                            public void blockDispatcherOnUnackedMsgs() {
                                beforeBlock();
                                super.blockDispatcherOnUnackedMsgs();
                            }
                        };
                    }
                    return new PersistentDispatcherMultipleConsumers(topic, cursor, this) {
                        @Override
                        public void blockDispatcherOnUnackedMsgs() {
                            beforeBlock();
                            super.blockDispatcherOnUnackedMsgs();
                        }
                    };
                }
            };
        }

        private void beforeBlock() {
            Runnable action = beforeBlock.getAndSet(null);
            if (action != null) {
                action.run();
            }
        }
    }

    private static void addAckId(CommandAck ack, MessageId messageId) {
        MessageIdImpl id = (MessageIdImpl) messageId;
        ack.addMessageId().setLedgerId(id.getLedgerId()).setEntryId(id.getEntryId());
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
