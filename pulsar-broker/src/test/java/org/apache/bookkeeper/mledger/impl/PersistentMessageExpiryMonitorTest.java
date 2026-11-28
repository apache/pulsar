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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentMessageExpiryMonitor;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class PersistentMessageExpiryMonitorTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        conf.setMessageExpiryCheckIntervalInMinutes(60);
        // The subscriptions have no active consumers, so don't cache added entries. The expiry check
        // must read them from the ledgers, which is what testTopicExpireMessages counts.
        conf.setManagedLedgerContinueCachingAddedEntriesAfterLastActiveCursorLeavesMillis(0L);
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        // testTopicExpiryCheckDoesNotReadWithoutBacklog reads the number of finds from the metrics
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    /***
     * Confirm the anti-concurrency mechanism "expirationCheckInProgressUpdater" works.
     */
    @Test
    void testConcurrentlyExpireMessages() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String cursorName = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscriptionAsync(topicName, cursorName, MessageId.earliest);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        producer.send("1");
        producer.send("2");
        producer.send("3");
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(cursorName);
        ManagedCursorImpl spyCursor = spy(cursor);

        // Make the mark-deleting delay.
        CountDownLatch firstFindingCompleted = new CountDownLatch(1);
        AtomicInteger calledFindPositionCount = new AtomicInteger();
        doAnswer(invocationOnMock -> {
            firstFindingCompleted.countDown();
            ml.getExecutor().execute(() -> {
                try {
                    Thread.sleep(3000);
                    invocationOnMock.callRealMethod();
                } catch (Throwable ex) {
                    log.error().exception(ex).log("Unexpected exception when calling mark delete");
                }
            });
            return true;
        }).when(spyCursor).asyncMarkDelete(any(Position.class), any(),
                any(AsyncCallbacks.MarkDeleteCallback.class), any());
        doAnswer(invocationOnMock -> {
            calledFindPositionCount.incrementAndGet();
            return invocationOnMock.callRealMethod();
        }).when(spyCursor).asyncFindNewestMatching(any(), any(), any(), any(), any(), any(), anyBoolean());
        doAnswer(invocationOnMock -> {
            calledFindPositionCount.incrementAndGet();
            return invocationOnMock.callRealMethod();
        }).when(spyCursor).asyncFindNewestMatching(any(), any(), any(), any(), anyBoolean());
        doAnswer(invocationOnMock -> {
            calledFindPositionCount.incrementAndGet();
            return invocationOnMock.callRealMethod();
        }).when(spyCursor).asyncFindNewestMatching(any(), any(), any(), any());

        // Sleep 2s to make "find(1s)" get a position.
        Thread.sleep(2000);

        // Start two expire tasks concurrently.
        PersistentMessageExpiryMonitor
                monitor = new PersistentMessageExpiryMonitor(persistentTopic, cursorName, spyCursor, null);
        CompletableFuture<Boolean> expireTask1 = new CompletableFuture<>();
        new Thread(() -> {
            expireTask1.complete(monitor.expireMessages(1));
        }).start();
        CompletableFuture<Boolean> expireTask2 = new CompletableFuture<>();
        new Thread(() -> {
            expireTask2.complete(monitor.expireMessages(1));
        }).start();
        firstFindingCompleted.await();
        CompletableFuture<Boolean> expireTask3 = new CompletableFuture<>();
        new Thread(() -> {
            expireTask3.complete(monitor.expireMessages(1));
        }).start();

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(monitor.getTotalMessageExpired(), 3);
        });
        // Verify: since the other 2 tasks have been prevented, the count of calling find position is 1.
        Thread.sleep(1000);
        assertEquals(1, calledFindPositionCount.get());

        // cleanup.
        producer.close();
        admin.topics().delete(topicName);
    }

    /***
     * Verify finding position task only executes once for multiple subscriptions of a topic.
     */
    @Test(invocationCount = 2)
    void testTopicExpireMessages() throws Exception {
        // Create topic.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        final String cursorName1 = "s1";
        final String cursorName2 = "s2";
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        admin.topics().createSubscription(topicName, cursorName1, MessageId.earliest);
        admin.topics().createSubscription(topicName, cursorName2, MessageId.earliest);
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedLedgerConfig configBeforeTtlUpdate = ml.getConfig();
        admin.topicPolicies().setMessageTTL(topicName, 1);
        // Applying the TTL update replaces the managed ledger config, which would drop the settings changed below
        Awaitility.await().untilAsserted(() -> {
            assertEquals(1, persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get().intValue());
            Assert.assertNotSame(ml.getConfig(), configBeforeTtlUpdate);
        });
        // The TTL update also scheduled an expiry check, which must not run after the messages are published
        pulsar.getBrokerService().getMessageExpiryMonitor().submit(() -> { }).get();
        ml.getConfig().setMaxEntriesPerLedger(2);
        ml.getConfig().setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        long firstLedger = ml.currentLedger.getId();
        System.out.println("maxEntriesPerLedger 1 : " + ml.getConfig().getMaxEntriesPerLedger());
        // Trigger 3 ledgers creation.
        for (int i = 0; i < 5; i++) {
            producer.send("" + i);
            Thread.sleep(100);
        }
        long lastPublishedAt = System.currentTimeMillis();
        System.out.println("maxEntriesPerLedger 2 : " + ml.getConfig().getMaxEntriesPerLedger());
        assertEquals(3, ml.getLedgersInfo().size());
        // Do a injection to count the access of the first ledger.
        AtomicInteger accessedCount = new AtomicInteger();
        ReadHandle readHandle = ml.getLedgerHandle(firstLedger).get();
        ReadHandle spyReadHandle = spy(readHandle);
        doAnswer(invocationOnMock -> {
            long startEntry = (long) invocationOnMock.getArguments()[0];
            if (startEntry == 0) {
                accessedCount.incrementAndGet();
            }
            return invocationOnMock.callRealMethod();
        }).when(spyReadHandle).readAsync(anyLong(), anyLong());
        ml.ledgerCache.put(firstLedger, CompletableFuture.completedFuture(spyReadHandle));
        // Let every message expire, so that the position to expire up to is in the current (open) ledger.
        Awaitility.await().until(() -> System.currentTimeMillis() - lastPublishedAt > TimeUnit.SECONDS.toMillis(1));
        // Verify: the first ledger will be accessed only once after expiry for two subscriptions.
        persistentTopic.checkMessageExpiry();
        Position lastPosition = ml.getLastConfirmedEntry();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(lastPosition, ml.getCursors().get(cursorName1).getMarkDeletedPosition());
            assertEquals(lastPosition, ml.getCursors().get(cursorName2).getMarkDeletedPosition());
        });
        assertEquals(1, accessedCount.get());

        // cleanup.
        producer.close();
        admin.topics().delete(topicName);
    }

    @DataProvider
    public static Object[][] topicOrSubscriptionCheck() {
        return new Object[][] {{true}, {false}};
    }

    /***
     * Verify the expiry check does not read the entries up to the mark-delete position, which retention keeps.
     */
    @Test(dataProvider = "topicOrSubscriptionCheck")
    void testExpiryCheckDoesNotReadEntriesUpToMarkDeletePosition(boolean topicCheck) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String cursorName = "s1";
        PersistentTopic persistentTopic = createTopicWithExpiredMessagesInThreeLedgers(topicName, cursorName);
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(cursorName);
        long firstLedger = ml.getLedgersInfo().firstKey();

        // Acknowledge the two closed ledgers. Without the lower bound, the find starts in one of them for any
        // managedLedgerCursorResetLedgerCloseTimestampMaxClockSkewMillis.
        admin.topics().skipMessages(topicName, cursorName, 4);
        Awaitility.await().untilAsserted(() -> assertThat(cursor.getNumberOfEntriesInBacklog(false))
                .as("backlog before the check").isEqualTo(1));
        Position markDeleteBeforeCheck = cursor.getMarkDeletedPosition();
        assertThat(markDeleteBeforeCheck.getLedgerId()).as("ledger of the mark-delete position before the check")
                .isGreaterThan(firstLedger);
        assertRetentionKeepsLedger(ml, firstLedger);

        List<Position> readPositions = recordReads(ml);
        try {
            if (topicCheck) {
                persistentTopic.checkMessageExpiry();
            } else {
                assertThat(persistentTopic.getSubscription(cursorName).expireMessages(1))
                        .as("the subscription started an expiry check").isTrue();
            }
            Position lastPosition = ml.getLastConfirmedEntry();
            Awaitility.await().untilAsserted(() -> assertThat(cursor.getMarkDeletedPosition())
                    .as("mark-delete position after the check").isEqualTo(lastPosition));
            assertThat(readPositions).as("entries read by the expiry check")
                    .isNotEmpty().allSatisfy(read -> assertThat(read).isGreaterThan(markDeleteBeforeCheck));
        } finally {
            pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor(null);
        }

        admin.topics().delete(topicName);
    }

    /***
     * Verify the expiry check of a topic reads no entry when no subscription has a backlog.
     */
    @Test
    void testTopicExpiryCheckDoesNotReadWithoutBacklog() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String cursorName = "s1";
        PersistentTopic persistentTopic = createTopicWithExpiredMessagesInThreeLedgers(topicName, cursorName);
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(cursorName);
        long firstLedger = ml.getLedgersInfo().firstKey();

        admin.topics().skipAllMessages(topicName, cursorName);
        Position lastPosition = ml.getLastConfirmedEntry();
        Awaitility.await().untilAsserted(() -> assertThat(cursor.getMarkDeletedPosition())
                .as("mark-delete position before the check").isEqualTo(lastPosition));
        assertRetentionKeepsLedger(ml, firstLedger);

        List<Position> readPositions = recordReads(ml);
        try {
            long findsBeforeCheck = expiryFindsWithoutPosition();
            persistentTopic.checkMessageExpiry();
            Awaitility.await().untilAsserted(() ->
                    assertThat(expiryFindsWithoutPosition()).as("finds that completed without a position")
                            .isEqualTo(findsBeforeCheck + 1));
            assertThat(readPositions).as("entries read by the expiry check").isEmpty();
            assertThat(cursor.getMarkDeletedPosition()).as("mark-delete position after the check")
                    .isEqualTo(lastPosition);
        } finally {
            pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor(null);
        }

        admin.topics().delete(topicName);
    }

    /**
     * Creates a topic with unlimited retention, a message TTL of 1 second and a subscription. Publishes 5 messages
     * into 3 ledgers of at most 2 entries, and returns once every message is older than the TTL.
     */
    private PersistentTopic createTopicWithExpiredMessagesInThreeLedgers(String topicName, String cursorName)
            throws Exception {
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, cursorName, MessageId.earliest);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        // Unlimited retention keeps the ledgers that every subscription has acknowledged.
        admin.topicPolicies().setRetention(topicName, new RetentionPolicies(-1, -1));
        Awaitility.await().untilAsserted(() ->
                assertThat(ml.getConfig().getRetentionTimeMillis()).as("retention time").isNegative());
        ManagedLedgerConfig configBeforeTtlUpdate = ml.getConfig();
        admin.topicPolicies().setMessageTTL(topicName, 1);
        // Applying the TTL update replaces the managed ledger config, which would drop the settings changed below
        Awaitility.await().untilAsserted(() -> {
            assertThat(persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get())
                    .as("message TTL").isEqualTo(1);
            assertThat(ml.getConfig()).as("managed ledger config after the TTL update")
                    .isNotSameAs(configBeforeTtlUpdate);
        });
        // The TTL update also scheduled an expiry check, which must not run after the messages are published
        pulsar.getBrokerService().getMessageExpiryMonitor().submit(() -> { }).get();
        // Without a minimum rollover time, a ledger rolls over as soon as it has 2 entries
        ml.getConfig().setMaxEntriesPerLedger(2);
        ml.getConfig().setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create()) {
            for (int i = 0; i < 5; i++) {
                producer.send("" + i);
            }
        }
        long lastPublishedAt = System.currentTimeMillis();
        Awaitility.await().untilAsserted(() -> assertThat(ml.getLedgersInfo()).as("ledgers").hasSize(3));
        Awaitility.await().until(() -> System.currentTimeMillis() - lastPublishedAt > TimeUnit.SECONDS.toMillis(1));
        return persistentTopic;
    }

    private static void assertRetentionKeepsLedger(ManagedLedgerImpl ml, long ledgerId) throws Exception {
        CompletableFuture<Void> trimmed = new CompletableFuture<>();
        ml.trimConsumedLedgersInBackground(trimmed);
        trimmed.get(30, TimeUnit.SECONDS);
        assertThat(ml.getLedgersInfo()).as("ledgers after the trim").containsKey(ledgerId);
    }

    /**
     * Records the positions that the broker reads from the ledgers of the managed ledger. The caller removes the read
     * handle interceptor.
     */
    private List<Position> recordReads(ManagedLedgerImpl ml) {
        Set<Long> ledgerIds = Set.copyOf(ml.getLedgersInfo().keySet());
        List<Position> readPositions = new CopyOnWriteArrayList<>();
        pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
            if (ledgerIds.contains(ledgerId)) {
                for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                    readPositions.add(PositionFactory.create(ledgerId, entryId));
                }
            }
            return CompletableFuture.completedFuture(entries);
        });
        return readPositions;
    }

    private long expiryFindsWithoutPosition() {
        return pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics().stream()
                .filter(metric -> metric.getName().equals(OpenTelemetryMessageFinderStats.FIND_DURATION_METRIC_NAME))
                .flatMap(metric -> metric.getHistogramData().getPoints().stream())
                .filter(point ->
                        "expiry".equals(point.getAttributes().get(OpenTelemetryMessageFinderStats.FIND_REASON))
                        && "not_found".equals(
                                point.getAttributes().get(OpenTelemetryMessageFinderStats.FIND_RESULT)))
                .mapToLong(HistogramPointData::getCount)
                .sum();
    }

    @Test
    void testExpireMessagesRaceWithTopicCloseDoesNotMarkDelete() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp-closing-expiry");
        final String subName = "closing-expiry-sub";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subName, MessageId.earliest);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        for (int i = 0; i < 3; i++) {
            producer.send("message-" + i);
        }
        producer.close();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        PersistentSubscription subscription = topic.getSubscription(subName);
        ManagedCursorImpl cursor = (ManagedCursorImpl) subscription.getCursor();
        Position markDeleteBeforeExpiry = cursor.getMarkDeletedPosition();

        BlockingPersistentMessageExpiryMonitor monitor =
                new BlockingPersistentMessageExpiryMonitor(topic, subName, cursor, subscription);
        CompletableFuture<Boolean> expireFuture = CompletableFuture.supplyAsync(() -> monitor.expireMessages(0));
        try {
            monitor.awaitBeforeCursorMarkDelete();
            CompletableFuture<Void> closeFuture;
            // Hold off subscription.close() so cursor closure cannot mask an unguarded mark-delete.
            synchronized (subscription) {
                closeFuture = CompletableFuture.supplyAsync(() -> topic.close(false)).thenCompose(future -> future);
                Awaitility.await().untilAsserted(() -> Assert.assertTrue(topic.isClosingOrDeleting()));
                Assert.assertFalse(cursor.isClosed());
                monitor.allowCursorMarkDelete();

                Assert.assertTrue(expireFuture.get(30, TimeUnit.SECONDS));
                Awaitility.await().until(() -> !monitor.isExpirationCheckInProgress()
                        || !cursor.getMarkDeletedPosition().equals(markDeleteBeforeExpiry));
                assertEquals(cursor.getMarkDeletedPosition(), markDeleteBeforeExpiry);
                Assert.assertFalse(monitor.isExpirationCheckInProgress());
                Assert.assertFalse(monitor.expireMessages(0));
                Assert.assertFalse(monitor.expireMessages(topic.getLastPosition()));
            }
            closeFuture.get(30, TimeUnit.SECONDS);
        } finally {
            monitor.allowCursorMarkDelete();
        }
    }

    private static class BlockingPersistentMessageExpiryMonitor extends PersistentMessageExpiryMonitor {
        private final CountDownLatch beforeCursorMarkDelete = new CountDownLatch(1);
        private final CountDownLatch continueCursorMarkDelete = new CountDownLatch(1);
        private final AtomicBoolean blocked = new AtomicBoolean();

        BlockingPersistentMessageExpiryMonitor(PersistentTopic topic, String subscriptionName, ManagedCursorImpl cursor,
                                               PersistentSubscription subscription) {
            super(topic, subscriptionName, cursor, subscription);
        }

        @Override
        protected void beforeCursorMarkDelete(Position position) {
            if (!blocked.compareAndSet(false, true)) {
                return;
            }
            beforeCursorMarkDelete.countDown();
            try {
                Assert.assertTrue(continueCursorMarkDelete.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void awaitBeforeCursorMarkDelete() throws InterruptedException {
            Assert.assertTrue(beforeCursorMarkDelete.await(30, TimeUnit.SECONDS));
        }

        void allowCursorMarkDelete() {
            continueCursorMarkDelete.countDown();
        }
    }
}
