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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class PersistentTopicProtectedMethodsTest extends ProducerConsumerBase {

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

    protected void doInitConf() throws Exception {
        this.conf.setManagedLedgerMaxEntriesPerLedger(2);
        this.conf.setManagedLedgerMaxLedgerRolloverTimeMinutes(10);
        this.conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    /***
     * Background: the steps for checking backlog metadata are as follows.
     * - Get the oldest cursor.
     * - Return the result if the oldest `cursor.md` equals LAC.
     * - Else, calculate the estimated backlog quota.
     *
     * What case been covered by this test.
     * - The method `PersistentTopic.estimatedTimeBasedBacklogQuotaCheck` may get an NPE when the
     *   `@param position(cursor.markDeletedPositon)` equals LAC and the latest ledger has been removed by a
     *   `ML.trimLedgers`, which was introduced by https://github.com/apache/pulsar/pull/21816.
     * - Q: The broker checked whether the oldest `cursor.md` equals LAC at step 2 above, why does it still call
     *      `PersistentTopic.estimatedTimeBasedBacklogQuotaCheck` with a param that equals `LAC`?
     *   - A: There may be some `acknowledgments` and `ML.trimLedgers` that happened between `step2 above and step 3`.
     */
    @Test
    public void testEstimatedTimeBasedBacklogQuotaCheckWhenNoBacklog() throws Exception {
        final String tp = BrokerTestUtil.newUniqueName("public/default/tp");
        admin.topics().createNonPartitionedTopic(tp);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(tp, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        Consumer c1 = pulsarClient.newConsumer().topic(tp).subscriptionName("s1").subscribe();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get("s1");

        // Generated multi ledgers.
        Producer<byte[]> p1 = pulsarClient.newProducer().topic(tp).create();
        byte[] content = new byte[]{1};
        for (int i = 0; i < 10; i++) {
            p1.send(content);
        }

        // Consume all messages.
        // Trim ledgers, then the LAC relates to a ledger who has been deleted.
        admin.topics().skipAllMessages(tp, "s1");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cursor.getNumberOfEntriesInBacklog(true), 0);
            // Use >= comparison: after skipAll, a ledger rollover may create a new empty ledger,
            // moving the cursor's mark-delete position past the LAC (e.g., 10:-1 vs 9:1).
            assertTrue(cursor.getMarkDeletedPosition().compareTo(ml.getLastConfirmedEntry()) >= 0);
        });
        CompletableFuture completableFuture = new CompletableFuture();
        ml.trimConsumedLedgersInBackground(completableFuture);
        completableFuture.join();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ml.getLedgersInfo().size(), 1);
            assertEquals(cursor.getNumberOfEntriesInBacklog(true), 0);
            assertTrue(cursor.getMarkDeletedPosition().compareTo(ml.getLastConfirmedEntry()) >= 0);
        });

        // Verify: "persistentTopic.estimatedTimeBasedBacklogQuotaCheck" will not get a NullPointerException.
        Position oldestPosition = ml.getCursors().getCursorWithOldestPosition().getPosition();
        persistentTopic.estimatedTimeBasedBacklogQuotaCheck(oldestPosition);

        p1.close();
        c1.close();
        admin.topics().delete(tp, false);
    }

    @Test
    public void testEstimatedTimeBasedBacklogQuotaCheckWithTopicUnloading() throws Exception {
        final String tp = BrokerTestUtil.newUniqueName("public/default/tp-with-topic-unloading");
        admin.topics().createNonPartitionedTopic(tp);

        Consumer<byte[]> c1 = pulsarClient.newConsumer().topic(tp).subscriptionName("s1").subscribe();
        Producer<byte[]> p1 = pulsarClient.newProducer().topic(tp).create();

        byte[] content = new byte[]{1};
        for (int i = 0; i < 10; i++) {
            p1.send(content);
        }

        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(tp, false).join().get();

        Awaitility.await().untilAsserted(() -> {
            admin.brokers().backlogQuotaCheck();
            assertTrue(persistentTopic.getBestEffortOldestUnacknowledgedMessageAgeSeconds() > 0);
        });

        for (int i = 0; i < 10; i++) {
            c1.acknowledge(c1.receive());
        }

        Awaitility.await().untilAsserted(() -> assertEquals(persistentTopic.getBacklogSize(), 0));
        admin.topics().unload(tp);
        for (int i = 0; i < 10; i++) {
            p1.send(content);
        }

        PersistentTopic persistentTopicNew = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(tp, false).join().get();
        Awaitility.await().untilAsserted(() -> {
            admin.brokers().backlogQuotaCheck();
            assertTrue(persistentTopicNew.getBestEffortOldestUnacknowledgedMessageAgeSeconds() > 0);
        });

        p1.close();
        c1.close();
        admin.topics().delete(tp, false);
    }

    /***
     * When the local cluster is removed from the replication clusters, checkReplication deletes the topic through
     * removeTopicIfLocalClusterNotAllowed. Every failure path of PersistentTopic.delete un-fences the topic, and
     * un-fencing runs checkReplication again, so a deletion that keeps failing must not be restarted every time
     * the previous attempt reports its failure: that is an unbounded hot loop for as long as the local cluster
     * stays out of the replication clusters. Retrying is only allowed again after a delay.
     */
    @Test
    public void testClusterRemovalDeletionIsNotRetriedInALoop() throws Exception {
        final String tp = BrokerTestUtil.newUniqueName("public/default/tp");
        admin.topics().createNonPartitionedTopic(tp);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(tp, false).join().get();

        PersistentTopic spyTopic = spy(persistentTopic);
        CompletableFuture<Void> firstDeletion = new CompletableFuture<>();
        AtomicInteger deletions = new AtomicInteger();
        doReturn(CompletableFuture.completedFuture(false)).when(spyTopic).checkAllowedCluster(anyString());
        doAnswer(invocation -> deletions.incrementAndGet() == 1
                ? firstDeletion : CompletableFuture.completedFuture(null))
                .when(spyTopic).deleteForcefully();
        // The scheduled retry is covered by testClusterRemovalDeletionIsRetriedWhenTheReplicationCheckFails.
        // Push it beyond the lifetime of this test so none of the assertions below can race the timer.
        doReturn(TimeUnit.HOURS.toSeconds(1)).when(spyTopic).getClusterRemovalDeletionRetryDelaySeconds();

        // The first check starts the deletion.
        CompletableFuture<Boolean> firstCheck = spyTopic.removeTopicIfLocalClusterNotAllowed();
        assertFalse(firstCheck.isDone(), "The deletion is still in progress");

        // A concurrent check must not start a second deletion, and must report the topic as being removed so the
        // caller does not start replicators for it. checkAllowedCluster is stubbed with an already completed
        // future, so deleteForcefully has been called by the time the check returns.
        CompletableFuture<Boolean> concurrentCheck = spyTopic.removeTopicIfLocalClusterNotAllowed();
        assertEquals(deletions.get(), 1);
        assertTrue(concurrentCheck.get(5, TimeUnit.SECONDS));

        // The deletion fails. This is what un-fences the topic and runs checkReplication again in production, so
        // the check that immediately follows the failure must not start yet another deletion.
        firstDeletion.completeExceptionally(
                new PulsarClientException.AlreadyClosedException("Producer already closed"));
        assertTrue(firstCheck.isCompletedExceptionally(), "The failure is still reported to the caller");
        CompletableFuture<Boolean> checkAfterFailure = spyTopic.removeTopicIfLocalClusterNotAllowed();
        assertEquals(deletions.get(), 1, "The failed deletion must not be restarted immediately");
        assertTrue(checkAfterFailure.get(5, TimeUnit.SECONDS));

        admin.topics().delete(tp, false);
    }

    /***
     * A scheduled retry of the deletion goes through checkReplication, which can fail before it reaches the
     * deletion at all, for example when reading the namespace policies fails. Nothing downstream schedules the
     * next attempt in that case, so the retry has to be rescheduled by the check itself; otherwise the topic is
     * left undeleted until an unrelated event happens to run another replication check. checkReplication is
     * overridable through TopicFactory, so it can also fail synchronously, before returning its future.
     */
    @Test
    public void testClusterRemovalDeletionIsRetriedWhenTheReplicationCheckFails() throws Exception {
        final String tp = BrokerTestUtil.newUniqueName("public/default/tp");
        admin.topics().createNonPartitionedTopic(tp);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(tp, false).join().get();

        PersistentTopic spyTopic = spy(persistentTopic);
        AtomicInteger clusterChecks = new AtomicInteger();
        AtomicInteger deletions = new AtomicInteger();
        // Checks 2 and 3 are the ones the scheduled retries run. Fail both before they can start a deletion, the
        // first synchronously and the second by returning a failed future.
        doAnswer(invocation -> {
            switch (clusterChecks.incrementAndGet()) {
                case 2: throw new RuntimeException("mocked synchronous policy read failure");
                case 3: return CompletableFuture.failedFuture(new RuntimeException("mocked policy read failure"));
                default: return CompletableFuture.completedFuture(false);
            }
        }).when(spyTopic).checkAllowedCluster(anyString());
        doAnswer(invocation -> deletions.incrementAndGet() == 1
                ? CompletableFuture.failedFuture(
                        new PulsarClientException.AlreadyClosedException("Producer already closed"))
                : CompletableFuture.completedFuture(null))
                .when(spyTopic).deleteForcefully();
        doReturn(1L).when(spyTopic).getClusterRemovalDeletionRetryDelaySeconds();

        // Check 1: the deletion is started and fails, so a retry is scheduled.
        CompletableFuture<Boolean> firstCheck = spyTopic.removeTopicIfLocalClusterNotAllowed();
        assertTrue(firstCheck.isCompletedExceptionally());
        assertEquals(deletions.get(), 1);

        // Checks 2 and 3 run from the scheduled retries and fail before starting a deletion. Check 4 must
        // therefore still happen, and its deletion succeeds.
        Awaitility.await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertEquals(deletions.get(), 2));
        assertEquals(clusterChecks.get(), 4);

        admin.topics().delete(tp, false);
    }
}
