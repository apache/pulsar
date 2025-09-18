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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
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
            assertEquals(cursor.getMarkDeletedPosition(), ml.getLastConfirmedEntry());
        });
        CompletableFuture completableFuture = new CompletableFuture();
        ml.trimConsumedLedgersInBackground(completableFuture);
        completableFuture.join();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ml.getLedgersInfo().size(), 1);
            assertEquals(cursor.getNumberOfEntriesInBacklog(true), 0);
            assertEquals(cursor.getMarkDeletedPosition(), ml.getLastConfirmedEntry());
        });

        // Verify: "persistentTopic.estimatedTimeBasedBacklogQuotaCheck" will not get a NullPointerException.
        PositionImpl oldestPosition = ml.getCursors().getCursorWithOldestPosition().getPosition();
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
}
