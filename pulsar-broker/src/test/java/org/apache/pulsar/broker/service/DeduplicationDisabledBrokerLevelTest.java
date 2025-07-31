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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DeduplicationDisabledBrokerLevelTest extends ProducerConsumerBase {

    private int deduplicationSnapshotFrequency = 5;
    private int brokerDeduplicationEntriesInterval = 1000;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void doInitConf() throws Exception {
        this.conf.setBrokerDeduplicationEnabled(false);
        this.conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(deduplicationSnapshotFrequency);
        this.conf.setBrokerDeduplicationEntriesInterval(brokerDeduplicationEntriesInterval);
    }

    @Test
    public void testNoBacklogOnDeduplication() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topic);
        final PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        final ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        // deduplication enabled:
        //   broker level: "false"
        //   topic level: "true".
        // So it is enabled.
        admin.topicPolicies().setDeduplicationStatus(topic, true);
        Awaitility.await().untilAsserted(() -> {
            ManagedCursorImpl cursor =
                    (ManagedCursorImpl) ml.getCursors().get(PersistentTopic.DEDUPLICATION_CURSOR_NAME);
            assertNotNull(cursor);
        });

        // Verify: regarding deduplication cursor, messages will be acknowledged automatically.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producer.send("1");
        producer.send("2");
        producer.send("3");
        producer.close();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(PersistentTopic.DEDUPLICATION_CURSOR_NAME);
        Awaitility.await().atMost(Duration.ofSeconds(deduplicationSnapshotFrequency * 3)).untilAsserted(() -> {
            PositionImpl LAC = (PositionImpl) ml.getLastConfirmedEntry();
            PositionImpl cursorMD = (PositionImpl) cursor.getMarkDeletedPosition();
            assertTrue(LAC.compareTo(cursorMD) <= 0);
        });

        // cleanup.
        admin.topics().delete(topic);
    }

    @Test
    public void testSnapshotCounterAfterUnload() throws Exception {
        final int originalDeduplicationSnapshotFrequency = deduplicationSnapshotFrequency;
        deduplicationSnapshotFrequency = 3600;
        cleanup();
        setup();

        // Create a topic and wait deduplication is started.
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topic);
        final PersistentTopic persistentTopic1 =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        final ManagedLedgerImpl ml1 = (ManagedLedgerImpl) persistentTopic1.getManagedLedger();
        admin.topicPolicies().setDeduplicationStatus(topic, true);
        Awaitility.await().untilAsserted(() -> {
            ManagedCursorImpl cursor1 =
                    (ManagedCursorImpl) ml1.getCursors().get(PersistentTopic.DEDUPLICATION_CURSOR_NAME);
            assertNotNull(cursor1);
        });
        final MessageDeduplication deduplication1 = persistentTopic1.getMessageDeduplication();

        // 1. Send 999 messages, it is less than "brokerDeduplicationEntriesIntervaddl".
        // 2. Unload topic.
        // 3. Send 1 messages, there are 1099 messages have not been snapshot now.
        // 4. Verify the snapshot has been taken.
        // step 1.
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        for (int i = 0; i < brokerDeduplicationEntriesInterval - 1; i++) {
            producer.send(i + "");
        }
        int snapshotCounter1 = WhiteboxImpl.getInternalState(deduplication1, "snapshotCounter");
        assertEquals(snapshotCounter1, brokerDeduplicationEntriesInterval - 1);
        admin.topics().unload(topic);
        PersistentTopic persistentTopic2 =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        ManagedLedgerImpl ml2 = (ManagedLedgerImpl) persistentTopic2.getManagedLedger();
        MessageDeduplication deduplication2 = persistentTopic2.getMessageDeduplication();
        admin.topicPolicies().setDeduplicationStatus(topic, true);
        Awaitility.await().untilAsserted(() -> {
            ManagedCursorImpl cursor =
                    (ManagedCursorImpl) ml2.getCursors().get(PersistentTopic.DEDUPLICATION_CURSOR_NAME);
            assertNotNull(cursor);
        });
        // step 3.
        producer.send("last message");
        ml2.asyncTrimConsumedLedgers();
        // step 4.
        Awaitility.await().untilAsserted(() -> {
            int snapshotCounter3 = WhiteboxImpl.getInternalState(deduplication2, "snapshotCounter");
            assertTrue(snapshotCounter3 < brokerDeduplicationEntriesInterval);
            // Since https://github.com/apache/pulsar/pull/22034 has not been cherry-pick into branch-3.0, there
            // should be 2 ledgers.
            // Verify: the previous ledger will be removed because all messages have been acked.
            assertEquals(ml2.getLedgersInfo().size(), 1);
        });

        // cleanup.
        producer.close();
        admin.topics().delete(topic);
        deduplicationSnapshotFrequency = originalDeduplicationSnapshotFrequency;
        cleanup();
        setup();
    }
}
