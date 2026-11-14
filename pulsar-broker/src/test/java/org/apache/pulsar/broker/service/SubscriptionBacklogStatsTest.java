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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SubscriptionBacklogStatsTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] subscriptionAndLedgerCounts() {
        return new Object[][] {{1, 1}, {1, 3}, {2, 1}, {2, 3}, {5, 1}, {5, 3}};
    }

    @Test(dataProvider = "subscriptionAndLedgerCounts", timeOut = 120000)
    public void testBacklogStatsWithClientAcksAndTopicReload(int subscriptionCount, int maxEntriesPerLedger)
            throws Exception {
        admin.namespaces().setDeduplicationStatus(getNamespace(), false);
        admin.namespaces().setRetention(getNamespace(), new RetentionPolicies(-1, -1));
        String topicName = newTopicName();
        int messageCount = 20;
        int[] acknowledged = new int[subscriptionCount];

        try (PulsarClient client = newPulsarClient()) {
            Producer<Integer> producer = client.newProducer(Schema.INT32).topic(topicName)
                    .enableBatching(false).create();
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName).orElseThrow();
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
            ledger.getConfig().setMaxEntriesPerLedger(maxEntriesPerLedger);
            ledger.getConfig().setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);

            List<Consumer<Integer>> consumers = new ArrayList<>();
            for (int i = 0; i < subscriptionCount; i++) {
                consumers.add(newConsumer(client, topicName, "sub-" + i));
            }
            for (int i = 0; i < messageCount; i++) {
                producer.send(i);
            }
            assertTrue(ledger.getLedgersInfoAsList().size() > 1, "The test must span multiple ledgers");
            assertBacklogStats(topicName, acknowledged, messageCount, true);

            // Mix untouched cursors, partially consumed ledgers, and fully consumed subscriptions.
            for (int i = 0; i < subscriptionCount; i++) {
                int consumed = subscriptionCount == 1 ? messageCount / 2
                        : i * messageCount / (subscriptionCount - 1);
                consumeThrough(consumers.get(i), 0, consumed);
                acknowledged[i] = consumed;
            }
            Awaitility.await().untilAsserted(() -> assertBacklogStats(topicName, acknowledged, messageCount, true));
            assertBacklogStats(topicName, acknowledged, messageCount, false);

            // REST stats must keep the same values after loading persisted subscription cursors.
            for (Consumer<Integer> consumer : consumers) {
                consumer.close();
            }
            producer.close();
            admin.topics().unload(topicName);
            admin.topics().getStats(topicName, true, true);
            Awaitility.await().untilAsserted(() -> assertBacklogStats(topicName, acknowledged, messageCount, true));

            for (int i = 0; i < subscriptionCount; i++) {
                Consumer<Integer> consumer = newConsumer(client, topicName, "sub-" + i);
                consumeThrough(consumer, acknowledged[i], messageCount);
            }
            Arrays.fill(acknowledged, messageCount);
            Awaitility.await().untilAsserted(() -> assertBacklogStats(topicName, acknowledged, messageCount, true));
            assertBacklogStats(topicName, acknowledged, messageCount, false);
        }
    }

    private Consumer<Integer> newConsumer(PulsarClient client, String topicName, String subscriptionName)
            throws Exception {
        return client.newConsumer(Schema.INT32).topic(topicName).subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .isAckReceiptEnabled(true).acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
    }

    private void consumeThrough(Consumer<Integer> consumer, int first, int endExclusive) throws Exception {
        Message<Integer> lastMessage = null;
        for (int i = first; i < endExclusive; i++) {
            Message<Integer> message = consumer.receive(10, TimeUnit.SECONDS);
            assertNotNull(message, "Missing message " + i);
            assertEquals(message.getValue().intValue(), i, "Messages must resume at the persisted cursor");
            lastMessage = message;
        }
        if (lastMessage != null) {
            consumer.acknowledgeCumulative(lastMessage);
        }
    }

    private void assertBacklogStats(String topicName, int[] acknowledged, int messageCount, boolean includeSize)
            throws Exception {
        TopicStats stats = admin.topics().getStats(topicName, true, includeSize);
        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName).orElseThrow();
        ManagedLedger ledger = topic.getManagedLedger();
        assertEquals(stats.getSubscriptions().size(), acknowledged.length);
        for (int i = 0; i < acknowledged.length; i++) {
            String subscriptionName = "sub-" + i;
            SubscriptionStats subscriptionStats = stats.getSubscriptions().get(subscriptionName);
            PersistentSubscription subscription = topic.getSubscription(subscriptionName);
            assertEquals(subscriptionStats.getMsgBacklog(), messageCount - acknowledged[i], subscriptionName);
            long expectedSize = includeSize
                    ? ledger.getEstimatedBacklogSize(subscription.getCursor().getMarkDeletedPosition()) : -1;
            assertEquals(subscriptionStats.getBacklogSize(), expectedSize, subscriptionName);
            if (includeSize) {
                if (acknowledged[i] == messageCount) {
                    assertEquals(subscriptionStats.getBacklogSize(), 0L, subscriptionName);
                } else {
                    assertTrue(subscriptionStats.getBacklogSize() > 0, subscriptionName);
                }
            }
        }
    }
}
