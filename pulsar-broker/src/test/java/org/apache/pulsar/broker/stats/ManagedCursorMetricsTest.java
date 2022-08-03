/**
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
package org.apache.pulsar.broker.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.stats.metrics.ManagedCursorMetrics;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PulsarTestClient;
import org.apache.pulsar.common.stats.Metrics;
import org.awaitility.Awaitility;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ManagedCursorMetricsTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected PulsarClient createNewPulsarClient(ClientBuilder clientBuilder) throws PulsarClientException {
        return PulsarTestClient.create(clientBuilder);
    }

    /***
     * This method has overridden these case:
     *    brk_ml_cursor_persistLedgerSucceed
     *    brk_ml_cursor_persistLedgerErrors
     *    brk_ml_cursor_persistZookeeperSucceed
     *    brk_ml_cursor_nonContiguousDeletedMessagesRange
     * But not overridden "brk_ml_cursor_nonContiguousDeletedMessagesRange".
     */
    @Test
    public void testManagedCursorMetrics() throws Exception {
        final String subName = "my-sub";
        final String topicName = "persistent://my-namespace/use/my-ns/my-topic1";
        /** Before create cursor. Verify metrics will not be generated. **/
        // Create ManagedCursorMetrics and verify empty.
        ManagedCursorMetrics metrics = new ManagedCursorMetrics(pulsar);
        List<Metrics> metricsList = metrics.generate();
        Assert.assertTrue(metricsList.isEmpty());
        /**
         * Trigger the cursor ledger creation.
         * After create cursor. Verify all metrics is zero.
         */
        // Trigger cursor creation.
        @Cleanup
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) this.pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .isAckReceiptEnabled(true)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = this.pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create();
        final PersistentSubscription persistentSubscription =
                (PersistentSubscription) pulsar.getBrokerService()
                        .getTopic(topicName, false).get().get().getSubscription(subName);
        final ManagedCursorImpl managedCursor = (ManagedCursorImpl) persistentSubscription.getCursor();
        ManagedCursorMXBean managedCursorMXBean = managedCursor.getStats();
        // Assert.
        metricsList = metrics.generate();
        Assert.assertFalse(metricsList.isEmpty());
        /*
          see: https://github.com/apache/pulsar/pull/17504
          "createNewMetadataLedger" triggers once BK write, and "initialize" triggers the execution of
          "createNewMetadataLedger". The logic of the branch master has been changed, so this line is different.
         */
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerSucceed"), 1L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerErrors"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperSucceed"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperErrors"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_nonContiguousDeletedMessagesRange"), 0L);
        /**
         * 1. Send many messages, and only ack half. After the cursor data is written to BK,
         *    verify "brk_ml_cursor_persistLedgerSucceed" and "brk_ml_cursor_nonContiguousDeletedMessagesRange".
         * 2. Ack another half, verify "brk_ml_cursor_nonContiguousDeletedMessagesRange" is zero.
         */
        // Send many message and ack half.
        List<MessageId> keepsMessageIdList = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            if (i < 10 || i > 20) {
                consumer.acknowledge(consumer.receive().getMessageId());
            } else {
                keepsMessageIdList.add(consumer.receive().getMessageId());
            }
        }
        // Wait persistent.
        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .until(() -> managedCursorMXBean.getPersistLedgerSucceed() > 0);
        // Assert.
        metricsList = metrics.generate();
        Assert.assertFalse(metricsList.isEmpty());
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerSucceed"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerErrors"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperSucceed"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperErrors"), 0L);
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_nonContiguousDeletedMessagesRange"),
                0L);
        // Ack another half.
        for (MessageId messageId : keepsMessageIdList){
            consumer.acknowledge(messageId);
        }
        // Wait persistent.
        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .until(() -> managedCursor.getTotalNonContiguousDeletedMessagesRange() == 0);
        // Assert.
        metricsList = metrics.generate();
        Assert.assertFalse(metricsList.isEmpty());
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerSucceed"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerErrors"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperSucceed"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperErrors"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_nonContiguousDeletedMessagesRange"), 0L);
        /**
         * Make BK error, and send many message, then wait cursor persistent finish.
         * After the cursor data is written to ZK, verify "brk_ml_cursor_persistLedgerErrors" and
         * "brk_ml_cursor_persistZookeeperSucceed".
         */
        // Send amd ack messages, at the same time makes BK error.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
            String message = UUID.randomUUID().toString();
            producer.send(message.getBytes());
            consumer.acknowledge(consumer.receive().getMessageId());
            // Make BK error.
            LedgerHandle ledgerHandle = Whitebox.getInternalState(managedCursor, "cursorLedger");
            ledgerHandle.close();
            return managedCursorMXBean.getPersistLedgerErrors() > 0
                    && managedCursorMXBean.getPersistZookeeperSucceed() > 0;
        });
        // Assert.
        metricsList = metrics.generate();
        Assert.assertFalse(metricsList.isEmpty());
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerSucceed"), 1L);
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerErrors"), 0L);
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperSucceed"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperErrors"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_nonContiguousDeletedMessagesRange"), 0L);
        /**
         * TODO verify "brk_ml_cursor_persistZookeeperErrors".
         * This is not easy to implement, we can use {@link #mockZooKeeper} to fail ZK, but we cannot identify whether
         * the request is triggered by the "create new ledger then write ZK" or the "persistent cursor then write ZK".
         * The cursor path is "/managed-ledgers/my-namespace/use/my-ns/persistent/my-topic1/my-sub". Maybe we can
         * mock/spy ManagedCursorImpl to overridden this case in another PR.
         */
        mockZooKeeper.unsetAlwaysFail();
        producer.close();
        consumer.close();
        managedCursor.close();
        admin.topics().delete(topicName, true);
    }

    @Test
    public void testCursorReadWriteMetrics() throws Exception {
        final String subName1 = "read-write-sub-1";
        final String subName2 = "read-write-sub-2";
        final String topicName = "persistent://my-namespace/use/my-ns/read-write";
        final int messageSize = 10;

        ManagedCursorMetrics metrics = new ManagedCursorMetrics(pulsar);

        List<Metrics> metricsList = metrics.generate();
        Assert.assertTrue(metricsList.isEmpty());

        metricsList = metrics.generate();
        Assert.assertTrue(metricsList.isEmpty());

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName1)
                .subscribe();

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName2)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        for (int i = 0; i < messageSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            if (i % 2 == 0) {
                consumer.acknowledge(consumer.receive().getMessageId());
            } else {
                consumer2.acknowledge(consumer.receive().getMessageId());
            }
        }
        metricsList = metrics.generate();
        Assert.assertEquals(metricsList.size(), 2);
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_writeLedgerSize"), 0L);
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_writeLedgerLogicalSize"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_readLedgerSize"), 0L);

        Assert.assertNotEquals(metricsList.get(1).getMetrics().get("brk_ml_cursor_writeLedgerSize"), 0L);
        Assert.assertNotEquals(metricsList.get(1).getMetrics().get("brk_ml_cursor_writeLedgerLogicalSize"), 0L);
        Assert.assertEquals(metricsList.get(1).getMetrics().get("brk_ml_cursor_readLedgerSize"), 0L);

        // cleanup.
        consumer.close();
        consumer2.close();
        producer.close();
        admin.topics().delete(topicName, true);
    }
}
