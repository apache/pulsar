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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class CustomizedManagedLedgerTest extends ProducerConsumerBase {

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

    @SuppressWarnings("deprecation")
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setManagedLedgerStorageClassName(
            CustomizedManagedLedgerStorageForTest.class.getName());
        conf.setManagedLedgerCacheSizeMB(10);
        conf.setManagedLedgerCacheEvictionFrequency(1000);
        conf.setManagedLedgerCacheEvictionTimeThresholdMillis(1000);
    }

    @Test
    public void testCursorStatsWithLegacyStorage() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/cursor-stats");
        String firstPartition = TopicName.get(topic).getPartition(0).toString();
        String secondPartition = TopicName.get(topic).getPartition(1).toString();
        String subscription = "sub";
        String retryHint = "includeIndividualDeletedMessages=true";
        admin.topics().createPartitionedTopic(topic, 2);
        try (var durable = pulsarClient.newConsumer().topic(firstPartition)
                .subscriptionName(subscription).subscribe()) {
            assertThat(durable.isConnected()).isTrue();
            assertThatThrownBy(() -> admin.topics().getSubscriptionCursorInternalStats(firstPartition, subscription))
                    .isInstanceOf(PulsarAdminException.PreconditionFailedException.class)
                    .hasMessageContaining(retryHint);
            assertThat(admin.topics().getSubscriptionCursorInternalStats(firstPartition, subscription, true)
                    .individuallyDeletedMessages).isNotNull();
            assertThatThrownBy(() -> admin.topics().getPartitionedSubscriptionCursorInternalStats(topic, subscription))
                    .isInstanceOfSatisfying(PulsarAdminException.class, error -> {
                        assertThat(error.getStatusCode()).isEqualTo(503);
                        assertThat(error.getHttpError()).contains(retryHint);
                    });

            // The test storage delegates non-durable cursors to ManagedCursorImpl, which supports both modes.
            try (var nonDurable = pulsarClient.newConsumer().topic(secondPartition).subscriptionName(subscription)
                    .subscriptionMode(SubscriptionMode.NonDurable).subscribe()) {
                assertThat(nonDurable.isConnected()).isTrue();
                var partial = admin.topics().getPartitionedSubscriptionCursorInternalStats(topic, subscription);
                assertThat(partial.partitions).containsOnlyKeys(secondPartition);
                assertThat(partial.errors).containsOnlyKeys(firstPartition);
                var error = partial.errors.get(firstPartition);
                assertThat(error.statusCode).isEqualTo(412);
                assertThat(error.errorCode).isEqualTo("PRECONDITION_FAILED");
                assertThat(error.reason).contains(retryHint);
                assertThat(admin.topics().getPartitionedSubscriptionCursorInternalStats(topic, subscription, true)
                        .complete).isTrue();
            }
        } finally {
            admin.topics().deletePartitionedTopic(topic, true);
        }
    }

    @Test
    public void testNoMemoryLeakWhenExpireMessages() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";

        // Create topic with "CustomizedManagedLedger", which will
        // call "PersistentTopic.checkMessageExpiryWithoutSharedPosition" when expiring messages.
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        int messageTTLInSecond = 1;
        ManagedLedger ml = pulsar.getDefaultManagedLedgerFactory()
            .open(TopicName.get(topic).getPersistenceNamingEncoding(), config);
        CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger customizedManagedLedger =
                (CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger) ml;
        assertTrue(ml instanceof CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subscription).subscribe();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        admin.topicPolicies().setMessageTTL(topic, messageTTLInSecond);
        for (int i = 0; i < 50; i++) {
            producer.send("msg-" + i);
        }

        // Trigger a messages expiring.
        ManagedCursor managedCursor = persistentTopic.getSubscription(subscription).getCursor();
        assertTrue(managedCursor instanceof CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator);
        CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator cursorDecorator =
                (CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator) managedCursor;
        Thread.sleep(messageTTLInSecond * 3);
        Awaitility.await().untilAsserted(() -> {
            persistentTopic.checkMessageExpiry();
            assertEquals(cursorDecorator.getNumberOfEntriesInBacklog(true), 0);
        });

        // Verify: no memory leak.
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            assertFalse(cursorDecorator.entryReleasedStatusMap.isEmpty());
            for (Boolean released : cursorDecorator.entryReleasedStatusMap.values()) {
                assertTrue(released);
            }
        });

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topic);
    }
}
