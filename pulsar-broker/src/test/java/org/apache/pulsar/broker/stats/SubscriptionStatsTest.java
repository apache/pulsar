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

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Test(groups = "broker")
public class SubscriptionStatsTest extends ProducerConsumerBase {

    @BeforeClass
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

    @Test
    public void testConsumersAfterMarkDelete() throws PulsarClientException, PulsarAdminException {
        final String topicName = "persistent://my-property/my-ns/testConsumersAfterMarkDelete-"
                + UUID.randomUUID().toString();
        final String subName = "my-sub";

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .receiverQueueSize(10)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        final int messages = 100;
        for (int i = 0; i < messages; i++) {
            producer.send(String.valueOf(i).getBytes());
        }

        // Receive by do not ack the message, so that the next consumer can added to the recentJoinedConsumer of the dispatcher.
        consumer1.receive();

        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topicName)
                .receiverQueueSize(10)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topicName);
        Assert.assertEquals(stats.getSubscriptions().size(), 1);
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue()
                .getConsumersAfterMarkDeletePosition().size(), 1);

        consumer1.close();
        consumer2.close();
        producer.close();
    }

    @Test
    public void testNonContiguousDeletedMessagesRanges() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testNonContiguousDeletedMessagesRanges-"
                + UUID.randomUUID().toString();
        final String subName = "my-sub";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        final int messages = 100;
        for (int i = 0; i < messages; i++) {
            producer.send(String.valueOf(i).getBytes());
        }

        for (int i = 0; i < messages; i++) {
            Message<byte[]> received = consumer.receive();
            if (i != 50) {
                consumer.acknowledge(received);
            }
        }

        Awaitility.await().untilAsserted(() -> {
            TopicStats stats = admin.topics().getStats(topicName);
            Assert.assertEquals(stats.getNonContiguousDeletedMessagesRanges(), 1);
            Assert.assertEquals(stats.getSubscriptions().size(), 1);
            Assert.assertEquals(stats.getSubscriptions().get(subName).getNonContiguousDeletedMessagesRanges(), 1);
            Assert.assertTrue(stats.getNonContiguousDeletedMessagesRangesSerializedSize() > 0);
            Assert.assertTrue(stats.getSubscriptions().get(subName)
                    .getNonContiguousDeletedMessagesRangesSerializedSize() > 0);
        });
    }
}
