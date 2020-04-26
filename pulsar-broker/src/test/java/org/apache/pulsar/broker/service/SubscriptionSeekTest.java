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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.RelativeTimeUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 */
@Test
public class SubscriptionSeekTest extends BrokerTestBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSeek() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testSeek";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        // Disable pre-fetch in consumer to track the messages received
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").receiverQueueSize(0).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);
        assertEquals(topicRef.getSubscriptions().size(), 1);

        List<MessageId> messageIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            MessageId msgId = producer.send(message.getBytes());
            messageIds.add(msgId);
        }

        PersistentSubscription sub = topicRef.getSubscription("my-subscription");
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        consumer.seek(MessageId.latest);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 0);

        // Wait for consumer to reconnect
        Thread.sleep(500);
        consumer.seek(MessageId.earliest);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        Thread.sleep(500);
        consumer.seek(messageIds.get(5));
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 5);
    }

    @Test
    public void testSeekOnPartitionedTopic() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testSeekPartitions";

        admin.topics().createPartitionedTopic(topicName, 2);
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").subscribe();

        try {
            consumer.seek(MessageId.latest);
            fail("Should not have succeeded");
        } catch (PulsarClientException e) {
            // Expected
        }
    }

    @Test
    public void testSeekTime() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testSeekTime";
        String resetTimeStr = "100s";
        long resetTimeInMillis = TimeUnit.SECONDS
                .toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(resetTimeStr));

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        // Disable pre-fetch in consumer to track the messages received
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").receiverQueueSize(0).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);
        assertEquals(topicRef.getSubscriptions().size(), 1);
        PersistentSubscription sub = topicRef.getSubscription("my-subscription");

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        long currentTimestamp = System.currentTimeMillis();
        consumer.seek(currentTimestamp);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 0);

        // Wait for consumer to reconnect
        Thread.sleep(1000);
        consumer.seek(currentTimestamp - resetTimeInMillis);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);
    }

    @Test
    public void testSeekTimeOnPartitionedTopic() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testSeekTimePartitions";
        final String resetTimeStr = "100s";
        final int partitions = 2;
        long resetTimeInMillis = TimeUnit.SECONDS
                .toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(resetTimeStr));
        admin.topics().createPartitionedTopic(topicName, partitions);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        // Disable pre-fetch in consumer to track the messages received
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").subscribe();

        List<PersistentSubscription> subs = new ArrayList<>();

        for (int i = 0; i < partitions; i++) {
            PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService()
                    .getTopicReference(topicName + TopicName.PARTITIONED_TOPIC_SUFFIX + i).get();
            assertNotNull(topicRef);
            assertEquals(topicRef.getProducers().size(), 1);
            assertEquals(topicRef.getSubscriptions().size(), 1);
            PersistentSubscription sub = topicRef.getSubscription("my-subscription");
            assertNotNull(sub);
            subs.add(sub);
        }

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        long backlogs = 0;
        for (PersistentSubscription sub : subs) {
            backlogs += sub.getNumberOfEntriesInBacklog(false);
        }

        assertEquals(backlogs, 10);

        backlogs = 0;
        long currentTimestamp = System.currentTimeMillis();
        consumer.seek(currentTimestamp);
        for (PersistentSubscription sub : subs) {
            backlogs += sub.getNumberOfEntriesInBacklog(false);
        }
        assertEquals(backlogs, 0);

        // Wait for consumer to reconnect
        Thread.sleep(1000);
        consumer.seek(currentTimestamp - resetTimeInMillis);
        backlogs = 0;

        for (PersistentSubscription sub : subs) {
            backlogs += sub.getNumberOfEntriesInBacklog(false);
        }
        assertEquals(backlogs, 10);
    }

}
