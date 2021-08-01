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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
@Test(groups = "broker")
public class ConsumerStatsTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setMaxUnackedMessagesPerConsumer(0);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConsumerStatsOnZeroMaxUnackedMessagesPerConsumer() throws PulsarClientException, InterruptedException, PulsarAdminException {
        Assert.assertEquals(pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer(), 0);
        final String topicName = "persistent://my-property/my-ns/testConsumerStatsOnZeroMaxUnackedMessagesPerConsumer";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            // don't ack messages here
            consumer.receive();
            received++;
        }

        Assert.assertEquals(received, messages);
        received = 0;

        TopicStats stats = admin.topics().getStats(topicName);
        Assert.assertEquals(stats.getSubscriptions().size(), 1);
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().size(), 1);
        Assert.assertFalse(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).isBlockedConsumerOnUnackedMsgs());
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).getUnackedMessages(), messages);

        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }

        Assert.assertEquals(received, messages);

        // wait acknowledge send
        Thread.sleep(2000);

        stats = admin.topics().getStats(topicName);

        Assert.assertFalse(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).isBlockedConsumerOnUnackedMsgs());
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).getUnackedMessages(), 0);
    }

    @Test
    public void testAckStatsOnPartitionedTopicForExclusiveSubscription() throws PulsarAdminException, PulsarClientException, InterruptedException {
        final String topic = "persistent://my-property/my-ns/testAckStatsOnPartitionedTopicForExclusiveSubscription";
        admin.topics().createPartitionedTopic(topic, 3);
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }
        Assert.assertEquals(messages, received);

        // wait acknowledge send
        Thread.sleep(2000);

        for (int i = 0; i < 3; i++) {
            TopicStats stats = admin.topics().getStats(topic + "-partition-" + i);
            Assert.assertEquals(stats.getSubscriptions().size(), 1);
            Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().size(), 1);
            Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).getUnackedMessages(), 0);
        }
    }

    @Test
    public void testUpdateStatsForActiveConsumerAndSubscription() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testUpdateStatsForActiveConsumerAndSubscription";
        pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-subscription")
                .subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        Assert.assertNotNull(topicRef);
        Assert.assertEquals(topicRef.getSubscriptions().size(), 1);
        List<org.apache.pulsar.broker.service.Consumer> consumers = topicRef.getSubscriptions()
                .get("my-subscription").getConsumers();
        Assert.assertEquals(consumers.size(), 1);
        ConsumerStatsImpl consumerStats = new ConsumerStatsImpl();
        consumerStats.msgOutCounter = 10;
        consumerStats.bytesOutCounter = 1280;
        consumers.get(0).updateStats(consumerStats);
        ConsumerStats updatedStats = consumers.get(0).getStats();

        Assert.assertEquals(updatedStats.getMsgOutCounter(), 10);
        Assert.assertEquals(updatedStats.getBytesOutCounter(), 1280);
    }

    @Test
    public void testConsumerStatsOutput() throws Exception {
        Set<String> allowedFields = Sets.newHashSet(
                "msgRateOut",
                "msgThroughputOut",
                "bytesOutCounter",
                "msgOutCounter",
                "msgRateRedeliver",
                "chunkedMessageRate",
                "consumerName",
                "availablePermits",
                "unackedMessages",
                "avgMessagesPerEntry",
                "blockedConsumerOnUnackedMsgs",
                "readPositionWhenJoining",
                "lastAckedTimestamp",
                "lastConsumedTimestamp",
                "keyHashRanges",
                "metadata",
                "address",
                "connectedSince",
                "clientVersion");

        final String topicName = "persistent://prop/use/ns-abc/testConsumerStatsOutput";
        final String subName = "my-subscription";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topicName);
        ObjectMapper mapper = ObjectMapperFactory.create();
        JsonNode node = mapper.readTree(mapper.writer().writeValueAsString(stats.getSubscriptions()
                .get(subName).getConsumers().get(0)));
        Iterator<String> itr = node.fieldNames();
        while (itr.hasNext()) {
            String field = itr.next();
            Assert.assertTrue(allowedFields.contains(field), field + " should not be exposed");
        }

        consumer.close();
    }
}
