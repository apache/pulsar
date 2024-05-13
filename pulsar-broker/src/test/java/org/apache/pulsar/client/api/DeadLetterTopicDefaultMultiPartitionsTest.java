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
package org.apache.pulsar.client.api;

import static org.apache.pulsar.client.util.RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class DeadLetterTopicDefaultMultiPartitionsTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.conf.setMaxMessageSize(5 * 1024);
        this.conf.setAllowAutoTopicCreation(true);
        this.conf.setDefaultNumPartitions(2);
        this.conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private void triggerDLQGenerate(String topic, String subscription) throws Exception {
        String DLQ = getDLQName(topic, subscription);
        String p0OfDLQ = TopicName.get(DLQ).getPartition(0).toString();
        Consumer consumer = pulsarClient.newConsumer().topic(topic).subscriptionName(subscription)
                .ackTimeout(1000, TimeUnit.MILLISECONDS)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(10)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(1).build())
                .subscribe();
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        producer.newMessage().value(new byte[]{1}).send();

        Message<Integer> message1 = consumer.receive();
        consumer.negativeAcknowledge(message1);
        Message<Integer> message2 = consumer.receive();
        consumer.negativeAcknowledge(message2);

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<String> topicList = pulsar.getPulsarResources().getTopicResources()
                    .listPersistentTopicsAsync(TopicName.get(topic).getNamespaceObject()).join();
            assertTrue(topicList.contains(DLQ) || topicList.contains(p0OfDLQ));
        });
        admin.topics().unload(topic);
    }

    private static String getDLQName(String primaryTopic, String subscription) {
        String domain = TopicName.get(primaryTopic).getDomain().toString();
        return domain + "://" + TopicName.get(primaryTopic)
                .toString().substring(( domain + "://").length())
                + "-" + subscription + DLQ_GROUP_TOPIC_SUFFIX;
    }

    @Test
    public void testGenerateNonPartitionedDLQ() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName( "persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topic);

        triggerDLQGenerate(topic, "s1");
        String DLQ = getDLQName(topic, subscription);
        String p0OfDLQ = TopicName.get(DLQ).getPartition(0).toString();

        // Verify: no partitioned DLQ.
        List<String> partitionedTopics = pulsar.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources()
                .listPartitionedTopicsAsync(TopicName.get(topic).getNamespaceObject(), TopicDomain.persistent).join();
        assertFalse(partitionedTopics.contains(DLQ));
        assertFalse(partitionedTopics.contains(p0OfDLQ));
        // Verify: non-partitioned DLQ exists.
        List<String> partitions = pulsar.getPulsarResources().getTopicResources()
                .listPersistentTopicsAsync(TopicName.get(topic).getNamespaceObject()).join();
        assertTrue(partitions.contains(DLQ));
        assertFalse(partitions.contains(p0OfDLQ));

        // cleanup.
        admin.topics().delete(topic, false);
    }

    @Test
    public void testManuallyCreatePartitionedDLQ() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName( "persistent://public/default/tp");
        final String subscription = "s1";
        String DLQ = getDLQName(topic, subscription);
        String p0OfDLQ = TopicName.get(DLQ).getPartition(0).toString();
        String p1OfDLQ = TopicName.get(DLQ).getPartition(1).toString();
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createPartitionedTopic(DLQ, 2);

        Awaitility.await().untilAsserted(() -> {
            // Verify: partitioned DLQ exists.
            List<String> partitionedTopics = pulsar.getPulsarResources().getNamespaceResources()
                    .getPartitionedTopicResources()
                    .listPartitionedTopicsAsync(TopicName.get(topic).getNamespaceObject(), TopicDomain.persistent).join();
            assertTrue(partitionedTopics.contains(DLQ));
            assertFalse(partitionedTopics.contains(p0OfDLQ));
            // Verify: DLQ partitions exists.
            List<String> partitions = pulsar.getPulsarResources().getTopicResources()
                    .listPersistentTopicsAsync(TopicName.get(topic).getNamespaceObject()).join();
            assertFalse(partitions.contains(DLQ));
            assertTrue(partitions.contains(p0OfDLQ));
            assertTrue(partitions.contains(p1OfDLQ));
        });

        // cleanup.
        admin.topics().delete(topic, false);
        admin.topics().deletePartitionedTopic(DLQ, false);
    }

    @Test
    public void testManuallyCreatePartitionedDLQ2() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName( "persistent://public/default/tp");
        final String subscription = "s1";
        final String p0OfTopic = TopicName.get(topic).getPartition(0).toString();
        String DLQ = getDLQName(p0OfTopic, subscription);
        String p0OfDLQ = TopicName.get(DLQ).getPartition(0).toString();
        admin.topics().createPartitionedTopic(topic, 10);
        try {
            admin.topics().createPartitionedTopic(DLQ, 2);
        } catch (Exception ex) {
            // Keep multiple versions compatible.
            if (ex.getMessage().contains("Partitioned Topic Name should not contain '-partition-'")){
                return;
            } else {
                fail("Failed to create partitioned DLQ");
            }
        }

        Awaitility.await().untilAsserted(() -> {
            // Verify: partitioned DLQ exists.
            List<String> partitionedTopics = pulsar.getPulsarResources().getNamespaceResources()
                    .getPartitionedTopicResources()
                    .listPartitionedTopicsAsync(TopicName.get(topic).getNamespaceObject(), TopicDomain.persistent).join();
            assertTrue(partitionedTopics.contains(DLQ));
            assertFalse(partitionedTopics.contains(p0OfDLQ));
            // Verify: DLQ partitions exists.
            List<String> partitions = pulsar.getPulsarResources().getTopicResources()
                    .listPersistentTopicsAsync(TopicName.get(topic).getNamespaceObject()).join();
            assertFalse(partitions.contains(DLQ));
        });

        // cleanup.
        admin.topics().deletePartitionedTopic(topic, false);
        admin.topics().deletePartitionedTopic(DLQ, false);
    }
}
