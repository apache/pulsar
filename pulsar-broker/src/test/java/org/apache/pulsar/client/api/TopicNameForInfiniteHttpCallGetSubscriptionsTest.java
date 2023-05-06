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
package org.apache.pulsar.client.api;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class TopicNameForInfiniteHttpCallGetSubscriptionsTest extends ProducerConsumerBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setAllowAutoTopicCreationType("partitioned");
        conf.setDefaultNumPartitions(1);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testInfiniteHttpCallGetSubscriptions() throws Exception {
        final String randomStr = UUID.randomUUID().toString().replaceAll("-", "");
        final String partitionedTopicName = "persistent://my-property/my-ns/tp1_" + randomStr;
        final String topic_p0 = partitionedTopicName + TopicName.PARTITIONED_TOPIC_SUFFIX + "0";
        final String subscriptionName = "sub1";
        final String topicDLQ = topic_p0 + "-" + subscriptionName + "-DLQ";

        admin.topics().createPartitionedTopic(partitionedTopicName, 2);

        // Do test.
        ProducerAndConsumerEntry pcEntry = triggerDLQCreated(topic_p0, topicDLQ, subscriptionName);
        admin.topics().getSubscriptions(topicDLQ);

        // cleanup.
        pcEntry.consumer.close();
        pcEntry.producer.close();
        admin.topics().deletePartitionedTopic(partitionedTopicName);
    }

    @Test
    public void testInfiniteHttpCallGetSubscriptions2() throws Exception {
        final String randomStr = UUID.randomUUID().toString().replaceAll("-", "");
        final String topicName = "persistent://my-property/my-ns/tp1_" + randomStr + "-partition-0-abc";
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();

        // Do test.
        admin.topics().getSubscriptions(topicName);

        // cleanup.
        producer.close();
    }

    @Test
    public void testInfiniteHttpCallGetSubscriptions3() throws Exception {
        final String randomStr = UUID.randomUUID().toString().replaceAll("-", "");
        final String topicName = "persistent://my-property/my-ns/tp1_" + randomStr + "-partition-0";
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();

        // Do test.
        admin.topics().getSubscriptions(topicName);

        // cleanup.
        producer.close();
    }

    @AllArgsConstructor
    private static class ProducerAndConsumerEntry {
        private Producer<String> producer;
        private Consumer<String> consumer;
    }

    private ProducerAndConsumerEntry triggerDLQCreated(String topicName, String DLQName, String subscriptionName) throws Exception {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder().deadLetterTopic(DLQName).maxRedeliverCount(2).build())
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();
        // send messages.
        for (int i = 0; i < 5; i++) {
            producer.newMessage()
                    .value("value-" + i)
                    .sendAsync();
        }
        producer.flush();
        // trigger the DLQ created.
        for (int i = 0; i < 20; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.reconsumeLater(msg, 1, TimeUnit.SECONDS);
            } else {
                break;
            }
        }

        return new ProducerAndConsumerEntry(producer, consumer);
    }
}