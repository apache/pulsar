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

import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class TopicGCTest extends ProducerConsumerBase {

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

    @EqualsAndHashCode.Include
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setBrokerDeleteInactiveTopicsEnabled(true);
        this.conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        this.conf.setBrokerDeleteInactiveTopicsFrequencySeconds(10);
    }

    @Test
    public void testCreateConsumerAfterOnePartDeleted() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String partition0 = topic + "-partition-0";
        final String partition1 = topic + "-partition-1";
        final String subscription = "s1";
        admin.topics().createPartitionedTopic(topic, 2);
        admin.topics().createSubscription(topic, subscription, MessageId.earliest);

        // create consumers and producers.
        Producer<String> producer0 = pulsarClient.newProducer(Schema.STRING).topic(partition0)
                .enableBatching(false).create();
        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING).topic(partition1)
                .enableBatching(false).create();
        org.apache.pulsar.client.api.Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subscription).isAckReceiptEnabled(true).subscribe();

        // Make consume all messages for one topic, do not consume any messages for another one.
        producer0.send("1");
        producer1.send("2");
        admin.topics().skipAllMessages(partition0, subscription);

        // Wait for topic GC.
        // Partition 0 will be deleted about 20s later, left 2min to avoid flaky.
        producer0.close();
        consumer1.close();
        Awaitility.await().atMost(2, TimeUnit.MINUTES).untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> tp1 = pulsar.getBrokerService().getTopic(partition0, false);
            CompletableFuture<Optional<Topic>> tp2 = pulsar.getBrokerService().getTopic(partition1, false);
            assertTrue(tp1 == null || !tp1.get().isPresent());
            assertTrue(tp2 != null && tp2.get().isPresent());
        });

        // Verify that the consumer subscribed with partitioned topic can be created successful.
        Consumer<String> consumerAllPartition = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subscription).isAckReceiptEnabled(true).subscribe();
        Message<String> msg = consumerAllPartition.receive(2, TimeUnit.SECONDS);
        String receivedMsgValue = msg.getValue();
        log.info("received msg: {}", receivedMsgValue);
        consumerAllPartition.acknowledge(msg);

        // cleanup.
        consumerAllPartition.close();
        producer0.close();
        producer1.close();
        admin.topics().deletePartitionedTopic(topic);
    }
}
