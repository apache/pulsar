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
package org.apache.pulsar.client.impl;


import java.nio.charset.StandardCharsets;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DynamicReceiverQueueSizeTest extends MockedPulsarServiceBaseTest {
    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConsumerImpl() throws PulsarClientException {
        String topic = "persistent://public/default/testConsumerImpl" + System.currentTimeMillis();
        @Cleanup
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-sub")
                .receiverQueueSize(5)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        byte[] data = "data".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 10; i++) {
            producer.send(data);
        }
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 5);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(consumer.getTotalIncomingMessages(), 5));
        Assert.assertEquals(consumer.getAvailablePermits(), 0);

        consumer.setCurrentReceiverQueueSize(8);
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 8);
        Assert.assertEquals(consumer.getAvailablePermits(), 3);

        consumer.setCurrentReceiverQueueSize(10);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(consumer.getTotalIncomingMessages(), 10));
        Assert.assertEquals(consumer.getAvailablePermits(), 0);

        consumer.setCurrentReceiverQueueSize(3);
        Assert.assertEquals(consumer.getAvailablePermits(), -7);
        for (int i = 0; i < 7; i++) {
            consumer.acknowledge(consumer.receive());
            Assert.assertEquals(consumer.getAvailablePermits(), -6 + i);
        }
        consumer.acknowledge(consumer.receive()); //8
        consumer.acknowledge(consumer.receive()); //9
        consumer.acknowledge(consumer.receive()); //10
        Assert.assertEquals(consumer.getAvailablePermits(), 0);

        for (int i = 0; i < 10; i++) {
            producer.send(data);
        }
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(consumer.getTotalIncomingMessages(), 3));
    }

    @Test
    public void testMultiConsumerImpl() throws Exception {
        String topic = "persistent://public/default/testMultiConsumerImpl" + System.currentTimeMillis();
        admin.topics().createPartitionedTopic(topic, 4);
        @Cleanup
        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-sub")
                .receiverQueueSize(5)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        byte[] data = "data".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 30; i++) {
            producer.send(data);
        }
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 5);
        for (ConsumerImpl<byte[]> c : consumer.getConsumers()) {
            Assert.assertEquals(c.getCurrentReceiverQueueSize(), 5);
        }
        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(consumer.getTotalIncomingMessages() >= 5);
            Assert.assertTrue(consumer.getTotalIncomingMessages() < 30);
        });
        consumer.setCurrentReceiverQueueSize(30);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(consumer.getTotalIncomingMessages(), 30);
        });
    }
}
