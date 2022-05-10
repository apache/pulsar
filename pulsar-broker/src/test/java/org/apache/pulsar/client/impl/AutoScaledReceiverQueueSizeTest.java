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

package org.apache.pulsar.client.impl;


import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class AutoScaledReceiverQueueSizeTest extends MockedPulsarServiceBaseTest {
    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @BeforeClass(alwaysRun = true)
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
                .receiverQueueSize(3)
                .autoScaledReceiverQueueSizeEnabled(true)
                .subscribe();
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 1);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        byte[] data = "data".getBytes(StandardCharsets.UTF_8);

        producer.send(data);
        Awaitility.await().until(consumer.scaleReceiverQueueHint::get);
        Assert.assertNotNull(consumer.receive());
        log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());

        //this will trigger receiver queue size expanding.
        Assert.assertNull(consumer.receive(0, TimeUnit.MILLISECONDS));

        log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 2);
        Assert.assertFalse(consumer.scaleReceiverQueueHint.get());

        for (int i = 0; i < 5; i++) {
            producer.send(data);
            producer.send(data);
            Awaitility.await().until(consumer.scaleReceiverQueueHint::get);
            Assert.assertNotNull(consumer.receive());
            Assert.assertNotNull(consumer.receive());
            // queue maybe full, but no empty receive, so no expanding
            Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 2);
        }

        producer.send(data);
        producer.send(data);
        Awaitility.await().until(consumer.scaleReceiverQueueHint::get);
        Assert.assertNotNull(consumer.receive());
        Assert.assertNotNull(consumer.receive());
        Assert.assertNull(consumer.receive(0, TimeUnit.MILLISECONDS));
        // queue is full, with empty receive, expanding to max size
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 3);
    }

    @Test
    public void testConsumerImplBatchReceive() throws PulsarClientException {
        String topic = "persistent://public/default/testConsumerImplBatchReceive" + System.currentTimeMillis();
        @Cleanup
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-sub")
                .batchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(5).build())
                .receiverQueueSize(20)
                .autoScaledReceiverQueueSizeEnabled(true)
                .subscribe();

        int currentSize = 8;
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), currentSize);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        byte[] data = "data".getBytes(StandardCharsets.UTF_8);

        for (int i = 0; i < 10; i++) { // just run a few times.
            for (int j = 0; j < 5; j++) {
                producer.send(data);
            }
            Awaitility.await().until(() -> consumer.incomingMessages.size() == 5);
            log.info("i={},expandReceiverQueueHint:{},local permits:{}",
                    i, consumer.scaleReceiverQueueHint.get(), consumer.getAvailablePermits());
            Assert.assertEquals(consumer.batchReceive().size(), 5);
            Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), currentSize);
            log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());
        }

        //clear local available permits.
        int n = currentSize / 2 - consumer.getAvailablePermits();
        for (int i = 0; i < n; i++) {
            producer.send(data);
            consumer.receive();
        }
        Assert.assertEquals(consumer.getAvailablePermits(), 0);

        for (int i = 0; i < currentSize; i++) {
            producer.send(data);
        }

        Awaitility.await().until(consumer.scaleReceiverQueueHint::get);
        Assert.assertEquals(consumer.batchReceive().size(), 5);

        //trigger expanding
        consumer.batchReceiveAsync();
        Awaitility.await().until(() -> consumer.getCurrentReceiverQueueSize() == currentSize * 2);
        log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());
    }

    @Test
    public void testMultiConsumerImpl() throws Exception {
        String topic = "persistent://public/default/testMultiConsumerImpl" + System.currentTimeMillis();
        admin.topics().createPartitionedTopic(topic, 3);
        @Cleanup
        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-sub")
                .receiverQueueSize(10)
                .autoScaledReceiverQueueSizeEnabled(true)
                .subscribe();

        //queue size will be adjusted to partition number.
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 3));

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        byte[] data = "data".getBytes(StandardCharsets.UTF_8);

        for (int i = 0; i < 3; i++) {
            producer.send(data);
        }
        Awaitility.await().until(consumer.scaleReceiverQueueHint::get);
        for (int i = 0; i < 3; i++) {
            Assert.assertNotNull(consumer.receive());
        }
        Assert.assertTrue(consumer.scaleReceiverQueueHint.get());
        log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 3); // queue size no change

        //this will trigger receiver queue size expanding.
        Assert.assertNull(consumer.receive(0, TimeUnit.MILLISECONDS));

        log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 6);
        Assert.assertFalse(consumer.scaleReceiverQueueHint.get()); //expandReceiverQueueHint is reset.

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 6; j++) {
                producer.send(data);
            }
            for (int j = 0; j < 6; j++) {
                Assert.assertNotNull(consumer.receive());
            }
            log.info("i={},currentReceiverQueueSize={},expandReceiverQueueHint={}", i,
                    consumer.getCurrentReceiverQueueSize(), consumer.scaleReceiverQueueHint);
            // queue maybe full, but no empty receive, so no expanding
            Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 6);
        }

        for (int j = 0; j < 6; j++) {
            producer.send(data);
        }
        Awaitility.await().until(() -> consumer.scaleReceiverQueueHint.get());
        for (int j = 0; j < 6; j++) {
            Assert.assertNotNull(consumer.receive());
        }
        Assert.assertNull(consumer.receive(0, TimeUnit.MILLISECONDS));
        // queue is full, with empty receive, expanding to max size
        log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 10);
    }

    @Test
    public void testMultiConsumerImplBatchReceive() throws PulsarClientException, PulsarAdminException {
        String topic = "persistent://public/default/testMultiConsumerImplBatchReceive" + System.currentTimeMillis();
        admin.topics().createPartitionedTopic(topic, 3);
        @Cleanup
        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-sub")
                .batchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(5).build())
                .receiverQueueSize(20)
                .autoScaledReceiverQueueSizeEnabled(true)
                .subscribe();

        //receiver queue size init as 5.
        int currentSize = 5;
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), currentSize);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        byte[] data = "data".getBytes(StandardCharsets.UTF_8);

        for (int i = 0; i < 10; i++) { // just run a few times.
            for (int j = 0; j < 5; j++) {
                producer.send(data);
            }
            log.info("i={},expandReceiverQueueHint:{},local permits:{}",
                    i, consumer.scaleReceiverQueueHint.get(), consumer.getAvailablePermits());
            Awaitility.await().until(consumer::hasEnoughMessagesForBatchReceive);
            Assert.assertEquals(consumer.batchReceive().size(), 5);
            Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), currentSize);
            log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());
        }

        for (int i = 0; i < currentSize; i++) {
            producer.send(data);
        }

        Awaitility.await().until(consumer.scaleReceiverQueueHint::get);
        Assert.assertEquals(consumer.batchReceive().size(), 5);

        //trigger expanding
        consumer.batchReceiveAsync();
        Awaitility.await().until(() -> consumer.getCurrentReceiverQueueSize() == currentSize * 2);
        log.info("getCurrentReceiverQueueSize={}", consumer.getCurrentReceiverQueueSize());
    }
}
