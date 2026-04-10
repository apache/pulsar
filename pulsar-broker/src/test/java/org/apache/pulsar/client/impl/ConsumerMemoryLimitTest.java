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

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.SizeUnit;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
@Slf4j
public class ConsumerMemoryLimitTest extends SharedPulsarBaseTest {

    @Test
    public void testConsumerMemoryLimit() throws Exception {
        String topic = newTopicName();

        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .memoryLimit(10, SizeUnit.KILO_BYTES);

        @Cleanup
        PulsarTestClient client = PulsarTestClient.create(clientBuilder);

        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) client.newProducer().topic(topic).enableBatching(false)
                .blockIfQueueFull(false)
                .create();

        @Cleanup
        ConsumerImpl<byte[]> c1 = (ConsumerImpl<byte[]>) client.newConsumer().subscriptionName("sub").topic(topic)
                .autoScaledReceiverQueueSizeEnabled(true).subscribe();
        @Cleanup
        ConsumerImpl<byte[]> c2 = (ConsumerImpl<byte[]>) client.newConsumer().subscriptionName("sub2").topic(topic)
                .autoScaledReceiverQueueSizeEnabled(true).subscribe();
        c2.updateAutoScaleReceiverQueueHint();
        int n = 5;
        for (int i = 0; i < n; i++) {
            producer.send(new byte[3000]);
        }
        Awaitility.await().until(c1.scaleReceiverQueueHint::get);


        c1.setCurrentReceiverQueueSize(10);
        Awaitility.await().until(() -> c1.incomingMessages.size() == n);
        log.info("memory usage:{}", client.getMemoryLimitController().currentUsagePercent());

        //1. check memory limit reached,
        Assert.assertTrue(client.getMemoryLimitController().currentUsagePercent() > 1);

        //2. check c2 can't expand receiver queue.
        Assert.assertEquals(c2.getCurrentReceiverQueueSize(), 1);
        for (int i = 0; i < n; i++) {
            Awaitility.await().until(() -> c2.incomingMessages.size() == 1);
            Assert.assertNotNull(c2.receive());
        }
        Assert.assertTrue(c2.scaleReceiverQueueHint.get());
        c2.receiveAsync(); //this should trigger c2 receiver queue size expansion.
        Awaitility.await().until(() -> !c2.pendingReceives.isEmpty()); //make sure expectMoreIncomingMessages is called.
        Assert.assertEquals(c2.getCurrentReceiverQueueSize(), 1);

        //3. producer can't send message;
        Assert.expectThrows(PulsarClientException.MemoryBufferIsFullError.class, () -> producer.send(new byte[10]));

        //4. ConsumerBase#reduceCurrentReceiverQueueSize is called already. Queue size reduced to 5.
        log.info("RQS:{}", c1.getCurrentReceiverQueueSize());
        Assert.assertEquals(c1.getCurrentReceiverQueueSize(), 5);

        for (int i = 0; i < n; i++) {
            c1.receive();
        }
    }

    @Test
    public void testMultiPulsarClientConsumerShareMemoryLimitController() throws Exception {
        int msgSize = 100;
        int msgCount = 3;
        int memoryLimit = msgSize * msgCount;
        String topic1 = newTopicName();
        String topic2 = newTopicName();
        PulsarClientSharedResources sharedResources = PulsarClientSharedResources.builder()
                .configureMemoryLimitController(
                        memoryLimitConfig -> memoryLimitConfig.memoryLimit(memoryLimit, SizeUnit.BYTES)).build();
        @Cleanup
        PulsarClientImpl pulsarClient =
                ((PulsarClientImpl) PulsarClient.builder().serviceUrl(getBrokerServiceUrl()).build());
        @Cleanup
        PulsarClientImpl pulsarClient1 = ((PulsarClientImpl) PulsarClient.builder().serviceUrl(getBrokerServiceUrl())
                .sharedResources(sharedResources).build());
        @Cleanup
        PulsarClientImpl pulsarClient2 = ((PulsarClientImpl) PulsarClient.builder().serviceUrl(getBrokerServiceUrl())
                .sharedResources(sharedResources).build());

        Assert.assertSame(pulsarClient1.getMemoryLimitController(), pulsarClient2.getMemoryLimitController());

        @Cleanup
        Producer<byte[]> topic1Producer =
                pulsarClient.newProducer().topic(topic1).enableBatching(false).blockIfQueueFull(false).create();
        @Cleanup
        Producer<byte[]> topic2Producer =
                pulsarClient.newProducer().topic(topic2).enableBatching(false).blockIfQueueFull(false).create();
        ConsumerImpl<byte[]> topic1Consumer =
                (ConsumerImpl<byte[]>) pulsarClient1.newConsumer().subscriptionName("topic1-sub").topic(topic1)
                        .autoScaledReceiverQueueSizeEnabled(true).subscribe();
        ConsumerImpl<byte[]> topic2Consumer =
                (ConsumerImpl<byte[]>) pulsarClient2.newConsumer().subscriptionName("topic2-sub").topic(topic2)
                        .autoScaledReceiverQueueSizeEnabled(true).subscribe();

        MemoryLimitController memoryLimitController1 = topic1Consumer.getMemoryLimitController().get();
        MemoryLimitController memoryLimitController2 = topic2Consumer.getMemoryLimitController().get();
        Assert.assertSame(memoryLimitController1, memoryLimitController2);

        Assert.assertEquals(topic1Consumer.getCurrentReceiverQueueSize(), 1);
        Assert.assertEquals(topic2Consumer.getCurrentReceiverQueueSize(), 1);


        topic1Producer.send(new byte[msgSize]);
        Awaitility.await().until(topic1Consumer.scaleReceiverQueueHint::get);

        Message<byte[]> topic1Message = topic1Consumer.receive();
        Assert.assertNotNull(topic1Message);
        Assert.assertEquals(topic1Consumer.getCurrentReceiverQueueSize(), 1);

        // Trigger ConsumerBase.expectMoreIncomingMessages() method to expand receiverQueueSize.
        topic1Consumer.receiveAsync();
        Awaitility.await().until(() -> topic1Consumer.getCurrentReceiverQueueSize() == 2);


        topic2Producer.send(new byte[msgSize]);
        Awaitility.await().until(topic2Consumer.scaleReceiverQueueHint::get);

        Message<byte[]> topic2Message = topic2Consumer.receive();
        Assert.assertNotNull(topic2Message);
        Assert.assertEquals(topic2Consumer.getCurrentReceiverQueueSize(), 1);

        // Trigger ConsumerBase.expectMoreIncomingMessages() method to expand receiverQueueSize.
        topic2Consumer.receiveAsync();
        Awaitility.await().until(() -> topic2Consumer.getCurrentReceiverQueueSize() == 2);


        // topic1Consumer.receiveAsync() will take one message, so we should send (msgCount + 1) messages.
        // Trigger ConsumerBase.reduceCurrentReceiverQueueSize() method to reduce receiverQueueSize.
        topic1Consumer.setCurrentReceiverQueueSize(msgCount);
        for (int i = 0; i < msgCount + 1; i++) {
            topic1Producer.send(new byte[msgSize]);
        }
        Awaitility.await().until(() -> memoryLimitController1.currentUsage() == memoryLimit);
        Awaitility.await().until(() -> topic1Consumer.getCurrentReceiverQueueSize() == 1);
        Awaitility.await().until(() -> topic2Consumer.getCurrentReceiverQueueSize() == 1);

        // topic2Consumer.receiveAsync() will take one message, so we should send (msgCount + 1) messages.
        for (int i = 0; i < msgCount + 1; i++) {
            topic2Producer.send(new byte[msgSize]);
        }
        // topic2Consumer will not expand receiverQueueSize due to memory limit reached.
        for (int i = 0; i < msgCount; i++) {
            topic2Message = topic2Consumer.receive();
            Assert.assertNotNull(topic2Message);
            Assert.assertEquals(topic2Consumer.getCurrentReceiverQueueSize(), 1);
        }

        // Close topic1Consumer to clear release memory.
        topic1Consumer.close();
        Assert.assertEquals(memoryLimitController1.currentUsage(), 0);

        // Trigger ConsumerBase.expectMoreIncomingMessages() method to expand receiverQueueSize.
        topic2Consumer.receiveAsync();
        Awaitility.await().until(() -> topic2Consumer.getCurrentReceiverQueueSize() == 2);

        // Close topic1Consumer to clear release memory.
        topic2Consumer.close();
        Assert.assertEquals(memoryLimitController2.currentUsage(), 0);
    }

}
