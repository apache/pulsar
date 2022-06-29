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

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
@Slf4j
public class ConsumerMemoryLimitTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConsumerMemoryLimit() throws Exception {
        String topic = newTopicName();

        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
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
}
