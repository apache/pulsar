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

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class ProducerConsumerDelayTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testDelayConsumerOneSec() throws PulsarClientException {
        final int numOfMsgs = 10;
        final int delayInMs = 1000;
        final String topicName = "persistent://my-property/my-ns/my-topic";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .receiverDelay(delayInMs, TimeUnit.MILLISECONDS)
                .subscriptionName("subscriber-exclusive-with-delay").subscribe();

        testConsumeWithDelay(numOfMsgs, delayInMs, producer, consumer);
    }

    @Test(timeOut = 30000)
    public void testDelayConsumerFiveSecs() throws PulsarClientException {
        final int numOfMsg = 100;
        final int delayInMs = 5000;
        final String topicName = "persistent://my-property/my-ns/my-topic";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Consumer<byte[]> consumerA = pulsarClient.newConsumer().topic(topicName)
                .receiverDelay(delayInMs, TimeUnit.MILLISECONDS)
                .subscriptionName("subscriber-shared-with-delay").subscribe();

        testConsumeWithDelay(numOfMsg, delayInMs, producer, consumerA);
    }

    private static void testConsumeWithDelay(int numOfMsg, int delayInMs, Producer<byte[]> producer,
                                             Consumer<byte[]> consumer)
            throws PulsarClientException {
        // Produce messages normally
        for (int i = 0; i < numOfMsg; i++) {
            producer.sendAsync(String.format("Message num %d", i).getBytes());
        }

        int recvMsgs = consumeMsgWithDelay(numOfMsg, delayInMs, consumer);
        consumer.unsubscribe();
        consumer.close();
        producer.close();
        Assert.assertEquals(numOfMsg, recvMsgs);
    }

    private static int consumeMsgWithDelay(int numOfMsg, int delayInMs, Consumer<byte[]> consumer)
            throws PulsarClientException {
        int i;
        for (i = 0; i < numOfMsg; i++) {
            Message msg = consumer.receive();
            Assert.assertNotNull(msg);
            long delay = System.currentTimeMillis() - msg.getPublishTime();
            Assert.assertTrue(delay >= delayInMs);
            consumer.acknowledge(msg);
        }
        return i;
    }
}
