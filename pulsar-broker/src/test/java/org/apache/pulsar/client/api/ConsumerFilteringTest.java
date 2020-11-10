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

import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ConsumerFilteringTest extends ProducerConsumerBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setManagedLedgerCacheEvictionFrequency(0.1);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private int messages = 100;

    private static boolean once = false;
    private static boolean once2 = false;

    @Test
    public void testConsumerFilterAnyRatioBatched() throws PulsarClientException {
        if (once)
            return;
        once = true;
        testConsumerFilterAnyRatio(true);
    }

    @Test
    public void testConsumerFilterAnyRatioNoBatching() throws PulsarClientException {
        if (once2)
            return;
        once2 = true;
        testConsumerFilterAnyRatio(false);
    }

    private void testConsumerFilterAnyRatio(boolean batching) throws PulsarClientException {
        testConsumerFilterAnyRatio(0, batching);
        testConsumerFilterAnyRatio(1, batching);
        testConsumerFilterAnyRatio(2, batching);
        testConsumerFilterAnyRatio(10, batching);
        testConsumerFilterAnyRatio(this.messages, batching);
    }

    private void  testConsumerFilterAnyRatio(int filterRatio, boolean batching) throws PulsarClientException {

        if (!conf.isEnableConsumerFilters())
            return;

        String topic = "persistent://my-property/my-ns/consumer-filter";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .property("anytag", "abc")
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(batching)
                //.batchingMaxMessages(5)
                //.batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        try {
            for (int i = 0; i < this.messages; i++) {
                String tag = filterRatio == 0 ? "xyz" : i % filterRatio == 0 ? "abc" : "xyz";
                producer.newMessage()
                        .property("tag", tag)
                        .value("my-message-" + i + "-eom")
                        .sendAsync();
            }

            int messageReceived = 0;

            while (true) {
                Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
                if (message == null)
                    break;
                assertEquals(message.getProperty("tag"), "abc");
                assertTrue(message.getValue().startsWith("my-message-"));
                assertTrue(message.getValue().endsWith("eom"));

                messageReceived++;
                consumer.acknowledge(message);

                System.out.println("received " + messageReceived + " from " + this.messages);
                assertEquals(messageReceived, filterRatio == 0 ? 0 : this.messages / filterRatio);
            }
        } finally {
            producer.close();
            consumer.close();
        }
    }
}

