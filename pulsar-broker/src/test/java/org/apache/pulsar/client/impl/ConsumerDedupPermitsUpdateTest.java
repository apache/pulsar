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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConsumerDedupPermitsUpdateTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "combinations")
    public Object[][] combinations() {
        return new Object[][] {
                // batching-enabled - queue-size
                { false, 0 },
                { false, 1 },
                { false, 10 },
                { false, 100 },
                { true, 1 },
                { true, 10 },
                { true, 100 },
        };
    }

    @Test(timeOut = 30000, dataProvider = "combinations")
    public void testConsumerDedup(boolean batchingEnabled, int receiverQueueSize) throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/my-topic");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test")
                // Use high ack delay to simulate a message being tracked as dup
                .acknowledgmentGroupTime(1, TimeUnit.HOURS)
                .receiverQueueSize(receiverQueueSize)
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(batchingEnabled)
                .batchingMaxMessages(10)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();

        for (int i = 0; i < 30; i++) {
            producer.sendAsync("hello-" + i);
        }
        producer.flush();

        // Consumer receives and acks all the messages, though the acks
        // are still cached in client lib
        for (int i = 0; i < 30; i++) {
            Message<String> msg = consumer.receive();
            assertEquals(msg.getValue(), "hello-" + i);
            consumer.acknowledge(msg);
        }

        // Trigger redelivery by unloading the topic.
        admin.topics().unload(topic);

        // Consumer dedup logic will detect the dups and not bubble them up to the application
        // (With zero-queue we cannot use receive with timeout)
        if (receiverQueueSize > 0) {
            Message<String> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(msg);
        }

        // The flow permits in consumer shouldn't have been messed up by the deduping
        // and we should be able to get new messages through
        for (int i = 0; i < 30; i++) {
            producer.sendAsync("new-message-" + i);
        }
        producer.flush();

        for (int i = 0; i < 30; i++) {
            Message<String> msg = consumer.receive();
            assertEquals(msg.getValue(), "new-message-" + i);
            consumer.acknowledge(msg);
        }
    }

}
