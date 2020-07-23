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

import lombok.Cleanup;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NonDurableSubscriptionTest  extends ProducerConsumerBase {

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

    @Test
    public void testNonDurableSubscription() throws Exception {
        String topicName = "persistent://my-property/my-ns/nonDurable-topic1";
        // 1 setup producer、consumer
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .readCompacted(true)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("my-nonDurable-subscriber")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        // 2 send message
        int messageNum = 10;
        for (int i = 0; i < messageNum; i++) {
            producer.send("message" + i);
        }
        // 3 receive the first 5 messages
        for (int i = 0; i < 5; i++) {
            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
            consumer.acknowledge(message);
        }
        // 4 trigger reconnect
        ((ConsumerImpl)consumer).getClientCnx().close();
        // 5 for non-durable we are going to restart from the next entry
        for (int i = 5; i < messageNum; i++) {
            Message<String> message = consumer.receive(3, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
        }

    }
}
