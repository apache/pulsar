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
package org.apache.pulsar.client.api;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PatternMultiTopicsConsumerTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(PatternMultiTopicsConsumerTest.class);


    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        isTcpLookup = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 5000)
    public void testSimple() throws Exception {
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topicsPattern("persistent://my-property/my-ns/topic.*")
                // Make sure topics are discovered before test times out
                .patternAutoDiscoveryPeriod(2, TimeUnit.SECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("subscriber-name").subscribe();
        testWithConsumer(consumer);
    }

    @Test(timeOut = 5000)
    public void testNotifications() throws Exception {
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topicsPattern("persistent://my-property/my-ns/topic.*")
                // Set auto-discovery period high so that only notifications can inform us about new topics
                .patternAutoDiscoveryPeriod(1, TimeUnit.MINUTES)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("subscriber-name").subscribe();
        testWithConsumer(consumer);
    }

    private void testWithConsumer(Consumer<byte[]> consumer) throws Exception {
        Map<String, List<String>> sentMessages = new HashMap<>();
        for (int p = 0; p < 10; ++p) {
            String name = "persistent://my-property/my-ns/topic-" + p;
            Producer<byte[]> producer = pulsarClient.newProducer().topic(name).create();
            for (int i = 0; i < 10; i++) {
                String message = "message-" + p + i;
                producer.send(message.getBytes());
                sentMessages.computeIfAbsent(name, topic -> new LinkedList<>()).add(message);
            }
        }

        Message<byte[]> msg;
        Map<String, List<String>> receivedMessages = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            receivedMessages.computeIfAbsent(msg.getTopicName(), topic -> new LinkedList<>()).add(receivedMessage);
        }

        Assert.assertEquals(receivedMessages, sentMessages);
        consumer.close();
    }

}
