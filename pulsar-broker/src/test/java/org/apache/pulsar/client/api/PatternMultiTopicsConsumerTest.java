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

import static org.testng.Assert.fail;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.impl.PatternMultiTopicsConsumerImpl;
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

    @Test(timeOut = 30000)
    public void testFailedSubscribe() throws Exception {
        final String topicName1 = BrokerTestUtil.newUniqueName("persistent://public/default/tp_test");
        final String topicName2 = BrokerTestUtil.newUniqueName("persistent://public/default/tp_test");
        final String topicName3 = BrokerTestUtil.newUniqueName("persistent://public/default/tp_test");
        final String subName = "s1";
        admin.topics().createPartitionedTopic(topicName1, 2);
        admin.topics().createPartitionedTopic(topicName2, 3);
        admin.topics().createNonPartitionedTopic(topicName3);

        // Register a exclusive consumer to makes the pattern consumer failed to subscribe.
        Consumer c1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName3).subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(subName).subscribe();

        try {
            PatternMultiTopicsConsumerImpl<String> consumer =
                (PatternMultiTopicsConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                    .topicsPattern("persistent://public/default/tp_test.*")
                    .subscriptionType(SubscriptionType.Failover)
                    .subscriptionName(subName)
                    .subscribe();
            fail("Expected a consumer busy error.");
        } catch (Exception ex) {
            log.info("consumer busy", ex);
        }

        c1.close();
        // Verify all internal consumer will be closed.
        // If delete topic without "-f" work, it means the internal consumers were closed.
        admin.topics().delete(topicName3);
        admin.topics().deletePartitionedTopic(topicName2);
        admin.topics().deletePartitionedTopic(topicName1);
    }

}
