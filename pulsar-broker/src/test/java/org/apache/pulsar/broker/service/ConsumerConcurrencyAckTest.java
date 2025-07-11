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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ConsumerConcurrencyAckTest extends BrokerTestBase {

    @DataProvider(name = "brokerParams")
    public static Object[][] brokerParams() {
        return new Object[][]{
                // subscriptionSharedUseClassicPersistentImplementation, acknowledgmentAtBatchIndexLevelEnabled
                {false, true},
                {false, false},
                {true, true},
                {true, false}
        };
    }

    @Factory(dataProvider = "brokerParams")
    public ConsumerConcurrencyAckTest(boolean subscriptionSharedUseClassicPersistentImplementation,
                                      boolean acknowledgmentAtBatchIndexLevelEnabled) {
        conf.setSubscriptionSharedUseClassicPersistentImplementation(
                subscriptionSharedUseClassicPersistentImplementation);
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(acknowledgmentAtBatchIndexLevelEnabled);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public Object[][] argsOfTestAcknowledgeConcurrently() {
        // enableBatchSend, enableBatchIndexAcknowledgment1, enableBatchIndexAcknowledgment2
        return new Object[][] {
                {true, true, true},
                {true, false, false},
                {true, true, false},
                {true, false, true},
                {false, true, true},
                {false, false, false},
                {false, true, false},
                {false, false, true},
        };
    }

    /**
     * Test: one message may be acknowledged by two consumers at the same time.
     * Verify: the metric "unackedMessages" should be "0" after acknowledged all messages.
     * 1. Consumer-1 received messages.
     * 2. Unload the topic.
     * 3. The message may be sent to consumer-2, but the consumption of consumer-1 is still in-progress now.
     * 4. Consumer-1 and consumer-2 acknowledge the message concurrently.
     */
    @Test(timeOut = 60_000, dataProvider = "argsOfTestAcknowledgeConcurrently")
    public void testAcknowledgeConcurrently(boolean enableBatchSend, boolean enableBatchIndexAcknowledgment1,
                                            boolean enableBatchIndexAcknowledgment2)
            throws Exception {
        log.info("start test. classic dispatcher: {}, broker enabled batch ack: {}, enableBatchSend: {},"
                + " enableBatchIndexAcknowledgment1: {}, enableBatchIndexAcknowledgment2: {}",
                pulsar.getConfig().isSubscriptionSharedUseClassicPersistentImplementation(),
                pulsar.getConfig().isAcknowledgmentAtBatchIndexLevelEnabled(),
                enableBatchSend, enableBatchIndexAcknowledgment1, enableBatchIndexAcknowledgment2
        );
        PulsarClientImpl client1 =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        PulsarClientImpl client2 =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        final int msgCount = 6;
        final CountDownLatch acknowledgeSignal1 = new CountDownLatch(1);
        final CountDownLatch acknowledgeSignal2 = new CountDownLatch(1);
        final CountDownLatch acknowledgeSignal3 = new CountDownLatch(1);
        final CountDownLatch acknowledgeFinishedSignal = new CountDownLatch(3);
        final String topic = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/tp");
        final String subscription1 = "s1";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, subscription1, MessageId.earliest);
        Producer<String> producer = client1.newProducer(Schema.STRING).topic(topic)
                .enableBatching(enableBatchSend).batchingMaxMessages(4).create();
        for (int i = 0; i < msgCount; i++) {
            producer.sendAsync(i + "");
        }

        // Consumer-1 using user threads to consume asynchronously, it will not acknowledge messages one by one.
        ConsumerImpl<String> consumer1 = (ConsumerImpl<String>) client1.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName(subscription1)
                .enableBatchIndexAcknowledgment(enableBatchIndexAcknowledgment1)
                .acknowledgmentGroupTime(1, TimeUnit.MILLISECONDS).consumerName("c1")
                .subscriptionType(SubscriptionType.Shared).subscribe();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(consumer1.numMessagesInQueue(), msgCount);
        });
        consumer1.pause();
        List<MessageId> msgReceivedC11 = new ArrayList<>();
        List<MessageId> msgReceivedC12 = new ArrayList<>();
        List<MessageId> msgReceivedC13 = new ArrayList<>();
        for (int i = 0; i < msgCount; i++) {
            Message<String> msg = consumer1.receive();
            assertNotNull(msg);
            if (i % 4 == 0) {
                msgReceivedC11.add(msg.getMessageId());
            } else if (i % 3 == 0) {
                msgReceivedC12.add(msg.getMessageId());
            } else if (i % 2 == 0) {
                msgReceivedC13.add(msg.getMessageId());
            } else {
                msgReceivedC13.add(msg.getMessageId());
            }
        }
        new Thread(() -> {
            try {
                acknowledgeSignal1.await();
                consumer1.acknowledge(msgReceivedC11);
                acknowledgeSignal2.await();
                consumer1.acknowledge(msgReceivedC12);
                acknowledgeSignal3.await();
                consumer1.acknowledge(msgReceivedC13);
                consumer1.resume();
                acknowledgeFinishedSignal.countDown();
            } catch (Exception e) {
                log.error("consumer-1 acknowledge failure", e);
                throw new RuntimeException(e);
            }
        }).start();

        // After a topic unloading, the messages will be resent to consumer-2.
        // Consumer-2 using user threads to consume asynchronously, it will not acknowledge messages one by one.
        ConsumerImpl<String> consumer2 = (ConsumerImpl<String>) client2.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName(subscription1)
                .enableBatchIndexAcknowledgment(enableBatchIndexAcknowledgment2)
                .acknowledgmentGroupTime(1, TimeUnit.MILLISECONDS).consumerName("c2")
                .subscriptionType(SubscriptionType.Shared).subscribe();
        admin.topics().unload(topic);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(consumer2.numMessagesInQueue(), msgCount);
        });
        List<MessageId> msgReceivedC21 = new ArrayList<>();
        List<MessageId> msgReceivedC22 = new ArrayList<>();
        List<MessageId> msgReceivedC23 = new ArrayList<>();
        for (int i = 0; i < msgCount; i++) {
            Message<String> msg = consumer2.receive();
            assertNotNull(msg);
            if (i % 4 == 0) {
                msgReceivedC21.add(msg.getMessageId());
            } else if (i % 3 == 0) {
                msgReceivedC22.add(msg.getMessageId());
            } else if (i % 2 == 0) {
                msgReceivedC23.add(msg.getMessageId());
            } else {
                msgReceivedC23.add(msg.getMessageId());
            }
        }
        new Thread(() -> {
            try {
                acknowledgeSignal1.await();
                consumer2.acknowledge(msgReceivedC21);
                acknowledgeSignal2.await();
                consumer2.acknowledge(msgReceivedC22);
                acknowledgeSignal3.await();
                consumer2.acknowledge(msgReceivedC23);
                acknowledgeFinishedSignal.countDown();
            } catch (Exception e) {
                log.error("consumer-2 acknowledge failure", e);
                throw new RuntimeException(e);
            }
        }).start();
        // Start another thread to mock a consumption repeatedly.
        new Thread(() -> {
            try {
                acknowledgeSignal1.await();
                consumer2.acknowledge(msgReceivedC21);
                acknowledgeSignal2.await();
                consumer2.acknowledge(msgReceivedC22);
                acknowledgeSignal3.await();
                consumer2.acknowledge(msgReceivedC23);
                acknowledgeFinishedSignal.countDown();
            } catch (Exception e) {
                log.error("consumer-2 acknowledge failure", e);
                throw new RuntimeException(e);
            }
        }).start();

        // Trigger concurrently acknowledge.
        acknowledgeSignal1.countDown();
        Thread.sleep(1000);
        acknowledgeSignal2.countDown();
        Thread.sleep(1000);
        acknowledgeSignal3.countDown();

        // Verify: the metric "unackedMessages" should be "0" after acknowledged all messages.
        acknowledgeFinishedSignal.await();
        Awaitility.await().untilAsserted(() -> {
            SubscriptionStats stats = admin.topics().getStats(topic).getSubscriptions().get(subscription1);
            log.info("backlog: {}, unack: {}", stats.getMsgBacklog(), stats.getUnackedMessages());
            assertEquals(stats.getMsgBacklog(), 0);
            assertEquals(stats.getUnackedMessages(), 0);
            for (ConsumerStats consumerStats : stats.getConsumers()) {
                assertEquals(consumerStats.getUnackedMessages(), 0);
            }
        });

        // cleanup.
        consumer1.close();
        consumer2.close();
        producer.close();
        client1.close();
        client2.close();
        admin.topics().delete(topic, false);
    }
}
