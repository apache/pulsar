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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.MessageRedeliveryController;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class MaxUnAckMessagesTest  extends ProducerConsumerBase {

    private int maxUnackedMessagesPerSubscription = 20;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        conf.setMaxUnackedMessagesPerSubscription(maxUnackedMessagesPerSubscription);
    }

    private String uniquePersistentTopicName() {
        return BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
    }

    @Test
    public void testLimitationTurnOnAndTurnOff() throws Exception {
        final String topicName = uniquePersistentTopicName();
        final String subName = "sub1";
        final int incomingQueueCapacity = maxUnackedMessagesPerSubscription * 2;
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subName, MessageId.earliest);

        @Cleanup
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(incomingQueueCapacity)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topicName)
                .create();

        // Send messages;
        int messageCount = incomingQueueCapacity * 2;
        for (int i = 0; i < messageCount; i++) {
            producer.newMessage().key("" + i).value("" + i).send();
        }

        // Wait the limitation of maxUnackedMessagesPerSubscription is turned on.
        Awaitility.await().untilAsserted(() -> {
            log.info("consumer.numMessagesInQueue: {}", consumer.numMessagesInQueue());
            assertTrue(consumer.numMessagesInQueue() >= maxUnackedMessagesPerSubscription);
        });

        // Verify consumer can receive all the messages.
        Set<String> receivedMessages = Collections.synchronizedSet(new LinkedHashSet<>());
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
            assertNotNull(message);
            log.info("received message: {}", message.getValue());
            receivedMessages.add(message.getValue());
            consumer.acknowledge(message);
            assertEquals(receivedMessages.size(), messageCount);
        });
    }

    @Test
    public void testReplayQueueInfiniteExpand() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        final String subName = "sub1";
        final long maxMemoryUsageOfReplayQueueInBytes = 32 * 1024;
        conf.setMaxMemoryUsageOfReplayQueueInBytesPerSubscription(maxMemoryUsageOfReplayQueueInBytes);

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(5)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(5)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topicName)
                .create();

        // Send 10,000 messages and only consumer1 works.
        AtomicInteger sendMessageCounter = new AtomicInteger();
        Thread sendTask = new Thread(() -> {
            while(sendMessageCounter.get() < 5000) {
                int i = sendMessageCounter.incrementAndGet();
                producer.newMessage().key("" + i).value("" + i).sendAsync().join();
                try {
                    // sleep 5 ms to avoid high cpu loads.
                    if (sendMessageCounter.get() % 500 == 0) {
                        Thread.sleep(5);
                    }
                } catch (InterruptedException e) {
                }
            }
        });
        AtomicBoolean consumeTaskShouldStop = new AtomicBoolean(true);
        Thread consumeTask = new Thread(() -> {
            while(true) {
                try {
                    Message<String> msg = consumer1.receive(2, TimeUnit.SECONDS);
                    if (msg != null) {
                        consumer1.acknowledge(msg);
                    } else if (consumeTaskShouldStop.get()) {
                        break;
                    }
                } catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
        });
        consumeTask.start();
        sendTask.start();
        sendTask.join();
        consumeTaskShouldStop.set(true);

        assertTrue(getMemoryUsageOfReplayQueue(topicName, subName) <= maxMemoryUsageOfReplayQueueInBytes * 2);

        consumer1.close();
        consumer2.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    private long getMemoryUsageOfReplayQueue(String topicName, String subName) {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        PersistentStickyKeyDispatcherMultipleConsumers dispatcher =
                (PersistentStickyKeyDispatcherMultipleConsumers) persistentTopic
                        .getSubscription(subName).getDispatcher();
        MessageRedeliveryController redeliveryMessages =
                WhiteboxImpl.getInternalState(dispatcher, "redeliveryMessages");
        return redeliveryMessages.getLongSizeInBytes();
    }
}
