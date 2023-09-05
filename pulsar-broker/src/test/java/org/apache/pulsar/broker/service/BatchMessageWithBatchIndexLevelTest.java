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
package org.apache.pulsar.broker.service;

import com.google.common.collect.Lists;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Slf4j
@Test(groups = "broker")
public class BatchMessageWithBatchIndexLevelTest extends BatchMessageTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);
        super.baseSetup();
    }

    @Test
    @SneakyThrows
    public void testBatchMessageAck() {
        int numMsgs = 40;
        final String topicName = "persistent://prop/ns-abc/batchMessageAck-" + UUID.randomUUID();
        final String subscriptionName = "sub-batch-1";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topicName)
                .batchingMaxMessages(20)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .enableBatching(true)
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("batch-message-" + i).getBytes();
            sendFutureList.add(producer.newMessage().value(message).sendAsync());
        }
        FutureUtil.waitForAll(sendFutureList).get();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentDispatcherMultipleConsumers dispatcher = (PersistentDispatcherMultipleConsumers) topic
                .getSubscription(subscriptionName).getDispatcher();
        Message<byte[]> receive1 = consumer.receive();
        Message<byte[]> receive2 = consumer.receive();
        consumer.acknowledge(receive1);
        consumer.acknowledge(receive2);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(dispatcher.getConsumers().get(0).getUnackedMessages(), 18);
        });
        Message<byte[]> receive3 = consumer.receive();
        Message<byte[]> receive4 = consumer.receive();
        consumer.acknowledge(receive3);
        consumer.acknowledge(receive4);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(dispatcher.getConsumers().get(0).getUnackedMessages(), 16);
        });
        // Block cmd-flow send until verify finish. see: https://github.com/apache/pulsar/pull/17436.
        consumer.pause();
        Message<byte[]> receive5 = consumer.receive();
        consumer.negativeAcknowledge(receive5);
        Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertEquals(dispatcher.getConsumers().get(0).getUnackedMessages(), 0);
        });
        // Unblock cmd-flow.
        consumer.resume();
        consumer.receive();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(dispatcher.getConsumers().get(0).getUnackedMessages(), 16);
        });
    }

    @Test
    public void testBatchMessageMultiNegtiveAck() throws Exception{
        final String topicName = "persistent://prop/ns-abc/batchMessageMultiNegtiveAck-" + UUID.randomUUID();
        final String subscriptionName = "sub-negtive-1";

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(10)
                .enableBatchIndexAcknowledgment(true)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .topic(topicName)
                .batchingMaxMessages(20)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .enableBatching(true)
                .create();

        final int N = 20;
        for (int i = 0; i < N; i++) {
            String value = "test-" + i;
            producer.sendAsync(value);
        }
        producer.flush();
        for (int i = 0; i < N; i++) {
            Message<String> msg = consumer.receive();
            if (i % 2 == 0) {
                consumer.acknowledgeAsync(msg);
            } else {
                consumer.negativeAcknowledge(msg);
            }
        }
        Awaitility.await().untilAsserted(() -> {
            long unackedMessages = admin.topics().getStats(topicName).getSubscriptions().get(subscriptionName)
                    .getUnackedMessages();
            assertEquals(unackedMessages, 10);
        });

        // Test negtive ack with sleep
        final String topicName2 = "persistent://prop/ns-abc/batchMessageMultiNegtiveAck2-" + UUID.randomUUID();
        final String subscriptionName2 = "sub-negtive-2";
        @Cleanup
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName2)
                .subscriptionName(subscriptionName2)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(10)
                .enableBatchIndexAcknowledgment(true)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .subscribe();
        @Cleanup
        Producer<String> producer2 = pulsarClient
                .newProducer(Schema.STRING)
                .topic(topicName2)
                .batchingMaxMessages(20)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .enableBatching(true)
                .create();

        for (int i = 0; i < N; i++) {
            String value = "test-" + i;
            producer2.sendAsync(value);
        }
        producer2.flush();
        for (int i = 0; i < N; i++) {
            Message<String> msg = consumer2.receive();
            if (i % 2 == 0) {
                consumer.acknowledgeAsync(msg);
            } else {
                consumer.negativeAcknowledge(msg);
                Thread.sleep(100);
            }
        }
        Awaitility.await().untilAsserted(() -> {
            long unackedMessages = admin.topics().getStats(topicName).getSubscriptions().get(subscriptionName)
                    .getUnackedMessages();
            assertEquals(unackedMessages, 10);
        });
    }

    @Test
    public void testAckMessageWithNotOwnerConsumerUnAckMessageCount() throws Exception {
        final String subName = "test";
        final String topicName = "persistent://prop/ns-abc/testAckMessageWithNotOwnerConsumerUnAckMessageCount-"
                + UUID.randomUUID();

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topicName)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .enableBatching(true)
                .create();

        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient
                .newConsumer()
                .topic(topicName)
                .consumerName("consumer-1")
                .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)
                .isAckReceiptEnabled(true)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient
                .newConsumer()
                .topic(topicName)
                .consumerName("consumer-2")
                .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)
                .isAckReceiptEnabled(true)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        for (int i = 0; i < 5; i++) {
            producer.newMessage().value(("Hello Pulsar - " + i).getBytes()).sendAsync();
        }

        // consume-1 receive 5 batch messages
        List<MessageId> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            list.add(consumer1.receive().getMessageId());
        }

        // consumer-1 redeliver the batch messages
        consumer1.negativeAcknowledge(list.get(0));

        // consumer-2 will receive the messages that the consumer-1 redelivered
        for (int i = 0; i < 5; i++) {
            consumer2.receive().getMessageId();
        }

        // consumer1 ack two messages in the batch message
        consumer1.acknowledge(list.get(1));
        consumer1.acknowledge(list.get(2));

        // consumer-2 redeliver the rest of the messages
        consumer2.negativeAcknowledge(list.get(1));

        // consume-1 close will redeliver the rest messages to consumer-2
        consumer1.close();

        // consumer-2 can receive the rest of 3 messages
        for (int i = 0; i < 3; i++) {
            consumer2.acknowledge(consumer2.receive().getMessageId());
        }

        // consumer-2 can't receive any messages, all the messages in batch has been acked
        Message<byte[]> message = consumer2.receive(1, TimeUnit.SECONDS);
        assertNull(message);

        // the number of consumer-2's unacked messages is 0
        Awaitility.await().until(() -> getPulsar().getBrokerService().getTopic(topicName, false)
                .get().get().getSubscription(subName).getConsumers().get(0).getUnackedMessages() == 0);
    }

    @Test
    public void testNegativeAckAndLongAckDelayWillNotLeadRepeatConsume() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/tp_");
        final String subscriptionName = "s1";
        final int redeliveryDelaySeconds = 2;

        // Create producer and consumer.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxMessages(1000)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .negativeAckRedeliveryDelay(redeliveryDelaySeconds, TimeUnit.SECONDS)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .acknowledgmentGroupTime(1, TimeUnit.HOURS)
                .subscribe();

        // Send 10 messages in batch.
        ArrayList<String> messagesSent = new ArrayList<>();
        List<CompletableFuture<MessageId>> sendTasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String msg = Integer.valueOf(i).toString();
            sendTasks.add(producer.sendAsync(Integer.valueOf(i).toString()));
            messagesSent.add(msg);
        }
        producer.flush();
        FutureUtil.waitForAll(sendTasks).join();

        // Receive messages.
        ArrayList<String> messagesReceived = new ArrayList<>();
        // NegativeAck "batchMessageIdIndex1" once.
        boolean index1HasBeenNegativeAcked = false;
        while (true) {
            Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            if (index1HasBeenNegativeAcked) {
                messagesReceived.add(message.getValue());
                consumer.acknowledge(message);
                continue;
            }
            if (message.getMessageId() instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) message.getMessageId();
                if (batchMessageId.getBatchIndex() == 1) {
                    consumer.negativeAcknowledge(message);
                    index1HasBeenNegativeAcked = true;
                    continue;
                }
            }
            messagesReceived.add(message.getValue());
            consumer.acknowledge(message);
        }

        // Receive negative acked messages.
        // Wait the message negative acknowledgment finished.
        int tripleRedeliveryDelaySeconds = redeliveryDelaySeconds * 3;
        while (true) {
            Message<String> message = consumer.receive(tripleRedeliveryDelaySeconds, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            messagesReceived.add(message.getValue());
            consumer.acknowledge(message);
        }

        log.info("messagesSent: {}, messagesReceived: {}", messagesSent, messagesReceived);
        Assert.assertEquals(messagesReceived.size(), messagesSent.size());

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName);
    }
}
