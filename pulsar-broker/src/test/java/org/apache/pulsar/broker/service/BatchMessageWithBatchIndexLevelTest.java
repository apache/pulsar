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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.carrotsearch.hppc.ObjectSet;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

        List<CompletableFuture<MessageId>> sendFutureList = new ArrayList<>();
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

    @DataProvider
    public Object[][] enabledBatchSend() {
        return new Object[][] {
                {false},
                {true}
        };
    }

    @Test(dataProvider = "enabledBatchSend")
    @SneakyThrows
    public void testBatchMessageNAck(boolean enabledBatchSend) {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/tp");
        final String subscriptionName = "s1";
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName)
                .receiverQueueSize(21)
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxMessages(20)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .enableBatching(enabledBatchSend)
                .create();
        final PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        final PersistentDispatcherMultipleConsumers dispatcher =
                (PersistentDispatcherMultipleConsumers) topic.getSubscription(subscriptionName).getDispatcher();

        // Send messages: 20 * 2.
        for (int i = 0; i < 40; i++) {
            byte[] message = ("batch-message-" + i).getBytes();
            if (i == 19 || i == 39) {
                producer.newMessage().value(message).send();
            } else {
                producer.newMessage().value(message).sendAsync();
            }
        }
        Awaitility.await().untilAsserted(() -> {
            if (enabledBatchSend) {
                assertEquals(consumer.numMessagesInQueue(), 40);
            } else {
                assertEquals(consumer.numMessagesInQueue(), 21);
            }
        });

        // Negative ack and verify result/
        Message<byte[]> receive1 = consumer.receive();
        consumer.pause();
        consumer.negativeAcknowledge(receive1);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(consumer.numMessagesInQueue(), 20);
            assertEquals(dispatcher.getConsumers().get(0).getUnackedMessages(), 20);
        });

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName);
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
            if (((MessageIdAdv) message.getMessageId()).getBatchIndex() == 1) {
                consumer.negativeAcknowledge(message);
                index1HasBeenNegativeAcked = true;
                continue;
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

    @Test
    public void testMixIndexAndNonIndexUnAckMessageCount() throws Exception {
        final String topicName = "persistent://prop/ns-abc/testMixIndexAndNonIndexUnAckMessageCount-";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .acknowledgmentGroupTime(100, TimeUnit.MILLISECONDS)
                .enableBatchIndexAcknowledgment(true)
                .isAckReceiptEnabled(true)
                .subscribe();

        // send two batch messages: [(1), (2,3)]
        producer.send("1".getBytes());
        producer.sendAsync("2".getBytes());
        producer.send("3".getBytes());

        Message<byte[]> message1 = consumer.receive();
        Message<byte[]> message2 = consumer.receive();
        Message<byte[]> message3 = consumer.receive();
        consumer.acknowledgeAsync(message1);
        consumer.acknowledge(message2);  // send group ack: non-index ack for 1, index ack for 2
        consumer.acknowledge(message3);  // index ack for 3

        assertEquals(admin.topics().getStats(topicName).getSubscriptions()
                .get("sub").getUnackedMessages(), 0);
    }

    @Test
    public void testUnAckMessagesWhenConcurrentDeliveryAndAck() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/tp");
        final String subName = "s1";
        final int receiverQueueSize = 500;
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subName, MessageId.earliest);
        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .receiverQueueSize(receiverQueueSize)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(true);

        // Send 100 messages.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        CompletableFuture<MessageId> lastSent = null;
        for (int i = 0; i < 100; i++) {
            lastSent = producer.sendAsync(i + "");
        }
        producer.flush();
        lastSent.join();

        // When consumer1 is closed, may some messages are in the client memory(it they are being acked now).
        Consumer<String> consumer1 = consumerBuilder.consumerName("c1").subscribe();
        Message[] messagesInClientMemory = new Message[2];
        for (int i = 0; i < 2; i++) {
            Message msg = consumer1.receive(2, TimeUnit.SECONDS);
            assertNotNull(msg);
            messagesInClientMemory[i] = msg;
        }
        ConsumerImpl<String> consumer2 = (ConsumerImpl<String>) consumerBuilder.consumerName("c2").subscribe();
        Awaitility.await().until(() -> consumer2.isConnected());

        // The consumer2 will receive messages after consumer1 closed.
        // Insert a delay mechanism to make the flow like below:
        //  1. Close consumer1, then the 100 messages will be redelivered.
        //  2. Read redeliver messages. No messages were acked at this time.
        //  3. The in-flight ack of two messages is finished.
        //  4. Send the messages to consumer2, consumer2 will get all the 100 messages.
        CompletableFuture<Void> receiveMessageSignal2 = new CompletableFuture<>();
        org.apache.pulsar.broker.service.Consumer serviceConsumer2 =
                makeConsumerReceiveMessagesDelay(topicName, subName, "c2", receiveMessageSignal2);
        // step 1: close consumer.
        consumer1.close();
        // step 2: wait for read messages from replay queue.
        Thread.sleep(2 * 1000);
        // step 3: wait for the in-flight ack.
        BitSetRecyclable bitSetRecyclable = createBitSetRecyclable(100);
        long ledgerId = 0, entryId = 0;
        for (Message message : messagesInClientMemory) {
            BatchMessageIdImpl msgId = (BatchMessageIdImpl) message.getMessageId();
            bitSetRecyclable.clear(msgId.getBatchIndex());
            ledgerId = msgId.getLedgerId();
            entryId = msgId.getEntryId();
        }
        getCursor(topicName, subName).delete(PositionImpl.get(ledgerId, entryId, bitSetRecyclable.toLongArray()));
        // step 4: send messages to consumer2.
        receiveMessageSignal2.complete(null);
        // Verify: Consumer2 will get all the 100 messages, and "unAckMessages" is 100.
        List<Message> messages2 = new ArrayList<>();
        while (true) {
            Message msg = consumer2.receive(2, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            messages2.add(msg);
        }
        assertEquals(messages2.size(), 100);
        assertEquals(serviceConsumer2.getUnackedMessages(), 100);
        // After the messages were pop out, the permits in the client memory went to 100.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(serviceConsumer2.getAvailablePermits() + consumer2.getAvailablePermits(),
                    receiverQueueSize);
        });

        // cleanup.
        producer.close();
        consumer2.close();
        admin.topics().delete(topicName, false);
    }

    private BitSetRecyclable createBitSetRecyclable(int batchSize) {
        BitSetRecyclable bitSetRecyclable = new BitSetRecyclable(batchSize);
        for (int i = 0; i < batchSize; i++) {
            bitSetRecyclable.set(i);
        }
        return bitSetRecyclable;
    }

    private ManagedCursorImpl getCursor(String topic, String sub) {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        PersistentDispatcherMultipleConsumers dispatcher =
                (PersistentDispatcherMultipleConsumers) persistentTopic.getSubscription(sub).getDispatcher();
        return (ManagedCursorImpl) dispatcher.getCursor();
    }

    /***
     * After {@param signal} complete, the consumer({@param consumerName}) start to receive messages.
     */
    private org.apache.pulsar.broker.service.Consumer makeConsumerReceiveMessagesDelay(String topic, String sub,
                                                            String consumerName,
                                                            CompletableFuture<Void> signal) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        PersistentDispatcherMultipleConsumers dispatcher =
                (PersistentDispatcherMultipleConsumers) persistentTopic.getSubscription(sub).getDispatcher();
        org.apache.pulsar.broker.service.Consumer serviceConsumer = null;
        for (org.apache.pulsar.broker.service.Consumer c : dispatcher.getConsumers()){
            if (c.consumerName().equals(consumerName)) {
                serviceConsumer = c;
                break;
            }
        }
        final org.apache.pulsar.broker.service.Consumer originalConsumer = serviceConsumer;

        // Insert a delay signal.
        org.apache.pulsar.broker.service.Consumer spyServiceConsumer = spy(originalConsumer);
        doAnswer(invocation -> {
            List<? extends Entry> entries = (List<? extends Entry>) invocation.getArguments()[0];
            EntryBatchSizes batchSizes = (EntryBatchSizes) invocation.getArguments()[1];
            EntryBatchIndexesAcks batchIndexesAcks = (EntryBatchIndexesAcks) invocation.getArguments()[2];
            int totalMessages = (int) invocation.getArguments()[3];
            long totalBytes = (long) invocation.getArguments()[4];
            long totalChunkedMessages = (long) invocation.getArguments()[5];
            RedeliveryTracker redeliveryTracker = (RedeliveryTracker) invocation.getArguments()[6];
            return signal.thenApply(__ -> originalConsumer.sendMessages(entries, batchSizes, batchIndexesAcks, totalMessages, totalBytes,
                    totalChunkedMessages, redeliveryTracker)).join();
        }).when(spyServiceConsumer)
                .sendMessages(anyList(), any(), any(), anyInt(), anyLong(), anyLong(), any());
        doAnswer(invocation -> {
            List<? extends Entry> entries = (List<? extends Entry>) invocation.getArguments()[0];
            EntryBatchSizes batchSizes = (EntryBatchSizes) invocation.getArguments()[1];
            EntryBatchIndexesAcks batchIndexesAcks = (EntryBatchIndexesAcks) invocation.getArguments()[2];
            int totalMessages = (int) invocation.getArguments()[3];
            long totalBytes = (long) invocation.getArguments()[4];
            long totalChunkedMessages = (long) invocation.getArguments()[5];
            RedeliveryTracker redeliveryTracker = (RedeliveryTracker) invocation.getArguments()[6];
            long epoch = (long) invocation.getArguments()[7];
            return signal.thenApply(__ -> originalConsumer.sendMessages(entries, batchSizes, batchIndexesAcks, totalMessages, totalBytes,
                    totalChunkedMessages, redeliveryTracker, epoch)).join();
        }).when(spyServiceConsumer)
                .sendMessages(anyList(), any(), any(), anyInt(), anyLong(), anyLong(), any(), anyLong());

        // Replace the consumer.
        Field fConsumerList = AbstractDispatcherMultipleConsumers.class.getDeclaredField("consumerList");
        Field fConsumerSet = AbstractDispatcherMultipleConsumers.class.getDeclaredField("consumerSet");
        fConsumerList.setAccessible(true);
        fConsumerSet.setAccessible(true);
        List<org.apache.pulsar.broker.service.Consumer> consumerList =
                (List<org.apache.pulsar.broker.service.Consumer>) fConsumerList.get(dispatcher);
        ObjectSet<org.apache.pulsar.broker.service.Consumer> consumerSet =
                (ObjectSet<org.apache.pulsar.broker.service.Consumer>) fConsumerSet.get(dispatcher);

        consumerList.remove(originalConsumer);
        consumerSet.removeAll(originalConsumer);
        consumerList.add(spyServiceConsumer);
        consumerSet.add(spyServiceConsumer);
        return originalConsumer;
    }

    /***
     * 1. Send a batch message contains 100 single messages.
     * 2. Ack 2 messages.
     * 3. Redeliver the batch message and ack them.
     * 4. Verify: the permits is correct.
     */
    @Test
    public void testPermitsIfHalfAckBatchMessage() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/tp");
        final String subName = "s1";
        final int receiverQueueSize = 1000;
        final int ackedMessagesCountInTheFistStep = 2;
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics(). createSubscription(topicName, subName, MessageId.earliest);
        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .receiverQueueSize(receiverQueueSize)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(true);

        // Send 100 messages.
        Producer<String>  producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        CompletableFuture<MessageId>  lastSent = null;
        for (int i = 1;  i <=  100;  i++) {
            lastSent = producer. sendAsync(i + "");
        }
        producer.flush();
        lastSent.join();

        // Ack 2 messages, and trigger a redelivery.
        Consumer<String>  consumer1 = consumerBuilder.subscribe();
        for (int i = 0;  i <  ackedMessagesCountInTheFistStep;  i++) {
            Message msg = consumer1. receive(2, TimeUnit.SECONDS);
            assertNotNull(msg);
            consumer1.acknowledge(msg);
        }
        consumer1.close();

        // Receive the left 98 messages, and ack them.
        // Verify the permits is correct.
        ConsumerImpl<String> consumer2 = (ConsumerImpl<String>) consumerBuilder.subscribe();
        Awaitility.await().until(() ->  consumer2.isConnected());
        List<MessageId>  messages = new ArrayList<>();
        int nextMessageValue = ackedMessagesCountInTheFistStep + 1;
        while (true) {
            Message<String> msg = consumer2.receive(2, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            assertEquals(msg.getValue(), nextMessageValue + "");
            messages.add(msg.getMessageId());
            nextMessageValue++;
        }
        assertEquals(messages.size(), 98);
        consumer2.acknowledge(messages);

        org.apache.pulsar.broker.service.Consumer serviceConsumer2 =
                getTheUniqueServiceConsumer(topicName, subName);
        Awaitility.await().untilAsserted(() ->  {
            // After the messages were pop out, the permits in the client memory went to 98.
            int permitsInClientMemory = consumer2.getAvailablePermits();
            int permitsInBroker = serviceConsumer2.getAvailablePermits();
            assertEquals(permitsInClientMemory + permitsInBroker, receiverQueueSize);
        });

        // cleanup.
        producer.close();
        consumer2.close();
        admin.topics().delete(topicName, false);
    }

    private org.apache.pulsar.broker.service.Consumer getTheUniqueServiceConsumer(String topic, String sub) {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService(). getTopic(topic, false).join().get();
        PersistentDispatcherMultipleConsumers dispatcher =
                (PersistentDispatcherMultipleConsumers) persistentTopic.getSubscription(sub).getDispatcher();
        return dispatcher.getConsumers().iterator().next();
    }
}
