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

import static org.apache.pulsar.client.api.SubscriptionType.Exclusive;
import static org.apache.pulsar.client.api.SubscriptionType.Failover;
import static org.apache.pulsar.client.api.SubscriptionType.Key_Shared;
import static org.apache.pulsar.client.api.SubscriptionType.Shared;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class UnloadSubscriptionTest extends SharedPulsarBaseTest {

    @DataProvider(name = "unloadCases")
    public Object[][] unloadCases (){
        // [msgCount, enabledBatch, maxMsgPerBatch, subType, ackMsgCount]
        return new Object[][]{
                {100, false, 1, Exclusive, 0},
                {100, false, 1, Failover, 0},
                {100, false, 1, Shared, 0},
                {100, false, 1, Key_Shared, 0},
                {100, true, 5, Exclusive, 0},
                {100, true, 5, Failover, 0},
                {100, true, 5, Shared, 0},
                {100, true, 5, Key_Shared, 0},
                {100, false, 1, Exclusive, 50},
                {100, false, 1, Failover, 50},
                {100, false, 1, Shared, 50},
                {100, false, 1, Key_Shared, 50},
                {100, true, 5, Exclusive, 50},
                {100, true, 5, Failover, 50},
                {100, true, 5, Shared, 50},
                {100, true, 5, Key_Shared, 50},
        };
    }

    @Test(dataProvider = "unloadCases")
    public void testSingleConsumer(int msgCount, boolean enabledBatch, int maxMsgPerBatch, SubscriptionType subType,
                                   int ackMsgCount) throws Exception {
        final String topicName = newTopicName();
        final String subName = "sub";
        Consumer<String> consumer = createConsumer(topicName, subName, subType);
        ProducerAndMessageIds producerAndMessageIds =
                createProducerAndSendMessages(topicName, msgCount, enabledBatch, maxMsgPerBatch);
        log.info("send message-ids:{}-{}", producerAndMessageIds.messageIds.size(),
                toString(producerAndMessageIds.messageIds));

        // Receive all messages and ack some.
        MessagesEntry messagesEntry = receiveMessages(consumer, msgCount);
        if (ackMsgCount > 0){
            LinkedHashSet<MessageId> ackedMessageIds = new LinkedHashSet<>();
            Iterator<MessageId> messageIdIterator = messagesEntry.messageIdSet.iterator();
            for (int i = ackMsgCount; i > 0; i--){
                ackedMessageIds.add(messageIdIterator.next());
            }
            consumer.acknowledge(ackedMessageIds.stream().toList());
            log.info("ack message-ids: {}", toString(ackedMessageIds.stream().toList()));
        }

        // Unload subscriber.
        PersistentTopic persistentTopic = getPersistentTopic(topicName);
        persistentTopic.unloadSubscription(subName);
        // Receive all messages for the second time.
        int expectedAfterUnload = msgCount - ackMsgCount;
        MessagesEntry messagesEntryForTheSecondTime = receiveMessages(consumer, expectedAfterUnload);
        log.info("received message-ids for the second time: {}",
                toString(messagesEntryForTheSecondTime.messageIdSet.stream().toList()));

        // cleanup.
        producerAndMessageIds.producer.close();
        consumer.close();
    }

    @Test(dataProvider = "unloadCases")
    public void testMultiConsumer(int msgCount, boolean enabledBatch, int maxMsgPerBatch, SubscriptionType subType,
                                  int ackMsgCount) throws Exception {
        if (subType == Exclusive){
            return;
        }
        final String topicName = newTopicName();
        final String subName = "sub";
        Consumer<String> consumer1 = createConsumer(topicName, subName, subType);
        Consumer<String> consumer2 = createConsumer(topicName, subName, subType);
        ProducerAndMessageIds producerAndMessageIds =
                createProducerAndSendMessages(topicName, msgCount, enabledBatch, maxMsgPerBatch);
        log.info("send message-ids:{}-{}", producerAndMessageIds.messageIds.size(),
                toString(producerAndMessageIds.messageIds));

        // Receive all messages and ack some.
        List<Consumer<String>> consumers = List.of(consumer1, consumer2);
        MessagesEntry messagesEntry = receiveMessages(consumers, msgCount);
        if (ackMsgCount > 0){
            LinkedHashSet<MessageId> ackedMessageIds = new LinkedHashSet<>();
            Iterator<MessageId> messageIdIterator = messagesEntry.messageIdSet.iterator();
            for (int i = ackMsgCount; i > 0; i--){
                ackedMessageIds.add(messageIdIterator.next());
            }
            consumer1.acknowledge(ackedMessageIds.stream().toList());
            log.info("ack message-ids: {}", toString(ackedMessageIds.stream().toList()));
        }

        // Unload subscriber.
        PersistentTopic persistentTopic = getPersistentTopic(topicName);
        persistentTopic.unloadSubscription(subName);

        // Receive all messages for the second time.
        int expectedAfterUnload = msgCount - ackMsgCount;
        MessagesEntry messagesEntryForTheSecondTime = receiveMessages(consumers, expectedAfterUnload);
        log.info("received message-ids for the second time: {}",
                toString(messagesEntryForTheSecondTime.messageIdSet.stream().toList()));
        assertEquals(messagesEntryForTheSecondTime.messageSet.size(), expectedAfterUnload);

        // cleanup.
        producerAndMessageIds.producer.close();
        consumer1.close();
        consumer2.close();
    }

    private static String toString(List<MessageId> messageIds){
        List<String> messageIdStrings = new ArrayList<>(messageIds.size());
        for (MessageId messageId : messageIds){
            MessageIdImpl messageIdImpl;
            if (messageId instanceof TopicMessageIdImpl) {
                TopicMessageIdImpl topicMessageId = (TopicMessageIdImpl) messageId;
                messageIdImpl = (MessageIdImpl) topicMessageId.getInnerMessageId();
            } else {
                messageIdImpl = (MessageIdImpl) messageId;
            }
            StringBuilder stringBuilder = new StringBuilder(String.valueOf(messageIdImpl.getEntryId()));
            if (messageIdImpl instanceof BatchMessageIdImpl){
                BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageIdImpl;
                stringBuilder.append("_")
                        .append(batchMessageId.getBatchIndex())
                        .append("/")
                        .append(batchMessageId.getBatchSize());
            }
            messageIdStrings.add(stringBuilder.toString());
        }
        return messageIdStrings.toString();
    }

    private PersistentTopic getPersistentTopic(String topicName) {
        return (PersistentTopic) getTopic(topicName, false).join().get();
    }

    private ProducerAndMessageIds createProducerAndSendMessages(String topicName, int msgCount, boolean enabledBatch,
                                                           int maxMsgPerBatch) throws Exception {
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(enabledBatch)
                .batchingMaxMessages(maxMsgPerBatch)
                .create();
        ArrayList<CompletableFuture<MessageId>> messageIds = new ArrayList<>();
        for (int i = 0; i < msgCount; i++) {
            messageIds.add(producer.newMessage().key(String.valueOf(i % 10)).value(String.valueOf(i)).sendAsync());
        }
        FutureUtil.waitForAll(messageIds).join();
        return new ProducerAndMessageIds(producer,
                messageIds.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }

    private record ProducerAndMessageIds(Producer<String> producer, List<MessageId> messageIds) {}

    private Consumer<String> createConsumer(String topicName, String subName, SubscriptionType subType)
            throws Exception {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(subType)
                .isAckReceiptEnabled(true)
                .subscribe();
        return consumer;
    }

    private MessagesEntry receiveMessages(Consumer<String> consumer, int expectedCount) throws Exception {
        final LinkedHashSet<String> messageSet = new LinkedHashSet<>();
        final LinkedHashSet<MessageId> messageIdSet = new LinkedHashSet<>();
        for (int i = 0; i < expectedCount; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg, "Expected " + expectedCount + " messages but only received " + i);
            messageIdSet.add(msg.getMessageId());
            messageSet.add(msg.getValue());
        }
        return new MessagesEntry(messageSet, messageIdSet);
    }

    private MessagesEntry receiveMessages(List<Consumer<String>> consumers, int expectedCount) throws Exception {
        final LinkedHashSet<String> messageSet = new LinkedHashSet<>();
        final LinkedHashSet<MessageId> messageIdSet = new LinkedHashSet<>();
        // Use receiveAsync on all consumers concurrently and collect until we have enough
        List<CompletableFuture<Message<String>>> pending = new ArrayList<>();
        for (Consumer<String> c : consumers) {
            pending.add(c.receiveAsync());
        }
        long deadline = System.currentTimeMillis() + 30_000;
        while (messageSet.size() < expectedCount && System.currentTimeMillis() < deadline) {
            CompletableFuture<Object> any = CompletableFuture.anyOf(pending.toArray(new CompletableFuture[0]));
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            try {
                any.get(remaining, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                break;
            }
            // Collect all completed futures
            List<CompletableFuture<Message<String>>> newPending = new ArrayList<>();
            for (int i = 0; i < pending.size(); i++) {
                CompletableFuture<Message<String>> f = pending.get(i);
                if (f.isDone()) {
                    Message<String> msg = f.join();
                    messageIdSet.add(msg.getMessageId());
                    messageSet.add(msg.getValue());
                    if (messageSet.size() < expectedCount) {
                        newPending.add(consumers.get(i).receiveAsync());
                    }
                } else {
                    newPending.add(f);
                }
            }
            pending = newPending;
        }
        assertEquals(messageSet.size(), expectedCount,
                "Expected " + expectedCount + " messages but received " + messageSet.size());
        // Cancel any remaining pending futures
        for (CompletableFuture<Message<String>> f : pending) {
            f.cancel(false);
        }
        return new MessagesEntry(messageSet, messageIdSet);
    }

    private record MessagesEntry(LinkedHashSet<String> messageSet, LinkedHashSet<MessageId> messageIdSet) {}

}
