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
package org.apache.pulsar.broker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LedgerLostAndSkipNonRecoverableTest extends ProducerConsumerBase {

    private static final String DEFAULT_NAMESPACE = "my-property/my-ns";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void doInitConf() throws Exception {
        conf.setAutoSkipNonRecoverableData(true);
    }

    @DataProvider(name = "batchEnabled")
    public Object[][] batchEnabled(){
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(timeOut = 30000, dataProvider = "batchEnabled")
    public void testMarkDeletedPositionCanForwardAfterTopicLedgerLost(boolean enabledBatch) throws Exception {
        String topicSimpleName = UUID.randomUUID().toString().replaceAll("-", "");
        String subName = UUID.randomUUID().toString().replaceAll("-", "");
        String topicName = String.format("persistent://%s/%s", DEFAULT_NAMESPACE, topicSimpleName);

        log.info("create topic and subscription.");
        Consumer sub = createConsumer(topicName, subName, enabledBatch);
        sub.redeliverUnacknowledgedMessages();
        sub.close();

        log.info("send many messages.");
        int ledgerCount = 3;
        int messageCountPerLedger = enabledBatch ? 25 : 5;
        int messageCountPerEntry = enabledBatch ? 5 : 1;
        List<MessageIdImpl>[] sendMessages =
                sendManyMessages(topicName, ledgerCount, messageCountPerLedger, messageCountPerEntry);
        int sendMessageCount = Arrays.asList(sendMessages).stream()
                .flatMap(s -> s.stream()).collect(Collectors.toList()).size();
        log.info("send {} messages", sendMessageCount);

        log.info("make individual ack.");
        ConsumerAndReceivedMessages consumerAndReceivedMessages1 =
                waitConsumeAndAllMessages(topicName, subName, enabledBatch,false);
        List<MessageIdImpl>[] messageIds = consumerAndReceivedMessages1.messageIds;
        Consumer consumer = consumerAndReceivedMessages1.consumer;
        MessageIdImpl individualPosition = messageIds[1].get(messageCountPerEntry - 1);
        MessageIdImpl expectedMarkDeletedPosition =
                new MessageIdImpl(messageIds[0].get(0).getLedgerId(), messageIds[0].get(0).getEntryId(), -1);
        MessageIdImpl lastPosition =
                new MessageIdImpl(messageIds[2].get(4).getLedgerId(), messageIds[2].get(4).getEntryId(), -1);
        consumer.acknowledge(individualPosition);
        consumer.acknowledge(expectedMarkDeletedPosition);
        waitPersistentCursorLedger(topicName, subName, expectedMarkDeletedPosition.getLedgerId(),
                expectedMarkDeletedPosition.getEntryId());
        consumer.close();

        log.info("Make lost ledger [{}].", individualPosition.getLedgerId());
        pulsar.getBrokerService().getTopic(topicName, false).get().get().close(false);
        mockBookKeeper.deleteLedger(individualPosition.getLedgerId());

        log.info("send some messages.");
        sendManyMessages(topicName, 3, messageCountPerEntry);

        log.info("receive all messages then verify mark deleted position");
        ConsumerAndReceivedMessages consumerAndReceivedMessages2 =
                waitConsumeAndAllMessages(topicName, subName, enabledBatch, true);
        waitMarkDeleteLargeAndEquals(topicName, subName, lastPosition.getLedgerId(), lastPosition.getEntryId());

        // cleanup
        consumerAndReceivedMessages2.consumer.close();
        admin.topics().delete(topicName);
    }

    private ManagedCursorImpl getCursor(String topicName, String subName) throws Exception {
        PersistentSubscription subscription_ =
                (PersistentSubscription) pulsar.getBrokerService().getTopic(topicName, false)
                        .get().get().getSubscription(subName);
        return  (ManagedCursorImpl) subscription_.getCursor();
    }

    private void waitMarkDeleteLargeAndEquals(String topicName, String subName, final long markDeletedLedgerId,
                                            final long markDeletedEntryId) throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            Position persistentMarkDeletedPosition = getCursor(topicName, subName).getMarkDeletedPosition();
            log.info("markDeletedPosition {}:{}, expected {}:{}", persistentMarkDeletedPosition.getLedgerId(),
                    persistentMarkDeletedPosition.getEntryId(), markDeletedLedgerId, markDeletedEntryId);
            Assert.assertTrue(persistentMarkDeletedPosition.getLedgerId() >= markDeletedLedgerId);
            if (persistentMarkDeletedPosition.getLedgerId() > markDeletedLedgerId){
                return;
            }
            Assert.assertTrue(persistentMarkDeletedPosition.getEntryId() >= markDeletedEntryId);
        });
    }

    private void waitPersistentCursorLedger(String topicName, String subName, final long markDeletedLedgerId,
                                            final long markDeletedEntryId) throws Exception {
        Awaitility.await().untilAsserted(() -> {
            Position persistentMarkDeletedPosition = getCursor(topicName, subName).getPersistentMarkDeletedPosition();
            Assert.assertEquals(persistentMarkDeletedPosition.getLedgerId(), markDeletedLedgerId);
            Assert.assertEquals(persistentMarkDeletedPosition.getEntryId(), markDeletedEntryId);
        });
    }

    private List<MessageIdImpl>[] sendManyMessages(String topicName, int ledgerCount, int messageCountPerLedger,
                                                   int messageCountPerEntry) throws Exception {
        List<MessageIdImpl>[] messageIds = new List[ledgerCount];
        for (int i = 0; i < ledgerCount; i++){
            admin.topics().unload(topicName);
            if (messageCountPerEntry == 1) {
                messageIds[i] = sendManyMessages(topicName, messageCountPerLedger);
            } else {
                messageIds[i] = sendManyBatchedMessages(topicName, messageCountPerEntry,
                        messageCountPerLedger / messageCountPerEntry);
            }
        }
        return messageIds;
    }

    private List<MessageIdImpl> sendManyMessages(String topicName, int messageCountPerLedger,
                                                   int messageCountPerEntry) throws Exception {
        if (messageCountPerEntry == 1) {
            return sendManyMessages(topicName, messageCountPerLedger);
        } else {
            return sendManyBatchedMessages(topicName, messageCountPerEntry,
                    messageCountPerLedger / messageCountPerEntry);
        }
    }

    private List<MessageIdImpl> sendManyMessages(String topicName, int msgCount) throws Exception {
        List<MessageIdImpl> messageIdList = new ArrayList<>();
        final Producer<String> producer = pulsarClient.newProducer(Schema.JSON(String.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < msgCount; i++){
            String messageSuffix = String.format("%s-%s", timestamp, i);
            MessageIdImpl messageIdSent = (MessageIdImpl) producer.newMessage()
                    .key(String.format("Key-%s", messageSuffix))
                    .value(String.format("Msg-%s", messageSuffix))
                    .send();
            messageIdList.add(messageIdSent);
        }
        producer.close();
        return messageIdList;
    }

    private List<MessageIdImpl> sendManyBatchedMessages(String topicName, int msgCountPerEntry, int entryCount)
            throws Exception {
        Producer<String> producer = pulsarClient.newProducer(Schema.JSON(String.class))
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(Integer.MAX_VALUE, TimeUnit.SECONDS)
                .batchingMaxMessages(Integer.MAX_VALUE)
                .create();
        List<CompletableFuture<MessageId>> messageIdFutures = new ArrayList<>();
        for (int i = 0; i < entryCount; i++){
            for (int j = 0; j < msgCountPerEntry; j++){
                CompletableFuture<MessageId> messageIdFuture =
                        producer.newMessage().value(String.format("entry-seq[%s], batch_index[%s]", i, j)).sendAsync();
                messageIdFutures.add(messageIdFuture);
            }
            producer.flush();
        }
        producer.close();
        FutureUtil.waitForAll(messageIdFutures).get();
        return messageIdFutures.stream().map(f -> (MessageIdImpl)f.join()).collect(Collectors.toList());
    }

    private ConsumerAndReceivedMessages waitConsumeAndAllMessages(String topicName, String subName,
                                                            final boolean enabledBatch,
                                                            boolean ack) throws Exception {
        List<MessageIdImpl> messageIds = new ArrayList<>();
        final Consumer consumer = createConsumer(topicName, subName, enabledBatch);
        while (true){
            Message message = consumer.receive(5, TimeUnit.SECONDS);
            if (message != null){
                messageIds.add((MessageIdImpl) message.getMessageId());
                if (ack) {
                    consumer.acknowledge(message);
                }
            } else {
                break;
            }
        }
        log.info("receive {} messages", messageIds.size());
        return new ConsumerAndReceivedMessages(consumer, sortMessageId(messageIds, enabledBatch));
    }

    @AllArgsConstructor
    private static class ConsumerAndReceivedMessages {
        private Consumer consumer;
        private List<MessageIdImpl>[] messageIds;
    }

    private List<MessageIdImpl>[] sortMessageId(List<MessageIdImpl> messageIds, boolean enabledBatch){
        Map<Long, List<MessageIdImpl>> map = messageIds.stream().collect(Collectors.groupingBy(v -> v.getLedgerId()));
        TreeMap<Long, List<MessageIdImpl>> sortedMap = new TreeMap<>(map);
        List<MessageIdImpl>[] res = new List[sortedMap.size()];
        Iterator<Map.Entry<Long, List<MessageIdImpl>>> iterator = sortedMap.entrySet().iterator();
        for (int i = 0; i < sortedMap.size(); i++){
            res[i] = iterator.next().getValue();
        }
        for (List<MessageIdImpl> list : res){
            list.sort((m1, m2) -> {
                if (enabledBatch){
                    BatchMessageIdImpl mb1 = (BatchMessageIdImpl) m1;
                    BatchMessageIdImpl mb2 = (BatchMessageIdImpl) m2;
                    return (int) (mb1.getLedgerId() * 1000000 + mb1.getEntryId() * 1000 + mb1.getBatchIndex() -
                            mb2.getLedgerId() * 1000000 + mb2.getEntryId() * 1000 + mb2.getBatchIndex());
                }
                return (int) (m1.getLedgerId() * 1000 + m1.getEntryId() -
                        m2.getLedgerId() * 1000 + m2.getEntryId());
            });
        }
        return res;
    }

    private Consumer<String> createConsumer(String topicName, String subName, boolean enabledBatch) throws Exception {
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.JSON(String.class))
                .subscriptionType(SubscriptionType.Failover)
                .isAckReceiptEnabled(true)
                .enableBatchIndexAcknowledgment(enabledBatch)
                .receiverQueueSize(1000)
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();
        return consumer;
    }
}
