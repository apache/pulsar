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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class StatsBackLogTest extends ProducerConsumerBase {

    private static final int MAX_ENTRY_COUNT_PER_LEDGER = 10;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    protected void doInitConf() throws Exception {
        conf.setManagedLedgerMaxEntriesPerLedger(5);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        conf.setRetentionCheckIntervalInSeconds(Integer.MAX_VALUE);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private PersistentTopic getPersistentTopic(String topicName){
        return (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
    }

    private ManagedLedgerImpl getManagedLedger(String topicName){
        return (ManagedLedgerImpl) getPersistentTopic(topicName).getManagedLedger();
    }

    private ManagedCursorImpl getManagedCursor(String topicName, String subName){
        for (ManagedCursor cursor : getManagedLedger (topicName).getCursors()){
            if (cursor.getName().equals(subName)){
                return (ManagedCursorImpl) cursor;
            }
        }
        return null;
    }

    private void trimLedger(String topicName){
        trimLedgerAsync(topicName).join();
    }

    private CompletableFuture trimLedgerAsync(String topicName){
        CompletableFuture future = new CompletableFuture();
        getManagedLedger(topicName).internalTrimLedgers(false, future);
        return future;
    }

    private void awaitForMarkDeletedPosition(String topicName, String subName, long ledgerId, long entryId){
        ManagedCursorImpl cursor = getManagedCursor(topicName, subName);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(cursor.getMarkDeletedPosition().getLedgerId() >= ledgerId);
            assertTrue(cursor.getMarkDeletedPosition().getEntryId() >= entryId);
        });
    }

    private void awaitForSlowestPosition(String topicName, long ledgerId, long entryId){
        ManagedLedgerImpl ledger = getManagedLedger(topicName);
        Awaitility.await().untilAsserted(() -> {
            PositionImpl slowestPosition = ledger.getCursors().getSlowestReaderPosition();
            assertTrue(slowestPosition.getLedgerId() >= ledgerId);
            assertTrue(slowestPosition.getEntryId() >= entryId);
        });
    }

    private SendInfo sendMessages(int ledgerCount, int entryCountPerLedger, Producer<String> producer,
                                      String topicName) throws Exception{
        List<MessageId> messageIds = new ArrayList<>(ledgerCount * entryCountPerLedger);
        MessageIdImpl startMessageId = null;
        MessageIdImpl lastMessageId = null;
        for (int m = 0; m < ledgerCount; m++) {
            for (int n = 0; n < entryCountPerLedger; n++) {
                lastMessageId = (MessageIdImpl) producer.send(String.format("%s:%s", m, n));
                if (startMessageId == null){
                    startMessageId = lastMessageId;
                }
                messageIds.add(lastMessageId);
            }
            if (entryCountPerLedger < MAX_ENTRY_COUNT_PER_LEDGER) {
                admin.topics().unload(topicName);
            }
        }
        return new SendInfo(messageIds, startMessageId, lastMessageId);
    }

    private void consumeAllMessages(Consumer<String> consumer) throws Exception {
        while (true){
            Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
            if (message == null){
                break;
            }
            consumer.acknowledge(message);
        }
    }

    @AllArgsConstructor
    private static class SendInfo {
        List<MessageId> messageIds;
        MessageIdImpl firstMessageId;
        MessageIdImpl lastMessageId;
    }

    @DataProvider(name = "entryCountPerLedger")
    public Object[][] entryCountPerLedger(){
        return new Object[][]{
            {5},
            {MAX_ENTRY_COUNT_PER_LEDGER}
        };
    }

    @Test(timeOut = 1000 * 3600)
    public void testBacklogIfCursorCreateConcurrentWithTrimLedger() throws Exception {
        String topicName = String.format("persistent://my-property/my-ns/%s",
                BrokerTestUtil.newUniqueName("tp_"));
        String subName1 = "sub1";
        String subName2 = "sub2";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName.toString())
                .enableBatching(false).create();

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .isAckReceiptEnabled(true)
                .subscriptionName(subName1).subscribe();

        int ledgerCount = 5;
        int entryCountPerLedger = 10;
        SendInfo sendInfo1 = sendMessages(ledgerCount, entryCountPerLedger, producer, topicName);
        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName1,
                getManagedCursor(topicName, subName1).getMessagesConsumedCounter());

        consumer1.acknowledge(sendInfo1.messageIds.stream()
                .filter(msgId -> msgId.compareTo(sendInfo1.lastMessageId) != 0).collect(Collectors.toList()));
        awaitForSlowestPosition(topicName, sendInfo1.lastMessageId.getLedgerId(), Long.MIN_VALUE);
        ProcessCoordinator.waitAndChangeStep(1);

        final CompletableFuture<Void> trimLedgerFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                trimLedger(topicName);
                trimLedgerFuture.complete(null);
            } catch (Exception e){
                trimLedgerFuture.completeExceptionally(e);
            }
        }).start();

        final CompletableFuture<Consumer<String>> createConsumer2Future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                createConsumer2Future.complete(pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscriptionName(subName2).subscribe());
            }catch (Exception e){
                createConsumer2Future.completeExceptionally(e);
            }
        }).start();

        trimLedgerFuture.join();
        createConsumer2Future.join();

        consumeAllMessages(createConsumer2Future.join());

        log.info("{}-entriesAddedCounter: {}", topicName, getManagedLedger(topicName).getEntriesAddedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName1,
                getManagedCursor(topicName, subName1).getMessagesConsumedCounter());
        log.info("{}-messagesConsumedCounter: {}", subName2,
                getManagedCursor(topicName, subName2).getMessagesConsumedCounter());

        TopicStats topicStats = admin.topics().getStats(topicName);
        assertEquals(topicStats.getSubscriptions().get(subName1).getMsgBacklog(), 1L);
        assertEquals(topicStats.getSubscriptions().get(subName2).getMsgBacklog(), 0L);

        // cleanup.
        producer.close();
        consumer1.close();
        createConsumer2Future.join().close();
        admin.topics().delete(topicName, false);
    }
}
