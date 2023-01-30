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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class StatsBackLogTest extends ProducerConsumerBase {

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

    private SendInfo sendMessages(int messageCount, Producer<String> producer) throws Exception{
        List<MessageId> messageIds = new ArrayList<>(messageCount);
        MessageIdImpl startMessageId = null;
        MessageIdImpl lastMessageId = null;
        for (int n = 0; n < messageCount; n++) {
            lastMessageId = (MessageIdImpl) producer.send(String.valueOf(n));
            if (startMessageId == null){
                startMessageId = lastMessageId;
            }
            messageIds.add(lastMessageId);
        }
        return new SendInfo(messageIds, startMessageId, lastMessageId);
    }

    private void consumeAllMessages(Consumer<String> consumer) throws Exception {
        Message<String> lastMessage;
        while (true){
            lastMessage = consumer.receive(2, TimeUnit.SECONDS);
            if (lastMessage == null){
                break;
            }
            consumer.acknowledge(lastMessage);
        }
    }

    @AllArgsConstructor
    private static class SendInfo {
        List<MessageId> messageIds;
        MessageIdImpl firstMessageId;
        MessageIdImpl lastMessageId;
    }

    @Test
    public void testMessagesConsumedCounterCorrect() throws Exception {
        String topicName = String.format("persistent://my-property/my-ns/%s",
                BrokerTestUtil.newUniqueName("tp_"));
        String subName1 = "sub1";
        String subName2 = "sub2";
        ProcessCoordinator.start();

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscriptionName(subName1).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName.toString())
                .enableBatching(false).create();

        CompletableFuture<SendInfo> sendFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                sendFuture.complete(sendMessages(10, producer));
            } catch (Exception e) {
                sendFuture.completeExceptionally(e);
            }
        }).start();

        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .isAckReceiptEnabled(true)
                .subscriptionName(subName2).subscribe();
        sendFuture.join();

        consumeAllMessages(consumer2);

        int backlog = 0;
        PositionImpl sub2MarkDeleted = (PositionImpl) getManagedCursor(topicName, subName2).getMarkDeletedPosition();
        for (MessageId msgId : sendFuture.join().messageIds){
            MessageIdImpl msgIdImpl = (MessageIdImpl) msgId;
            if (msgIdImpl.getLedgerId() > sub2MarkDeleted.getLedgerId()){
                backlog++;
            } else if (msgIdImpl.getLedgerId() == sub2MarkDeleted.getLedgerId()){
                if (msgIdImpl.getEntryId() > sub2MarkDeleted.getEntryId()){
                    backlog++;
                }
            }
        }

        assertEquals(getManagedCursor(topicName, subName1).getNumberOfEntriesInBacklog(false),
                sendFuture.join().messageIds.size());
        assertEquals(getManagedCursor(topicName, subName1).getNumberOfEntriesInBacklog(true),
                sendFuture.join().messageIds.size());
        assertEquals(getManagedCursor(topicName, subName2).getNumberOfEntriesInBacklog(false), backlog);
        assertEquals(getManagedCursor(topicName, subName2).getNumberOfEntriesInBacklog(true), backlog);

        assertTrue(getManagedCursor(topicName, subName2).getMessagesConsumedCounter()
                <= sendFuture.join().messageIds.size());

        // cleanup.
        producer.close();
        consumer1.close();
        consumer2.close();
        admin.topics().delete(topicName, false);
    }
}
