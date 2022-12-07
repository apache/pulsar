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
package org.apache.pulsar.compaction;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class GetLastMessageIdCompactedTest extends ProducerConsumerBase {

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

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // Disable the scheduled task: compaction.
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(Integer.MAX_VALUE);
        // Disable the scheduled task: retention.
        conf.setRetentionCheckIntervalInSeconds(Integer.MAX_VALUE);
    }

    private void sendManyBatchedMessages(int msgCountPerEntry, int entryCount, String topicName)
            throws Exception {
        sendManyBatchedMessages(msgCountPerEntry, entryCount, topicName, "1");
    }

    private MessageIdImpl getLastMessageIdByTopic(String topicName) throws Exception{
        return (MessageIdImpl) pulsar.getBrokerService().getTopic(topicName, false)
                .get().get().getLastMessageId().get();
    }

    private void sendManyBatchedMessages(int msgCountPerEntry, int entryCount, String topicName, String key)
            throws Exception {
        Producer<String> producer = pulsarClient.newProducer(Schema.JSON(String.class))
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(Integer.MAX_VALUE, TimeUnit.SECONDS)
                .batchingMaxMessages(Integer.MAX_VALUE)
                .create();
        for (int i = 0; i < entryCount; i++){
            for (int j = 0; j < msgCountPerEntry; j++){
                producer.newMessage().key(key).value(String.format("entry-seq[%s], batch_index[%s]", i, j)).sendAsync();
            }
            producer.flush();
        }
        producer.close();
    }

    private void triggerCompactionAndWait(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        persistentTopic.triggerCompaction();
        Awaitility.await().untilAsserted(() -> {
            PositionImpl lastConfirmPos = (PositionImpl) persistentTopic.getManagedLedger().getLastConfirmedEntry();
            PositionImpl markDeletePos = (PositionImpl) persistentTopic
                    .getSubscription(Compactor.COMPACTION_SUBSCRIPTION).getCursor().getMarkDeletedPosition();
            assertEquals(markDeletePos.getLedgerId(), lastConfirmPos.getLedgerId());
            assertEquals(markDeletePos.getEntryId(), lastConfirmPos.getEntryId());
        });
    }

    @Test
    public void testGetLastMessageIdWhenLedgerEmpty() throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .subscribe();
        MessageIdImpl messageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(messageId.getLedgerId(), -1);
        assertEquals(messageId.getEntryId(), -1);

        // cleanup.
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    @DataProvider(name = "enabledBatch")
    public Object[][] enabledBatch(){
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdBeforeCompaction(boolean enabledBatch) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(enabledBatch)
                .create();

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        sendFutures.add(producer.newMessage().key("k0").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v2").sendAsync());
        producer.flush();
        sendFutures.add(producer.newMessage().key("k1").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v2").sendAsync());
        producer.flush();
        FutureUtil.waitForAll(sendFutures).join();

        MessageIdImpl lastMessageIdByTopic = getLastMessageIdByTopic(topicName);
        MessageIdImpl messageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(messageId.getLedgerId(), lastMessageIdByTopic.getLedgerId());
        assertEquals(messageId.getEntryId(), lastMessageIdByTopic.getEntryId());
        if (enabledBatch){
            BatchMessageIdImpl lastBatchMessageIdByTopic = (BatchMessageIdImpl) getLastMessageIdByTopic(topicName);
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) consumer.getLastMessageId();
            assertEquals(batchMessageId.getBatchSize(), lastBatchMessageIdByTopic.getBatchSize());
            assertEquals(batchMessageId.getBatchIndex(), lastBatchMessageIdByTopic.getBatchIndex());
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdBeforeCompactionEndWithNullMsg(boolean enabledBatch) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(enabledBatch)
                .create();

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        sendFutures.add(producer.newMessage().key("k0").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v2").sendAsync());
        producer.flush();
        // TODO 这个问题解不了。明天开会说下吧。
        sendFutures.add(producer.newMessage().key("k1").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value(null).sendAsync());
        producer.flush();
        FutureUtil.waitForAll(sendFutures).join();

        MessageIdImpl lastMessageIdExpected = (MessageIdImpl) sendFutures.get(2).get();
        MessageIdImpl messageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(messageId.getLedgerId(), lastMessageIdExpected.getLedgerId());
        assertEquals(messageId.getEntryId(), lastMessageIdExpected.getEntryId());
        if (enabledBatch){
            BatchMessageIdImpl lastBatchMessageIdExpected = (BatchMessageIdImpl) getLastMessageIdByTopic(topicName);
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) consumer.getLastMessageId();
            assertEquals(batchMessageId.getBatchSize(), lastBatchMessageIdExpected.getBatchSize());
            assertEquals(batchMessageId.getBatchIndex(), lastBatchMessageIdExpected.getBatchIndex());
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdBeforeCompactionAllNullMsg(boolean enabledBatch) throws Exception {
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompaction(boolean enabledBatch) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(enabledBatch)
                .create();

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        sendFutures.add(producer.newMessage().key("k0").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v2").sendAsync());
        producer.flush();
        sendFutures.add(producer.newMessage().key("k1").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v2").sendAsync());
        producer.flush();
        FutureUtil.waitForAll(sendFutures).join();

        triggerCompactionAndWait(topicName);

        MessageIdImpl lastMessageIdByTopic = getLastMessageIdByTopic(topicName);
        MessageIdImpl messageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(messageId.getLedgerId(), lastMessageIdByTopic.getLedgerId());
        assertEquals(messageId.getEntryId(), lastMessageIdByTopic.getEntryId());
        if (enabledBatch){
            BatchMessageIdImpl lastBatchMessageIdByTopic = (BatchMessageIdImpl) lastMessageIdByTopic;
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) consumer.getLastMessageId();
            assertEquals(batchMessageId.getBatchSize(), lastBatchMessageIdByTopic.getBatchSize());
            assertEquals(batchMessageId.getBatchIndex(), lastBatchMessageIdByTopic.getBatchIndex());
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompactionAndEndWithNullMsg(boolean enabledBatch) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(enabledBatch)
                .create();

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        sendFutures.add(producer.newMessage().key("k0").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value(null).sendAsync());
        producer.flush();
        sendFutures.add(producer.newMessage().key("k1").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value(null).sendAsync());
        producer.flush();
        FutureUtil.waitForAll(sendFutures).join();

        triggerCompactionAndWait(topicName);

        MessageIdImpl lastMessageIdByTopic = getLastMessageIdByTopic(topicName);
        MessageIdImpl messageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(messageId.getLedgerId(), lastMessageIdByTopic.getLedgerId());
        assertEquals(messageId.getEntryId(), lastMessageIdByTopic.getEntryId());
        if (enabledBatch){
            BatchMessageIdImpl lastBatchMessageIdByTopic = (BatchMessageIdImpl) lastMessageIdByTopic;
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) consumer.getLastMessageId();
            assertEquals(batchMessageId.getBatchSize(), lastBatchMessageIdByTopic.getBatchSize());
            assertEquals(batchMessageId.getBatchIndex(), lastBatchMessageIdByTopic.getBatchIndex());
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }
}
