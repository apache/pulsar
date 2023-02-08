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
package org.apache.pulsar.compaction;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
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

    private MessageIdImpl getLastMessageIdByTopic(String topicName) throws Exception{
        return (MessageIdImpl) pulsar.getBrokerService().getTopic(topicName, false)
                .get().get().getLastMessageId().get();
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

    private void triggerLedgerSwitch(String topicName) throws Exception{
        admin.topics().unload(topicName);
        Awaitility.await().until(() -> {
            CompletableFuture<Optional<Topic>> topicFuture =
                    pulsar.getBrokerService().getTopic(topicName, false);
            if (!topicFuture.isDone() || topicFuture.isCompletedExceptionally()){
                return false;
            }
            Optional<Topic> topicOptional = topicFuture.join();
            if (!topicOptional.isPresent()){
                return false;
            }
            PersistentTopic persistentTopic = (PersistentTopic) topicOptional.get();
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            return managedLedger.getState() == ManagedLedgerImpl.State.LedgerOpened;
        });
    }

    private void clearAllTheLedgersOutdated(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            CompletableFuture<Void> future = new CompletableFuture();
            managedLedger.trimConsumedLedgersInBackground(future);
            future.join();
            return managedLedger.getLedgersInfo().size() == 1;
        });
    }

    @Test
    public void testGetLastMessageIdWhenLedgerEmpty() throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = createConsumer(topicName, subName);
        MessageIdImpl messageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(messageId.getLedgerId(), -1);
        assertEquals(messageId.getEntryId(), -1);

        // cleanup.
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    private Producer<String> createProducer(boolean enabledBatch, String topicName) throws Exception {
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(enabledBatch);
        if (enabledBatch){
            producerBuilder.batchingMaxBytes(Integer.MAX_VALUE)
                    .batchingMaxPublishDelay(3, TimeUnit.HOURS)
                    .batchingMaxBytes(Integer.MAX_VALUE);
        }
        return producerBuilder.create();
    }

    private Consumer<String> createConsumer(String topicName, String subName) throws Exception {
        return pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .subscribe();
    }

    @Test
    public void testGetLastMessageIdWhenNoNonEmptyLedgerExists() throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        ReaderImpl<String> reader = (ReaderImpl<String>) pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .startMessageId(MessageId.earliest)
                .readCompacted(false)
                .create();

        Producer<String> producer = createProducer(false, topicName);

        producer.newMessage().key("k0").value("v0").sendAsync().get();
        reader.readNext();
        triggerLedgerSwitch(topicName);
        clearAllTheLedgersOutdated(topicName);

        MessageIdImpl messageId = (MessageIdImpl) reader.getConsumer().getLastMessageId();
        assertEquals(messageId.getLedgerId(), -1);
        assertEquals(messageId.getEntryId(), -1);

        // cleanup.
        reader.close();
        producer.close();
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
        Consumer<String> consumer = createConsumer(topicName, subName);
        Producer<String> producer = createProducer(enabledBatch, topicName);

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

        MessageIdImpl lastMessageIdExpected = getLastMessageIdByTopic(topicName);
        MessageIdImpl lastMessageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(lastMessageId.getLedgerId(), lastMessageIdExpected.getLedgerId());
        assertEquals(lastMessageId.getEntryId(), lastMessageIdExpected.getEntryId());
        if (enabledBatch){
            BatchMessageIdImpl lastBatchMessageIdByTopic = (BatchMessageIdImpl) lastMessageIdExpected;
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) lastMessageId;
            assertEquals(batchMessageId.getBatchSize(), lastBatchMessageIdByTopic.getBatchSize());
            assertEquals(batchMessageId.getBatchIndex(), lastBatchMessageIdByTopic.getBatchIndex());
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompaction(boolean enabledBatch) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = createConsumer(topicName, subName);
        Producer<String> producer = createProducer(enabledBatch, topicName);

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
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            assertEquals(batchMessageId.getBatchSize(), lastBatchMessageIdByTopic.getBatchSize());
            assertEquals(batchMessageId.getBatchIndex(), lastBatchMessageIdByTopic.getBatchIndex());
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompactionEndWithNullMsg(boolean enabledBatch) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = createConsumer(topicName, subName);
        Producer<String> producer = createProducer(enabledBatch, topicName);

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        sendFutures.add(producer.newMessage().key("k0").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v2").sendAsync());
        producer.flush();
        sendFutures.add(producer.newMessage().key("k1").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value(null).sendAsync());
        sendFutures.add(producer.newMessage().key("k2").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k2").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k2").value(null).sendAsync());
        producer.flush();
        FutureUtil.waitForAll(sendFutures).join();

        triggerCompactionAndWait(topicName);

        MessageIdImpl lastMessageIdExpected = (MessageIdImpl) sendFutures.get(2).get();
        MessageIdImpl lastMessageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(lastMessageId.getLedgerId(), lastMessageIdExpected.getLedgerId());
        assertEquals(lastMessageId.getEntryId(), lastMessageIdExpected.getEntryId());
        if (enabledBatch){
            BatchMessageIdImpl lastBatchMessageIdExpected = (BatchMessageIdImpl) lastMessageIdExpected;
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) lastMessageId;
            assertEquals(batchMessageId.getBatchSize(), lastBatchMessageIdExpected.getBatchSize());
            assertEquals(batchMessageId.getBatchIndex(), lastBatchMessageIdExpected.getBatchIndex());
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompactionEndWithNullMsg2(boolean enabledBatch) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = createConsumer(topicName, subName);
        Producer<String> producer = createProducer(enabledBatch, topicName);

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        sendFutures.add(producer.newMessage().key("k0").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value("v1").sendAsync());
        producer.flush();
        sendFutures.add(producer.newMessage().key("k1").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value("v2").sendAsync());
        sendFutures.add(producer.newMessage().key("k2").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k2").value("v1").sendAsync());
        sendFutures.add(producer.newMessage().key("k2").value(null).sendAsync());
        producer.flush();
        FutureUtil.waitForAll(sendFutures).join();

        triggerCompactionAndWait(topicName);

        MessageIdImpl lastMessageIdExpected = (MessageIdImpl) sendFutures.get(4).get();
        MessageIdImpl lastMessageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(lastMessageId.getLedgerId(), lastMessageIdExpected.getLedgerId());
        assertEquals(lastMessageId.getEntryId(), lastMessageIdExpected.getEntryId());
        if (enabledBatch){
            BatchMessageIdImpl lastBatchMessageIdExpected = (BatchMessageIdImpl) lastMessageIdExpected;
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) lastMessageId;
            assertEquals(batchMessageId.getBatchSize(), lastBatchMessageIdExpected.getBatchSize());
            assertEquals(batchMessageId.getBatchIndex(), lastBatchMessageIdExpected.getBatchIndex());
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompactionAllNullMsg(boolean enabledBatch) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        Consumer<String> consumer = createConsumer(topicName, subName);
        Producer<String> producer = createProducer(enabledBatch, topicName);

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        sendFutures.add(producer.newMessage().key("k0").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k0").value(null).sendAsync());
        producer.flush();
        sendFutures.add(producer.newMessage().key("k1").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k1").value(null).sendAsync());
        sendFutures.add(producer.newMessage().key("k2").value("v0").sendAsync());
        sendFutures.add(producer.newMessage().key("k2").value(null).sendAsync());
        producer.flush();
        FutureUtil.waitForAll(sendFutures).join();

        triggerCompactionAndWait(topicName);

        MessageIdImpl lastMessageId = (MessageIdImpl) consumer.getLastMessageId();
        assertFalse(lastMessageId instanceof BatchMessageIdImpl);
        assertEquals(lastMessageId.getLedgerId(), -1);
        assertEquals(lastMessageId.getEntryId(), -1);

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }
}
