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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class GetLastMessageIdCompactedTest extends SharedPulsarBaseTest {

    @org.testng.annotations.BeforeMethod(alwaysRun = true)
    public void disableDedup() throws Exception {
        // Disable dedup so the pulsar.dedup cursor doesn't block ledger trimming
        admin.namespaces().setDeduplicationStatus(getNamespace(), false);
    }

    private MessageIdImpl getLastMessageIdByTopic(String topicName) throws Exception{
        return (MessageIdImpl) getTopic(topicName, false)
                .get().get().getLastMessageId().get();
    }

    private void triggerCompactionAndWait(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topicName, false).get().get();
        persistentTopic.triggerCompaction();
        Awaitility.await().untilAsserted(() -> {
            Position lastConfirmPos = persistentTopic.getManagedLedger().getLastConfirmedEntry();
            Position markDeletePos = persistentTopic
                    .getSubscription(Compactor.COMPACTION_SUBSCRIPTION).getCursor().getMarkDeletedPosition();
            assertEquals(markDeletePos.getLedgerId(), lastConfirmPos.getLedgerId());
            assertEquals(markDeletePos.getEntryId(), lastConfirmPos.getEntryId());
        });
    }

    private void triggerLedgerSwitch(String topicName) throws Exception{
        admin.topics().unload(topicName);
        Awaitility.await().until(() -> {
            CompletableFuture<Optional<Topic>> topicFuture =
                    getTopic(topicName, false);
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

    @SuppressWarnings("unchecked")
    private void clearAllTheLedgersOutdated(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topicName, false).get().get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            CompletableFuture<Void> future = new CompletableFuture();
            managedLedger.trimConsumedLedgersInBackground(future);
            future.join();
            return managedLedger.getLedgersInfo().size() == 1;
        });
    }
    @SuppressWarnings("deprecation")

    @Test
    public void testGetLastMessageIdWhenLedgerEmpty() throws Exception {
        String topicName = newTopicName();
        String subName = "sub";
        Consumer<String> consumer = createConsumer(topicName, subName);
        MessageIdImpl messageId = (MessageIdImpl) consumer.getLastMessageId();
        assertEquals(messageId.getLedgerId(), -1);
        assertEquals(messageId.getEntryId(), -1);

        // cleanup.
        consumer.close();
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
    @SuppressWarnings("deprecation")

    @Test
    public void testGetLastMessageIdWhenNoNonEmptyLedgerExists() throws Exception {
        String topicName = newTopicName();
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
    }

    @DataProvider(name = "enabledBatch")
    public Object[][] enabledBatch(){
        return new Object[][]{
                {true},
                {false}
        };
    }
    @SuppressWarnings("deprecation")

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdBeforeCompaction(boolean enabledBatch) throws Exception {
        String topicName = newTopicName();
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
    }
    @SuppressWarnings("deprecation")

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompaction(boolean enabledBatch) throws Exception {
        String topicName = newTopicName();
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
    }
    @SuppressWarnings("deprecation")

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompactionWithCompression(boolean enabledBatch) throws Exception {
        String topicName = newTopicName();
        String subName = "sub";
        Consumer<String> consumer = createConsumer(topicName, subName);
        var producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .batchingMaxPublishDelay(3, TimeUnit.HOURS)
                .batchingMaxBytes(Integer.MAX_VALUE)
                .compressionType(CompressionType.ZSTD)
                .enableBatching(enabledBatch).create();

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
    }

    @Test
    public void testReadMsgsAfterDisableCompaction() throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topicPolicies().setCompactionThreshold(topicName, 1);
        admin.topics().createSubscription(topicName, "s1", MessageId.earliest);
        var producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create();
        producer.newMessage().key("k0").value("v0").send();
        producer.newMessage().key("k1").value("v1").send();
        producer.newMessage().key("k2").value("v2").send();
        triggerCompactionAndWait(topicName);
        admin.topics().deleteSubscription(topicName, "s1");

        // Disable compaction.
        admin.topicPolicies().removeCompactionThreshold(topicName);

        // Create a reader with start at earliest.
        // Verify: the reader will receive 3 messages.
        admin.topics().unload(topicName);
        Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topicName).readCompacted(true)
                .startMessageId(MessageId.earliest).create();
        producer.newMessage().key("k3").value("v3").send();
        assertTrue(reader.hasMessageAvailable());
        Message<String> m0 = reader.readNext(10, TimeUnit.SECONDS);
        assertEquals(m0.getValue(), "v0");
        assertTrue(reader.hasMessageAvailable());
        Message<String> m1 = reader.readNext(10, TimeUnit.SECONDS);
        assertEquals(m1.getValue(), "v1");
        assertTrue(reader.hasMessageAvailable());
        Message<String> m2 = reader.readNext(10, TimeUnit.SECONDS);
        assertEquals(m2.getValue(), "v2");
        assertTrue(reader.hasMessageAvailable());
        Message<String> m3 = reader.readNext(10, TimeUnit.SECONDS);
        assertEquals(m3.getValue(), "v3");

        // cleanup.
        producer.close();
        reader.close();
    }
    @SuppressWarnings("deprecation")

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompactionEndWithNullMsg(boolean enabledBatch) throws Exception {
        String topicName = newTopicName();
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
    }
    @SuppressWarnings("deprecation")

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompactionEndWithNullMsg2(boolean enabledBatch) throws Exception {
        String topicName = newTopicName();
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
    }
    @SuppressWarnings("deprecation")

    @Test(dataProvider = "enabledBatch")
    public void testGetLastMessageIdAfterCompactionAllNullMsg(boolean enabledBatch) throws Exception {
        String topicName = newTopicName();
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
    }

    @Test(dataProvider = "enabledBatch")
    public void testReaderStuckWithCompaction(boolean enabledBatch) throws Exception {
        String topicName = newTopicName();
        String subName = "sub";
        Producer<String> producer = createProducer(enabledBatch, topicName);
        producer.newMessage().key("k0").value("v0").sendAsync();
        producer.newMessage().key("k0").value("v1").sendAsync();
        producer.flush();

        triggerCompactionAndWait(topicName);
        triggerLedgerSwitch(topicName);
        clearAllTheLedgersOutdated(topicName);

        var reader = pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .startMessageId(MessageId.earliest)
                .create();
        while (reader.hasMessageAvailable()) {
            Message<String> message = reader.readNext(5, TimeUnit.SECONDS);
            assertNotEquals(message, null);
        }
    }

    @Test(timeOut = 30000)
    public void testGetLastMessageIdForEncryptedMessage() throws Exception {
        final var topic = newTopicName();
        final var ecdsaPublickeyFile = "file:./src/test/resources/certificate/public-key.client-ecdsa.pem";
        final String ecdsaPrivateKeyFile = "file:./src/test/resources/certificate/private-key.client-ecdsa.pem";
        @Cleanup final var producer = pulsarClient.newProducer(Schema.STRING).topic(topic)
                .batchingMaxBytes(Integer.MAX_VALUE)
                .batchingMaxMessages(Integer.MAX_VALUE)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .addEncryptionKey("client-ecdsa.pem")
                .defaultCryptoKeyReader(ecdsaPublickeyFile)
                .create();
        producer.newMessage().key("k0").value("v0").sendAsync();
        producer.newMessage().key("k0").value("v1").sendAsync();
        producer.newMessage().key("k1").value("v0").sendAsync();
        producer.newMessage().key("k1").value(null).sendAsync();
        producer.flush();
        triggerCompactionAndWait(topic);

        @Cleanup final var consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub")
                .readCompacted(true).defaultCryptoKeyReader(ecdsaPrivateKeyFile).subscribe();
        final var msgId = (MessageIdAdv) consumer.getLastMessageIds().get(0);
        // Compaction does not work for encrypted messages
        assertEquals(msgId.getBatchIndex(), 3);

        @Cleanup final var reader = pulsarClient.newReader(Schema.STRING).topic(topic)
                .startMessageId(MessageId.earliest).topic(topic).readCompacted(true)
                .defaultCryptoKeyReader(ecdsaPrivateKeyFile).create();
        MessageIdAdv readMsgId = (MessageIdAdv) MessageId.earliest;
        while (reader.hasMessageAvailable()) {
            final var msg = reader.readNext();
            log.info("Read key: {}, value: {}", msg.getKey(), Optional.ofNullable(msg.getValue()).orElse("(null)"));
            readMsgId = (MessageIdAdv) msg.getMessageId();
        }
        assertEquals(readMsgId, msgId);
    }
}
