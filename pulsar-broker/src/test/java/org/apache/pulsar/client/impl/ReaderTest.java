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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.schema.Schemas;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class ReaderTest extends MockedPulsarServiceBaseTest {

    private static final String subscription = "reader-sub";

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private Set<String> publishMessages(String topic, int count, boolean enableBatch) throws Exception {
        Set<String> keys = new HashSet<>();
        ProducerBuilder<byte[]> builder = pulsarClient.newProducer();
        builder.messageRoutingMode(MessageRoutingMode.SinglePartition);
        builder.maxPendingMessages(count);
        // disable periodical flushing
        builder.batchingMaxPublishDelay(1, TimeUnit.DAYS);
        builder.topic(topic);
        if (enableBatch) {
            builder.enableBatching(true);
            builder.batchingMaxMessages(count);
        } else {
            builder.enableBatching(false);
        }
        try (Producer<byte[]> producer = builder.create()) {
            Future<?> lastFuture = null;
            for (int i = 0; i < count; i++) {
                String key = "key"+i;
                byte[] data = ("my-message-" + i).getBytes();
                lastFuture = producer.newMessage().key(key).value(data).sendAsync();
                keys.add(key);
            }
            producer.flush();
            lastFuture.get();
        }
        return keys;
    }

    @Test
    public void testReadMessageWithoutBatching() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic";
        testReadMessages(topic, false);
    }

    @Test
    public void testReadMessageWithoutBatchingWithMessageInclusive() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic-inclusive";
        Set<String> keys = publishMessages(topic, 10, false);

        Reader<byte[]> reader = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                                            .startMessageIdInclusive().readerName(subscription).create();

        Assert.assertTrue(reader.hasMessageAvailable());
        Assert.assertTrue(keys.remove(reader.readNext().getKey()));
        Assert.assertFalse(reader.hasMessageAvailable());
    }

    @Test
    public void testReadMessageWithBatching() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic-with-batching";
        testReadMessages(topic, true);
    }

    @DataProvider
    public static Object[][] seekBeforeHasMessageAvailable() {
        return new Object[][] { { true }, { false } };
    }

    @Test(timeOut = 20000, dataProvider = "seekBeforeHasMessageAvailable")
    public void testReadMessageWithBatchingWithMessageInclusive(boolean seekBeforeHasMessageAvailable)
            throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic-with-batching-inclusive";
        Set<String> keys = publishMessages(topic, 10, true);

        Reader<byte[]> reader = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                                            .startMessageIdInclusive().readerName(subscription).create();

        if (seekBeforeHasMessageAvailable) {
            reader.seek(0L); // it should seek to the earliest
        }

        assertTrue(reader.hasMessageAvailable());
        final Message<byte[]> msg = reader.readNext();
        assertTrue(keys.remove(msg.getKey()));
        // start from latest with start message inclusive should only read the last message in batch
        assertEquals(keys.size(), 9);

        final MessageIdAdv msgId = (MessageIdAdv) msg.getMessageId();
        if (seekBeforeHasMessageAvailable) {
            assertEquals(msgId.getBatchIndex(), 0);
            assertFalse(keys.contains("key0"));
            assertTrue(reader.hasMessageAvailable());
        } else {
            assertEquals(msgId.getBatchIndex(), 9);
            assertFalse(reader.hasMessageAvailable());
            assertFalse(keys.contains("key9"));
            assertFalse(reader.hasMessageAvailable());
        }
    }

    private void testReadMessages(String topic, boolean enableBatch) throws Exception {
        int numKeys = 10;

        Set<String> keys = publishMessages(topic, numKeys, enableBatch);
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .readerName(subscription)
                .create();

        while (reader.hasMessageAvailable()) {
            Message<byte[]> message = reader.readNext();
            Assert.assertTrue(keys.remove(message.getKey()));
        }
        Assert.assertTrue(keys.isEmpty());

        Reader<byte[]> readLatest = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                                                .readerName(subscription + "latest").create();
        Assert.assertFalse(readLatest.hasMessageAvailable());
    }

    @Test
    public void testMultiTopicSeekByFunction() throws Exception {
        final String topicName = "persistent://my-property/my-ns/test" + UUID.randomUUID();
        int msgNum = 10;
        publishMessages(topicName, msgNum, false);
        Reader<byte[]> reader = pulsarClient
                .newReader().startMessageIdInclusive().startMessageId(MessageId.latest)
                .topic(topicName).subscriptionName("my-sub").create();
        long now = System.currentTimeMillis();
        reader.seek((topic) -> now);
        assertNull(reader.readNext(1, TimeUnit.SECONDS));
        // seek by time
        reader.seek((topic) -> {
            assertFalse(TopicName.get(topic).isPartitioned());
            return now - 999999;
        });
        int count = 0;
        while (true) {
            Message<byte[]> message = reader.readNext(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count++;
        }
        assertEquals(count, msgNum);
        // seek by msg id
        reader.seek((topic) -> {
            assertFalse(TopicName.get(topic).isPartitioned());
            return MessageId.earliest;
        });
        count = 0;
        while (true) {
            Message<byte[]> message = reader.readNext(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count++;
        }
        assertEquals(count, msgNum);
    }

    @Test
    public void testReadFromPartition() throws Exception {
        String topic = "persistent://my-property/my-ns/testReadFromPartition";
        String partition0 = topic + "-partition-0";
        admin.topics().createPartitionedTopic(topic, 4);
        int numKeys = 10;

        Set<String> keys = publishMessages(partition0, numKeys, false);
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(partition0)
                .startMessageId(MessageId.earliest)
                .create();

        while (reader.hasMessageAvailable()) {
            Message<byte[]> message = reader.readNext();
            Assert.assertTrue(keys.remove(message.getKey()));
        }
        Assert.assertTrue(keys.isEmpty());
    }

    /**
     * It verifies that reader can set initial position based on provided rollback time.
     * <pre>
     * 1. publish messages which are 5 hour old
     * 2. publish messages which are 1 hour old
     * 3. Create reader with rollback time 2 hours
     * 4. Reader should be able to read only messages which are only 2 hours old
     * </pre>
     * @throws Exception
     */
    @Test
    public void testReaderWithTimeLong() throws Exception {
        String ns = "my-property/my-ns";
        String topic = "persistent://" + ns + "/testReaderWithTimeLong";
        RetentionPolicies retention = new RetentionPolicies(-1, -1);
        admin.namespaces().setRetention(ns, retention);

        ProducerBuilder<byte[]> produceBuilder = pulsarClient.newProducer();
        produceBuilder.topic(topic);
        produceBuilder.enableBatching(false);
        Producer<byte[]> producer = produceBuilder.create();
        MessageId lastMsgId = null;
        int totalMsg = 10;
        // (1) Publish 10 messages with publish-time 5 HOUR back
        long oldMsgPublishTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(5); // 5 hours old
        for (int i = 0; i < totalMsg; i++) {
            TypedMessageBuilderImpl<byte[]> msg = (TypedMessageBuilderImpl<byte[]>) producer.newMessage()
                    .value(("old" + i).getBytes());
            msg.getMetadataBuilder()
                .setPublishTime(oldMsgPublishTime)
                .setSequenceId(i)
                .setProducerName(producer.getProducerName())
                .setReplicatedFrom("us-west1");
            lastMsgId = msg.send();
        }

        // (2) Publish 10 messages with publish-time 1 HOUR back
        long newMsgPublishTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1); // 1 hour old
        MessageId firstMsgId = null;
        for (int i = 0; i < totalMsg; i++) {
            TypedMessageBuilderImpl<byte[]> msg = (TypedMessageBuilderImpl<byte[]>) producer.newMessage()
                    .value(("new" + i).getBytes());
            msg.getMetadataBuilder()
                    .setPublishTime(newMsgPublishTime)
                    .setProducerName(producer.getProducerName())
                    .setReplicatedFrom("us-west1");
            MessageId msgId = msg.send();
            if (firstMsgId == null) {
                firstMsgId = msgId;
            }
        }

        // (3) Create reader and set position 1 hour back so, it should only read messages which are 2 hours old which
        // published on step 2
        Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageFromRollbackDuration(2, TimeUnit.HOURS).create();

        List<MessageId> receivedMessageIds = new ArrayList<>();

        while (reader.hasMessageAvailable()) {
            Message<byte[]> msg = reader.readNext(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            System.out.println("msg.getMessageId()=" + msg.getMessageId() + ", data=" + (new String(msg.getData())));
            receivedMessageIds.add(msg.getMessageId());
        }

        assertEquals(receivedMessageIds.size(), totalMsg);
        assertEquals(receivedMessageIds.get(0), firstMsgId);

        restartBroker();

        assertFalse(reader.hasMessageAvailable());
    }

    /**
     * We need to ensure that delete subscription of read also need to delete the
     * non-durable cursor, because data deletion depends on the mark delete position of all cursors.
     */
    @Test
    public void testRemoveSubscriptionForReaderNeedRemoveCursor() throws IOException, PulsarAdminException {

        final String topic = "persistent://my-property/my-ns/testRemoveSubscriptionForReaderNeedRemoveCursor";

        @Cleanup
        Reader<byte[]> reader1 = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
            .create();

        @Cleanup
        Reader<byte[]> reader2 = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
            .create();

        Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions().size(), 2);
        Assert.assertEquals(admin.topics().getInternalStats(topic, false).cursors.size(), 2);

        reader1.close();

        Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions().size(), 1);
        Assert.assertEquals(admin.topics().getInternalStats(topic, false).cursors.size(), 1);

        reader2.close();

        Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions().size(), 0);
        Assert.assertEquals(admin.topics().getInternalStats(topic, false).cursors.size(), 0);

    }

    @Test
    public void testReaderHasMessageAvailable() throws Exception {
        final String topic = "persistent://my-property/my-ns/testReaderHasMessageAvailable" + System.currentTimeMillis();
        @Cleanup
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .startMessageId(MessageId.latest)
                .startMessageIdInclusive()
                .create();
        assertFalse(reader.hasMessageAvailable());
    }

    @Test
    public void testKeyHashRangeReader() throws IOException {
         final List<String> keys = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        final String topic = "persistent://my-property/my-ns/testKeyHashRangeReader";

        try {
            pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .keyHashRange(Range.of(0, 10000), Range.of(8000, 12000))
                    .create();
            fail("should failed with unexpected key hash range");
        } catch (IllegalArgumentException e) {
            log.error("Create key hash range failed", e);
        }

        try {
            pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .keyHashRange(Range.of(30000, 20000))
                    .create();
            fail("should failed with unexpected key hash range");
        } catch (IllegalArgumentException e) {
            log.error("Create key hash range failed", e);
        }

        try {
            pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .keyHashRange(Range.of(80000, 90000))
                    .create();
            fail("should failed with unexpected key hash range");
        } catch (IllegalArgumentException e) {
            log.error("Create key hash range failed", e);
        }

        @Cleanup
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .keyHashRange(Range.of(0, StickyKeyConsumerSelector.DEFAULT_RANGE_SIZE / 2))
                .create();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();

        int expectedMessages = 0;
        for (String key : keys) {
            int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes())
                    % StickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
            if (slot <= StickyKeyConsumerSelector.DEFAULT_RANGE_SIZE / 2) {
                expectedMessages++;
            }
            producer.newMessage()
                    .key(key)
                    .value(key)
                    .send();
            log.info("Publish message to slot {}", slot);
        }

        List<String> receivedMessages = new ArrayList<>();

        Message<String> msg;
        do {
            msg = reader.readNext(1, TimeUnit.SECONDS);
            if (msg != null) {
                receivedMessages.add(msg.getValue());
            }
        } while (msg != null);

        assertTrue(expectedMessages > 0);
        assertEquals(receivedMessages.size(), expectedMessages);
        for (String receivedMessage : receivedMessages) {
            log.info("Receive message {}", receivedMessage);
            assertTrue(Integer.parseInt(receivedMessage) <= StickyKeyConsumerSelector.DEFAULT_RANGE_SIZE / 2);
        }

    }

    @Test
    public void testReaderSubName() throws Exception {
        doTestReaderSubName(true);
        doTestReaderSubName(false);
    }

    private void doTestReaderSubName(boolean setPrefix) throws Exception {
        final String topic = "persistent://my-property/my-ns/testReaderSubName" + System.currentTimeMillis();
        final String subName = "my-sub-name";

        ReaderBuilder<String> builder = pulsarClient.newReader(Schema.STRING)
                .subscriptionName(subName)
                .topic(topic)
                .startMessageId(MessageId.earliest);
        if (setPrefix) {
            builder = builder.subscriptionRolePrefix(subName + System.currentTimeMillis());
        }
        Reader<String> reader = builder.create();
        ReaderImpl<String> readerImpl = (ReaderImpl<String>) reader;
        assertEquals(readerImpl.getConsumer().getSubscription(), subName);
        reader.close();

        final String topic2 = "persistent://my-property/my-ns/testReaderSubName2" + System.currentTimeMillis();
        admin.topics().createPartitionedTopic(topic2, 3);
        builder = pulsarClient.newReader(Schema.STRING)
                .subscriptionName(subName)
                .topic(topic2)
                .startMessageId(MessageId.earliest);
        if (setPrefix) {
            builder = builder.subscriptionRolePrefix(subName + System.currentTimeMillis());
        }
        reader = builder.create();
        MultiTopicsReaderImpl<String> multiTopicsReader = (MultiTopicsReaderImpl<String>) reader;
        multiTopicsReader.getMultiTopicsConsumer().getConsumers()
                .forEach(consumerImpl -> assertEquals(consumerImpl.getSubscription(), subName));
        multiTopicsReader.close();
    }

    @Test
    public void testSameSubName() throws Exception {
        final String topic = "persistent://my-property/my-ns/testSameSubName";
        final String subName = "my-sub-name";

        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .subscriptionName(subName)
                .topic(topic)
                .startMessageId(MessageId.earliest).create();
        //We can not create a new reader with the same subscription name
        try (Reader<String> ignored = pulsarClient.newReader(Schema.STRING)
                .subscriptionName(subName)
                .topic(topic)
                .startMessageId(MessageId.earliest).create()) {
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e instanceof PulsarClientException.ConsumerBusyException);
            assertTrue(e.getMessage().contains("Exclusive consumer is already connected"));
        }
        //It is possible to create a new reader with the same subscription name after closing the first reader
        reader.close();
        pulsarClient.newReader(Schema.STRING)
                .subscriptionName(subName)
                .topic(topic)
                .startMessageId(MessageId.earliest).create().close();

    }

    @Test(timeOut = 30000)
    public void testAvoidUsingIoThreadToGetValueOfMessage() throws Exception {
        final String topic = "persistent://my-property/my-ns/testAvoidUsingIoThreadToGetValueOfMessage";

        @Cleanup
        Producer<Schemas.PersonOne> producer = pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic)
                .create();

        producer.send(new Schemas.PersonOne(1));

        @Cleanup
        Reader<Schemas.PersonOne> reader = pulsarClient.newReader(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create();

        CountDownLatch latch = new CountDownLatch(1);
        List<Schemas.PersonOne> received = new ArrayList<>(1);
        // Make sure the message is added to the incoming queue
        Awaitility.await().untilAsserted(() ->
                assertTrue(((ReaderImpl<?>) reader).getConsumer().incomingMessages.size() > 0));
        reader.hasMessageAvailableAsync().whenComplete((has, e) -> {
            if (e == null && has) {
                CompletableFuture<Message<Schemas.PersonOne>> future = reader.readNextAsync();
                // Make sure the future completed
                Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).untilAsserted(future::isDone);
                future.whenComplete((msg, ex) -> {
                    if (ex == null) {
                        received.add(msg.getValue());
                    }
                    latch.countDown();
                });
            } else {
                latch.countDown();
            }
        });
        latch.await();
        Assert.assertEquals(received.size(), 1);
    }

    @Test(timeOut = 1000 * 10)
    public void removeNonPersistentTopicReaderTest() throws Exception {
        final String topic = "non-persistent://my-property/my-ns/non-topic";

        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create();
        Reader<byte[]> reader2 = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create();

        Awaitility.await()
                .pollDelay(3, TimeUnit.SECONDS)
                .until(() -> {
                    TopicStats topicStats = admin.topics().getStats(topic);
                    System.out.println("subscriptions size: " + topicStats.getSubscriptions().size());
                    return topicStats.getSubscriptions().size() == 2;
                });

        reader.close();
        reader2.close();

        Awaitility.await().until(() -> {
            TopicStats topicStats = admin.topics().getStats(topic);
            System.out.println("subscriptions size: " + topicStats.getSubscriptions().size());
            return topicStats.getSubscriptions().size() == 0;
        });

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        consumer.close();

        Awaitility.await()
                .pollDelay(3, TimeUnit.SECONDS)
                .until(() -> {
            TopicStats topicStats = admin.topics().getStats(topic);
            System.out.println("subscriptions size: " + topicStats.getSubscriptions().size());
            return topicStats.getSubscriptions().size() == 1;
        });
    }

    @Test
    public void testReaderCursorStatsCorrect() throws Exception {
        final String readerNotAckTopic = "persistent://my-property/my-ns/testReaderCursorStatsCorrect";
        @Cleanup
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(readerNotAckTopic)
                .startMessageId(MessageId.earliest)
                .create();
        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(readerNotAckTopic);
        Assert.assertEquals(internalStats.cursors.size(), 1);
        String key = new ArrayList<>(internalStats.cursors.keySet()).get(0);
        ManagedLedgerInternalStats.CursorStats cursor = internalStats.cursors.get(key);
        Assert.assertEquals(cursor.state, "Open");
        reader.close();
        internalStats = admin.topics().getInternalStats(readerNotAckTopic);
        Assert.assertEquals(internalStats.cursors.size(), 0);
    }

    @Test
    public void testReaderListenerAcknowledgement()
            throws IOException, InterruptedException, PulsarAdminException {
        // non-partitioned topic
        final String topic = "persistent://my-property/my-ns/" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        final Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        producer.send("0".getBytes(StandardCharsets.UTF_8));
        // non-pool
        final CountDownLatch readerNonPoolLatch = new CountDownLatch(1);
        final Reader<byte[]> readerNonPool = pulsarClient.newReader()
                .topic(topic)
                .subscriptionName("reader-non-pool")
                .startMessageId(MessageId.earliest)
                .readerListener((innerReader, message) -> {
                    // no operation
                    readerNonPoolLatch.countDown();
                }).create();
        readerNonPoolLatch.await();
        Awaitility.await().untilAsserted(() -> {
            final PersistentTopicInternalStats internal = admin.topics().getInternalStats(topic);
            final String lastConfirmedEntry = internal.lastConfirmedEntry;
            Assert.assertTrue(internal.cursors.containsKey("reader-non-pool"));
            Assert.assertEquals(internal.cursors.get("reader-non-pool").markDeletePosition, lastConfirmedEntry);
        });
        // pooled
        final CountDownLatch readerPooledLatch = new CountDownLatch(1);
        final Reader<byte[]> readerPooled = pulsarClient.newReader()
                .topic(topic)
                .subscriptionName("reader-pooled")
                .startMessageId(MessageId.earliest)
                .poolMessages(true)
                .readerListener((innerReader, message) -> {
                    try {
                        // no operation
                        readerPooledLatch.countDown();
                    } finally {
                        message.release();
                    }
                }).create();
        readerPooledLatch.await();
        Awaitility.await().untilAsserted(() -> {
            final PersistentTopicInternalStats internal = admin.topics().getInternalStats(topic);
            final String lastConfirmedEntry = internal.lastConfirmedEntry;
            Assert.assertTrue(internal.cursors.containsKey("reader-pooled"));
            Assert.assertEquals(internal.cursors.get("reader-pooled").markDeletePosition, lastConfirmedEntry);
        });
        producer.close();
        readerNonPool.close();
        readerPooled.close();
        admin.topics().delete(topic);
        // ---- partitioned topic
        final String partitionedTopic = "persistent://my-property/my-ns/" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(partitionedTopic, 2);
        final Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(partitionedTopic)
                .create();
        producer2.send("0".getBytes(StandardCharsets.UTF_8));
        // non-pool
        final CountDownLatch readerNonPoolLatch2 = new CountDownLatch(1);
        final Reader<byte[]> readerNonPool2 = pulsarClient.newReader()
                .topic(partitionedTopic)
                .subscriptionName("reader-non-pool")
                .startMessageId(MessageId.earliest)
                .readerListener((innerReader, message) -> {
                    // no operation
                    readerNonPoolLatch2.countDown();
                }).create();
        readerNonPoolLatch2.await();
        Awaitility.await().untilAsserted(() -> {
            PartitionedTopicInternalStats partitionedInternal =
                    admin.topics().getPartitionedInternalStats(partitionedTopic);
            for (PersistentTopicInternalStats internal : partitionedInternal.partitions.values()) {
                final String lastConfirmedEntry = internal.lastConfirmedEntry;
                Assert.assertTrue(internal.cursors.containsKey("reader-non-pool"));
                Assert.assertEquals(internal.cursors.get("reader-non-pool").markDeletePosition, lastConfirmedEntry);
            }
        });
        // pooled
        final CountDownLatch readerPooledLatch2 = new CountDownLatch(1);
        final Reader<byte[]> readerPooled2 = pulsarClient.newReader()
                .topic(partitionedTopic)
                .subscriptionName("reader-pooled")
                .startMessageId(MessageId.earliest)
                .poolMessages(true)
                .readerListener((innerReader, message) -> {
                    try {
                        // no operation
                        readerPooledLatch2.countDown();
                    } finally {
                        message.release();
                    }
                }).create();
        readerPooledLatch2.await();
        Awaitility.await().untilAsserted(() -> {
            PartitionedTopicInternalStats partitionedInternal =
                    admin.topics().getPartitionedInternalStats(partitionedTopic);
            for (PersistentTopicInternalStats internal : partitionedInternal.partitions.values()) {
                final String lastConfirmedEntry = internal.lastConfirmedEntry;
                Assert.assertTrue(internal.cursors.containsKey("reader-pooled"));
                Assert.assertEquals(internal.cursors.get("reader-pooled").markDeletePosition, lastConfirmedEntry);
            }
        });
        producer2.close();
        readerNonPool2.close();
        readerPooled2.close();
        admin.topics().deletePartitionedTopic(partitionedTopic);
    }

    @Test
    public void testReaderReconnectedFromNextEntry() throws Exception {
        final String topic = "persistent://my-property/my-ns/testReaderReconnectedFromNextEntry";
        Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topic).receiverQueueSize(1)
                .startMessageId(MessageId.earliest).create();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();

        // Send 3 and consume 1.
        producer.send("1");
        producer.send("2");
        producer.send("3");
        Message<String> msg1 = reader.readNext(2, TimeUnit.SECONDS);
        assertEquals(msg1.getValue(), "1");

        // Trigger reader reconnect.
        admin.topics().unload(topic);

        // For non-durable we are going to restart from the next entry.
        Message<String> msg2 = reader.readNext(2, TimeUnit.SECONDS);
        assertEquals(msg2.getValue(), "2");
        Message<String> msg3 = reader.readNext(2, TimeUnit.SECONDS);
        assertEquals(msg3.getValue(), "3");

        // cleanup.
        reader.close();
        producer.close();
        admin.topics().delete(topic, false);
    }

    @DataProvider
    public static Object[][] initializeLastMessageIdInBroker() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "initializeLastMessageIdInBroker")
    public void testHasMessageAvailableAfterSeek(boolean initializeLastMessageIdInBroker) throws Exception {
        final String topic = "persistent://my-property/my-ns/test-has-message-available-after-seek";
        @Cleanup Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topic).receiverQueueSize(1)
                .startMessageId(MessageId.earliest).create();

        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producer.send("msg");

        if (initializeLastMessageIdInBroker) {
            assertTrue(reader.hasMessageAvailable());
        } // else: lastMessageIdInBroker is earliest

        reader.seek(MessageId.latest);
        // lastMessageIdInBroker is the last message ID, while startMessageId is still earliest
        assertFalse(reader.hasMessageAvailable());

        producer.send("msg");
        assertTrue(reader.hasMessageAvailable());
    }

    @Test(dataProvider = "initializeLastMessageIdInBroker")
    public void testHasMessageAvailableAfterSeekTimestamp(boolean initializeLastMessageIdInBroker) throws Exception {
        final String topic = "persistent://my-property/my-ns/test-has-message-available-after-seek-timestamp";

        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        final long timestampBeforeSend = System.currentTimeMillis();
        final MessageId sentMsgId = producer.send("msg");

        final List<MessageId> messageIds = new ArrayList<>();
        messageIds.add(MessageId.earliest);
        messageIds.add(sentMsgId);
        messageIds.add(MessageId.latest);

        for (MessageId messageId : messageIds) {
            @Cleanup Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topic).receiverQueueSize(1)
                    .startMessageId(messageId).create();
            if (initializeLastMessageIdInBroker) {
                if (messageId == MessageId.earliest) {
                    assertTrue(reader.hasMessageAvailable());
                } else {
                    assertFalse(reader.hasMessageAvailable());
                }
            } // else: lastMessageIdInBroker is earliest
            reader.seek(System.currentTimeMillis());
            assertFalse(reader.hasMessageAvailable());
        }

        for (MessageId messageId : messageIds) {
            @Cleanup Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topic).receiverQueueSize(1)
                    .startMessageId(messageId).create();
            if (initializeLastMessageIdInBroker) {
                if (messageId == MessageId.earliest) {
                    assertTrue(reader.hasMessageAvailable());
                } else {
                    assertFalse(reader.hasMessageAvailable());
                }
            } // else: lastMessageIdInBroker is earliest
            reader.seek(timestampBeforeSend);
            assertTrue(reader.hasMessageAvailable());
        }
    }

    @Test
    public void testHasMessageAvailableAfterSeekTimestampWithMessageIdInclusive() throws Exception {
        final String topic = "persistent://my-property/my-ns/" +
                "testHasMessageAvailableAfterSeekTimestampWithMessageInclusive";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        final long timestampBeforeSend = System.currentTimeMillis();
        final MessageId sentMsgId = producer.send("msg");

        final List<MessageId> messageIds = new ArrayList<>();
        messageIds.add(MessageId.earliest);
        messageIds.add(sentMsgId);
        messageIds.add(MessageId.latest);

        for (MessageId messageId : messageIds) {
            @Cleanup
            Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topic).receiverQueueSize(1)
                    .startMessageIdInclusive()
                    .startMessageId(messageId).create();
            assertTrue(reader.hasMessageAvailable());

            reader.seek(System.currentTimeMillis());
            assertFalse(reader.hasMessageAvailable());
            Message<String> message = reader.readNext(10, TimeUnit.SECONDS);
            assertNull(message);
        }

        for (MessageId messageId : messageIds) {
            @Cleanup
            Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topic).receiverQueueSize(1)
                    .startMessageIdInclusive()
                    .startMessageId(messageId).create();
            assertTrue(reader.hasMessageAvailable());

            reader.seek(timestampBeforeSend);
            assertTrue(reader.hasMessageAvailable());
        }
    }

    @Test
    public void testReaderBuilderStateOnRetryFailure() throws Exception {
        String ns = "my-property/my-ns";
        String topic = "persistent://" + ns + "/testRetryReader";
        RetentionPolicies retention = new RetentionPolicies(-1, -1);
        admin.namespaces().setRetention(ns, retention);
        String badUrl = "pulsar://bad-host:8080";

        PulsarClient client = PulsarClient.builder().serviceUrl(badUrl).build();

        ReaderBuilder<byte[]> readerBuilder = client.newReader().topic(topic).startMessageFromRollbackDuration(100,
                TimeUnit.SECONDS);

        for (int i = 0; i < 3; i++) {
            try {
                readerBuilder.createAsync().get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                log.info("It should time out due to invalid url");
            } catch (IllegalArgumentException e) {
                fail("It should not fail with corrupt reader state");
            }
        }
    }
}
