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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "flaky")
public class MultiTopicsReaderTest extends MockedPulsarServiceBaseTest {

    private static final String subscription = "reader-multi-topics-sub";

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        Policies policies = new Policies();
        policies.replication_clusters = Sets.newHashSet("test");
        // infinite retention
        policies.retention_policies = new RetentionPolicies(-1, -1);
        admin.namespaces().createNamespace("my-property/my-ns", policies);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testReadMessageWithoutBatching() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        testReadMessages(topic, false);
    }

    @Test(timeOut = 20000)
    public void testReadMessageWithoutBatchingWithMessageInclusive() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic-inclusive" + UUID.randomUUID();
        int topicNum = 3;
        admin.topics().createPartitionedTopic(topic, topicNum);
        Set<String> keys = publishMessages(topic, 10, false);

        Reader<byte[]> reader = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                .startMessageIdInclusive().readerName(subscription).create();
        int count = 0;
        while (reader.hasMessageAvailable()) {
            if (keys.remove(reader.readNext(5, TimeUnit.SECONDS).getKey())) {
                count++;
            }
        }
        Assert.assertEquals(count, topicNum);
        Assert.assertFalse(reader.hasMessageAvailable());
        reader.close();
    }

    @Test(timeOut = 10000)
    public void testReadMessageWithBatching() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic-with-batching" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        testReadMessages(topic, true);
    }

    @Test(timeOut = 10000)
    public void testHasMessageAvailableAsync() throws Exception {
        String topic = "persistent://my-property/my-ns/testHasMessageAvailableAsync";
        String content = "my-message-";
        int msgNum = 10;
        admin.topics().createPartitionedTopic(topic, 2);
        // stop retention from cleaning up
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic).readCompacted(true)
                .startMessageId(MessageId.earliest).create()) {
            Assert.assertFalse(reader.hasMessageAvailable());
            Assert.assertFalse(reader.hasMessageAvailableAsync().get(10, TimeUnit.SECONDS));
        }

        try (Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic).startMessageId(MessageId.earliest).create()) {
            try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create()) {
                for (int i = 0; i < msgNum; i++) {
                    producer.newMessage().key(content + i)
                            .value((content + i).getBytes(StandardCharsets.UTF_8)).send();
                }
            }
            // Should have message available
            Assert.assertTrue(reader.hasMessageAvailableAsync().get());
            try {
                // Should have message available too
                Assert.assertTrue(reader.hasMessageAvailable());
            } catch (PulsarClientException e) {
                fail("Expect success but failed.", e);
            }
            List<Message<byte[]>> msgs = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch latch = new CountDownLatch(1);
            readMessageUseAsync(reader, msgs, latch);
            latch.await();
            Assert.assertEquals(msgs.size(), msgNum);
        }
    }

    private static <T> void readMessageUseAsync(Reader<T> reader, List<Message<T>> msgs, CountDownLatch latch) {
        reader.hasMessageAvailableAsync().thenAccept(hasMessageAvailable -> {
            if (hasMessageAvailable) {
                reader.readNextAsync().whenComplete((msg, ex) -> {
                    if (ex != null) {
                        log.error("Read message failed.", ex);
                        latch.countDown();
                        return;
                    }
                    msgs.add(msg);
                    readMessageUseAsync(reader, msgs, latch);
                });
            } else {
                latch.countDown();
            }
        }).exceptionally(throwable -> {
            log.error("Read message failed.", throwable);
            latch.countDown();
            return null;
        });
    }

    @Test(timeOut = 10000)
    public void testReadMessageWithBatchingWithMessageInclusive() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic-with-batching-inclusive" + UUID.randomUUID();
        int topicNum = 3;
        int msgNum = 15;
        admin.topics().createPartitionedTopic(topic, topicNum);
        Set<String> keys = publishMessages(topic, msgNum, true);

        Reader<byte[]> reader = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                .startMessageIdInclusive().readerName(subscription).create();

        while (reader.hasMessageAvailable()) {
            keys.remove(reader.readNext(2, TimeUnit.SECONDS).getKey());
        }
        // start from latest with start message inclusive should only read the last 3 message from 3 partition
        Assert.assertEquals(keys.size(), msgNum - topicNum);
        Assert.assertFalse(keys.contains("key14"));
        Assert.assertFalse(keys.contains("key13"));
        Assert.assertFalse(keys.contains("key12"));
        Assert.assertFalse(reader.hasMessageAvailable());
        reader.close();
    }

    @Test(timeOut = 10000)
    public void testReaderWithTimeLong() throws Exception {
        String ns = "my-property/my-ns";
        String topic = "persistent://" + ns + "/testReadFromPartition" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        RetentionPolicies retention = new RetentionPolicies(-1, -1);
        admin.namespaces().setRetention(ns, retention);

        ProducerBuilder<byte[]> produceBuilder = pulsarClient.newProducer();
        produceBuilder.topic(topic);
        produceBuilder.enableBatching(false);
        Producer<byte[]> producer = produceBuilder.create();
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
            msg.send();
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

        List<MessageId> receivedMessageIds = Lists.newArrayList();

        while (reader.hasMessageAvailable()) {
            Message<byte[]> msg = reader.readNext(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            receivedMessageIds.add(msg.getMessageId());
        }

        assertEquals(receivedMessageIds.size(), totalMsg);

        restartBroker();

        assertFalse(reader.hasMessageAvailable());

        reader.close();
        producer.close();
    }

    @Test(timeOut = 10000)
    public void testRemoveSubscriptionForReaderNeedRemoveCursor() throws IOException, PulsarAdminException {

        final String topic = "persistent://my-property/my-ns/testRemoveSubscriptionForReaderNeedRemoveCursor"
                + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
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

        Assert.assertEquals(admin.topics().getSubscriptions(topic).size(), 2);
        for (PersistentTopicInternalStats value : admin.topics().getPartitionedInternalStats(topic).partitions.values()) {
            Assert.assertEquals(value.cursors.size(), 2);
        }

        reader1.close();

        Assert.assertEquals(admin.topics().getSubscriptions(topic).size(), 1);
        for (PersistentTopicInternalStats value : admin.topics().getPartitionedInternalStats(topic).partitions.values()) {
            Assert.assertEquals(value.cursors.size(), 1);
        }

        reader2.close();

        Assert.assertEquals(admin.topics().getSubscriptions(topic).size(), 0);
        for (PersistentTopicInternalStats value : admin.topics().getPartitionedInternalStats(topic).partitions.values()) {
            Assert.assertEquals(value.cursors.size(), 0);
        }

    }

    @Test(timeOut = 10000)
    public void testMultiReaderSeek() throws Exception {
        String topic = "persistent://my-property/my-ns/testKeyHashRangeReader" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        publishMessages(topic,100,false);
    }

    @Test
    public void testMultiTopicSeekByFunction() throws Exception {
        final String topicName = "persistent://my-property/my-ns/test" + UUID.randomUUID();
        int partitionNum = 4;
        int msgNum = 20;
        admin.topics().createPartitionedTopic(topicName, partitionNum);
        publishMessages(topicName, msgNum, false);
        Reader<byte[]> reader = pulsarClient
                .newReader().startMessageIdInclusive().startMessageId(MessageId.latest)
                .topic(topicName).subscriptionName("my-sub").create();
        long now = System.currentTimeMillis();
        reader.seek((topic) -> now);
        assertNull(reader.readNext(1, TimeUnit.SECONDS));

        reader.seek((topic) -> {
            TopicName name = TopicName.get(topic);
            switch (name.getPartitionIndex()) {
                case 0:
                    return MessageId.latest;
                case 1:
                    return MessageId.earliest;
                case 2:
                    return now;
                case 3:
                    return now - 999999;
                default:
                    return null;
            }
        });
        int count = 0;
        while (true) {
            Message<byte[]> message = reader.readNext(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count++;
        }
        int msgNumInPartition0 = 0;
        int msgNumInPartition1 = msgNum / partitionNum;
        int msgNumInPartition2 = 0;
        int msgNumInPartition3 = msgNum / partitionNum;

        assertEquals(count, msgNumInPartition0 + msgNumInPartition1 + msgNumInPartition2 + msgNumInPartition3);
    }

    @Test
    public void testMultiTopicSeekByFunctionWithException() throws Exception {
        final String topicName = "persistent://my-property/my-ns/test" + UUID.randomUUID();
        int partitionNum = 4;
        int msgNum = 20;
        admin.topics().createPartitionedTopic(topicName, partitionNum);
        publishMessages(topicName, msgNum, false);
        Reader<byte[]> reader = pulsarClient
                .newReader().startMessageIdInclusive().startMessageId(MessageId.latest)
                .topic(topicName).subscriptionName("my-sub").create();
        long now = System.currentTimeMillis();
        reader.seek((topic) -> now);
        assertNull(reader.readNext(1, TimeUnit.SECONDS));
        try {
            reader.seek((topic) -> {
                TopicName name = TopicName.get(topic);
                switch (name.getPartitionIndex()) {
                    case 0:
                        throw new RuntimeException("test");
                    case 1:
                        return MessageId.latest;
                    case 2:
                        return MessageId.earliest;
                    case 3:
                        return now - 999999;
                    default:
                        return null;
                }
            });
        } catch (Exception e) {
            assertEquals(e.getMessage(), "test");
            assertTrue(e instanceof RuntimeException);
        }
    }

    @Test(timeOut = 20000)
    public void testMultiTopic() throws Exception {
        final String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        final String topic2 = "persistent://my-property/my-ns/topic2" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic2, 3);
        final String topic3 = "persistent://my-property/my-ns/topic3" + UUID.randomUUID();
        List<String> topics = Arrays.asList(topic, topic2, topic3);
        PulsarClientImpl client = (PulsarClientImpl) pulsarClient;
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .startMessageId(MessageId.earliest)
                .topics(topics).readerName("my-reader").create();
        // create producer and send msg
        List<Producer<String>> producerList = new ArrayList<>();
        for (String topicName : topics) {
            producerList.add(pulsarClient.newProducer(Schema.STRING).topic(topicName).create());
        }
        int msgNum = 10;
        Set<String> messages = new HashSet<>();
        for (int i = 0; i < producerList.size(); i++) {
            Producer<String> producer = producerList.get(i);
            for (int j = 0; j < msgNum; j++) {
                String msg = i + "msg" + j;
                producer.send(msg);
                messages.add(msg);
            }
        }
        // receive messages
        while (reader.hasMessageAvailable()) {
            messages.remove(reader.readNext(5, TimeUnit.SECONDS).getValue());
        }
        assertEquals(messages.size(), 0);
        assertEquals(client.consumersCount(), 1);
        // clean up
        for (Producer<String> producer : producerList) {
            producer.close();
        }
        reader.close();
        Awaitility.await().untilAsserted(() -> assertEquals(client.consumersCount(), 0));
    }

    @Test(timeOut = 20000)
    public void testMultiNonPartitionedTopicWithStartMessageId() throws Exception {
        final String topic1 = "persistent://my-property/my-ns/topic1" + UUID.randomUUID();
        final String topic2 = "persistent://my-property/my-ns/topic2" + UUID.randomUUID();
        List<String> topics = Arrays.asList(topic1, topic2);
        PulsarClientImpl client = (PulsarClientImpl) pulsarClient;

        // create producer and send msg
        List<Producer<String>> producerList = new ArrayList<>();
        for (String topicName : topics) {
            producerList.add(pulsarClient.newProducer(Schema.STRING).topic(topicName).create());
        }
        int msgNum = 10;
        Set<String> messages = new HashSet<>();
        for (int i = 0; i < producerList.size(); i++) {
            Producer<String> producer = producerList.get(i);
            for (int j = 0; j < msgNum; j++) {
                String msg = i + "msg" + j;
                producer.send(msg);
                messages.add(msg);
            }
        }
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .startMessageId(MessageId.earliest)
                .topics(topics).readerName("my-reader").create();
        // receive messages
        while (reader.hasMessageAvailable()) {
            messages.remove(reader.readNext(5, TimeUnit.SECONDS).getValue());
        }
        assertEquals(messages.size(), 0);
        assertEquals(client.consumersCount(), 1);
        // clean up
        for (Producer<String> producer : producerList) {
            producer.close();
        }
        reader.close();
        Awaitility.await().untilAsserted(() -> assertEquals(client.consumersCount(), 0));
    }

    @Test(timeOut = 20000)
    public void testMultiNonPartitionedTopicWithRollbackDuration() throws Exception {
        final String topic1 = "persistent://my-property/my-ns/topic1" + UUID.randomUUID();
        final String topic2 = "persistent://my-property/my-ns/topic2" + UUID.randomUUID();
        List<String> topics = Arrays.asList(topic1, topic2);
        PulsarClientImpl client = (PulsarClientImpl) pulsarClient;

        // create producer and send msg
        List<Producer<String>> producerList = new ArrayList<>();
        for (String topicName : topics) {
            producerList.add(pulsarClient.newProducer(Schema.STRING).topic(topicName).create());
        }
        int totalMsg = 10;
        Set<String> messages = new HashSet<>();
        long oldMsgPublishTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(5); // 5 hours old
        long newMsgPublishTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1); // 5 hours old
        for (int i = 0; i < producerList.size(); i++) {
            Producer<String> producer = producerList.get(i);
            // (1) Publish 10 messages with publish-time 5 HOUR back
            for (int j = 0; j < totalMsg; j++) {
                TypedMessageBuilderImpl<String> msg = (TypedMessageBuilderImpl<String>) producer.newMessage()
                        .value(i + "-old-msg-" + j);
                msg.getMetadataBuilder()
                        .setPublishTime(oldMsgPublishTime)
                        .setProducerName(producer.getProducerName())
                        .setReplicatedFrom("us-west1");
                msg.send();
                messages.add(msg.getMessage().getValue());
            }
            // (2) Publish 10 messages with publish-time 1 HOUR back
            for (int j = 0; j < totalMsg; j++) {
                TypedMessageBuilderImpl<String> msg = (TypedMessageBuilderImpl<String>) producer.newMessage()
                        .value(i + "-new-msg-" + j);
                msg.getMetadataBuilder()
                        .setPublishTime(newMsgPublishTime)
                        .setProducerName(producer.getProducerName())
                        .setReplicatedFrom("us-west1");
                msg.send();
                messages.add(msg.getMessage().getValue());
            }
        }

        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .startMessageFromRollbackDuration(2, TimeUnit.HOURS)
                .topics(topics).readerName("my-reader").create();
        // receive messages
        while (reader.hasMessageAvailable()) {
            messages.remove(reader.readNext(5, TimeUnit.SECONDS).getValue());
        }
        assertEquals(messages.size(), 2 * totalMsg);
        for (String message : messages) {
            assertTrue(message.contains("old-msg"));
        }
        assertEquals(client.consumersCount(), 1);
        // clean up
        for (Producer<String> producer : producerList) {
            producer.close();
        }
        reader.close();
        Awaitility.await().untilAsserted(() -> assertEquals(client.consumersCount(), 0));
    }

    @Test(timeOut = 10000)
    public void testKeyHashRangeReader() throws Exception {
        final List<String> keys = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        final String topic = "persistent://my-property/my-ns/testKeyHashRangeReader" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);

        try {
            pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .keyHashRange(Range.of(0, 10000), Range.of(8000, 12000))
                    .create();
            fail("should failed with unexpected key hash range");
        } catch (IllegalArgumentException e) {
        }

        try {
            pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .keyHashRange(Range.of(30000, 20000))
                    .create();
            fail("should failed with unexpected key hash range");
        } catch (IllegalArgumentException e) {
        }

        try {
            pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .keyHashRange(Range.of(80000, 90000))
                    .create();
            fail("should failed with unexpected key hash range");
        } catch (IllegalArgumentException e) {
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
            assertTrue(Integer.parseInt(receivedMessage) <= StickyKeyConsumerSelector.DEFAULT_RANGE_SIZE / 2);
        }

    }

    @Test
    void shouldSupportCancellingReadNextAsync() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        MultiTopicsReaderImpl<byte[]> reader = (MultiTopicsReaderImpl<byte[]>) pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .readerName(subscription)
                .create();
        // given
        CompletableFuture<Message<byte[]>> future = reader.readNextAsync();
        Awaitility.await().untilAsserted(() -> {
            AssertJUnit.assertTrue(reader.getMultiTopicsConsumer().hasNextPendingReceive());
        });

        // when
        future.cancel(false);

        // then
        AssertJUnit.assertFalse(reader.getMultiTopicsConsumer().hasNextPendingReceive());
    }


    private void testReadMessages(String topic, boolean enableBatch) throws Exception {
        int numKeys = 9;

        Set<String> keys = publishMessages(topic, numKeys, enableBatch);
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .readerName(subscription)
                .create();

        while (reader.hasMessageAvailable()) {
            keys.remove(reader.readNext(5, TimeUnit.SECONDS).getKey());
        }
        Assert.assertEquals(keys.size(), 0);

        Reader<byte[]> readLatest = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                .readerName(subscription + "latest").create();
        Assert.assertFalse(readLatest.hasMessageAvailable());
    }

    private Set<String> publishMessages(String topic, int count, boolean enableBatch) throws Exception {
        Set<String> keys = new HashSet<>();
        ProducerBuilder<byte[]> builder = pulsarClient.newProducer();
        builder.messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        // disable periodical flushing
        builder.batchingMaxPublishDelay(1, TimeUnit.DAYS);
        builder.topic(topic);
        if (enableBatch) {
            builder.enableBatching(true);
            builder.batchingMaxMessages(count);
        } else {
            builder.enableBatching(false);
            builder.maxPendingMessages(1);
        }
        try (Producer<byte[]> producer = builder.create()) {
            List<CompletableFuture<MessageId>> list = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                String key = "key" + i;
                byte[] data = ("my-message-" + i).getBytes();
                if (enableBatch) {
                    list.add(producer.newMessage().key(key).value(data).sendAsync());
                } else {
                    producer.newMessage().key(key).value(data).send();
                }
                keys.add(key);
            }
            producer.flush();
            FutureUtil.waitForAll(list).get();
        }
        return keys;
    }

}
