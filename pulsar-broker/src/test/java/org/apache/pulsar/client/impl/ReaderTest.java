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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata.Builder;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ReaderTest extends MockedPulsarServiceBaseTest {

    private static final String subscription = "reader-sub";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                new ClusterData(pulsar.getWebServiceAddress()));
        admin.tenants().createTenant("my-property",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private Set<String> publishMessages(String topic, int count, boolean enableBatch) throws Exception {
        Set<String> keys = new HashSet<>();
        ProducerBuilder<byte[]> builder = pulsarClient.newProducer();
        builder.messageRoutingMode(MessageRoutingMode.SinglePartition);
        builder.maxPendingMessages(count);
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

    @Test
    public void testReadMessageWithBatchingWithMessageInclusive() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic-with-batching-inclusive";
        Set<String> keys = publishMessages(topic, 10, true);

        Reader<byte[]> reader = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                                            .startMessageIdInclusive().readerName(subscription).create();

        while (reader.hasMessageAvailable()) {
            Assert.assertTrue(keys.remove(reader.readNext().getKey()));
        }
        // start from latest with start message inclusive should only read the last message in batch
        Assert.assertTrue(keys.size() == 9);
        Assert.assertFalse(keys.contains("key9"));
        Assert.assertFalse(reader.hasMessageAvailable());
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
        String topic = "persistent://" + ns + "/testReadFromPartition";
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
            Builder metadataBuilder = msg.getMetadataBuilder();
            metadataBuilder.setPublishTime(oldMsgPublishTime).setSequenceId(i);
            metadataBuilder.setProducerName(producer.getProducerName()).setReplicatedFrom("us-west1");
            lastMsgId = msg.send();
        }

        // (2) Publish 10 messages with publish-time 1 HOUR back
        long newMsgPublishTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1); // 1 hour old
        MessageId firstMsgId = null;
        for (int i = 0; i < totalMsg; i++) {
            TypedMessageBuilderImpl<byte[]> msg = (TypedMessageBuilderImpl<byte[]>) producer.newMessage()
                    .value(("new" + i).getBytes());
            Builder metadataBuilder = msg.getMetadataBuilder();
            metadataBuilder.setPublishTime(newMsgPublishTime);
            metadataBuilder.setProducerName(producer.getProducerName()).setReplicatedFrom("us-west1");
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

        Assert.assertEquals(admin.topics().getStats(topic).subscriptions.size(), 2);
        Assert.assertEquals(admin.topics().getInternalStats(topic).cursors.size(), 2);

        reader1.close();

        Assert.assertEquals(admin.topics().getStats(topic).subscriptions.size(), 1);
        Assert.assertEquals(admin.topics().getInternalStats(topic).cursors.size(), 1);

        reader2.close();

        Assert.assertEquals(admin.topics().getStats(topic).subscriptions.size(), 0);
        Assert.assertEquals(admin.topics().getInternalStats(topic).cursors.size(), 0);

    }
}
