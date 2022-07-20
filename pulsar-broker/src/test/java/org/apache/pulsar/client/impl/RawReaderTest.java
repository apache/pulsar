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

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class RawReaderTest extends MockedPulsarServiceBaseTest {

    private static final String subscription = "foobar-sub";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private Set<String> publishMessages(String topic, int count) throws Exception {
        return publishMessages(topic, count, false);
    }

    private Set<String> publishMessages(String topic, int count, boolean batching) throws Exception {
        Set<String> keys = new HashSet<>();

        try (Producer<byte[]> producer = pulsarClient.newProducer()
            .enableBatching(batching)
            // easier to create enough batches with a small batch size
            .batchingMaxMessages(10)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .maxPendingMessages(count)
            .topic(topic)
            .create()) {
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

    public static String extractKey(RawMessage m) {
        ByteBuf headersAndPayload = m.getHeadersAndPayload();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        return msgMetadata.getPartitionKey();
    }

    @Test
    public void testHasMessageAvailableWithoutBatch() throws Exception {
        int numKeys = 10;
        String topic = "persistent://my-property/my-ns/my-raw-topic";
        Set<String> keys = publishMessages(topic, numKeys);
        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        while (true) {
            boolean hasMsg = reader.hasMessageAvailableAsync().get();
            if (hasMsg && keys.isEmpty()) {
                Assert.fail("HasMessageAvailable shows still has message when there is no message");
            }
            if (hasMsg) {
                try (RawMessage m = reader.readNextAsync().get()) {
                    Assert.assertTrue(keys.remove(extractKey(m)));
                }
            } else {
                break;
            }
        }
        Assert.assertTrue(keys.isEmpty());
    }

    @Test
    public void testHasMessageAvailableWithBatch() throws Exception {
        int numKeys = 20;
        String topic = "persistent://my-property/my-ns/my-raw-topic";
        Set<String> keys = publishMessages(topic, numKeys, true);
        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        int messageCount = 0;
        while (true) {
            boolean hasMsg = reader.hasMessageAvailableAsync().get();
            if (hasMsg && (messageCount == numKeys)) {
                Assert.fail("HasMessageAvailable shows still has message when there is no message");
            }
            if (hasMsg) {
                try (RawMessage m = reader.readNextAsync().get()) {
                    MessageMetadata meta = Commands.parseMessageMetadata(m.getHeadersAndPayload());
                    messageCount += meta.getNumMessagesInBatch();
                    RawBatchConverter.extractIdsAndKeysAndSize(m).forEach(batchInfo -> {
                        String key = batchInfo.getMiddle();
                        Assert.assertTrue(keys.remove(key));
                    });

                }
            } else {
                break;
            }
        }
        Assert.assertEquals(messageCount, numKeys);
        Assert.assertTrue(keys.isEmpty());
    }

    @Test
    public void testRawReader() throws Exception {
        int numKeys = 10;

        String topic = "persistent://my-property/my-ns/my-raw-topic";

        Set<String> keys = publishMessages(topic, numKeys);

        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();

        MessageId lastMessageId = reader.getLastMessageIdAsync().get();
        while (true) {
            try (RawMessage m = reader.readNextAsync().get()) {
                Assert.assertTrue(keys.remove(extractKey(m)));
                if (lastMessageId.compareTo(m.getMessageId()) == 0) {
                    break;
                }
            }
        }
        Assert.assertTrue(keys.isEmpty());
    }

    @Test
    public void testSeekToStart() throws Exception {
        int numKeys = 10;
        String topic = "persistent://my-property/my-ns/my-raw-topic";

        publishMessages(topic, numKeys);

        Set<String> readKeys = new HashSet<>();
        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        MessageId lastMessageId = reader.getLastMessageIdAsync().get();
        while (true) {
            try (RawMessage m = reader.readNextAsync().get()) {
                readKeys.add(extractKey(m));
                if (lastMessageId.compareTo(m.getMessageId()) == 0) {
                    break;
                }
            }
        }
        Assert.assertEquals(readKeys.size(), numKeys);

        // seek to start, read all keys again,
        // assert that we read all keys we had read previously
        reader.seekAsync(MessageId.earliest).get();
        while (true) {
            try (RawMessage m = reader.readNextAsync().get()) {
                Assert.assertTrue(readKeys.remove(extractKey(m)));
                if (lastMessageId.compareTo(m.getMessageId()) == 0) {
                    break;
                }
            }
        }
        Assert.assertTrue(readKeys.isEmpty());
    }

    @Test
    public void testSeekToMiddle() throws Exception {
        int numKeys = 10;
        String topic = "persistent://my-property/my-ns/my-raw-topic";

        publishMessages(topic, numKeys);

        Set<String> readKeys = new HashSet<>();
        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        int i = 0;
        MessageId seekTo = null;
        MessageId lastMessageId = reader.getLastMessageIdAsync().get();

        while (true) {
            try (RawMessage m = reader.readNextAsync().get()) {
                i++;
                if (i > numKeys/2) {
                    if (seekTo == null) {
                        seekTo = m.getMessageId();
                    }
                    readKeys.add(extractKey(m));
                }
                if (lastMessageId.compareTo(m.getMessageId()) == 0) {
                    break;
                }
            }
        }
        Assert.assertEquals(readKeys.size(), numKeys/2);

        // seek to middle, read all keys again,
        // assert that we read all keys we had read previously
        reader.seekAsync(seekTo).get();
        while (true) { // should break out with TimeoutException
            try (RawMessage m = reader.readNextAsync().get()) {
                Assert.assertTrue(readKeys.remove(extractKey(m)));
                if (lastMessageId.compareTo(m.getMessageId()) == 0) {
                    break;
                }
            }
        }
        Assert.assertTrue(readKeys.isEmpty());
    }

    /**
     * Try to fill the receiver queue, and drain it multiple times
     */
    @Test
    public void testFlowControl() throws Exception {
        int numMessages = RawReaderImpl.DEFAULT_RECEIVER_QUEUE_SIZE * 5;
        String topic = "persistent://my-property/my-ns/my-raw-topic";

        publishMessages(topic, numMessages);

        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        List<Future<RawMessage>> futures = new ArrayList<>();
        Set<String> keys = new HashSet<>();

        // +1 to make sure we read past the end
        for (int i = 0; i < numMessages + 1; i++) {
            futures.add(reader.readNextAsync());
        }
        int timeouts = 0;
        for (Future<RawMessage> f : futures) {
            try (RawMessage m = f.get(1, TimeUnit.SECONDS)) {
                // Assert each key is unique
                String key = extractKey(m);
                Assert.assertTrue(
                    keys.add(key),
                    "Received duplicated key '" + key + "' : already received keys = " + keys);
            } catch (TimeoutException te) {
                timeouts++;
            }
        }
        Assert.assertEquals(timeouts, 1);
        Assert.assertEquals(keys.size(), numMessages);
    }

    @Test
    public void testFlowControlBatch() throws Exception {
        int numMessages = RawReaderImpl.DEFAULT_RECEIVER_QUEUE_SIZE * 5;
        String topic = "persistent://my-property/my-ns/my-raw-topic";

        publishMessages(topic, numMessages, true);

        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        Set<String> keys = new HashSet<>();

        while (true) {
            try (RawMessage m = reader.readNextAsync().get(1, TimeUnit.SECONDS)) {
                Assert.assertTrue(RawBatchConverter.isReadableBatch(m));
                List<ImmutableTriple<MessageId, String, Integer>> batchKeys = RawBatchConverter.extractIdsAndKeysAndSize(m);
                // Assert each key is unique
                for (ImmutableTriple<MessageId, String, Integer> pair : batchKeys) {
                    String key = pair.middle;
                    Assert.assertTrue(
                            keys.add(key),
                            "Received duplicated key '" + key + "' : already received keys = " + keys);
                }
            } catch (TimeoutException te) {
                break;
            }
        }
        Assert.assertEquals(keys.size(), numMessages);
    }

    @Test
    public void testBatchingExtractKeysAndIds() throws Exception {
        String topic = "persistent://my-property/my-ns/my-raw-topic";

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
            .maxPendingMessages(3)
            .enableBatching(true)
            .batchingMaxMessages(3)
            .batchingMaxPublishDelay(1, TimeUnit.HOURS)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create()) {
            producer.newMessage().key("key1").value("my-content-1".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-content-2".getBytes()).sendAsync();
            producer.newMessage().key("key3").value("my-content-3".getBytes()).send();
        }

        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        try (RawMessage m = reader.readNextAsync().get()) {
            List<ImmutableTriple<MessageId, String, Integer>> idsAndKeys = RawBatchConverter.extractIdsAndKeysAndSize(m);

            Assert.assertEquals(idsAndKeys.size(), 3);

            // assert message ids are in correct order
            Assert.assertTrue(idsAndKeys.get(0).getLeft().compareTo(idsAndKeys.get(1).getLeft()) < 0);
            Assert.assertTrue(idsAndKeys.get(1).getLeft().compareTo(idsAndKeys.get(2).getLeft()) < 0);

            // assert keys are as expected
            Assert.assertEquals(idsAndKeys.get(0).getMiddle(), "key1");
            Assert.assertEquals(idsAndKeys.get(1).getMiddle(), "key2");
            Assert.assertEquals(idsAndKeys.get(2).getMiddle(), "key3");
        } finally {
            reader.closeAsync().get();
        }
    }

    @Test
    public void testBatchingRebatch() throws Exception {
        String topic = "persistent://my-property/my-ns/my-raw-topic";

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
            .maxPendingMessages(3)
            .enableBatching(true)
            .batchingMaxMessages(3)
            .batchingMaxPublishDelay(1, TimeUnit.HOURS)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create()) {
            producer.newMessage().key("key1").value("my-content-1".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-content-2".getBytes()).sendAsync();
            producer.newMessage().key("key3").value("my-content-3".getBytes()).send();
        }

        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        try (RawMessage m1 = reader.readNextAsync().get()) {
            RawMessage m2 = RawBatchConverter.rebatchMessage(m1, (key, id) -> key.equals("key2")).get();
            List<ImmutableTriple<MessageId, String, Integer>> idsAndKeys = RawBatchConverter.extractIdsAndKeysAndSize(m2);
            Assert.assertEquals(idsAndKeys.size(), 1);
            Assert.assertEquals(idsAndKeys.get(0).getMiddle(), "key2");
            m2.close();
            Assert.assertEquals(m1.getHeadersAndPayload().refCnt(), 1);
        } finally {
            reader.closeAsync().get();
        }
    }

    @Test
    public void testAcknowledgeWithProperties() throws Exception {
        int numKeys = 10;

        String topic = "persistent://my-property/my-ns/my-raw-topic";

        Set<String> keys = publishMessages(topic, numKeys);

        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        MessageId lastMessageId = reader.getLastMessageIdAsync().get();

        while (true) {
            try (RawMessage m = reader.readNextAsync().get()) {
                Assert.assertTrue(keys.remove(extractKey(m)));

                if (lastMessageId.compareTo(m.getMessageId()) == 0) {
                    break;
                }
            }
        }
        Assert.assertTrue(keys.isEmpty());

        Map<String,Long> properties = new HashMap<>();
        properties.put("foobar", 0xdeadbeefdecaL);
        reader.acknowledgeCumulativeAsync(lastMessageId, properties).get();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        ManagedLedger ledger = topicRef.getManagedLedger();

        Awaitility.await()
                
                .untilAsserted(() ->
                        Assert.assertEquals(
                                ledger.openCursor(subscription).getProperties().get("foobar"),
                                Long.valueOf(0xdeadbeefdecaL)));

    }

    @Test
    public void testReadCancellationOnClose() throws Exception {
        int numKeys = 10;

        String topic = "persistent://my-property/my-ns/my-raw-topic";
        publishMessages(topic, numKeys/2);

        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();
        List<Future<RawMessage>> futures = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            futures.add(reader.readNextAsync());
        }

        for (int i = 0; i < numKeys/2; i++) {
            futures.remove(0).get(); // complete successfully
        }
        reader.closeAsync().get();
        while (!futures.isEmpty()) {
            try {
                futures.remove(0).get();
                Assert.fail("Should have been cancelled");
            } catch (CancellationException ee) {
                // correct behaviour
            }
        }
    }
}
