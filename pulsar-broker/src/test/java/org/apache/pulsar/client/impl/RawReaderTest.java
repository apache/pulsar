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
import java.util.function.Consumer;

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;

public class RawReaderTest extends MockedPulsarServiceBaseTest {
    private static final int BATCH_MAX_MESSAGES = 10;
    private static final String subscription = "foobar-sub";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use",
                new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
        admin.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private Set<String> publishMessagesBase(String topic, int count, boolean batching) throws Exception {
        Set<String> keys = new HashSet<>();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).maxPendingMessages(count)
                .enableBatching(batching).batchingMaxMessages(BATCH_MAX_MESSAGES)
                .batchingMaxPublishDelay(Long.MAX_VALUE, TimeUnit.DAYS).create()) {
            Future<?> lastFuture = null;
            for (int i = 0; i < count; i++) {
                String key = "key"+i;
                byte[] data = ("my-message-" + i).getBytes();
                lastFuture = producer.sendAsync(MessageBuilder.create()
                                                .setKey(key)
                                                .setContent(data).build());
                keys.add(key);
            }
            lastFuture.get();
        }
        return keys;
    }

    private Set<String> publishMessages(String topic, int count) throws Exception {
        return publishMessagesBase(topic, count, false);
    }

    private Set<String> publishMessagesInBatches(String topic, int count) throws Exception {
        return publishMessagesBase(topic, count, true);
    }

    public static String extractKey(RawMessage m) throws Exception {
        ByteBuf headersAndPayload = m.getHeadersAndPayload();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        return msgMetadata.getPartitionKey();
    }

    @Test
    public void testRawReader() throws Exception {
        int numKeys = 10;

        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

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
        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

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
        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

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
        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

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
                Assert.assertTrue(keys.add(extractKey(m)));
            } catch (TimeoutException te) {
                timeouts++;
            }
        }
        Assert.assertEquals(timeouts, 1);
        Assert.assertEquals(keys.size(), numMessages);
    }

    @Test
    public void testBatching() throws Exception {
        int numMessages = BATCH_MAX_MESSAGES * 5;
        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

        Set<String> keys = publishMessagesInBatches(topic, numMessages);

        RawReader reader = RawReader.create(pulsarClient, topic, subscription).get();

        Consumer<RawMessage> consumer = new Consumer<RawMessage>() {
                BatchMessageIdImpl lastId = new BatchMessageIdImpl(-1, -1, -1, -1);

                @Override
                public void accept(RawMessage m) {
                    try {
                        Assert.assertTrue(keys.remove(extractKey(m)));
                        Assert.assertTrue(m.getMessageId() instanceof BatchMessageIdImpl);
                        BatchMessageIdImpl id = (BatchMessageIdImpl)m.getMessageId();

                        // id should be greater than lastId
                        Assert.assertEquals(id.compareTo(lastId), 1);
                    } catch (Exception e) {
                        Assert.fail("Error checking message", e);
                    }
                }
            };
        MessageId lastMessageId = reader.getLastMessageIdAsync().get();
        while (true) {
            try (RawMessage m = reader.readNextAsync().get()) {
                if (RawBatchConverter.isBatch(m)) {
                    RawBatchConverter.explodeBatch(m).forEach(consumer);
                } else {
                    consumer.accept(m);
                }
                if (lastMessageId.compareTo(m.getMessageId()) == 0) {
                    break;
                }
            }
        }
        Assert.assertTrue(keys.isEmpty());
    }

    @Test
    public void testAcknowledgeWithProperties() throws Exception {
        int numKeys = 10;

        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

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

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic);
        ManagedLedger ledger = topicRef.getManagedLedger();
        for (int i = 0; i < 30; i++) {
            if (ledger.openCursor(subscription).getProperties().get("foobar") == Long.valueOf(0xdeadbeefdecaL)) {
                break;
            }
            Thread.sleep(100);
        }
        Assert.assertEquals(ledger.openCursor(subscription).getProperties().get("foobar"),
                Long.valueOf(0xdeadbeefdecaL));
    }

    @Test
    public void testReadCancellationOnClose() throws Exception {
        int numKeys = 10;

        String topic = "persistent://my-property/use/my-ns/my-raw-topic";
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
