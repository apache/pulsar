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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link org.apache.pulsar.client.impl.TableViewImpl}.
 */
@Slf4j
@Test(groups = "broker-impl")
public class TableViewTest extends MockedPulsarServiceBaseTest {

    private static final String ECDSA_PUBLIC_KEY = "src/test/resources/certificate/public-key.client-ecdsa.pem";
    private static final String ECDSA_PRIVATE_KEY = "src/test/resources/certificate/private-key.client-ecdsa.pem";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAllowAutoTopicCreation(true);
        super.internalSetup(conf);

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        // so that clients can test short names
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "topicDomain")
    public static Object[] topicDomain() {
       return new Object[]{ TopicDomain.persistent.value(), TopicDomain.non_persistent.value()};
    }

    private Set<String> publishMessages(String topic, int count, boolean enableBatch) throws Exception {
        return publishMessages(topic, 0, count, enableBatch, false);
    }

    private Set<String> publishMessages(String topic, int keyStartPosition, int count, boolean enableBatch,
                                        boolean enableEncryption) throws Exception {
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
        if (enableEncryption) {
            builder.addEncryptionKey("client-ecdsa.pem")
                .defaultCryptoKeyReader("file:./" + ECDSA_PUBLIC_KEY);
        }
        try (Producer<byte[]> producer = builder.create()) {
            CompletableFuture<?> lastFuture = null;
            for (int i = keyStartPosition; i < keyStartPosition + count; i++) {
                String key = "key"+ i;
                byte[] data = ("my-message-" + i).getBytes();
                lastFuture = producer.newMessage().key(key).value(data).sendAsync();
                keys.add(key);
            }
            producer.flush();
            lastFuture.get();
        }
        return keys;
    }

    @DataProvider(name = "partition")
    public static Object[][] partition () {
        return new Object[][] {
                { 3 }, { 0 }
        };
    }

    /**
     * Case1:
     * 1. Slow down the rate of reading messages.
     * 2. Send some messages
     * 3. Call new `refresh` API, it will wait for reading all the messages completed.
     * Case2:
     * 1. No new messages.
     * 2. Call new `refresh` API, it will be completed immediately.
     * Case3:
     * 1. multi-partition topic, p1, p2 has new message, p3 has no new messages.
     * 2. Call new `refresh` API, it will be completed after read new messages.
     */
    @Test(dataProvider = "partition")
    public void testRefreshAPI(int partition) throws Exception {
        // 1. Prepare resource.
        String topic = "persistent://public/default/testRefreshAPI" + RandomUtils.nextLong();
        if (partition == 0) {
            admin.topics().createNonPartitionedTopic(topic);
        } else {
            admin.topics().createPartitionedTopic(topic, partition);
        }

        @Cleanup
        TableView<byte[]> tv = pulsarClient.newTableView(Schema.BYTES)
                .topic(topic)
                .create();
        // Verify refresh can handle the case when the topic is empty
        tv.refreshAsync().get(3, TimeUnit.SECONDS);

        // 2. Add a listen action to provide the test environment.
        // The listen action will be triggered when there are incoming messages every time.
        // This is a sync operation, so sleep in the listen action can slow down the reading rate of messages.
        tv.listen((k, v) -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        // 3. Send 20 messages. After refresh, all the messages should be received.
        int count = 20;
        Set<String> keys = this.publishMessages(topic, count, false);
        // After message sending completely, the table view will take at least 2 seconds to receive all the messages.
        // If there is not the refresh operation, all messages will not be received.
        tv.refresh();
        // The key of each message is different.
        assertEquals(tv.size(), count);
        assertEquals(tv.keySet(), keys);
        // 4. Test refresh operation can be completed when there is a partition with on new messages
        // or no new message for no partition topic.
        if (partition > 0) {
            publishMessages(topic, partition - 1, count, false, false);
            tv.refreshAsync().get(5, TimeUnit.SECONDS);
            assertEquals(tv.size(), count + partition - 1);
        } else {
            tv.refreshAsync().get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Case1:
     * 1. Slow down the read of reading messages.
     * 2. Send some messages.
     * 3. Call new `refresh` API.
     * 4. Close the reader of the tableview.
     * 5. The refresh operation will be failed with a `AlreadyClosedException`.
     * Case2:
     * 1. Close the reader of the tableview.
     * 2. Call new `refresh` API.
     * 3. The refresh operation will be fail with a `AlreadyClosedException`.
     */
    @Test
    public void testRefreshTaskCanBeCompletedWhenReaderClosed() throws Exception {
        // 1. Prepare resource.
        String topic1 = "persistent://public/default/testRefreshTaskCanBeCompletedWhenReaderClosed-1";
        admin.topics().createNonPartitionedTopic(topic1);
        String topic2 = "persistent://public/default/testRefreshTaskCanBeCompletedWhenReaderClosed-2";
        admin.topics().createNonPartitionedTopic(topic2);
        @Cleanup
        TableView<byte[]> tv1 = pulsarClient.newTableView(Schema.BYTES)
                .topic(topic1)
                .create();
        @Cleanup
        TableView<byte[]> tv2 = pulsarClient.newTableView(Schema.BYTES)
                .topic(topic1)
                .create();
        // 2. Slow down the rate of reading messages.
        tv1.listen((k, v) -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        publishMessages(topic1, 20, false);
        AtomicBoolean completedExceptionally = new AtomicBoolean(false);
        // 3. Test failing `refresh` in the reading process.
        tv1.refreshAsync().exceptionally(ex -> {
            if (ex.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                completedExceptionally.set(true);
            }
            return null;
        });
        tv1.close();

        // 4. Test failing `refresh` when get last message IDs. The topic2 has no available messages.
        tv2.close();
        try {
            tv2.refresh();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.AlreadyClosedException);
        }
        Awaitility.await().untilAsserted(() -> assertTrue(completedExceptionally.get()));
    }

    @Test(timeOut = 30 * 1000)
    public void testTableView() throws Exception {
        String topic = "persistent://public/default/tableview-test";
        admin.topics().createPartitionedTopic(topic, 3);
        int count = 20;
        Set<String> keys = this.publishMessages(topic, count, false);
        @Cleanup
        TableView<byte[]> tv = pulsarClient.newTableViewBuilder(Schema.BYTES)
                .topic(topic)
                .autoUpdatePartitionsInterval(60, TimeUnit.SECONDS)
                .create();
        log.info("start tv size: {}", tv.size());
        tv.forEachAndListen((k, v) -> log.info("{} -> {}", k, new String(v)));
        Awaitility.await().untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count);
        });
        assertEquals(tv.keySet(), keys);
        tv.forEachAndListen((k, v) -> log.info("checkpoint {} -> {}", k, new String(v)));

        // Send more data
        Set<String> keys2 = this.publishMessages(topic, count * 2, false);
        Awaitility.await().untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count * 2);
        });
        assertEquals(tv.keySet(), keys2);
        // Test collection
        try {
            tv.keySet().clear();
            fail("Should fail here");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof UnsupportedOperationException);
        }
        try {
            tv.entrySet().clear();
            fail("Should fail here");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof UnsupportedOperationException);
        }
        try {
            tv.values().clear();
            fail("Should fail here");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof UnsupportedOperationException);
        }
    }

    @Test
    public void testNewTableView() throws Exception {
        String topic = "persistent://public/default/new-tableview-test";
        admin.topics().createPartitionedTopic(topic, 2);
        Set<String> keys = this.publishMessages(topic, 10, false);
        @Cleanup
        TableView<byte[]> tv = pulsarClient.newTableView()
                .topic(topic)
                .autoUpdatePartitionsInterval(60, TimeUnit.SECONDS)
                .create();
        tv.forEachAndListen((k, v) -> log.info("{} -> {}", k, new String(v)));
        Awaitility.await().untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), 10);
        });
        assertEquals(tv.keySet(), keys);
    }

    @Test(timeOut = 30 * 1000, dataProvider = "topicDomain")
    public void testTableViewUpdatePartitions(String topicDomain) throws Exception {
        String topic = topicDomain + "://public/default/tableview-test-update-partitions";
        admin.topics().createPartitionedTopic(topic, 3);
        int count = 20;
        // For non-persistent topic, this keys will never be received.
        Set<String> keys = this.publishMessages(topic, count, false);
        @Cleanup
        TableView<byte[]> tv = pulsarClient.newTableViewBuilder(Schema.BYTES)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();
        log.info("start tv size: {}", tv.size());
        if (topicDomain.equals(TopicDomain.non_persistent.value())) {
            keys = this.publishMessages(topic, count, false);
        }
        tv.forEachAndListen((k, v) -> log.info("{} -> {}", k, new String(v)));
        Awaitility.await().untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count);
        });
        assertEquals(tv.keySet(), keys);
        tv.forEachAndListen((k, v) -> log.info("checkpoint {} -> {}", k, new String(v)));

        admin.topics().updatePartitionedTopic(topic, 4);
        TopicName topicName = TopicName.get(topic);

        // Make sure the new partition-3 consumer already started.
        if (topic.startsWith(TopicDomain.non_persistent.toString())) {
            TimeUnit.SECONDS.sleep(6);
        }
        // Send more data to partition 3, which is not in the current TableView, need update partitions
        Set<String> keys2 =
                this.publishMessages(topicName.getPartition(3).toString(), count * 2, false);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count * 2);
        });
        assertEquals(tv.keySet(), keys2);
    }

    @Test(timeOut = 30 * 1000, dataProvider = "topicDomain")
    public void testPublishNullValue(String topicDomain) throws Exception {
        String topic = topicDomain + "://public/default/tableview-test-publish-null-value";
        admin.topics().createPartitionedTopic(topic, 3);

        final TableView<String> tv = pulsarClient.newTableViewBuilder(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();

        producer.newMessage().key("key1").value("value1").send();

        Awaitility.await().untilAsserted(() -> assertEquals(tv.get("key1"), "value1"));
        assertEquals(tv.size(), 1);

        // Try to remove key1 by publishing the tombstones message.
        producer.newMessage().key("key1").value(null).send();
        Awaitility.await().untilAsserted(() -> assertEquals(tv.size(), 0));

        producer.newMessage().key("key2").value("value2").send();
        Awaitility.await().untilAsserted(() -> assertEquals(tv.get("key2"), "value2"));
        assertEquals(tv.size(), 1);

        tv.close();

        @Cleanup
        TableView<String> tv1 = pulsarClient.newTableView(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();

        if (topicDomain.equals(TopicDomain.persistent.value())) {
            assertEquals(tv1.size(), 1);
            assertEquals(tv.get("key2"), "value2");
        } else {
            assertEquals(tv1.size(), 0);
        }
    }

    @DataProvider(name = "partitionedTopic")
    public static Object[][] partitioned() {
        return new Object[][] {{true}, {false}};
    }

    @Test(timeOut = 30 * 1000, dataProvider = "partitionedTopic")
    public void testAck(boolean partitionedTopic) throws Exception {
        String topic = null;
        if (partitionedTopic) {
            topic = "persistent://public/default/tableview-ack-test";
            admin.topics().createPartitionedTopic(topic, 3);
        } else {
            topic = "persistent://public/default/tableview-no-partition-ack-test";
            admin.topics().createNonPartitionedTopic(topic);
        }

        @Cleanup
        TableView<String> tv1 = pulsarClient.newTableViewBuilder(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();

        ConsumerBase consumerBase;
        if (partitionedTopic) {
            MultiTopicsReaderImpl<String> reader =
                    ((CompletableFuture<MultiTopicsReaderImpl<String>>) FieldUtils
                            .readDeclaredField(tv1, "reader", true)).get();
            consumerBase = spy(reader.getMultiTopicsConsumer());
            FieldUtils.writeDeclaredField(reader, "multiTopicsConsumer", consumerBase, true);
        } else {
            ReaderImpl<String> reader = ((CompletableFuture<ReaderImpl<String>>) FieldUtils
                    .readDeclaredField(tv1, "reader", true)).get();
            consumerBase = spy(reader.getConsumer());
            FieldUtils.writeDeclaredField(reader, "consumer", consumerBase, true);
        }

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();

        int msgCount = 20;
        for (int i = 0; i < msgCount; i++) {
            producer.newMessage().key("key:" + i).value("value" + i).send();
        }

        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(Duration.ofMillis(5000))
                .untilAsserted(()
                        -> verify(consumerBase, times(msgCount)).acknowledgeCumulativeAsync(any(MessageId.class)));


    }

    @Test(timeOut = 30 * 1000)
    public void testListen() throws Exception {
        String topic = "persistent://public/default/tableview-listen-test";
        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();

        for (int i = 0; i < 5; i++) {
            producer.newMessage().key("key:" + i).value("value" + i).send();
        }

        @Cleanup
        TableView<String> tv = pulsarClient.newTableViewBuilder(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();

        class MockAction implements BiConsumer<String, String> {
            int acceptedCount = 0;
            @Override
            public void accept(String s, String s2) {
                acceptedCount++;
            }
        }
        MockAction mockAction = new MockAction();
        tv.listen((k, v) -> mockAction.accept(k, v));

        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(Duration.ofMillis(5000))
                .until(() -> tv.size() == 5);

        assertEquals(mockAction.acceptedCount, 0);

        for (int i = 5; i < 10; i++) {
            producer.newMessage().key("key:" + i).value("value" + i).send();
        }

        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(Duration.ofMillis(5000))
                .until(() -> tv.size() == 10);

        assertEquals(mockAction.acceptedCount, 5);
    }

    @Test(timeOut = 30 * 1000)
    public void testTableViewWithEncryptedMessages() throws Exception {
        String topic = "persistent://public/default/tableview-encryption-test";
        admin.topics().createPartitionedTopic(topic, 3);

        // publish encrypted messages
        int count = 20;
        Set<String> keys = this.publishMessages(topic, 0, count, false, true);

        // TableView can read them using the private key
        @Cleanup
        TableView<byte[]> tv = pulsarClient.newTableViewBuilder(Schema.BYTES)
            .topic(topic)
            .autoUpdatePartitionsInterval(60, TimeUnit.SECONDS)
            .defaultCryptoKeyReader("file:" + ECDSA_PRIVATE_KEY)
            .create();
        log.info("start tv size: {}", tv.size());
        tv.forEachAndListen((k, v) -> log.info("{} -> {}", k, new String(v)));
        Awaitility.await().untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count);
        });
        assertEquals(tv.keySet(), keys);
    }

    @Test(timeOut = 30 * 1000)
    public void testTableViewTailMessageReadRetry() throws Exception {
        String topic = "persistent://public/default/tableview-is-interrupted-test";
        admin.topics().createNonPartitionedTopic(topic);
        @Cleanup
        TableView<byte[]> tv = pulsarClient.newTableView(Schema.BYTES)
                .topic(topic)
                .autoUpdatePartitionsInterval(60, TimeUnit.SECONDS)
                .create();

        // inject failure on consumer.receiveAsync()
        var reader = ((CompletableFuture<Reader<byte[]>>)
                FieldUtils.readDeclaredField(tv, "reader", true)).join();
        var consumer = spy((ConsumerImpl<byte[]>)
                FieldUtils.readDeclaredField(reader, "consumer", true));

        var errorCnt = new AtomicInteger(3);
        doAnswer(invocationOnMock -> {
            if (errorCnt.decrementAndGet() > 0) {
                return CompletableFuture.failedFuture(new RuntimeException());
            }
            // Call the real method
            reset(consumer);
            return consumer.receiveAsync();
        }).when(consumer).receiveAsync();
        FieldUtils.writeDeclaredField(reader, "consumer", consumer, true);

        int msgCnt = 2;
        this.publishMessages(topic, 0, msgCnt, false, false);
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
            assertEquals(tv.size(), msgCnt);
        });
        verify(consumer, times(msgCnt)).receiveAsync();
    }

    @Test
    public void testBuildTableViewWithMessagesAlwaysAvailable() throws Exception {
        String topic = "persistent://public/default/testBuildTableViewWithMessagesAlwaysAvailable";
        admin.topics().createPartitionedTopic(topic, 10);
        @Cleanup
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        // Prepare real data to do test.
        for (int i = 0; i < 1000; i++) {
            producer.newMessage().send();
        }
        List<TopicMessageId> lastMessageIds = reader.getLastMessageIds();

        // Use mock reader to build tableview. In the old implementation, the readAllExistingMessages method
        // will not be completed because the `mockReader.hasMessageAvailable()` always return ture.
        Reader<byte[]> mockReader = spy(reader);
        when(mockReader.hasMessageAvailable()).thenReturn(true);
        when(mockReader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(lastMessageIds));
        AtomicInteger index = new AtomicInteger(lastMessageIds.size());
        when(mockReader.readNextAsync()).thenAnswer(invocation -> {
            Message<byte[]> message = spy(Message.class);
            int localIndex = index.decrementAndGet();
            if (localIndex >= 0) {
                when(message.getTopicName()).thenReturn(lastMessageIds.get(localIndex).getOwnerTopic());
                when(message.getMessageId()).thenReturn(lastMessageIds.get(localIndex));
                when(message.hasKey()).thenReturn(false);
                doNothing().when(message).release();
            }
            return CompletableFuture.completedFuture(message);
        });
        @Cleanup
        TableViewImpl<byte[]> tableView = (TableViewImpl<byte[]>) pulsarClient.newTableView()
                .topic(topic)
                .createAsync()
                .get();
        TableViewImpl<byte[]> mockTableView = spy(tableView);
        Method readAllExistingMessagesMethod = TableViewImpl.class
                .getDeclaredMethod("readAllExistingMessages", Reader.class);
        readAllExistingMessagesMethod.setAccessible(true);
        CompletableFuture<Reader<?>> future =
                (CompletableFuture<Reader<?>>) readAllExistingMessagesMethod.invoke(mockTableView, mockReader);

        // The future will complete after receive all the messages from lastMessageIds.
        future.get(3, TimeUnit.SECONDS);
        assertTrue(index.get() <= 0);
    }
}
