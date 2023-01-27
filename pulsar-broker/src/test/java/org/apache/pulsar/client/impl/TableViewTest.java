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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
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
        return publishMessages(topic, count, enableBatch, false);
    }

    private Set<String> publishMessages(String topic, int count, boolean enableBatch, boolean enableEncryption) throws Exception {
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
            for (int i = 0; i < count; i++) {
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
        Set<String> keys = this.publishMessages(topic, count, false, true);

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
}
