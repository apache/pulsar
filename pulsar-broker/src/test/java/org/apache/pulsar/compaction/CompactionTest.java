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

import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.apache.pulsar.broker.BrokerTestUtil.spyWithoutRecordingInvocations;
import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.SystemTopic;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
@Slf4j
public class CompactionTest extends MockedPulsarServiceBaseTest {
    protected ScheduledExecutorService compactionScheduler;
    protected BookKeeper bk;
    private TwoPhaseCompactor compactor;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster(configClusterName,
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());

        admin.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Set.of(configClusterName)));
        admin.namespaces().createNamespace("my-tenant/my-ns");

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compaction-%d").setDaemon(true).build());
        bk = pulsar.getBookKeeperClientFactory().create(this.conf, null, null, Optional.empty(), null).get();
        compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        bk.close();
        if (compactionScheduler != null) {
            compactionScheduler.shutdownNow();
        }
    }

    protected long compact(String topic) throws ExecutionException, InterruptedException {
        return compactor.compact(topic).get();
    }


    protected long compact(String topic, CryptoKeyReader cryptoKeyReader)
            throws ExecutionException, InterruptedException {
        return compactor.compact(topic).get();
    }

    protected TwoPhaseCompactor getCompactor() {
        return compactor;
    }

    @Test
    public void testCompaction() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";
        final int numMessages = 20;
        final int maxKeys = 10;

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        Map<String, byte[]> expected = new HashMap<>();
        List<Pair<String, byte[]>> all = new ArrayList<>();
        Random r = new Random(0);

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.newMessage().key(key).value(data).send();
            expected.put(key, data);
            all.add(Pair.of(key, data));
        }

        compact(topic);

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topic, false);
        // Compacted topic ledger should have same number of entry equals to number of unique key.
        assertEquals(expected.size(), internalStats.compactedLedger.entries);
        assertTrue(internalStats.compactedLedger.ledgerId > -1);
        assertFalse(internalStats.compactedLedger.offloaded);

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            while (true) {
                Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
                assertEquals(expected.remove(m.getKey()), m.getData());
                if (expected.isEmpty()) {
                    break;
                }
            }
            assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            while (true) {
                Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
                Pair<String, byte[]> expectedMessage = all.remove(0);
                assertEquals(expectedMessage.getLeft(), m.getKey());
                assertEquals(expectedMessage.getRight(), m.getData());
                if (all.isEmpty()) {
                    break;
                }
            }
            assertTrue(all.isEmpty());
        }
    }

    @Test
    public void testCompactionWithReader() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";
        final int numMessages = 20;
        final int maxKeys = 10;

        // Configure retention to ensue data is retained for reader
        admin.namespaces().setRetention("my-tenant/my-ns", new RetentionPolicies(-1, -1));

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        Map<String, String> expected = new HashMap<>();
        List<Pair<String, String>> all = new ArrayList<>();
        Random r = new Random(0);

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            String value = "my-message-" + key + "-" + j;
            producer.newMessage().key(key).value(value.getBytes()).send();
            expected.put(key, value);
            all.add(Pair.of(key, value));
        }

        compact(topic);


        // consumer with readCompacted enabled only get compacted entries
        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic).readCompacted(true)
                .startMessageId(MessageId.earliest).create()) {
            while (true) {
                Message<byte[]> m = reader.readNext(2, TimeUnit.SECONDS);
                assertEquals(expected.remove(m.getKey()), new String(m.getData()));
                if (expected.isEmpty()) {
                    break;
                }
            }
            assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic).readCompacted(false)
                .startMessageId(MessageId.earliest).create()) {
            while (true) {
                Message<byte[]> m = reader.readNext(2, TimeUnit.SECONDS);
                Pair<String, String> expectedMessage = all.remove(0);
                assertEquals(expectedMessage.getLeft(), m.getKey());
                assertEquals(expectedMessage.getRight(), new String(m.getData()));
                if (all.isEmpty()) {
                    break;
                }
            }
            assertTrue(all.isEmpty());
        }
    }


    @Test
    public void testReadCompactedBeforeCompaction() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value("content0".getBytes()).send();
        producer.newMessage().key("key0").value("content1".getBytes()).send();
        producer.newMessage().key("key0").value("content2".getBytes()).send();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content0".getBytes());

            m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content1".getBytes());

            m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content2".getBytes());
        }

        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testReadEntriesAfterCompaction() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value("content0".getBytes()).send();
        producer.newMessage().key("key0").value("content1".getBytes()).send();
        producer.newMessage().key("key0").value("content2".getBytes()).send();

        compact(topic);

        producer.newMessage().key("key0").value("content3".getBytes()).send();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content2".getBytes());

            m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content3".getBytes());
        }
    }

    @Test
    public void testSeekEarliestAfterCompaction() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        producer.newMessage().key("key0").value("content0".getBytes()).send();
        producer.newMessage().key("key0").value("content1".getBytes()).send();
        producer.newMessage().key("key0").value("content2".getBytes()).send();

        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.seek(MessageId.earliest);
            Message<byte[]> m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content2".getBytes());
        }

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            consumer.seek(MessageId.earliest);

            Message<byte[]> m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content0".getBytes());

            m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content1".getBytes());

            m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testBrokerRestartAfterCompaction() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value("content0".getBytes()).send();
        producer.newMessage().key("key0").value("content1".getBytes()).send();
        producer.newMessage().key("key0").value("content2".getBytes()).send();

        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content2".getBytes());
        }

        stopBroker();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.receive();
            fail("Shouldn't have been able to receive anything");
        } catch (PulsarClientException e) {
            // correct behaviour
        }
        startBroker();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        compact(topic);

        producer.newMessage().key("key0").value("content0".getBytes()).send();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            assertEquals(m.getKey(), "key0");
            assertEquals(m.getData(), "content0".getBytes());
        }
    }

    @Test
    public void testFirstMessageRetained() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create()) {
            producer.newMessage().key("key1").value("my-message-1".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-message-2".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-message-3".getBytes()).send();
        }

        // Read messages before compaction to get ids
        List<Message<byte[]>> messages = new ArrayList<>();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            messages.add(consumer.receive());
            messages.add(consumer.receive());
            messages.add(consumer.receive());
        }

        // compact the topic
        compact(topic);

        // Check that messages after compaction have same ids
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key1");
            assertEquals(new String(message1.getData()), "my-message-1");
            assertEquals(message1.getMessageId(), messages.get(0).getMessageId());

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key2");
            assertEquals(new String(message2.getData()), "my-message-3");
            assertEquals(message2.getMessageId(), messages.get(2).getMessageId());
        }
    }

    @Test
    public void testBatchMessageIdsDontChange() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
            .maxPendingMessages(3)
            .enableBatching(true)
            .batchingMaxMessages(3)
            .batchingMaxPublishDelay(1, TimeUnit.HOURS)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create()
        ) {
            producer.newMessage().key("key1").value("my-message-1".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-message-2".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-message-3".getBytes()).send();
        }

        // Read messages before compaction to get ids
        List<Message<byte[]>> messages = new ArrayList<>();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
             .subscriptionName("sub1").readCompacted(true).subscribe()) {
            messages.add(consumer.receive());
            messages.add(consumer.receive());
            messages.add(consumer.receive());
        }

        // Ensure all messages are in same batch
        assertEquals(((BatchMessageIdImpl)messages.get(0).getMessageId()).getLedgerId(),
                            ((BatchMessageIdImpl)messages.get(1).getMessageId()).getLedgerId());
        assertEquals(((BatchMessageIdImpl)messages.get(0).getMessageId()).getLedgerId(),
                            ((BatchMessageIdImpl)messages.get(2).getMessageId()).getLedgerId());
        assertEquals(((BatchMessageIdImpl)messages.get(0).getMessageId()).getEntryId(),
                            ((BatchMessageIdImpl)messages.get(1).getMessageId()).getEntryId());
        assertEquals(((BatchMessageIdImpl)messages.get(0).getMessageId()).getEntryId(),
                            ((BatchMessageIdImpl)messages.get(2).getMessageId()).getEntryId());

        // compact the topic
        compact(topic);

        // Check that messages after compaction have same ids
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key1");
            assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key2");
            assertEquals(new String(message2.getData()), "my-message-3");
            if (getCompactor() instanceof StrategicTwoPhaseCompactor) {
                assertEquals(message1.getMessageId(), messages.get(0).getMessageId());
                assertEquals(message2.getMessageId(), messages.get(1).getMessageId());
            } else {
                assertEquals(message1.getMessageId(), messages.get(0).getMessageId());
                assertEquals(message2.getMessageId(), messages.get(2).getMessageId());
            }
        }
    }

    @Test
    public void testBatchMessageWithNullValue() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .receiverQueueSize(1).readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
            .maxPendingMessages(3)
            .enableBatching(true)
            .batchingMaxMessages(3)
            .batchingMaxPublishDelay(1, TimeUnit.HOURS)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create()
        ) {
            // batch 1
            producer.newMessage().key("key1").value("my-message-1".getBytes()).sendAsync();
            producer.newMessage().key("key1").value(null).sendAsync();
            producer.newMessage().key("key2").value("my-message-3".getBytes()).send();

            // batch 2
            producer.newMessage().key("key3").value("my-message-4".getBytes()).sendAsync();
            producer.newMessage().key("key3").value("my-message-5".getBytes()).sendAsync();
            producer.newMessage().key("key3").value("my-message-6".getBytes()).send();

            // batch 3
            producer.newMessage().key("key4").value("my-message-7".getBytes()).sendAsync();
            producer.newMessage().key("key4").value(null).sendAsync();
            producer.newMessage().key("key5").value("my-message-9".getBytes()).send();
        }


        // compact the topic
        compact(topic);

        // Read messages before compaction to get ids
        List<Message<byte[]>> messages = new ArrayList<>();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
             .subscriptionName("sub1").receiverQueueSize(1).readCompacted(true).subscribe()) {
            while (true) {
                Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
                if (message ==  null) {
                    break;
                }
                messages.add(message);
            }
        }

        assertEquals(messages.size(), 3);
        assertEquals(messages.get(0).getKey(), "key2");
        assertEquals(messages.get(1).getKey(), "key3");
        assertEquals(messages.get(2).getKey(), "key5");
    }

    @Test
    public void testWholeBatchCompactedOut() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic)
                 .enableBatching(false)
                 .messageRoutingMode(MessageRoutingMode.SinglePartition)
                 .create();
             Producer<byte[]> producerBatch = pulsarClient.newProducer().topic(topic)
                 .maxPendingMessages(3)
                 .enableBatching(true)
                 .batchingMaxMessages(3)
                 .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                 .messageRoutingMode(MessageRoutingMode.SinglePartition)
                 .create()) {
            producerBatch.newMessage().key("key1").value("my-message-1".getBytes()).sendAsync();
            producerBatch.newMessage().key("key1").value("my-message-2".getBytes()).sendAsync();
            producerBatch.newMessage().key("key1").value("my-message-3".getBytes()).sendAsync();
            producerNormal.newMessage().key("key1").value("my-message-4".getBytes()).send();
        }

        // compact the topic
        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message = consumer.receive();
            assertEquals(message.getKey(), "key1");
            assertEquals(new String(message.getData()), "my-message-4");
        }
    }

    @DataProvider(name = "retainNullKey")
    public static Object[][] retainNullKey() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "retainNullKey")
    public void testKeyLessMessagesPassThrough(boolean retainNullKey) throws Exception {
        conf.setTopicCompactionRetainNullKey(retainNullKey);
        restartBroker();
        FieldUtils.writeDeclaredField(compactor, "topicCompactionRetainNullKey", retainNullKey, true);

        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic).create();
                Producer<byte[]> producerBatch = pulsarClient.newProducer().topic(topic).maxPendingMessages(3)
                .enableBatching(true).batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producerNormal.newMessage().value("my-message-1".getBytes()).send();

            producerBatch.newMessage().value("my-message-2".getBytes()).sendAsync();
            producerBatch.newMessage().key("key1").value("my-message-3".getBytes()).sendAsync();
            producerBatch.newMessage().key("key1").value("my-message-4".getBytes()).send();

            producerBatch.newMessage().key("key2").value("my-message-5".getBytes()).sendAsync();
            producerBatch.newMessage().key("key2").value("my-message-6".getBytes()).sendAsync();
            producerBatch.newMessage().value("my-message-7".getBytes()).send();

            producerNormal.newMessage().value("my-message-8".getBytes()).send();
        }

        // compact the topic
        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            if (getCompactor() instanceof StrategicTwoPhaseCompactor) {
                Message<byte[]> message3 = consumer.receive();
                assertEquals(message3.getKey(), "key1");
                assertEquals(new String(message3.getData()), "my-message-4");

                Message<byte[]> message4 = consumer.receive();
                assertEquals(message4.getKey(), "key2");
                assertEquals(new String(message4.getData()), "my-message-6");

                Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
                assertNull(m);
            } else {
                List<Pair<String, String>> result = new ArrayList<>();
                while (true) {
                    Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
                    if (message == null) {
                        break;
                    }
                    result.add(Pair.of(message.getKey(), message.getData() == null ? null : new String(message.getData())));
                }

                List<Pair<String, String>> expectList;
                if (retainNullKey) {
                    expectList = List.of(
                        Pair.of(null, "my-message-1"), Pair.of(null, "my-message-2"),
                        Pair.of("key1", "my-message-4"), Pair.of("key2", "my-message-6"),
                        Pair.of(null, "my-message-7"), Pair.of(null, "my-message-8"));
                } else {
                    expectList = List.of(Pair.of("key1", "my-message-4"), Pair.of("key2", "my-message-6"));
                }
                assertEquals(result, expectList);
            }
        }
    }


    @Test
    public void testEmptyPayloadDeletes() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer()
                 .topic(topic)
                 .enableBatching(false)
                 .create();
             Producer<byte[]> producerBatch = pulsarClient.newProducer()
                 .topic(topic)
                 .maxPendingMessages(3)
                 .enableBatching(true)
                 .batchingMaxMessages(3)
                 .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                 .create()) {

            // key0 persists through it all
            producerNormal.newMessage()
                    .key("key0")
                    .value("my-message-0".getBytes())
                    .send();

            // key1 is added but then deleted
            producerNormal.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes())
                    .send();

            producerNormal.newMessage()
                    .key("key1")
                    .send();

            // key2 is added but deleted in same batch
            producerBatch.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes())
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key3")
                    .value("my-message-3".getBytes())
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key2").send();

            // key3 is added in previous batch, deleted in this batch
            producerBatch.newMessage()
                                    .key("key3")
                                    .sendAsync();
            producerBatch.newMessage()
                    .key("key4")
                    .value("my-message-3".getBytes())
                    .sendAsync();
            producerBatch.newMessage()
                                    .key("key4")
                                    .send();

            // key4 is added, deleted, then resurrected
            producerNormal.newMessage()
                    .key("key4")
                    .value("my-message-4".getBytes())
                    .send();
        }

        // compact the topic
        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key0");
            assertEquals(new String(message1.getData()), "my-message-0");

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key4");
            assertEquals(new String(message2.getData()), "my-message-4");
        }
    }

    @Test
    public void testEmptyPayloadDeletesWhenCompressed() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer()
                 .topic(topic)
                 .enableBatching(false)
                 .compressionType(CompressionType.LZ4)
                 .create();
             Producer<byte[]> producerBatch = pulsarClient.newProducer()
                 .topic(topic)
                 .maxPendingMessages(3)
                 .enableBatching(true)
                 .compressionType(CompressionType.LZ4)
                 .batchingMaxMessages(3)
                 .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                 .create()) {

            // key0 persists through it all
            producerNormal.newMessage()
                    .key("key0")
                    .value("my-message-0".getBytes()).send();

            // key1 is added but then deleted
            producerNormal.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes()).send();

            producerNormal.newMessage()
                    .key("key1").send();

            // key2 is added but deleted in same batch
            producerBatch.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes()).sendAsync();
            producerBatch.newMessage()
                    .key("key3")
                    .value("my-message-3".getBytes()).sendAsync();
            producerBatch.newMessage()
                    .key("key2").send();

            // key3 is added in previous batch, deleted in this batch
            producerBatch.newMessage()
                    .key("key3")
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key4")
                    .value("my-message-3".getBytes())
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key4")
                    .send();

            // key4 is added, deleted, then resurrected
            producerNormal.newMessage()
                    .key("key4")
                    .value("my-message-4".getBytes())
                    .send();
        }

        // compact the topic
        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key0");
            assertEquals(new String(message1.getData()), "my-message-0");

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key4");
            assertEquals(new String(message2.getData()), "my-message-4");
        }
    }

    // test compact no keys

    @Test
    public void testCompactorReadsCompacted() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // capture opened ledgers
        Set<Long> ledgersOpened = Sets.newConcurrentHashSet();
        when(pulsarTestContext.getBookKeeperClient().newOpenLedgerOp()).thenAnswer(
                (invocation) -> {
                    OpenBuilder builder = (OpenBuilder)spyWithoutRecordingInvocations(invocation.callRealMethod());
                    when(builder.withLedgerId(anyLong())).thenAnswer(
                            (invocation2) -> {
                                ledgersOpened.add((Long)invocation2.getArguments()[0]);
                                return invocation2.callRealMethod();
                            });
                    return builder;
                });

        // subscribe before sending anything, so that we get all messages in sub1
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

        // create the topic on the broker
        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic).create()) {
            producerNormal.newMessage()
                    .key("key0")
                    .value("my-message-0".getBytes())
                    .send();
        }

        // force ledger roll
        pulsar.getBrokerService().getTopicReference(topic).get().close(false).get();

        // write a message to avoid issue #1517
        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic).create()) {
            producerNormal.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes())
                    .send();
        }

        // verify second ledger created
        String managedLedgerName = ((PersistentTopic)pulsar.getBrokerService().getTopicReference(topic).get())
            .getManagedLedger().getName();
        ManagedLedgerInfo info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(managedLedgerName);
        assertEquals(info.ledgers.size(), 2);
        assertTrue(ledgersOpened.isEmpty()); // no ledgers should have been opened

        // compact the topic
        compact(topic);

        // should have opened all except last to read
        assertTrue(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        assertFalse(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        ledgersOpened.clear();

        // force broker to close resources for topic
        pulsar.getBrokerService().getTopicReference(topic).get().close(false).get();

        // write a message to avoid issue #1517
        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic).create()) {
            producerNormal.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes())
                    .send();
        }

        info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(managedLedgerName);
        assertEquals(info.ledgers.size(), 3);

        // should only have opened the penultimate ledger to get stat
        assertFalse(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        assertFalse(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        assertFalse(ledgersOpened.contains(info.ledgers.get(2).ledgerId));
        ledgersOpened.clear();

        // compact the topic again
        compact(topic);

        // shouldn't have opened first ledger (already compacted), penultimate would have some uncompacted data.
        // last ledger already open for writing
        assertFalse(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        assertTrue(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        assertFalse(ledgersOpened.contains(info.ledgers.get(2).ledgerId));

        // all three messages should be there when we read compacted
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key0");
            assertEquals(new String(message1.getData()), "my-message-0");

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key1");
            assertEquals(new String(message2.getData()), "my-message-1");

            Message<byte[]> message3 = consumer.receive();
            assertEquals(message3.getKey(), "key2");
            assertEquals(new String(message3.getData()), "my-message-2");
        }
    }

    @Test
    public void testCompactCompressedNoBatch() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .compressionType(CompressionType.LZ4).enableBatching(false).create()) {
            producer.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-3".getBytes())
                    .send();
        }

        // compact the topic
        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key1");
            assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key2");
            assertEquals(new String(message2.getData()), "my-message-3");
        }
    }

    @Test
    public void testCompactCompressedBatching() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .compressionType(CompressionType.LZ4)
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-3".getBytes())
                    .send();
        }

        // compact the topic
        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key1");
            assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key2");
            assertEquals(new String(message2.getData()), "my-message-3");
        }
    }

    public static class EncKeyReader implements CryptoKeyReader {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
            String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
            if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                    return keyInfo;
                } catch (IOException e) {
                    fail("Failed to read certificate from " + CERT_FILE_PATH);
                }
            } else {
                fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
            }
            return null;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
            String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
            if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                    return keyInfo;
                } catch (IOException e) {
                    fail("Failed to read certificate from " + CERT_FILE_PATH);
                }
            } else {
                fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
            }
            return null;
        }
    }

    @Test
    public void testCompactEncryptedNoBatch() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .enableBatching(false).create()) {
            producer.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-3".getBytes())
                    .send();
        }

        // compact the topic
        compact(topic, new EncKeyReader());

        // Check that messages after compaction have same ids
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key1");
            assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key2");
            assertEquals(new String(message2.getData()), "my-message-3");
        }
    }

    @Test
    public void testCompactEncryptedBatching() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-3".getBytes())
                    .send();
        }

        // compact the topic
        compact(topic, new EncKeyReader());

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key1");
            assertEquals(new String(message1.getData()), "my-message-1");

            if (getCompactor() instanceof StrategicTwoPhaseCompactor) {
                Message<byte[]> message3 = consumer.receive();
                assertEquals(message3.getKey(), "key2");
                assertEquals(new String(message3.getData()), "my-message-3");
            } else {
                // with encryption, all messages are passed through compaction as it doesn't
                // have the keys to decrypt the batch payload
                Message<byte[]> message2 = consumer.receive();
                assertEquals(message2.getKey(), "key2");
                assertEquals(new String(message2.getData()), "my-message-2");

                Message<byte[]> message3 = consumer.receive();
                assertEquals(message3.getKey(), "key2");
                assertEquals(new String(message3.getData()), "my-message-3");
            }
        }
    }

    @Test
    public void testCompactEncryptedAndCompressedNoBatch() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .compressionType(CompressionType.LZ4)
                .enableBatching(false).create()) {
            producer.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-3".getBytes())
                    .send();
        }

        // compact the topic
        compact(topic, new EncKeyReader());

        // Check that messages after compaction have same ids
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key1");
            assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            assertEquals(message2.getKey(), "key2");
            assertEquals(new String(message2.getData()), "my-message-3");
        }
    }

    @Test
    public void testCompactEncryptedAndCompressedBatching() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .compressionType(CompressionType.LZ4)
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes())
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value("my-message-3".getBytes())
                    .send();
        }

        // compact the topic
        compact(topic, new EncKeyReader());

        // with encryption, all messages are passed through compaction as it doesn't
        // have the keys to decrypt the batch payload
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key1");
            assertEquals(new String(message1.getData()), "my-message-1");


            if (getCompactor() instanceof StrategicTwoPhaseCompactor) {
                Message<byte[]> message3 = consumer.receive();
                assertEquals(message3.getKey(), "key2");
                assertEquals(new String(message3.getData()), "my-message-3");
            } else {
                Message<byte[]> message2 = consumer.receive();
                assertEquals(message2.getKey(), "key2");
                assertEquals(new String(message2.getData()), "my-message-2");

                Message<byte[]> message3 = consumer.receive();
                assertEquals(message3.getKey(), "key2");
                assertEquals(new String(message3.getData()), "my-message-3");
            }
        }
    }

    @Test
    public void testEmptyPayloadDeletesWhenEncrypted() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer()
                 .topic(topic)
                 .enableBatching(false)
                 .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                 .create();
             Producer<byte[]> producerBatch = pulsarClient.newProducer()
                 .topic(topic)
                 .maxPendingMessages(3)
                 .enableBatching(true)
                 .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                 .batchingMaxMessages(3)
                 .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                 .create()) {

            // key0 persists through it all
            producerNormal.newMessage()
                    .key("key0")
                    .value("my-message-0".getBytes()).send();

            // key1 is added but then deleted
            producerNormal.newMessage()
                    .key("key1")
                    .value("my-message-1".getBytes()).send();

            producerNormal.newMessage()
                    .key("key1")
                    .send();

            // key2 is added but deleted in same batch
            producerBatch.newMessage()
                    .key("key2")
                    .value("my-message-2".getBytes()).sendAsync();
            producerBatch.newMessage()
                    .key("key3")
                    .value("my-message-3".getBytes()).sendAsync();
            producerBatch.newMessage()
                    .key("key2").send();

            // key4 is added, deleted, then resurrected
            producerNormal.newMessage()
                    .key("key4")
                    .value("my-message-4".getBytes()).send();
        }

        // compact the topic
        compact(topic, new EncKeyReader());

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .cryptoKeyReader(new EncKeyReader())
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            assertEquals(message1.getKey(), "key0");
            assertEquals(new String(message1.getData()), "my-message-0");

            if (getCompactor() instanceof StrategicTwoPhaseCompactor) {
                Message<byte[]> message3 = consumer.receive();
                assertEquals(message3.getKey(), "key3");
                assertEquals(new String(message3.getData()), "my-message-3");

                Message<byte[]> message5 = consumer.receive();
                assertEquals(message5.getKey(), "key4");
                assertEquals(new String(message5.getData()), "my-message-4");
            } else {
                // see all messages from batch
                Message<byte[]> message2 = consumer.receive();
                assertEquals(message2.getKey(), "key2");
                assertEquals(new String(message2.getData()), "my-message-2");

                Message<byte[]> message3 = consumer.receive();
                assertEquals(message3.getKey(), "key3");
                assertEquals(new String(message3.getData()), "my-message-3");

                Message<byte[]> message4 = consumer.receive();
                assertEquals(message4.getKey(), "key2");
                assertNull(message4.getData());

                Message<byte[]> message5 = consumer.receive();
                assertEquals(message5.getKey(), "key4");
                assertEquals(new String(message5.getData()), "my-message-4");
            }
        }
    }

    @DataProvider(name = "lastDeletedBatching")
    public static Object[][] lastDeletedBatching() {
        return new Object[][] {{true}, {false}};
    }

    @Test(timeOut = 20000, dataProvider = "lastDeletedBatching")
    public void testCompactionWithLastDeletedKey(boolean batching) throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(batching)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value("1".getBytes()).send();
        producer.newMessage().key("2").value("2".getBytes()).send();
        producer.newMessage().key("3").value("3".getBytes()).send();
        producer.newMessage().key("1").value("".getBytes()).send();
        producer.newMessage().key("2").value("".getBytes()).send();

        compact(topic);

        Set<String> expected = Sets.newHashSet("3");
        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
            assertTrue(expected.remove(m.getKey()));
        }
    }

    @Test(timeOut = 20000, dataProvider = "lastDeletedBatching")
    public void testEmptyCompactionLedger(boolean batching) throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(batching)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value("1".getBytes()).send();
        producer.newMessage().key("2").value("2".getBytes()).send();
        producer.newMessage().key("1").value("".getBytes()).send();
        producer.newMessage().key("2").value("".getBytes()).send();

        compact(topic);

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(m);
        }
    }

    @Test(timeOut = 20000, dataProvider = "lastDeletedBatching")
    public void testAllEmptyCompactionLedger(boolean batchEnabled) throws Exception {
        final String topic = "persistent://my-tenant/my-ns/testAllEmptyCompactionLedger" + UUID.randomUUID().toString();

        final int messages = 10;

        // 1.create producer and publish message to the topic.
        ProducerBuilder<byte[]> builder = pulsarClient.newProducer().topic(topic);
        if (!batchEnabled) {
            builder.enableBatching(false);
        } else {
            builder.batchingMaxMessages(messages / 5);
        }

        Producer<byte[]> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().keyBytes("1".getBytes()).value("".getBytes()).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        compact(topic);

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(m);
        }
    }

    @Test(timeOut = 20000)
    public void testBatchAndNonBatchWithoutEmptyPayload() throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "persistent://my-tenant/my-ns/testBatchAndNonBatchWithoutEmptyPayload" + UUID.randomUUID().toString();

        // 1.create producer and publish message to the topic.
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();

        final String k1 = "k1";
        final String k2 = "k2";
        producer.newMessage().key(k1).value("0".getBytes()).send();
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(7);
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value((i + 1 + "").getBytes()).sendAsync());
        }
        producer.flush();
        producer.newMessage().key(k1).value("3".getBytes()).send();
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value((i + 4 + "").getBytes()).sendAsync());
        }
        producer.flush();

        for (int i = 0; i < 3; i++) {
            futures.add(producer.newMessage().key(k2).value((i + "").getBytes()).sendAsync());
        }

        producer.newMessage().key(k2).value("3".getBytes()).send();
        producer.flush();
        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        compact(topic);

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<byte[]> m1 = consumer.receive(2, TimeUnit.SECONDS);
            Message<byte[]> m2 = consumer.receive(2, TimeUnit.SECONDS);
            assertNotNull(m1);
            assertNotNull(m2);
            assertEquals(m1.getKey(), k1);
            assertEquals(new String(m1.getValue()), "5");
            assertEquals(m2.getKey(), k2);
            assertEquals(new String(m2.getValue()), "3");
            Message<byte[]> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }
    @Test(timeOut = 20000)
    public void testBatchAndNonBatchWithEmptyPayload() throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "persistent://my-tenant/my-ns/testBatchAndNonBatchWithEmptyPayload" + UUID.randomUUID().toString();

        // 1.create producer and publish message to the topic.
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();

        final String k1 = "k1";
        final String k2 = "k2";
        final String k3 = "k3";
        producer.newMessage().key(k1).value("0".getBytes()).send();
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(7);
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value((i + 1 + "").getBytes()).sendAsync());
        }
        producer.flush();
        producer.newMessage().key(k1).value("3".getBytes()).send();
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value((i + 4 + "").getBytes()).sendAsync());
        }
        producer.flush();

        for (int i = 0; i < 3; i++) {
            futures.add(producer.newMessage().key(k2).value((i + 10 + "").getBytes()).sendAsync());
        }
        producer.flush();

        producer.newMessage().key(k2).value("".getBytes()).send();

        producer.newMessage().key(k3).value("0".getBytes()).send();

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        compact(topic);

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<byte[]> m1 = consumer.receive();
            Message<byte[]> m2 = consumer.receive();
            assertNotNull(m1);
            assertNotNull(m2);
            assertEquals(m1.getKey(), k1);
            assertEquals(m2.getKey(), k3);
            assertEquals(new String(m1.getValue()), "5");
            assertEquals(new String(m2.getValue()), "0");
            Message<byte[]> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @Test(timeOut = 20000)
    public void testBatchAndNonBatchEndOfEmptyPayload() throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "persistent://my-tenant/my-ns/testBatchAndNonBatchWithEmptyPayload" + UUID.randomUUID().toString();

        // 1.create producer and publish message to the topic.
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();

        final String k1 = "k1";
        final String k2 = "k2";
        producer.newMessage().key(k1).value("0".getBytes()).send();
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(7);
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value((i + 1 + "").getBytes()).sendAsync());
        }
        producer.flush();
        producer.newMessage().key(k1).value("3".getBytes()).send();
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value((i + 4 + "").getBytes()).sendAsync());
        }
        producer.flush();

        for (int i = 0; i < 3; i++) {
            futures.add(producer.newMessage().key(k2).value((i + 10 + "").getBytes()).sendAsync());
        }
        producer.flush();

        producer.newMessage().key(k2).value("".getBytes()).send();

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        compact(topic);

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<byte[]> m1 = consumer.receive();
            assertNotNull(m1);
            assertEquals(m1.getKey(), k1);
            assertEquals(new String(m1.getValue()), "5");
            Message<byte[]> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @Test(timeOut = 20000, dataProvider = "lastDeletedBatching")
    public void testCompactMultipleTimesWithoutEmptyMessage(boolean batchEnabled) throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "persistent://my-tenant/my-ns/testCompactMultipleTimesWithoutEmptyMessage" + UUID.randomUUID().toString();

        final int messages = 10;
        final String key = "1";

        // 1.create producer and publish message to the topic.
        ProducerBuilder<byte[]> builder = pulsarClient.newProducer().topic(topic);
        if (!batchEnabled) {
            builder.enableBatching(false);
        } else {
            builder.batchingMaxMessages(messages / 5);
        }

        Producer<byte[]> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value((i + "").getBytes()).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        compact(topic);

        // 3. Send more ten messages
        futures.clear();
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value((i + 10 + "").getBytes()).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();

        // 4.compact again.
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<byte[]> m1 = consumer.receive();
            assertNotNull(m1);
            assertEquals(m1.getKey(), key);
            assertEquals(new String(m1.getValue()), "19");
            Message<byte[]> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @Test(timeOut = 2000000, dataProvider = "lastDeletedBatching")
    public void testReadUnCompacted(boolean batchEnabled) throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "persistent://my-tenant/my-ns/testReadUnCompacted" + UUID.randomUUID().toString();

        final int messages = 10;
        final String key = "1";

        // 1.create producer and publish message to the topic.
        ProducerBuilder<byte[]> builder = pulsarClient.newProducer().topic(topic);
        if (!batchEnabled) {
            builder.enableBatching(false);
        } else {
            builder.batchingMaxMessages(messages / 5);
        }

        Producer<byte[]> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value((i + "").getBytes()).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        compact(topic);

        // 3. Send more ten messages
        futures.clear();
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value((i + 10 + "").getBytes()).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub1")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            for (int i = 0; i < 11; i++) {
                Message<byte[]> received = consumer.receive();
                assertNotNull(received);
                assertEquals(received.getKey(), key);
                assertEquals(new String(received.getValue()), i + 9 + "");
                consumer.acknowledge(received);
            }
            Message<byte[]> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }

        // 4.Send empty message to delete the key-value in the compacted topic.
        producer.newMessage().key(key).value(("").getBytes()).send();

        // 5.compact the topic.
        compact(topic);

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub2")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            Message<byte[]> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }

        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value((i + 20 + "").getBytes()).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub3")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            for (int i = 0; i < 10; i++) {
                Message<byte[]> received = consumer.receive();
                assertNotNull(received);
                assertEquals(received.getKey(), key);
                assertEquals(new String(received.getValue()), i + 20 + "");
                consumer.acknowledge(received);
            }
            Message<byte[]> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @SneakyThrows
    @Test
    public void testHealthCheckTopicNotCompacted() {
        NamespaceName heartbeatNamespaceV1 = NamespaceService.getHeartbeatNamespace(pulsar.getBrokerId(), pulsar.getConfiguration());
        String topicV1 = "persistent://" + heartbeatNamespaceV1.toString() + "/healthcheck";
        NamespaceName heartbeatNamespaceV2 = NamespaceService.getHeartbeatNamespaceV2(pulsar.getBrokerId(), pulsar.getConfiguration());
        String topicV2 = heartbeatNamespaceV2.toString() + "/healthcheck";
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicV1).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicV2).create();
        Optional<Topic> topicReferenceV1 = pulsar.getBrokerService().getTopic(topicV1, false).join();
        Optional<Topic> topicReferenceV2 = pulsar.getBrokerService().getTopic(topicV2, false).join();
        assertFalse(((SystemTopic)topicReferenceV1.get()).isCompactionEnabled());
        assertFalse(((SystemTopic)topicReferenceV2.get()).isCompactionEnabled());
        producer1.close();
        producer2.close();
    }

    @Test(timeOut = 60000)
    public void testCompactionWithMarker() throws Exception {
        String namespace = "my-tenant/my-ns";
        final TopicName dest = TopicName.get(
                newUniqueName("persistent://" + namespace + "/testWriteMarker"));
        admin.topics().createNonPartitionedTopic(dest.toString());
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(dest.toString())
                .subscriptionName("test-compaction-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(dest.toString())
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.send("msg-1".getBytes(StandardCharsets.UTF_8));
        Optional<Topic> topic = pulsar.getBrokerService().getTopic(dest.toString(), true).join();
        assertTrue(topic.isPresent());
        PersistentTopic persistentTopic = (PersistentTopic) topic.get();
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int rad = random.nextInt(3);
            ByteBuf marker;
            if (rad == 0) {
                marker = Markers.newTxnCommitMarker(-1L, 0, i);
            } else if (rad == 1) {
                marker = Markers.newTxnAbortMarker(-1L, 0, i);
            } else {
                marker = Markers.newReplicatedSubscriptionsSnapshotRequest(UUID.randomUUID().toString(), "r1");
            }
            persistentTopic.getManagedLedger().asyncAddEntry(marker, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    //
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    //
                }
            }, null);
            marker.release();
        }
        producer.send("msg-2".getBytes(StandardCharsets.UTF_8));
        admin.topics().triggerCompaction(dest.toString());
        Awaitility.await()
                .atMost(50, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    long ledgerId = admin.topics().getInternalStats(dest.toString()).compactedLedger.ledgerId;
                    assertNotEquals(ledgerId, -1L);
                });
    }

    @Test(timeOut = 100000)
    public void testReceiverQueueSize() throws Exception {
        final String topicName = newUniqueName("persistent://my-tenant/my-ns/testReceiverQueueSize");
        final String subName = "my-sub";
        final int receiveQueueSize = 1;
        @Cleanup
        PulsarClient client = newPulsarClient(lookupUrl.toString(), 100);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage().key(String.valueOf(i % 2)).value(String.valueOf(i)).sendAsync();
        }
        producer.flush();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                .topic(topicName).readCompacted(true).receiverQueueSize(receiveQueueSize).subscriptionName(subName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        //Give some time to consume
        Awaitility.await()
                .untilAsserted(() -> assertEquals(consumer.getStats().getMsgNumInReceiverQueue().intValue(),
                        receiveQueueSize));
        consumer.close();
        producer.close();
    }

    @Test
    public void testDispatcherMaxReadSizeBytes() throws Exception {
        final String topicName =
                newUniqueName("persistent://my-tenant/my-ns/testDispatcherMaxReadSizeBytes");
        final String subName = "my-sub";
        final int receiveQueueSize = 1;
        @Cleanup
        PulsarClient client = newPulsarClient(lookupUrl.toString(), 100);
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topicName).create();

        for (int i = 0; i < 10; i+=2) {
            producer.newMessage().key(UUID.randomUUID().toString()).value(new byte[4*1024*1024]).send();
        }
        producer.flush();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        admin.topics().unload(topicName);

        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) client.newConsumer(Schema.BYTES)
                .topic(topicName).readCompacted(true).receiverQueueSize(receiveQueueSize).subscriptionName(subName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription persistentSubscription = topic.getSubscriptions().get(subName);
        PersistentDispatcherSingleActiveConsumer dispatcher =
                Mockito.spy((PersistentDispatcherSingleActiveConsumer) persistentSubscription.getDispatcher());
        FieldUtils.writeDeclaredField(persistentSubscription, "dispatcher", dispatcher, true);

        Awaitility.await().untilAsserted(() -> {
            assertSame(consumer.getStats().getMsgNumInReceiverQueue(), 1);
        });

        consumer.increaseAvailablePermits(2);

        Thread.sleep(2000);

        Mockito.verify(dispatcher, Mockito.atLeastOnce())
                .readEntriesComplete(Mockito.argThat(argument -> argument.size() == 1),
                        Mockito.any(org.apache.pulsar.broker.service.Consumer.class), Mockito.anyLong());

        consumer.close();
        producer.close();
    }

    @Test
    public void testCompactionDuplicate() throws Exception {
        String topic = "persistent://my-tenant/my-ns/testCompactionDuplicate";
        final int numMessages = 1000;
        final int maxKeys = 800;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // trigger compaction (create __compaction cursor)
        admin.topics().triggerCompaction(topic);

        Map<String, byte[]> expected = new HashMap<>();
        Random r = new Random(0);

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.newMessage().key(key).value(data).send();
            expected.put(key, data);
        }

        producer.flush();

        // trigger compaction
        admin.topics().triggerCompaction(topic);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topics().compactionStatus(topic).status,
                    LongRunningProcessStatus.Status.RUNNING);
        });

        // Wait for phase one to complete
        Thread.sleep(500);

        // Unload topic make reader of compaction reconnect
        admin.topics().unload(topic);

        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topic, false);
            // Compacted topic ledger should have same number of entry equals to number of unique key.
            assertEquals(internalStats.compactedLedger.entries, expected.size());
            assertTrue(internalStats.compactedLedger.ledgerId > -1);
            assertFalse(internalStats.compactedLedger.offloaded);
        });

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            while (true) {
                Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
                assertEquals(expected.remove(m.getKey()), m.getData());
                if (expected.isEmpty()) {
                    break;
                }
            }
        }
    }

    @Test
    public void testDeleteCompactedLedger() throws Exception {
        String topicName = "persistent://my-tenant/my-ns/testDeleteCompactedLedger";

        final String subName = "my-sub";
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();

        pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).readCompacted(true).subscribe().close();

        for (int i = 0; i < 10; i++) {
            producer.newMessage().key(String.valueOf(i % 2)).value(String.valueOf(i)).sendAsync();
        }
        producer.flush();

        compact(topicName);

        MutableLong compactedLedgerId = new MutableLong(-1);
        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats stats = admin.topics().getInternalStats(topicName);
            assertNotEquals(stats.compactedLedger.ledgerId, -1L);
            compactedLedgerId.setValue(stats.compactedLedger.ledgerId);
            assertEquals(stats.compactedLedger.entries, 2L);
        });

        // delete compacted ledger
        admin.topics().deleteSubscription(topicName, "__compaction");

        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats stats = admin.topics().getInternalStats(topicName);
            assertEquals(stats.compactedLedger.ledgerId, -1L);
            assertEquals(stats.compactedLedger.entries, -1L);
            assertThrows(BKException.BKNoSuchLedgerExistsException.class, () -> pulsarTestContext.getBookKeeperClient()
                        .openLedger(compactedLedgerId.getValue(), BookKeeper.DigestType.CRC32C, new byte[]{}));
        });

        compact(topicName);

        MutableLong compactedLedgerId2 = new MutableLong(-1);
        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats stats = admin.topics().getInternalStats(topicName);
            assertNotEquals(stats.compactedLedger.ledgerId, -1L);
            compactedLedgerId2.setValue(stats.compactedLedger.ledgerId);
            assertEquals(stats.compactedLedger.entries, 2L);
        });

        producer.close();
        admin.topics().delete(topicName);

        Awaitility.await().untilAsserted(() -> assertThrows(BKException.BKNoSuchLedgerExistsException.class,
                () -> pulsarTestContext.getBookKeeperClient().openLedger(
                        compactedLedgerId2.getValue(), BookKeeper.DigestType.CRC32, new byte[]{})));
    }

    @Test(timeOut = 10000)
    public void testDeleteCompactedLedgerWithSlowAck() throws Exception {
        String topicName = newUniqueName("persistent://my-tenant/my-ns/testDeleteCompactedLedgerWithSlowAck");
        admin.topics().createNonPartitionedTopic(topicName);
        // minimum compaction threshold
        admin.topicPolicies().setCompactionThreshold(topicName, 1);
        // infinite retention
        admin.topicPolicies().setRetention(topicName, new RetentionPolicies(-1, -1));

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();

        // send a single message
        producer.newMessage().key(String.valueOf(0)).value("0").send();
        // trigger compaction once to create __compaction subscription
        triggerCompactionAndWait(topicName);

        int numberOfMessages = 10;
        for (int i = 0; i < numberOfMessages; i++) {
            producer.newMessage().key(String.valueOf(i % 2)).value(String.valueOf(i)).sendAsync();
        }
        producer.flush();

        // replace the PersistentSubscription with a spy
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subscription =
                spyWithoutRecordingInvocations(topic.getSubscription(Compactor.COMPACTION_SUBSCRIPTION));
        topic.getSubscriptions().put(Compactor.COMPACTION_SUBSCRIPTION, subscription);

        // delay the ack of compaction
        CountDownLatch compactionAckedLatch = new CountDownLatch(1);
        AtomicLong compactedLedgerId = new AtomicLong(-1);
        Mockito.doAnswer(invocationOnMock -> {
            List<Position> positions = invocationOnMock.getArgument(0);
            Map<String, Long> properties = invocationOnMock.getArgument(2);
            log.info("acknowledgeMessage positions: {} properties: {}", positions, properties);
            compactedLedgerId.set(properties.get(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY));
            try {
                return invocationOnMock.callRealMethod();
            } finally {
                log.info("acknowledgeMessage completed {}", positions);
                compactionAckedLatch.countDown();
                // add delay here to introduce possible races with deletion
                Thread.sleep(500);
            }
        }).when(subscription).acknowledgeMessage(Mockito.any(), Mockito.eq(
                CommandAck.AckType.Cumulative), Mockito.any());

        // trigger compaction
        admin.topics().triggerCompaction(topicName);

        // wait for compaction to acknowledge
        compactionAckedLatch.await(9, TimeUnit.SECONDS);

        // close the producer
        producer.close();

        // delete compacted ledger
        admin.topics().delete(topicName, true);

        // ensure that the compacted ledger is deleted
        Awaitility.await().untilAsserted(() -> assertThrows(BKException.BKNoSuchLedgerExistsException.class,
                () -> pulsarTestContext.getBookKeeperClient().openLedger(
                        compactedLedgerId.get(), BookKeeper.DigestType.CRC32, new byte[]{})));
    }

    private void triggerCompactionAndWait(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        persistentTopic.triggerCompaction();
        CompletableFuture<Long> currentCompaction =
                (CompletableFuture<Long>) FieldUtils.readDeclaredField(persistentTopic, "currentCompaction", true);
        currentCompaction.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testCompactionWithTTL() throws Exception {
        String topicName = "persistent://my-tenant/my-ns/testCompactionWithTTL";
        String subName = "sub";
        pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subName).readCompacted(true)
                .subscribe().close();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();

        producer.newMessage().key("K1").value("V1").send();
        producer.newMessage().key("K2").value("V2").send();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        producer.newMessage().key("K1").value("V3").send();
        producer.newMessage().key("K2").value("V4").send();

        Thread.sleep(1000);

        // expire messages
        admin.topics().expireMessagesForAllSubscriptions(topicName, 1);

        // trim the topic
        admin.topics().unload(topicName);

        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicName, false);
            assertEquals(internalStats.numberOfEntries, 4);
        });

        producer.newMessage().key("K3").value("V5").send();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName("sub-2")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        List<String> result = new ArrayList<>();
        while (true) {
            Message<String> receive = consumer.receive(2, TimeUnit.SECONDS);
            if (receive == null) {
                break;
            }

            result.add(receive.getValue());
        }

        assertEquals(result, List.of("V3", "V4", "V5"));
    }

    @Test
    public void testAcknowledgeWithReconnection() throws Exception {
        final String topicName = newUniqueName("persistent://my-tenant/my-ns/testAcknowledge");
        final String subName = "my-sub";
        @Cleanup
        PulsarClient client = newPulsarClient(lookupUrl.toString(), 100);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().key(String.valueOf(i)).value(String.valueOf(i)).send();
            expected.add(String.valueOf(i));
        }
        producer.flush();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        // trim the topic
        admin.topics().unload(topicName);

        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicName, false);
            assertEquals(internalStats.numberOfEntries, 0);
        });

        ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                .topic(topicName).readCompacted(true).receiverQueueSize(1).subscriptionName(subName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .isAckReceiptEnabled(true)
                .subscribe();

        List<String> results = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Message<String> message = consumer.receive(3, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            results.add(message.getValue());
            consumer.acknowledge(message);
        }

        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.topics().getStats(topicName, true).getSubscriptions().get(subName).getMsgBacklog(),
                        0));

        // Make consumer reconnect to broker
        admin.topics().unload(topicName);

        // Wait for consumer to reconnect and clear incomingMessages
        consumer.pause();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(consumer.numMessagesInQueue(), 0);
        });
        consumer.resume();

        for (int i = 0; i < 5; i++) {
            Message<String> message = consumer.receive(3, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            results.add(message.getValue());
            consumer.acknowledge(message);
        }

        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.topics().getStats(topicName, true).getSubscriptions().get(subName).getMsgBacklog(),
                        0));

        assertEquals(results, expected);

        Message<String> message = consumer.receive(3, TimeUnit.SECONDS);
        assertNull(message);

        // Make consumer reconnect to broker
        admin.topics().unload(topicName);

        producer.newMessage().key("K").value("V").send();
        Message<String> message2 = consumer.receive(3, TimeUnit.SECONDS);
        assertEquals(message2.getValue(), "V");
        consumer.acknowledge(message2);

        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicName);
            assertEquals(internalStats.lastConfirmedEntry,
                    internalStats.cursors.get(subName).markDeletePosition);
        });

        consumer.close();
        producer.close();
    }

    @Test(timeOut = 120 * 1000)
    public void testConcurrentCompactionAndTopicDelete() throws Exception {
        final String topicName = newUniqueName("persistent://my-tenant/my-ns/tp");
        admin.topics().createNonPartitionedTopic(topicName);
        // Load up the topic.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();

        // Inject a reading delay to the compaction task,
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursor compactionCursor = ml.openCursor(COMPACTION_SUBSCRIPTION);
        ManagedCursor spyCompactionCursor = spy(compactionCursor);
        CountDownLatch delayReadSignal = new CountDownLatch(1);
        Answer answer = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                delayReadSignal.await();
                return invocationOnMock.callRealMethod();
            }
        };
        doAnswer(answer).when(spyCompactionCursor).asyncReadEntries(anyInt(),
                any(AsyncCallbacks.ReadEntriesCallback.class), any(), any(PositionImpl.class));
        doAnswer(answer).when(spyCompactionCursor).asyncReadEntries(anyInt(), anyLong(),
                any(AsyncCallbacks.ReadEntriesCallback.class), any(), any(PositionImpl.class));
        doAnswer(answer).when(spyCompactionCursor).asyncReadEntriesOrWait(anyInt(), anyLong(),
                any(AsyncCallbacks.ReadEntriesCallback.class), any(), any(PositionImpl.class));
        ml.getCursors().removeCursor(COMPACTION_SUBSCRIPTION);
        ml.getCursors().add(spyCompactionCursor, ml.getLastConfirmedEntry());

        // Trigger a compaction task.
        for (int i = 0; i < 2000; i++) {
            producer.newMessage().key(String.valueOf(i)).value(String.valueOf(i)).send();
        }
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).readCompacted(true).subscriptionName("s1")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        persistentTopic.triggerCompaction();
        Awaitility.await().untilAsserted(() -> {
           assertEquals(persistentTopic.getSubscriptions().get(COMPACTION_SUBSCRIPTION).getConsumers().size(), 1);
        });

        // Since we injected a delay reading, the compaction task started and not finish yet.
        // Call topic deletion, they two tasks are concurrently executed.
        producer.close();
        consumer.close();
        CompletableFuture<Void> deleteTopicFuture = persistentTopic.deleteForcefully();

        // Remove the injection after 3s.
        Thread.sleep(3000);
        delayReadSignal.countDown();

        // Verify: topic deletion is successfully executed.
        Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(deleteTopicFuture.isDone());
        });
    }

    @Test
    public void testEarliestSubsAfterRollover() throws Exception {
        final String topicName = newUniqueName("persistent://my-tenant/my-ns/testEarliestSubsAfterRollover");
        final String subName = "my-sub";
        @Cleanup
        PulsarClient client = newPulsarClient(lookupUrl.toString(), 100);
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().key(String.valueOf(i)).value(String.valueOf(i)).send();
            expected.add(String.valueOf(i));
        }
        producer.flush();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        // trim the topic
        admin.topics().unload(topicName);

        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicName, false);
            assertEquals(internalStats.numberOfEntries, 0);
        });

        // Make ml.getFirstPosition() return new ledger first position
        producer.newMessage().key("K").value("V").send();
        expected.add("V");

        @Cleanup
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                .topic(topicName).readCompacted(true).receiverQueueSize(1).subscriptionName(subName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .isAckReceiptEnabled(true)
                .subscribe();

        List<String> results = new ArrayList<>();
        while (true) {
            Message<String> message = consumer.receive(3, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }

            results.add(message.getValue());
            consumer.acknowledge(message);
        }

        assertEquals(results, expected);
    }
}
