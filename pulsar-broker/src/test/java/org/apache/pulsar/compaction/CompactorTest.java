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

import static org.apache.pulsar.client.impl.RawReaderTest.extractKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class CompactorTest extends MockedPulsarServiceBaseTest {

    private ScheduledExecutorService compactionScheduler;
    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compactor").setDaemon(true).build());
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        compactionScheduler.shutdownNow();
    }

    private List<String> compactAndVerify(String topic, Map<String, byte[]> expected, boolean checkMetrics)
            throws Exception {
        BookKeeper bk = pulsar.getBookKeeperClientFactory().create(
                this.conf, null, null, Optional.empty(), null);
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        long compactedLedgerId = compactor.compact(topic).get();

        LedgerHandle ledger = bk.openLedger(compactedLedgerId,
                Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD);
        Assert.assertEquals(ledger.getLastAddConfirmed() + 1, // 0..lac
                expected.size(),
                "Should have as many entries as there is keys");

        List<String> keys = new ArrayList<>();
        Enumeration<LedgerEntry> entries = ledger.readEntries(0, ledger.getLastAddConfirmed());
        while (entries.hasMoreElements()) {
            ByteBuf buf = entries.nextElement().getEntryBuffer();
            RawMessage m = RawMessageImpl.deserializeFrom(buf);
            String key = extractKey(m);
            keys.add(key);

            ByteBuf payload = extractPayload(m);
            byte[] bytes = new byte[payload.readableBytes()];
            payload.readBytes(bytes);
            Assert.assertEquals(bytes, expected.remove(key),
                    "Compacted version should match expected version");
            m.close();
        }
        if (checkMetrics) {
            CompactionRecord compactionRecord = compactor.getStats().getCompactionRecordForTopic(topic).get();
            long compactedTopicRemovedEventCount = compactionRecord.getLastCompactionRemovedEventCount();
            long lastCompactSucceedTimestamp = compactionRecord.getLastCompactionSucceedTimestamp();
            long lastCompactFailedTimestamp = compactionRecord.getLastCompactionFailedTimestamp();
            long lastCompactDurationTimeInMills = compactionRecord.getLastCompactionDurationTimeInMills();
            Assert.assertTrue(compactedTopicRemovedEventCount >= 1);
            Assert.assertTrue(lastCompactSucceedTimestamp >= 1L);
            Assert.assertTrue(lastCompactDurationTimeInMills >= 0L);
            Assert.assertEquals(lastCompactFailedTimestamp, 0L);
        }
        Assert.assertTrue(expected.isEmpty(), "All expected keys should have been found");
        return keys;
    }

    @Test
    public void testCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 1000;
        final int maxKeys = 10;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Map<String, byte[]> expected = new HashMap<>();
        Random r = new Random(0);

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.newMessage()
                    .key(key)
                    .value(data)
                    .send();
            expected.put(key, data);
        }
        compactAndVerify(topic, expected, true);
    }

    @Test
    public void testAllCompactedOut() throws Exception {
        String topicName = "persistent://my-property/use/my-ns/testAllCompactedOut";
        // set retain null key to true
        boolean oldRetainNullKey = pulsar.getConfig().isTopicCompactionRetainNullKey();
        pulsar.getConfig().setTopicCompactionRetainNullKey(true);
        this.restartBroker();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true).topic(topicName).batchingMaxMessages(3).create();

        producer.newMessage().key("K1").value("V1").sendAsync();
        producer.newMessage().key("K2").value("V2").sendAsync();
        producer.newMessage().key("K2").value(null).sendAsync();
        producer.flush();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        producer.newMessage().key("K1").value(null).sendAsync();
        producer.flush();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        @Cleanup
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .subscriptionName("reader-test")
                .topic(topicName)
                .readCompacted(true)
                .startMessageId(MessageId.earliest)
                .create();
        while (reader.hasMessageAvailable()) {
            Message<String> message = reader.readNext(3, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
        }
        // set retain null key back to avoid affecting other tests
        pulsar.getConfig().setTopicCompactionRetainNullKey(oldRetainNullKey);
    }

    @Test
    public void testCompactAddCompact() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Map<String, byte[]> expected = new HashMap<>();

        producer.newMessage()
                .key("a")
                .value("A_1".getBytes())
                .send();
        producer.newMessage()
                .key("b")
                .value("B_1".getBytes())
                .send();
        producer.newMessage()
                .key("a")
                .value("A_2".getBytes())
                .send();
        expected.put("a", "A_2".getBytes());
        expected.put("b", "B_1".getBytes());

        compactAndVerify(topic, new HashMap<>(expected), false);

        producer.newMessage()
                .key("b")
                .value("B_2".getBytes())
                .send();
        expected.put("b", "B_2".getBytes());

        compactAndVerify(topic, expected, false);
    }

    @Test
    public void testCompactedInOrder() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        producer.newMessage()
                .key("c")
                .value("C_1".getBytes()).send();
        producer.newMessage()
                .key("a")
                .value("A_1".getBytes()).send();
        producer.newMessage()
                .key("b")
                .value("B_1".getBytes()).send();
        producer.newMessage()
                .key("a")
                .value("A_2".getBytes()).send();
        Map<String, byte[]> expected = new HashMap<>();
        expected.put("a", "A_2".getBytes());
        expected.put("b", "B_1".getBytes());
        expected.put("c", "C_1".getBytes());

        List<String> keyOrder = compactAndVerify(topic, expected, false);

        Assert.assertEquals(keyOrder, Lists.newArrayList("c", "b", "a"));
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // trigger creation of topic on server side
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

        BookKeeper bk = pulsar.getBookKeeperClientFactory().create(
                this.conf, null, null, Optional.empty(), null);
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();
    }

    @Test
    public void testPhaseOneLoopTimeConfiguration() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setBrokerServiceCompactionPhaseOneLoopTimeInSeconds(60);
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(mockClient.getCnxPool()).thenReturn(connectionPool);
        TwoPhaseCompactor compactor = new TwoPhaseCompactor(configuration, mockClient,
                Mockito.mock(BookKeeper.class), compactionScheduler);
        Assert.assertEquals(compactor.getPhaseOneLoopReadTimeoutInSeconds(), 60);

    }

    @Test
    public void testCompactedWithConcurrentSend() throws Exception {
        String topic = "persistent://my-property/use/my-ns/testCompactedWithConcurrentSend";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();


        BookKeeper bk = pulsar.getBookKeeperClientFactory().create(
                this.conf, null, null, Optional.empty(), null);
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 100; i++) {
                try {
                    producer.newMessage().key(String.valueOf(i)).value(String.valueOf(i).getBytes()).send();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        CompactedTopic compactedTopic = persistentTopic.getCompactedTopic();

        Awaitility.await().untilAsserted(() -> {
            long compactedLedgerId = compactor.compact(topic).get();
            Thread.sleep(300);
            Optional<CompactedTopicContext> compactedTopicContext = persistentTopic.getCompactedTopicContext();
            Assert.assertTrue(compactedTopicContext.isPresent());
            Assert.assertEquals(compactedTopicContext.get().ledger.getId(), compactedLedgerId);
        });

        Position lastCompactedPosition = compactedTopic.getCompactionHorizon().get();
        Entry lastCompactedEntry = compactedTopic.readLastEntryOfCompactedLedger().get();

        Assert.assertTrue(PositionImpl.get(lastCompactedPosition.getLedgerId(), lastCompactedPosition.getEntryId())
                .compareTo(PositionImpl.get(lastCompactedEntry.getLedgerId(), lastCompactedEntry.getEntryId())) >= 0);

        future.join();
    }

    public ByteBuf extractPayload(RawMessage m) throws Exception {
        ByteBuf payloadAndMetadata = m.getHeadersAndPayload();
        Commands.skipChecksumIfPresent(payloadAndMetadata);
        int metadataSize = payloadAndMetadata.readInt(); // metadata size
         byte[] metadata = new byte[metadataSize];
        payloadAndMetadata.readBytes(metadata);
        return payloadAndMetadata.slice();
    }
}
