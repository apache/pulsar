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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import lombok.Cleanup;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class CompactedTopicTest extends MockedPulsarServiceBaseTest {
    private final Random r = new Random(0);

    @DataProvider(name = "batchEnabledProvider")
    public Object[][] batchEnabledProvider() {
        return new Object[][] {
                { Boolean.FALSE },
                { Boolean.TRUE }
        };
    }

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Build a compacted ledger, and return the id of the ledger, the position of the different
     * entries in the ledger, and a list of gaps, and the entry which should be returned after the gap.
     */
    private Triple<Long, List<Pair<MessageIdData,Long>>, List<Pair<MessageIdData,Long>>>
        buildCompactedLedger(BookKeeper bk, int count)
            throws Exception {
        LedgerHandle lh = bk.createLedger(1, 1,
                                          Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                                          Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD);
        List<Pair<MessageIdData,Long>> positions = new ArrayList<>();
        List<Pair<MessageIdData,Long>> idsInGaps = new ArrayList<>();

        AtomicLong ledgerIds = new AtomicLong(10L);
        AtomicLong entryIds = new AtomicLong(0L);
        CompletableFuture.allOf(
                IntStream.range(0, count).mapToObj((i) -> {
                        List<MessageIdData> idsInGap = new ArrayList<>();
                        if (r.nextInt(10) == 1) {
                            long delta = r.nextInt(10) + 1;
                            idsInGap.add(new MessageIdData()
                                         .setLedgerId(ledgerIds.get())
                                         .setEntryId(entryIds.get() + 1));
                            ledgerIds.addAndGet(delta);
                            entryIds.set(0);
                        }
                        long delta = r.nextInt(5);
                        if (delta != 0) {
                            idsInGap.add(new MessageIdData()
                                         .setLedgerId(ledgerIds.get())
                                         .setEntryId(entryIds.get() + 1));
                        }
                        MessageIdData id = new MessageIdData()
                            .setLedgerId(ledgerIds.get())
                            .setEntryId(entryIds.addAndGet(delta + 1));

                        @Cleanup
                        RawMessage m = new RawMessageImpl(id, Unpooled.EMPTY_BUFFER);

                        CompletableFuture<Void> f = new CompletableFuture<>();
                        ByteBuf buffer = m.serialize();

                        lh.asyncAddEntry(buffer,
                                (rc, ledger, eid, ctx) -> {
                                     if (rc != BKException.Code.OK) {
                                         f.completeExceptionally(BKException.create(rc));
                                     } else {
                                         positions.add(Pair.of(id, eid));
                                         idsInGap.forEach((gid) -> idsInGaps.add(Pair.of(gid, eid)));
                                         f.complete(null);
                                     }
                                }, null);
                        return f;
                    }).toArray(CompletableFuture[]::new)).get();
        lh.close();

        return Triple.of(lh.getId(), positions, idsInGaps);
    }

    @Test
    public void testEntryLookup() throws Exception {
        BookKeeper bk = pulsar.getBookKeeperClientFactory().create(
                this.conf, null, null, Optional.empty(), null);

        Triple<Long, List<Pair<MessageIdData, Long>>, List<Pair<MessageIdData, Long>>> compactedLedgerData
            = buildCompactedLedger(bk, 500);

        List<Pair<MessageIdData, Long>> positions = compactedLedgerData.getMiddle();
        List<Pair<MessageIdData, Long>> idsInGaps = compactedLedgerData.getRight();

        LedgerHandle lh = bk.openLedger(compactedLedgerData.getLeft(),
                                        Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                                        Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD);
        long lastEntryId = lh.getLastAddConfirmed();
        AsyncLoadingCache<Long,MessageIdData> cache = CompactedTopicImpl.createCache(lh, 50);

        MessageIdData firstPositionId = positions.get(0).getLeft();
        Pair<MessageIdData, Long> lastPosition = positions.get(positions.size() - 1);

        // check ids before and after ids in compacted ledger
        Assert.assertEquals(CompactedTopicImpl.findStartPoint(new PositionImpl(0, 0), lastEntryId, cache).get(),
                            Long.valueOf(0));
        Assert.assertEquals(CompactedTopicImpl.findStartPoint(new PositionImpl(Long.MAX_VALUE, 0),
                                                              lastEntryId, cache).get(),
                            Long.valueOf(CompactedTopicImpl.NEWER_THAN_COMPACTED));

        // entry 0 is never in compacted ledger due to how we generate dummy
        Assert.assertEquals(CompactedTopicImpl.findStartPoint(new PositionImpl(firstPositionId.getLedgerId(), 0),
                                                              lastEntryId, cache).get(),
                            Long.valueOf(0));
        // check next id after last id in compacted ledger
        Assert.assertEquals(CompactedTopicImpl.findStartPoint(new PositionImpl(lastPosition.getLeft().getLedgerId(),
                                                                               lastPosition.getLeft().getEntryId() + 1),
                                                              lastEntryId, cache).get(),
                            Long.valueOf(CompactedTopicImpl.NEWER_THAN_COMPACTED));

        // shuffle to make cache work hard
        Collections.shuffle(positions, r);
        Collections.shuffle(idsInGaps, r);

        // Check ids we know are in compacted ledger
        for (Pair<MessageIdData, Long> p : positions) {
            PositionImpl pos = new PositionImpl(p.getLeft().getLedgerId(), p.getLeft().getEntryId());
            Long got = CompactedTopicImpl.findStartPoint(pos, lastEntryId, cache).get();
            Assert.assertEquals(got, p.getRight());
        }

        // Check ids we know are in the gaps of the compacted ledger
        for (Pair<MessageIdData, Long> gap : idsInGaps) {
            PositionImpl pos = new PositionImpl(gap.getLeft().getLedgerId(), gap.getLeft().getEntryId());
            Assert.assertEquals(CompactedTopicImpl.findStartPoint(pos, lastEntryId, cache).get(), gap.getRight());
        }
    }

    @Test
    public void testCleanupOldCompactedTopicLedger() throws Exception {
        BookKeeper bk = pulsar.getBookKeeperClientFactory().create(
                this.conf, null, null, Optional.empty(), null);

        LedgerHandle oldCompactedLedger = bk.createLedger(1, 1,
                Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD);
        oldCompactedLedger.close();
        LedgerHandle newCompactedLedger = bk.createLedger(1, 1,
                Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD);
        newCompactedLedger.close();

        // set the compacted topic ledger
        CompactedTopicImpl compactedTopic = new CompactedTopicImpl(bk);
        compactedTopic.newCompactedLedger(new PositionImpl(1,2), oldCompactedLedger.getId()).get();

        // ensure both ledgers still exist, can be opened
        bk.openLedger(oldCompactedLedger.getId(),
                      Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                      Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD).close();
        bk.openLedger(newCompactedLedger.getId(),
                      Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                      Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD).close();

        // update the compacted topic ledger
        compactedTopic.newCompactedLedger(new PositionImpl(1,2), newCompactedLedger.getId()).get();

        // old ledger should be deleted, new still there
        try {
            bk.openLedger(oldCompactedLedger.getId(),
                          Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                          Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD).close();
            Assert.fail("Should have failed to open old ledger");
        } catch (BKException.BKNoSuchLedgerExistsException
            | BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
            // correct, expected behaviour
        }
        bk.openLedger(newCompactedLedger.getId(),
                      Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                      Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD).close();
    }

    @Test(dataProvider = "batchEnabledProvider")
    public void testCompactWithEmptyMessage(boolean batchEnabled) throws Exception {
        final String key = "1";
        byte[] msgBytes = "".getBytes();
        final String topic = "persistent://my-property/use/my-ns/testCompactWithEmptyMessage-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 1);
        final int messages = 10;

        ProducerBuilder<byte[]> builder = pulsarClient.newProducer().topic(topic);
        if (!batchEnabled) {
            builder.enableBatching(false);
        } else {
            builder.batchingMaxMessages(messages / 2);
        }
        Producer<byte[]> producer = builder.create();

        List<CompletableFuture<MessageId>> list = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            list.add(producer.newMessage().keyBytes(key.getBytes(Charset.defaultCharset())).value(msgBytes).sendAsync());
        }

        FutureUtil.waitForAll(list).get();
        admin.topics().triggerCompaction(topic);

        boolean succeed = retryStrategically((test) -> {
            try {
                return LongRunningProcessStatus.Status.SUCCESS.equals(admin.topics().compactionStatus(topic).status);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 10, 200);

        Assert.assertTrue(succeed);

        // Send more messages and trigger compaction again.

        list.clear();
        for (int i = 0; i < messages; i++) {
            list.add(producer.newMessage().key(key).value(msgBytes).sendAsync());
        }

        FutureUtil.waitForAll(list).get();
        admin.topics().triggerCompaction(topic);

        succeed = retryStrategically((test) -> {
            try {
                return LongRunningProcessStatus.Status.SUCCESS.equals(admin.topics().compactionStatus(topic).status);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 10, 200);
        Assert.assertTrue(succeed);

        producer.close();
    }

    @Test(timeOut = 30000)
    public void testReadMessageFromCompactedLedger() throws Exception {
        final String key = "1";
        String msg = "test compaction msg";
        final String topic = "persistent://my-property/use/my-ns/testCompactWithEmptyMessage-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 1);
        final int numMessages = 10;

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).enableBatching(false).create();
        for (int i = 0; i < numMessages; ++i) {
            producer.newMessage().key(key).value(msg).send();
        }

        admin.topics().triggerCompaction(topic);
        boolean succeed = retryStrategically((test) -> {
            try {
                return LongRunningProcessStatus.Status.SUCCESS.equals(admin.topics().compactionStatus(topic).status);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 10, 200);

        Assert.assertTrue(succeed);


        final String newKey = "2";
        String newMsg = "test compaction msg v2";
        for (int i = 0; i < numMessages; ++i) {
            producer.newMessage().key(newKey).value(newMsg).send();
        }

        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .subscriptionName("test")
                .readCompacted(true)
                .startMessageId(MessageId.earliest)
                .create();

        int compactedMsgCount = 0;
        int nonCompactedMsgCount = 0;
        while (reader.hasMessageAvailable()) {
            Message<String> message = reader.readNext();
            if (key.equals(message.getKey()) && msg.equals(message.getValue())) {
                compactedMsgCount++;
            } else if (newKey.equals(message.getKey()) && newMsg.equals(message.getValue())) {
                nonCompactedMsgCount++;
            }
        }

        Assert.assertEquals(compactedMsgCount, 1);
        Assert.assertEquals(nonCompactedMsgCount, numMessages);
    }

    @Test
    public void testLastMessageIdForCompactedLedger() throws Exception {
        String topic = "persistent://my-property/use/my-ns/testLastMessageIdForCompactedLedger-" + UUID.randomUUID();
        final String key = "1";
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).enableBatching(false).create();
        final int numMessages = 10;
        final String msg = "test compaction msg";
        for (int i = 0; i < numMessages; ++i) {
            producer.newMessage().key(key).value(msg).send();
        }
        admin.topics().triggerCompaction(topic);
        boolean succeed = retryStrategically((test) -> {
            try {
                return LongRunningProcessStatus.Status.SUCCESS.equals(admin.topics().compactionStatus(topic).status);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 10, 200);

        Assert.assertTrue(succeed);

        PersistentTopicInternalStats stats0 = admin.topics().getInternalStats(topic);
        admin.topics().unload(topic);
        PersistentTopicInternalStats stats1 = admin.topics().getInternalStats(topic);
        // Make sure the ledger rollover has triggered.
        Assert.assertTrue(stats0.currentLedgerSize != stats1.currentLedgerSize);

        Optional<Topic> topicRef = pulsar.getBrokerService().getTopicIfExists(topic).get();
        Assert.assertTrue(topicRef.isPresent());
        PersistentTopic persistentTopic = (PersistentTopic) topicRef.get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl)persistentTopic.getManagedLedger();
        managedLedger.maybeUpdateCursorBeforeTrimmingConsumedLedger();

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(managedLedger.getCurrentLedgerEntries(), 0);
            Assert.assertTrue(managedLedger.getLastConfirmedEntry().getEntryId() != -1);
            Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), 1);
        });

        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .subscriptionName("test")
                .readCompacted(true)
                .startMessageId(MessageId.earliest)
                .create();

        Assert.assertTrue(reader.hasMessageAvailable());
        Message<String> received = reader.readNext();
        Assert.assertEquals(msg, received.getValue());
        MessageId messageId = ((ReaderImpl<String>) reader).getConsumer().getLastMessageId();
        Assert.assertEquals(messageId, received.getMessageId());
        Assert.assertFalse(reader.hasMessageAvailable());
        reader.close();

        // Unload the topic again to simulate entry ID with -1 after all data has been compacted.
        admin.topics().unload(topic);
        PersistentTopicInternalStats stats2 = admin.topics().getInternalStats(topic);
        Assert.assertTrue(stats2.lastConfirmedEntry.endsWith(":-1"));
        Assert.assertTrue(stats2.compactedLedger.ledgerId > 0);

        reader = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .subscriptionName("test")
                .readCompacted(true)
                .startMessageId(MessageId.earliest)
                .create();
        Assert.assertTrue(reader.hasMessageAvailable());
        reader.readNext();
        Assert.assertFalse(reader.hasMessageAvailable());
    }
}
