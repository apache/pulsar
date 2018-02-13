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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;


import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.impl.RawMessageImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CompactedTopicTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(CompactedTopicTest.class);

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

    /**
     * Build a compacted ledger, and return the id of the ledger, the position of the different
     * entries in the ledger, and a list of gaps, and the entry which should be returned after the gap.
     */
    private Triple<Long, List<Pair<MessageIdData,Long>>, List<Pair<MessageIdData,Long>>>
        buildCompactedLedger(BookKeeper bk, int seed, int count)
            throws Exception {
        Random r = new Random(seed);
        LedgerHandle lh = bk.createLedger(1, 1,
                                          Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                                          Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD);
        List<Pair<MessageIdData,Long>> positions = new ArrayList<>();
        List<Pair<MessageIdData,Long>> idsInGaps = new ArrayList<>();
        ByteBuf emptyBuffer = Unpooled.buffer(0);

        AtomicLong ledgerIds = new AtomicLong(10L);
        AtomicLong entryIds = new AtomicLong(0L);
        CompletableFuture.allOf(
                IntStream.range(0, count).mapToObj((i) -> {
                        List<MessageIdData> idsInGap = new ArrayList<MessageIdData>();
                        if (r.nextInt(10) == 1) {
                            long delta = r.nextInt(10) + 1;
                            idsInGap.add(MessageIdData.newBuilder()
                                         .setLedgerId(ledgerIds.get())
                                         .setEntryId(entryIds.get() + 1)
                                         .build());
                            ledgerIds.addAndGet(delta);
                            entryIds.set(0);
                        }
                        long delta = r.nextInt(5);
                        if (delta != 0) {
                            idsInGap.add(MessageIdData.newBuilder()
                                         .setLedgerId(ledgerIds.get())
                                         .setEntryId(entryIds.get() + 1)
                                         .build());
                        }
                        MessageIdData id = MessageIdData.newBuilder()
                            .setLedgerId(ledgerIds.get())
                            .setEntryId(entryIds.addAndGet(delta + 1)).build();
                        RawMessage m = new RawMessageImpl(id, emptyBuffer);

                        CompletableFuture<Void> f = new CompletableFuture<>();
                        lh.asyncAddEntry(m.serialize(),
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
                this.conf, null);

        Triple<Long, List<Pair<MessageIdData, Long>>, List<Pair<MessageIdData, Long>>> compactedLedgerData
            = buildCompactedLedger(bk, 0, 500);

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
        Collections.shuffle(positions);
        Collections.shuffle(idsInGaps);

        // Check ids we know are in compacted ledger
        for (Pair<MessageIdData, Long> p : positions) {
            PositionImpl pos = new PositionImpl(p.getLeft().getLedgerId(), p.getLeft().getEntryId());
            if (p.equals(lastPosition)) {
                Assert.assertEquals(CompactedTopicImpl.findStartPoint(pos, lastEntryId, cache).get(),
                                    Long.valueOf(CompactedTopicImpl.NEWER_THAN_COMPACTED));
            } else {
                Assert.assertEquals(CompactedTopicImpl.findStartPoint(pos, lastEntryId, cache).get(),
                            Long.valueOf(p.getRight() + 1));
            }
        }

        // Check ids we know are in the gaps of the compacted ledger
        for (Pair<MessageIdData, Long> gap : idsInGaps) {
            PositionImpl pos = new PositionImpl(gap.getLeft().getLedgerId(), gap.getLeft().getEntryId());
            Assert.assertEquals(CompactedTopicImpl.findStartPoint(pos, lastEntryId, cache).get(),
                                Long.valueOf(gap.getRight()));
        }
    }
}
