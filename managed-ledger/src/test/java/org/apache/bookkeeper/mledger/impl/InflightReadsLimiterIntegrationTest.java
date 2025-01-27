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
package org.apache.bookkeeper.mledger.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.cache.InflightReadsLimiter;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheManagerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class InflightReadsLimiterIntegrationTest extends MockedBookKeeperTestCase {

    @DataProvider
    public Object[][] readMissingCases() {
        return new Object[][]{
                {"missRight"},
                {"missLeft"},
                {"bothMiss"}
        };
    }

    @Test(dataProvider = "readMissingCases")
    public void testPreciseLimitation(String missingCase) throws Exception {
        final long start1 = 50;
        final long start2 = "missLeft".endsWith(missingCase) || "bothMiss".equals(missingCase) ? 30 : 50;
        final long end1 = 99;
        final long end2 = "missRight".endsWith(missingCase) || "bothMiss".equals(missingCase) ? 109 : 99;
        final HashSet<Long> secondReadEntries = new HashSet<>();
        if (start2 < start1) {
            secondReadEntries.add(start2);
        }
        if (end2 > end1) {
            secondReadEntries.add(end1 + 1);
        }
        final int readCount1 = (int) (end1 - start1 + 1);
        final int readCount2 = (int) (end2 - start2 + 1);

        final DefaultThreadFactory threadFactory = new DefaultThreadFactory(UUID.randomUUID().toString());
        final ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(100000);
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setCacheEvictionIntervalMs(3600 * 1000);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(1000_000);
        final ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        final ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        final RangeEntryCacheImpl entryCache = (RangeEntryCacheImpl) ml.entryCache;
        final RangeEntryCacheManagerImpl rangeEntryCacheManager =
                (RangeEntryCacheManagerImpl) factory.getEntryCacheManager();
        final InflightReadsLimiter limiter = rangeEntryCacheManager.getInflightReadsLimiter();
        final long totalCapacity =limiter.getRemainingBytes();
        // final ManagedCursorImpl c1 = (ManagedCursorImpl) ml.openCursor("c1");
        for (byte i = 1; i < 127; i++) {
            log.info("add entry: " + i);
            ml.addEntry(new byte[]{i});
        }
        // Evict cached entries.
        entryCache.evictEntries(ml.currentLedgerSize);
        Assert.assertEquals(entryCache.getSize(), 0);

        CountDownLatch readCompleteSignal1 = new CountDownLatch(1);
        CountDownLatch readCompleteSignal2 = new CountDownLatch(1);
        CountDownLatch firstReadingStarted = new CountDownLatch(1);
        LedgerHandle currentLedger = ml.currentLedger;
        LedgerHandle spyCurrentLedger = Mockito.spy(currentLedger);
        ml.currentLedger = spyCurrentLedger;
        Answer answer = invocation -> {
            long firstEntry = (long) invocation.getArguments()[0];
            log.info("reading entry: {}", firstEntry);
            if (firstEntry == start1) {
                // Wait 3s to make
                firstReadingStarted.countDown();
                readCompleteSignal1.await();
                Object res = invocation.callRealMethod();
                return res;
            } else if(secondReadEntries.contains(firstEntry)) {
                final CompletableFuture res = new CompletableFuture<>();
                threadFactory.newThread(() -> {
                    try {
                        readCompleteSignal2.await();
                        CompletableFuture<LedgerEntries> future =
                                (CompletableFuture<LedgerEntries>) invocation.callRealMethod();
                        future.thenAccept(v -> {
                            res.complete(v);
                        }).exceptionally(ex -> {
                            res.completeExceptionally(ex);
                            return null;
                        });
                    } catch (Throwable ex) {
                        res.completeExceptionally(ex);
                    }
                }).start();
                return res;
            } else {
                return invocation.callRealMethod();
            }
        };
        doAnswer(answer).when(spyCurrentLedger).readAsync(anyLong(), anyLong());
        doAnswer(answer).when(spyCurrentLedger).readUnconfirmedAsync(anyLong(), anyLong());

        // Initialize "entryCache.estimatedEntrySize" to the correct value.
        Object ctx = new Object();
        SimpleReadEntriesCallback cb0 = new SimpleReadEntriesCallback();
        entryCache.asyncReadEntry(spyCurrentLedger, 125, 125, true, cb0, ctx);
        cb0.entries.join();
        Long sizePerEntry1 = WhiteboxImpl.getInternalState(entryCache, "estimatedEntrySize");
        Assert.assertEquals(sizePerEntry1, 1);
        Awaitility.await().untilAsserted(() -> {
            long remainingBytes =limiter.getRemainingBytes();
            Assert.assertEquals(remainingBytes, totalCapacity);
        });
        log.info("remainingBytes 0: {}", limiter.getRemainingBytes());

        // Concurrency reading.

        SimpleReadEntriesCallback cb1 = new SimpleReadEntriesCallback();
        SimpleReadEntriesCallback cb2 = new SimpleReadEntriesCallback();
        threadFactory.newThread(() -> {
            entryCache.asyncReadEntry(spyCurrentLedger, start1, end1, true, cb1, ctx);
        }).start();
        threadFactory.newThread(() -> {
            try {
                firstReadingStarted.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            entryCache.asyncReadEntry(spyCurrentLedger, start2, end2, true, cb2, ctx);
        }).start();

        long bytesAcquired1 = calculateBytesSizeBeforeFirstReading(readCount1 + readCount2, 1);
        long remainingBytesExpected1 = totalCapacity - bytesAcquired1;
        log.info("acquired : {}", bytesAcquired1);
        log.info("remainingBytesExpected 0 : {}", remainingBytesExpected1);
        Awaitility.await().untilAsserted(() -> {
            log.info("remainingBytes 0: {}", limiter.getRemainingBytes());
            Assert.assertEquals(limiter.getRemainingBytes(), remainingBytesExpected1);
        });

        // Complete the read1.
        Thread.sleep(3000);
        readCompleteSignal1.countDown();
        cb1.entries.join();
        Long sizePerEntry2 = WhiteboxImpl.getInternalState(entryCache, "estimatedEntrySize");
        Assert.assertEquals(sizePerEntry2, 1);
        long bytesAcquired2 = calculateBytesSizeBeforeFirstReading(readCount2, 1);
        long remainingBytesExpected2 = totalCapacity - bytesAcquired2;
        log.info("acquired : {}", bytesAcquired2);
        log.info("remainingBytesExpected 1: {}", remainingBytesExpected2);
        Awaitility.await().untilAsserted(() -> {
            log.info("remainingBytes 1: {}", limiter.getRemainingBytes());
            Assert.assertEquals(limiter.getRemainingBytes(), remainingBytesExpected2);
        });

        readCompleteSignal2.countDown();
        cb2.entries.join();
        Long sizePerEntry3 = WhiteboxImpl.getInternalState(entryCache, "estimatedEntrySize");
        Assert.assertEquals(sizePerEntry3, 1);
        Awaitility.await().untilAsserted(() -> {
            long remainingBytes = limiter.getRemainingBytes();
            log.info("remainingBytes 2: {}", remainingBytes);
            Assert.assertEquals(remainingBytes, totalCapacity);
        });
        // cleanup
        ml.delete();
        factory.shutdown();
    }

    private long calculateBytesSizeBeforeFirstReading(int entriesCount, int perEntrySize) {
        return entriesCount * (perEntrySize + RangeEntryCacheImpl.BOOKKEEPER_READ_OVERHEAD_PER_ENTRY);
    }

    class SimpleReadEntriesCallback implements AsyncCallbacks.ReadEntriesCallback {

        CompletableFuture<List<Byte>> entries = new CompletableFuture<>();

        @Override
        public void readEntriesComplete(List<Entry> entriesRead, Object ctx) {
            List<Byte> list = new ArrayList<>(entriesRead.size());
            for (Entry entry : entriesRead) {
                byte b = entry.getDataBuffer().readByte();
                list.add(b);
                entry.release();
            }
            this.entries.complete(list);
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            this.entries.completeExceptionally(exception);
        }
    }
}
