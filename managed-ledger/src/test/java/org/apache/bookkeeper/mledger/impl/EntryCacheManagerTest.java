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
package org.apache.bookkeeper.mledger.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EntryCacheManagerTest extends MockedBookKeeperTestCase {

    ManagedLedgerImpl ml1;
    ManagedLedgerImpl ml2;

    @Override
    protected void setUpTestCase() throws Exception {
        OrderedScheduler executor = OrderedScheduler.newSchedulerBuilder().numThreads(1).build();

        ml1 = mock(ManagedLedgerImpl.class);
        when(ml1.getScheduledExecutor()).thenReturn(executor);
        when(ml1.getName()).thenReturn("cache1");
        when(ml1.getMBean()).thenReturn(new ManagedLedgerMBeanImpl(ml1));
        when(ml1.getExecutor()).thenReturn(super.executor);

        ml2 = mock(ManagedLedgerImpl.class);
        when(ml2.getScheduledExecutor()).thenReturn(executor);
        when(ml2.getName()).thenReturn("cache2");
    }

    @Test
    public void simple() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(10);
        config.setCacheEvictionWatermark(0.8);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache(ml1);
        EntryCache cache2 = cacheManager.getEntryCache(ml2);

        cache1.insert(EntryImpl.create(1, 1, new byte[4]));
        cache1.insert(EntryImpl.create(1, 0, new byte[3]));

        assertEquals(cache1.getSize(), 7);
        assertEquals(cacheManager.getSize(), 7);

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMaxSize(), 10);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 7);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);

        cache2.insert(EntryImpl.create(2, 0, new byte[1]));
        cache2.insert(EntryImpl.create(2, 1, new byte[1]));
        cache2.insert(EntryImpl.create(2, 2, new byte[1]));

        assertEquals(cache2.getSize(), 3);
        assertEquals(cacheManager.getSize(), 10);

        // Next insert should trigger a cache eviction to force the size to 8
        // The algorithm should evict entries from cache1
        cache2.insert(EntryImpl.create(2, 3, new byte[1]));

        // Wait for eviction to be completed in background
        Thread.sleep(100);
        assertEquals(cacheManager.getSize(), 7);
        assertEquals(cache1.getSize(), 4);
        assertEquals(cache2.getSize(), 3);

        cacheManager.removeEntryCache("cache1");
        assertEquals(cacheManager.getSize(), 3);
        assertEquals(cache2.getSize(), 3);

        // Should remove 1 entry
        cache2.invalidateEntries(new PositionImpl(2, 1));
        assertEquals(cacheManager.getSize(), 2);
        assertEquals(cache2.getSize(), 2);

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);

        assertEquals(cacheManager.mlFactoryMBean.getCacheMaxSize(), 10);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 2);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 1);
    }

    @Test
    public void doubleInsert() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(10);
        config.setCacheEvictionWatermark(0.8);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache(ml1);

        assertTrue(cache1.insert(EntryImpl.create(1, 1, new byte[4])));
        assertTrue(cache1.insert(EntryImpl.create(1, 0, new byte[3])));

        assertEquals(cache1.getSize(), 7);
        assertEquals(cacheManager.getSize(), 7);

        assertFalse(cache1.insert(EntryImpl.create(1, 0, new byte[5])));

        assertEquals(cache1.getSize(), 7);
        assertEquals(cacheManager.getSize(), 7);
    }

    @Test
    public void cacheSizeUpdate() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(200);
        config.setCacheEvictionWatermark(0.8);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache(ml1);
        List<EntryImpl> entries = new ArrayList<>();

        // Put entries into cache.
        for (int i = 0; i < 20; i++) {
            entries.add(EntryImpl.create(1, i, new byte[i + 1]));
            assertTrue(cache1.insert(entries.get(i)));
        }
        assertEquals(210, cacheManager.getSize());

        // Consume some entries.
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            if (random.nextBoolean()) {
                (entries.get(i).getDataBuffer()).readBytes(new byte[entries.get(i).getDataBuffer().readableBytes()]);
            }
        }

        cacheManager.removeEntryCache(ml1.getName());
        assertTrue(cacheManager.getSize() > 0);
    }


    @Test
    public void cacheDisabled() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);
        config.setCacheEvictionWatermark(0.8);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache(ml1);
        EntryCache cache2 = cacheManager.getEntryCache(ml2);

        assertTrue(cache1 instanceof EntryCacheManager.EntryCacheDisabled);
        assertTrue(cache2 instanceof EntryCacheManager.EntryCacheDisabled);

        cache1.insert(EntryImpl.create(1, 1, new byte[4]));
        cache1.insert(EntryImpl.create(1, 0, new byte[3]));

        assertEquals(cache1.getSize(), 0);
        assertEquals(cacheManager.getSize(), 0);

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMaxSize(), 0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);

        cache2.insert(EntryImpl.create(2, 0, new byte[1]));
        cache2.insert(EntryImpl.create(2, 1, new byte[1]));
        cache2.insert(EntryImpl.create(2, 2, new byte[1]));

        assertEquals(cache2.getSize(), 0);
        assertEquals(cacheManager.getSize(), 0);
    }

    @Test
    public void verifyNoCacheIfNoConsumer() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(7 * 10);
        config.setCacheEvictionWatermark(0.8);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory2.open("ledger");
        EntryCache cache1 = ledger.entryCache;

        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }

        assertEquals(cache1.getSize(), 0);
        assertEquals(cacheManager.getSize(), 0);

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMaxSize(), 7 * 10);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void verifyHitsMisses() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(7 * 10);
        config.setCacheEvictionWatermark(0.8);
        config.setCacheEvictionFrequency(1);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory2.open("ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ManagedCursorImpl c2 = (ManagedCursorImpl) ledger.openCursor("c2");

        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 70);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 10);
        entries.forEach(e -> e.release());

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 70);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 10.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 70.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);

        ledger.deactivateCursor(c1);

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 70);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);

        entries = c2.readEntries(10);
        assertEquals(entries.size(), 10);

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 70);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 10.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 70.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);

        PositionImpl pos = (PositionImpl) entries.get(entries.size() - 1).getPosition();
        c2.setReadPosition(pos);
        ledger.discardEntriesFromCache(c2, pos);
        entries.forEach(e -> e.release());

        cacheManager.mlFactoryMBean.refreshStats(1, TimeUnit.SECONDS);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 7);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void verifyTimeBasedEviction() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(1000);
        config.setCacheEvictionFrequency(100);
        config.setCacheEvictionTimeThresholdMillis(100);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("test");
        ManagedCursor c1 = ledger.openCursor("c1");
        c1.setActive();
        ManagedCursor c2 = ledger.openCursor("c2");
        c2.setActive();

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        assertEquals(cacheManager.getSize(), 0);

        EntryCache cache = cacheManager.getEntryCache(ledger);
        assertEquals(cache.getSize(), 0);

        ledger.addEntry(new byte[4]);
        ledger.addEntry(new byte[3]);

        // Cache eviction should happen every 10 millis and clean all the entries older that 100ms
        Thread.sleep(1000);

        c1.close();
        c2.close();

        assertEquals(cacheManager.getSize(), 0);
        assertEquals(cache.getSize(), 0);
    }

    @Test(timeOut = 5000)
    void entryCacheDisabledAsyncReadEntry() throws Exception {
        ReadHandle lh = EntryCacheTest.getLedgerHandle();

        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);
        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        EntryCache entryCache = cacheManager.getEntryCache(ml1);

        final CountDownLatch counter = new CountDownLatch(1);
        entryCache.asyncReadEntry(lh, new PositionImpl(1L ,1L), new AsyncCallbacks.ReadEntryCallback() {
            public void readEntryComplete(Entry entry, Object ctx) {
                Assert.assertNotEquals(entry, null);
                entry.release();
                counter.countDown();
            }

            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
                counter.countDown();
            }
        }, null);
        counter.await();

        verify(lh).readAsync(anyLong(), anyLong());
    }

}
