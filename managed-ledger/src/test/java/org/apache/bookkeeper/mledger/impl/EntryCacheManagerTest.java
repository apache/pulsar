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
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheDisabled;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheManagerImpl;
import org.apache.bookkeeper.mledger.impl.cache.SharedEntryCacheManagerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EntryCacheManagerTest extends MockedBookKeeperTestCase {

    ManagedLedgerImpl ml1;
    ManagedLedgerImpl ml2;

    @DataProvider(name = "EntryCacheManagerClass")
    public static Object[][] primeNumbers() {
        return new Object[][]{
                {SharedEntryCacheManagerImpl.class.getName()},
                {RangeEntryCacheManagerImpl.class.getName()}};
    }


    @Override
    protected void setUpTestCase() throws Exception {
        OrderedScheduler executor = OrderedScheduler.newSchedulerBuilder().numThreads(1).build();

        ml1 = mock(ManagedLedgerImpl.class);
        when(ml1.getScheduledExecutor()).thenReturn(executor);
        when(ml1.getName()).thenReturn("cache1");
        when(ml1.getMbean()).thenReturn(new ManagedLedgerMBeanImpl(ml1));
        when(ml1.getExecutor()).thenReturn(super.executor);
        when(ml1.getFactory()).thenReturn(factory);

        ml2 = mock(ManagedLedgerImpl.class);
        when(ml2.getScheduledExecutor()).thenReturn(executor);
        when(ml2.getName()).thenReturn("cache2");
    }

    @Test(dataProvider = "EntryCacheManagerClass")
    public void simple(String entryCacheManagerClass) throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(10);
        config.setCacheEvictionWatermark(0.8);
        config.setEntryCacheManagerClassName(entryCacheManagerClass);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache(ml1);
        EntryCache cache2 = cacheManager.getEntryCache(ml2);

        cache1.insert(EntryImpl.create(1, 1, new byte[4]));
        cache1.insert(EntryImpl.create(1, 0, new byte[3]));

        assertTrue(cacheManager.getSize() > 0);

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);
        assertEquals(factory2.getMbean().getCacheMaxSize(), 10);
        assertTrue(factory2.getMbean().getCacheUsedSize() > 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 0.0);
        assertEquals(factory2.getMbean().getNumberOfCacheEvictions(), 0);

        cache2.insert(EntryImpl.create(2, 0, new byte[1]));
        cache2.insert(EntryImpl.create(2, 1, new byte[1]));
        cache2.insert(EntryImpl.create(2, 2, new byte[1]));

        assertTrue(cacheManager.getSize() > 0);

        // Next insert should trigger a cache eviction to force the size to 8
        // The algorithm should evict entries from cache1
        cache2.insert(EntryImpl.create(2, 3, new byte[1]));

        // Wait for eviction to be completed in background
        Thread.sleep(100);
        assertTrue(cacheManager.getSize() > 0);

        cacheManager.removeEntryCache("cache1");
        assertTrue(cacheManager.getSize() > 0);

        // Should remove 1 entry
        cache2.invalidateEntries(new PositionImpl(2, 1));
        assertTrue(cacheManager.getSize() > 0);

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);

        assertEquals(factory2.getMbean().getCacheMaxSize(), 10);
        assertTrue(factory2.getMbean().getCacheUsedSize() > 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 0.0);
    }

    @Test(dataProvider = "EntryCacheManagerClass")
    public void doubleInsert(String entryCacheManager) throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(10);
        config.setCacheEvictionWatermark(0.8);
        config.setEntryCacheManagerClassName(entryCacheManager);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache(ml1);

        assertTrue(cache1.insert(EntryImpl.create(1, 1, new byte[4])));
        assertTrue(cache1.insert(EntryImpl.create(1, 0, new byte[3])));

        assertTrue(cacheManager.getSize() > 0);

        assertFalse(cache1.insert(EntryImpl.create(1, 0, new byte[5])));

        assertTrue(cacheManager.getSize() > 0);
    }

    @Test
    public void cacheSizeUpdate() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(200);
        config.setCacheEvictionWatermark(0.8);
        config.setEntryCacheManagerClassName(RangeEntryCacheManagerImpl.class.getName());

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


    @Test(dataProvider = "EntryCacheManagerClass")
    public void cacheDisabled(String entryCacheManager) throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);
        config.setCacheEvictionWatermark(0.8);
        config.setEntryCacheManagerClassName(entryCacheManager);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache(ml1);
        EntryCache cache2 = cacheManager.getEntryCache(ml2);

        assertTrue(cache1 instanceof EntryCacheDisabled);
        assertTrue(cache2 instanceof EntryCacheDisabled);

        cache1.insert(EntryImpl.create(1, 1, new byte[4]));
        cache1.insert(EntryImpl.create(1, 0, new byte[3]));

        assertEquals(cache1.getSize(), 0);
        assertEquals(cacheManager.getSize(), 0);

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);
        assertEquals(factory2.getMbean().getCacheMaxSize(), 0);
        assertEquals(factory2.getMbean().getCacheUsedSize(), 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 0.0);
        assertEquals(factory2.getMbean().getNumberOfCacheEvictions(), 0);

        cache2.insert(EntryImpl.create(2, 0, new byte[1]));
        cache2.insert(EntryImpl.create(2, 1, new byte[1]));
        cache2.insert(EntryImpl.create(2, 2, new byte[1]));

        assertEquals(cache2.getSize(), 0);
        assertEquals(cacheManager.getSize(), 0);
    }

    @Test(dataProvider = "EntryCacheManagerClass")
    public void verifyNoCacheIfNoConsumer(String entryCacheManager) throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(7 * 10);
        config.setCacheEvictionWatermark(0.8);
        config.setEntryCacheManagerClassName(entryCacheManager);

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

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);
        assertEquals(factory2.getMbean().getCacheMaxSize(), 7 * 10);
        assertEquals(factory2.getMbean().getCacheUsedSize(), 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 0.0);
        assertEquals(factory2.getMbean().getNumberOfCacheEvictions(), 0);
    }

    @Test(dataProvider = "EntryCacheManagerClass")
    public void verifyHitsMisses(String entryCacheManager) throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(150);
        config.setCacheEvictionWatermark(0.8);
        config.setEntryCacheManagerClassName(entryCacheManager);
        config.setCacheEvictionIntervalMs(1000);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory2.getEntryCacheManager();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory2.open("ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ManagedCursorImpl c2 = (ManagedCursorImpl) ledger.openCursor("c2");

        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);
        assertTrue(factory2.getMbean().getCacheUsedSize() > 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 0.0);
        assertEquals(factory2.getMbean().getNumberOfCacheEvictions(), 0);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 10);
        entries.forEach(Entry::release);

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);
        assertTrue(factory2.getMbean().getCacheUsedSize() > 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 10.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 70.0);
        assertEquals(factory2.getMbean().getNumberOfCacheEvictions(), 0);

        ledger.deactivateCursor(c1);

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);
        assertTrue(factory2.getMbean().getCacheUsedSize() > 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 0.0);
        assertEquals(factory2.getMbean().getNumberOfCacheEvictions(), 0);

        entries = c2.readEntries(10);
        assertEquals(entries.size(), 10);

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);
        assertTrue(factory2.getMbean().getCacheUsedSize() > 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 10.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 70.0);
        assertEquals(factory2.getMbean().getNumberOfCacheEvictions(), 0);

        PositionImpl pos = (PositionImpl) entries.get(entries.size() - 1).getPosition();
        c2.setReadPosition(pos);
        ledger.discardEntriesFromCache(c2, pos);
        entries.forEach(Entry::release);

        factory2.getMbean().refreshStats(1, TimeUnit.SECONDS);
        assertTrue(factory2.getMbean().getCacheUsedSize() > 0);
        assertEquals(factory2.getMbean().getCacheHitsRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheMissesRate(), 0.0);
        assertEquals(factory2.getMbean().getCacheHitsThroughput(), 0.0);
        assertEquals(factory2.getMbean().getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void verifyTimeBasedEviction() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(1000);
        config.setCacheEvictionIntervalMs(10);
        config.setCacheEvictionTimeThresholdMillis(100);
        // This is only relevant for this specific implementation
        config.setEntryCacheManagerClassName(RangeEntryCacheManagerImpl.class.getName());

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

    @Test(dataProvider = "EntryCacheManagerClass")
    void entryCacheDisabledAsyncReadEntry(String entryCacheManager) throws Exception {
        ReadHandle lh = EntryCacheTest.getLedgerHandle();

        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);
        config.setEntryCacheManagerClassName(entryCacheManager);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);
        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        EntryCache entryCache = cacheManager.getEntryCache(ml1);

        final CountDownLatch counter = new CountDownLatch(1);
        entryCache.asyncReadEntry(lh, new PositionImpl(1L,1L), new AsyncCallbacks.ReadEntryCallback() {
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
