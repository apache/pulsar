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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class ReadOnlyEntryCacheOwnershipTest extends MockedBookKeeperTestCase {
    private final List<ReadOnlyManagedLedgerImpl> readers = new ArrayList<>();

    @Override
    protected void initManagedLedgerFactoryConfig(ManagedLedgerFactoryConfig config) {
        super.initManagedLedgerFactoryConfig(config);
        config.setCacheEvictionIntervalMs(TimeUnit.MINUTES.toMillis(1));
        config.setCacheEvictionTimeThresholdMillis(TimeUnit.MINUTES.toMillis(1));
        config.setCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(0);
        config.setCacheEvictionExtendTTLOfRecentlyAccessed(false);
    }

    @Override
    protected void setUpTestCase() {
        readers.clear();
    }

    @Override
    protected void cleanUpTestCase() throws Exception {
        for (ReadOnlyManagedLedgerImpl reader : readers) {
            reader.close();
        }
    }

    private RangeEntryCacheManagerImpl manager() {
        return (RangeEntryCacheManagerImpl) factory.getEntryCacheManager();
    }

    private ManagedLedgerImpl writer(String name) throws Exception {
        ManagedLedgerImpl writer = (ManagedLedgerImpl) factory.open(name,
                defaultConfig().setRetentionTime(1, TimeUnit.DAYS));
        writer.addEntry(new byte[] {1});
        return writer;
    }

    private ReadOnlyManagedLedgerImpl reader(String name, ManagedLedgerConfig config) throws Exception {
        ReadOnlyManagedLedgerImpl reader = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                config, executor, name);
        readers.add(reader);
        reader.initialize().get(5, TimeUnit.SECONDS);
        return reader;
    }

    private static void insert(EntryCache cache, Position position, int length) {
        EntryImpl entry = EntryImpl.create(position.getLedgerId(), position.getEntryId(), new byte[length]);
        try {
            assertThat(cache.insert(entry)).isTrue();
        } finally {
            entry.release();
        }
    }

    @Test
    public void testReadOnlyCloseReleasesItsCacheWithoutWritableOwner() throws Exception {
        ManagedLedgerImpl writer = writer("read-only-cache-only-owner");
        String name = writer.getName();
        ManagedLedgerConfig config = writer.getConfig();
        writer.close();
        ReadOnlyManagedLedgerImpl reader = reader(name, config);
        EntryCache cache = manager().getEntryCache(reader);
        assertThat(manager().getReadOnlyEntryCacheCount()).isEqualTo(1);
        Position position = PositionFactory.create(123, 0);
        insert(cache, position, 17);
        assertThat(manager().getSize()).isEqualTo(17);
        assertThat(factory.getMbean().getCacheEntriesCount()).isEqualTo(1);

        reader.close();

        assertThat(manager().getReadOnlyEntryCacheCount()).isZero();
        assertThat(cache.getSize()).isZero();
        assertThat(manager().getSize()).isZero();
        assertThat(factory.getMbean().getCacheEntriesCount()).isZero();
        EntryImpl lateEntry = EntryImpl.create(123, 1, new byte[9]);
        ByteBuf lateData = lateEntry.getDataBuffer();
        try {
            assertThat(cache.insert(lateEntry)).isFalse();
        } finally {
            lateEntry.release();
        }
        assertThat(lateData.refCnt()).isZero();
        assertThat(manager().getSize()).isZero();
    }

    @Test
    public void testFactoryShutdownDisposesCacheAndRejectsLateInsertions() throws Exception {
        ManagedLedgerImpl writer = writer("cache-after-factory-shutdown");
        EntryCache cache = manager().getEntryCache(writer);
        insert(cache, PositionFactory.create(writer.getLastConfirmedEntry().getLedgerId(), 100), 16);

        factory.shutdownAsync().get(5, TimeUnit.SECONDS);

        assertThat(cache.getSize()).isZero();
        assertThat(manager().getSize()).isZero();
        EntryImpl lateEntry = EntryImpl.create(writer.getLastConfirmedEntry().getLedgerId(), 101, new byte[8]);
        try {
            assertThat(cache.insert(lateEntry)).isFalse();
            assertThat(manager().getSize()).isZero();
            assertThat(manager().getEntryCache(writer)).isInstanceOf(EntryCacheDisabled.class);
        } finally {
            lateEntry.release();
        }
    }

    @Test
    public void testSameNameReadersAndWriterHaveIndependentCaches() throws Exception {
        ManagedLedgerImpl writer = writer("independent-read-only-entry-caches");
        ReadOnlyManagedLedgerImpl first = reader(writer.getName(), writer.getConfig());
        ReadOnlyManagedLedgerImpl second = reader(writer.getName(), writer.getConfig());
        EntryCache writerCache = manager().getEntryCache(writer);
        EntryCache firstCache = manager().getEntryCache(first);
        EntryCache secondCache = manager().getEntryCache(second);
        assertThat(firstCache).isNotSameAs(writerCache).isNotSameAs(secondCache);
        assertThat(secondCache).isNotSameAs(writerCache);
        assertThat(((RangeEntryCacheImpl) firstCache).getManagedLedger()).isSameAs(first);
        assertThat(((RangeEntryCacheImpl) secondCache).getManagedLedger()).isSameAs(second);
        manager().clear();
        Position position = PositionFactory.create(123, 0);
        insert(writerCache, position, 5);
        insert(firstCache, position, 4);
        insert(secondCache, position, 6);
        assertThat(manager().getSize()).isEqualTo(15);

        firstCache.invalidateAllEntries(position.getLedgerId());

        assertThat(firstCache.getSize()).isZero();
        assertThat(writerCache.getSize()).isEqualTo(5);
        assertThat(secondCache.getSize()).isEqualTo(6);
        assertThat(manager().getSize()).isEqualTo(11);
        insert(firstCache, position, 7);

        writer.close();

        assertThat(writerCache.getSize()).isZero();
        assertThat(firstCache.getSize()).isEqualTo(7);
        assertThat(secondCache.getSize()).isEqualTo(6);
        assertThat(manager().getReadOnlyEntryCacheCount()).isEqualTo(2);
        assertThat(manager().getSize()).isEqualTo(13);

        first.close();

        assertThat(firstCache.getSize()).isZero();
        assertThat(secondCache.hasEntry(position)).isTrue();
        assertThat(manager().getReadOnlyEntryCacheCount()).isEqualTo(1);
        assertThat(manager().getSize()).isEqualTo(6);
        second.close();
        assertThat(manager().getReadOnlyEntryCacheCount()).isZero();
        assertThat(manager().getSize()).isZero();
        assertThat(factory.getMbean().getCacheEntriesCount()).isZero();
    }

    @Test
    public void testGlobalEvictionAndClearIncludeReadOnlyCaches() throws Exception {
        ManagedLedgerImpl writer = writer("evict-read-only-entry-caches");
        ReadOnlyManagedLedgerImpl first = reader(writer.getName(), writer.getConfig());
        ReadOnlyManagedLedgerImpl second = reader(writer.getName(), writer.getConfig());
        EntryCache writerCache = manager().getEntryCache(writer);
        EntryCache firstCache = manager().getEntryCache(first);
        EntryCache secondCache = manager().getEntryCache(second);
        manager().clear();
        Position position = PositionFactory.create(123, 0);
        insert(writerCache, position, 20);
        insert(firstCache, position, 20);
        insert(secondCache, position, 20);
        assertThat(manager().getSize()).isEqualTo(60);
        manager().updateCacheSizeAndThreshold(50);
        manager().updateCacheEvictionWatermark(0.5);

        manager().doCacheEviction();

        assertThat(writerCache.getSize()).isZero();
        assertThat(firstCache.getSize()).isZero();
        assertThat(secondCache.getSize()).isEqualTo(20);
        assertThat(manager().getSize()).isEqualTo(20);

        manager().clear();

        assertThat(secondCache.getSize()).isZero();
        assertThat(manager().getSize()).isZero();
        assertThat(factory.getMbean().getCacheEntriesCount()).isZero();
        assertThat(manager().getReadOnlyEntryCacheCount()).isEqualTo(2);
        insert(firstCache, PositionFactory.create(123, 1), 8);
        assertThat(manager().getSize()).isEqualTo(8);
    }

    @Test
    public void testInsertionRacingWithOwnerCloseReleasesLateEntry() throws Exception {
        ManagedLedgerImpl writer = writer("read-only-entry-cache-late-insert");
        CountDownLatch insertionStarted = new CountDownLatch(1);
        CountDownLatch allowInsertion = new CountDownLatch(1);
        manager().setEntryLengthFunction((ledger, entry) -> {
            insertionStarted.countDown();
            try {
                if (!allowInsertion.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to complete cache insertion");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(error);
            }
            return entry.getLength();
        });
        ReadOnlyManagedLedgerImpl reader = reader(writer.getName(), writer.getConfig());
        EntryCache cache = manager().getEntryCache(reader);
        Position position = PositionFactory.create(123, 0);
        EntryImpl entry = EntryImpl.create(123, 0, new byte[11]);
        ByteBuf data = entry.getDataBuffer();
        CompletableFuture<Boolean> inserted = new CompletableFuture<>();
        Thread insertionThread = new Thread(() -> {
            try {
                inserted.complete(cache.insert(entry));
            } catch (Throwable error) {
                inserted.completeExceptionally(error);
            } finally {
                entry.release();
            }
        }, "read-only-entry-cache-insertion");
        try {
            insertionThread.start();
            assertThat(insertionStarted.await(5, TimeUnit.SECONDS)).isTrue();

            reader.close();

            assertThat(manager().getReadOnlyEntryCacheCount()).isZero();
            allowInsertion.countDown();
            assertThat(inserted.get(5, TimeUnit.SECONDS)).isFalse();
            insertionThread.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(insertionThread.isAlive()).isFalse();
            assertThat(data.refCnt()).isZero();
            assertThat(cache.hasEntry(position)).isFalse();
            assertThat(cache.getSize()).isZero();
            assertThat(manager().getSize()).isZero();
            assertThat(factory.getMbean().getCacheEntriesCount()).isZero();
        } finally {
            allowInsertion.countDown();
            insertionThread.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(insertionThread.isAlive()).isFalse();
        }
    }
}
