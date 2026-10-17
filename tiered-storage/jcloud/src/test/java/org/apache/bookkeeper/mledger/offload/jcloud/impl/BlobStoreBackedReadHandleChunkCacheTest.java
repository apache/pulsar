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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.LedgerOffloaderStatsDisable;
import org.apache.bookkeeper.mledger.OffloadedReadCacheConfig;
import org.apache.bookkeeper.mledger.offload.jcloud.JCloudLedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.testng.annotations.Test;

/** Reads of offloaded ledgers through read handles that share an {@link OffloadedDataChunkCache}. */
public class BlobStoreBackedReadHandleChunkCacheTest extends BlobStoreManagedLedgerOffloaderBase {
    // Small blocks, with padding at their ends, and chunks that entries straddle
    private static final int BLOCK_SIZE = 4096;
    private static final int READ_BUFFER_SIZE = 1000;

    BlobStoreBackedReadHandleChunkCacheTest() throws Exception {
        super();
    }

    private BlobStoreManagedLedgerOffloader offloader(OffloadedDataChunkCache cache) throws IOException {
        Map<String, String> settings = new HashMap<>();
        settings.put(TieredStorageConfiguration.OFFLOADER_PROPERTY_PREFIX + "MaxBlockSizeInBytes",
                Integer.toString(BLOCK_SIZE));
        settings.put(TieredStorageConfiguration.OFFLOADER_PROPERTY_PREFIX + "ReadBufferSizeInBytes",
                Integer.toString(READ_BUFFER_SIZE));
        TieredStorageConfiguration configuration = getConfiguration(BUCKET, settings);
        return BlobStoreManagedLedgerOffloader.create(configuration, new HashMap<>(), scheduler, scheduler,
                LedgerOffloaderStatsDisable.INSTANCE, entryOffsetsCache, cache);
    }

    private static void assertSameEntries(ReadHandle expected, ReadHandle actual, long first, long last)
            throws Exception {
        try (LedgerEntries expectedEntries = expected.read(first, last);
             LedgerEntries actualEntries = actual.read(first, last)) {
            Iterator<LedgerEntry> expectedIterator = expectedEntries.iterator();
            Iterator<LedgerEntry> actualIterator = actualEntries.iterator();
            while (expectedIterator.hasNext()) {
                LedgerEntry expectedEntry = expectedIterator.next();
                LedgerEntry actualEntry = actualIterator.next();
                assertThat(actualEntry.getEntryId()).isEqualTo(expectedEntry.getEntryId());
                assertThat(actualEntry.getEntryBuffer()).isEqualTo(expectedEntry.getEntryBuffer());
            }
            assertThat(actualIterator.hasNext()).isFalse();
        }
    }

    @Test
    public void readsEntriesAcrossChunksAndBlocksForCursorsAtDifferentPositions() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1 << 20, 60, 1, 2);
        @Cleanup
        ReadHandle toWrite = buildReadHandle(BLOCK_SIZE, 5);
        @Cleanup
        BlobStoreManagedLedgerOffloader offloader = offloader(cache);
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();
        long lastEntry = toWrite.getLastAddConfirmed();

        @Cleanup
        ReadHandle handle = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        // two cursors on the same handle, the second half the ledger ahead, reading 7 entries in turn
        long first = 0;
        long second = lastEntry / 2;
        while (first <= lastEntry || second <= lastEntry) {
            if (first <= lastEntry) {
                long last = Math.min(first + 6, lastEntry);
                assertSameEntries(toWrite, handle, first, last);
                first = last + 1;
            }
            if (second <= lastEntry) {
                long last = Math.min(second + 6, lastEntry);
                assertSameEntries(toWrite, handle, second, last);
                second = last + 1;
            }
        }
        // the whole ledger, once more, from the cache
        long fetches = cache.fetches();
        assertSameEntries(toWrite, handle, 0, lastEntry);
        assertThat(cache.fetches()).isEqualTo(fetches);
    }

    @Test
    public void readHandlesShareTheChunksAndReleaseThemWhenClosed() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1 << 20, 60, 0, 1);
        @Cleanup
        ReadHandle toWrite = buildReadHandle(BLOCK_SIZE, 3);
        @Cleanup
        BlobStoreManagedLedgerOffloader offloader = offloader(cache);
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();
        long lastEntry = toWrite.getLastAddConfirmed();

        ReadHandle first = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertSameEntries(toWrite, first, 0, lastEntry);
        long fetches = cache.fetches();
        assertThat(fetches).isGreaterThan(0);
        ReadHandle second = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertSameEntries(toWrite, second, lastEntry / 3, lastEntry);
        // the second handle read what the first had fetched
        assertThat(cache.fetches()).isEqualTo(fetches);

        first.closeAsync().get();
        second.closeAsync().get();
        // with the handles closed, only the cache holds chunks
        cache.clear();
        assertThat(cache.liveChunks()).isZero();
    }

    @Test
    public void theFactorysOffloadersShareOneCacheUntilItIsClosed() throws Exception {
        JCloudLedgerOffloaderFactory factory = new JCloudLedgerOffloaderFactory();
        Properties properties = new Properties();
        properties.putAll(getConfiguration(BUCKET).getConfigProperties());
        OffloadPoliciesImpl policies = OffloadPoliciesImpl.create(properties);
        LedgerOffloaderStats stats = LedgerOffloaderStatsDisable.INSTANCE;
        OffloadedReadCacheConfig config = new OffloadedReadCacheConfig(1 << 20, 60, 1, 1);
        assertThat(factory.create(policies, new HashMap<>(), null, scheduler, scheduler, stats, config)).isNotNull();
        // another caller's settings don't replace the cache's
        assertThat(factory.create(policies, new HashMap<>(), scheduler, stats)).isNotNull();
        factory.close();
        assertThatThrownBy(() -> factory.create(policies, new HashMap<>(), null, scheduler, scheduler, stats,
                config)).isInstanceOf(IOException.class);
    }
}
