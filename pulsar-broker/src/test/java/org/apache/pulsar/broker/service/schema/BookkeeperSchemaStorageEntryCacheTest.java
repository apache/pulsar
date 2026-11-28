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
package org.apache.pulsar.broker.service.schema;

import static org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage.bkException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import com.github.benmanes.caffeine.cache.Ticker;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 30000)
public class BookkeeperSchemaStorageEntryCacheTest {
    private static final byte[] SchemaData = new byte[] {1, 2, 3};
    private static final AtomicLong StoreIds = new AtomicLong();

    private static byte[] dataForVersion(int version) {
        return version == 0 ? SchemaData : ("schema-v" + version).getBytes(StandardCharsets.UTF_8);
    }

    private static final class Harness implements AutoCloseable {
        private final MetadataStoreExtended store;
        final BookKeeper bookKeeper = mock(BookKeeper.class);
        final LedgerHandle handle = mock(LedgerHandle.class);
        final AtomicInteger openCount = new AtomicInteger();
        private final AtomicInteger nextOpenRc = new AtomicInteger(BKException.Code.OK);
        private final LinkedBlockingQueue<PendingRead> pendingReads = new LinkedBlockingQueue<>();
        private final Map<Long, byte[]> storedEntries = new ConcurrentHashMap<>();
        private final Map<Long, LedgerHandle> readHandles = new ConcurrentHashMap<>();
        final BookkeeperSchemaStorage storage;

        Harness(int versions) throws Exception {
            this(versions, false, Ticker.systemTicker());
        }

        Harness(int versions, boolean latestOnlyLocator) throws Exception {
            this(versions, latestOnlyLocator, Ticker.systemTicker());
        }

        Harness(Ticker cacheTicker) throws Exception {
            this(1, false, cacheTicker);
        }

        private Harness(int versions, boolean latestOnlyLocator, Ticker cacheTicker) throws Exception {
            String metadataUrl = "memory:schema-entry-cache-" + StoreIds.incrementAndGet();
            store = MetadataStoreExtended.create(metadataUrl, MetadataStoreConfig.builder().build());

            when(handle.getId()).thenReturn(1L);
            stubReadCapture(handle);
            stubImmediateClose(handle);
            readHandles.put(1L, handle);

            List<IndexEntry> indexes = new ArrayList<>(versions);
            for (int version = 0; version < versions; version++) {
                SchemaEntry schemaEntry = new SchemaEntry().setSchemaData(dataForVersion(version));
                for (IndexEntry previous : indexes) {
                    schemaEntry.addIndex().copyFrom(previous);
                }
                storedEntries.put(version + 1L, schemaEntry.toByteArray());

                IndexEntry index = new IndexEntry();
                index.setVersion(version);
                index.setHash(new byte[0]);
                index.setPosition().setLedgerId(version + 1L).setEntryId(0L);
                indexes.add(index);
            }

            doAnswer(invocation -> {
                long ledgerId = invocation.getArgument(0);
                int rc = nextOpenRc.getAndSet(BKException.Code.OK);
                LedgerHandle ledgerHandle = rc == BKException.Code.OK
                        ? readHandles.computeIfAbsent(ledgerId, id -> {
                            LedgerHandle newHandle = mock(LedgerHandle.class);
                            when(newHandle.getId()).thenReturn(id);
                            stubReadCapture(newHandle);
                            stubImmediateClose(newHandle);
                            return newHandle;
                        }) : null;
                openCount.incrementAndGet();
                AsyncCallback.OpenCallback callback = invocation.getArgument(3);
                callback.openComplete(rc, ledgerHandle, invocation.getArgument(4));
                return null;
            }).when(bookKeeper).asyncOpenLedger(anyLong(), any(), any(), any(), isNull(), eq(true));

            SchemaLocator locator = new SchemaLocator();
            if (latestOnlyLocator) {
                locator.addIndex().copyFrom(indexes.get(indexes.size() - 1));
            } else {
                for (IndexEntry index : indexes) {
                    locator.addIndex().copyFrom(index);
                }
            }
            IndexEntry latest = indexes.get(indexes.size() - 1);
            locator.setInfo().copyFrom(latest);
            store.put("/schemas/test", locator.toByteArray(), Optional.empty()).get(5, TimeUnit.SECONDS);

            BookKeeperClientFactory clientFactory = mock(BookKeeperClientFactory.class);
            when(clientFactory.create(any(), eq(store), isNull(), eq(Optional.empty()), isNull()))
                    .thenReturn(CompletableFuture.completedFuture(bookKeeper));
            PulsarService pulsar = mock(PulsarService.class);
            when(pulsar.getLocalMetadataStore()).thenReturn(store);
            when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
            when(pulsar.getBookKeeperClientFactory()).thenReturn(clientFactory);
            storage = new BookkeeperSchemaStorage(pulsar, cacheTicker);
            storage.start();
        }

        void failNextOpen(int rc) {
            nextOpenRc.set(rc);
        }

        private void stubReadCapture(LedgerHandle ledgerHandle) {
            doAnswer(invocation -> {
                pendingReads.add(new PendingRead(invocation.getArgument(2), ledgerHandle));
                return null;
            }).when(ledgerHandle).asyncReadEntries(eq(0L), eq(0L), any(), isNull());
        }

        private void stubImmediateClose(LedgerHandle ledgerHandle) {
            doAnswer(invocation -> {
                AsyncCallback.CloseCallback callback = invocation.getArgument(0);
                callback.closeComplete(BKException.Code.OK, ledgerHandle, null);
                return null;
            }).when(ledgerHandle).asyncClose(any(), isNull());
        }

        void completePendingRead(int rc) {
            PendingRead pendingRead;
            try {
                pendingRead = pendingReads.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            if (pendingRead == null) {
                throw new IllegalStateException("No pending BookKeeper read to complete");
            }
            byte[] storedEntry = storedEntries.get(pendingRead.handle().getId());
            if (storedEntry == null) {
                throw new IllegalStateException("No stored entry for ledger " + pendingRead.handle().getId());
            }
            LedgerEntry entry = mock(LedgerEntry.class);
            when(entry.getEntry()).thenReturn(storedEntry);
            pendingRead.callback().readComplete(rc, pendingRead.handle(),
                    Collections.enumeration(List.of(entry)), null);
        }

        @Override
        public void close() throws Exception {
            storage.close();
            store.close();
        }
    }

    private record PendingRead(AsyncCallback.ReadCallback callback, LedgerHandle handle) {
    }

    @Test
    public void testVersionedReadCacheSharesHistoryAndTargetLoads() throws Exception {
        try (Harness harness = new Harness(3)) {
            List<CompletableFuture<StoredSchema>> firstHistory =
                    harness.storage.getAllForVersionedRead("test").get(5, TimeUnit.SECONDS);
            CompletableFuture<StoredSchema> target =
                    harness.storage.get("test", new LongSchemaVersion(0));
            List<CompletableFuture<StoredSchema>> secondHistory =
                    harness.storage.getAllForVersionedRead("test").get(5, TimeUnit.SECONDS);

            assertFalse(firstHistory.get(0).isDone());
            assertFalse(target.isDone());
            assertFalse(secondHistory.get(0).isDone());

            completeReadsAndAssertSchemas(harness, firstHistory);
            assertSchemas(secondHistory);
            StoredSchema targetSchema = target.get(5, TimeUnit.SECONDS);
            StoredSchema historySchema = firstHistory.get(0).get(5, TimeUnit.SECONDS);
            assertEquals(targetSchema.data, dataForVersion(0));
            assertNotSame(historySchema.data, targetSchema.data);

            historySchema.data[0] = 99;
            assertEquals(targetSchema.data, dataForVersion(0));
            for (int version = 0; version < 3; version++) {
                assertEquals(harness.storage.get("test", new LongSchemaVersion(version))
                        .get(5, TimeUnit.SECONDS).data, dataForVersion(version));
            }
            assertEquals(harness.openCount.get(), 3);
        }
    }

    @Test
    public void testExpiredFailedVersionedReadIsRetried() throws Exception {
        AtomicLong tickerNanos = new AtomicLong();
        try (Harness harness = new Harness(tickerNanos::get)) {
            CompletableFuture<StoredSchema> initial =
                    harness.storage.get("test", new LongSchemaVersion(0));
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(initial.get(5, TimeUnit.SECONDS).data, SchemaData);

            tickerNanos.addAndGet(TimeUnit.MINUTES.toNanos(59));
            assertEquals(harness.storage.get("test", new LongSchemaVersion(0))
                    .get(5, TimeUnit.SECONDS).data, SchemaData);
            assertEquals(harness.openCount.get(), 1);

            tickerNanos.addAndGet(TimeUnit.MINUTES.toNanos(2));
            harness.failNextOpen(BKException.Code.ReadException);
            CompletableFuture<StoredSchema> failed =
                    harness.storage.get("test", new LongSchemaVersion(0));
            Throwable error = failed.handle((__, ex) -> FutureUtil.unwrapCompletionException(ex))
                    .get(5, TimeUnit.SECONDS);
            assertEquals(error.getMessage(), bkException("Failed to open ledger",
                    BKException.Code.ReadException, 1, -1, false).getMessage());

            CompletableFuture<StoredSchema> retry =
                    harness.storage.get("test", new LongSchemaVersion(0));
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(retry.get(5, TimeUnit.SECONDS).data, SchemaData);
            assertEquals(harness.openCount.get(), 3);
        }
    }

    @Test
    public void testNestedHistoryLookupOnlyCachesFinalVersionPayload() throws Exception {
        try (Harness harness = new Harness(3, true)) {
            CompletableFuture<StoredSchema> first =
                    harness.storage.get("test", new LongSchemaVersion(0));
            harness.completePendingRead(BKException.Code.OK);
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(first.get(5, TimeUnit.SECONDS).data, dataForVersion(0));
            assertEquals(harness.openCount.get(), 2);

            CompletableFuture<StoredSchema> second =
                    harness.storage.get("test", new LongSchemaVersion(0));
            // The checkpoint is read again, while the final version-0 payload is cached.
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(second.get(5, TimeUnit.SECONDS).data, dataForVersion(0));
            assertEquals(harness.openCount.get(), 3);
        }
    }

    @Test
    public void testLatestAndRawGetAllBypassVersionedReadCache() throws Exception {
        try (Harness harness = new Harness(3)) {
            CompletableFuture<StoredSchema> versioned =
                    harness.storage.get("test", new LongSchemaVersion(0));
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(versioned.get(5, TimeUnit.SECONDS).data, dataForVersion(0));

            CompletableFuture<StoredSchema> firstLatest =
                    harness.storage.get("test", SchemaVersion.Latest);
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(firstLatest.get(5, TimeUnit.SECONDS).data, dataForVersion(2));

            CompletableFuture<StoredSchema> secondLatest =
                    harness.storage.get("test", SchemaVersion.Latest);
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(secondLatest.get(5, TimeUnit.SECONDS).data, dataForVersion(2));
            assertEquals(harness.openCount.get(), 3);

            assertRawGetAllReadsEveryVersionFromBookKeeper(harness);
            assertEquals(harness.openCount.get(), 6);

            assertRawGetAllReadsEveryVersionFromBookKeeper(harness);
            assertEquals(harness.openCount.get(), 9);

            assertEquals(harness.storage.get("test", new LongSchemaVersion(0))
                    .get(5, TimeUnit.SECONDS).data, dataForVersion(0));
            assertEquals(harness.openCount.get(), 9);
        }
    }

    private static void assertRawGetAllReadsEveryVersionFromBookKeeper(Harness harness) throws Exception {
        List<CompletableFuture<StoredSchema>> schemas = harness.storage.getAll("test")
                .get(5, TimeUnit.SECONDS);
        completeReadsAndAssertSchemas(harness, schemas);
    }

    private static void completeReadsAndAssertSchemas(
            Harness harness, List<CompletableFuture<StoredSchema>> schemas) throws Exception {
        assertEquals(schemas.size(), 3);
        for (int version = 0; version < 3; version++) {
            harness.completePendingRead(BKException.Code.OK);
        }
        assertSchemas(schemas);
    }

    private static void assertSchemas(List<CompletableFuture<StoredSchema>> schemas) throws Exception {
        assertEquals(schemas.size(), 3);
        for (int version = 0; version < 3; version++) {
            StoredSchema schema = schemas.get(version).get(5, TimeUnit.SECONDS);
            assertEquals(((LongSchemaVersion) schema.version).getVersion(), version);
            assertEquals(schema.data, dataForVersion(version));
        }
    }
}
