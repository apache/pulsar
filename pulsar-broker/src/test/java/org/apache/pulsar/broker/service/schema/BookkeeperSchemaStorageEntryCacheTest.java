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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import java.nio.charset.StandardCharsets;
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
import org.apache.bookkeeper.client.api.CreateBuilder;
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

    private static byte[] dataForVersion(int version) {
        return version == 0 ? SchemaData : ("schema-v" + version).getBytes(StandardCharsets.UTF_8);
    }

    private static final class Harness implements AutoCloseable {
        final MetadataStoreExtended store;
        final BookKeeper bookKeeper = mock(BookKeeper.class);
        final LedgerHandle handle = mock(LedgerHandle.class);
        final AtomicInteger openCount = new AtomicInteger();
        private final LinkedBlockingQueue<PendingRead> pendingReads = new LinkedBlockingQueue<>();
        // Serialized SchemaEntry bytes served back when a ledger is read. Entries written through
        // put() are recorded here, so reads return exactly what was written.
        private final Map<Long, byte[]> writtenEntries = new ConcurrentHashMap<>();
        private final Map<Long, LedgerHandle> readHandles = new ConcurrentHashMap<>();
        private final AtomicLong nextCreatedLedgerId = new AtomicLong(1000);
        BookkeeperSchemaStorage storage;

        Harness() throws Exception {
            this(1);
        }

        Harness(int versions) throws Exception {
            this(new ServiceConfiguration(), versions);
        }

        Harness(ServiceConfiguration config, int versions) throws Exception {
            store = MetadataStoreExtended.create("memory:schema-entry-cache",
                    MetadataStoreConfig.builder().build());
            when(handle.getId()).thenReturn(1L);
            stubReadCapture(handle);
            stubImmediateClose(handle);
            readHandles.put(1L, handle);
            for (int version = 0; version < versions; version++) {
                writtenEntries.put(version + 1L,
                        new SchemaEntry().setSchemaData(dataForVersion(version)).toByteArray());
            }

            doAnswer(invocation -> {
                long ledgerId = invocation.getArgument(0);
                LedgerHandle ledgerHandle = readHandles.computeIfAbsent(ledgerId, id -> {
                    LedgerHandle newHandle = mock(LedgerHandle.class);
                    when(newHandle.getId()).thenReturn(id);
                    stubReadCapture(newHandle);
                    stubImmediateClose(newHandle);
                    return newHandle;
                });
                openCount.incrementAndGet();
                AsyncCallback.OpenCallback callback = invocation.getArgument(3);
                callback.openComplete(BKException.Code.OK, ledgerHandle, invocation.getArgument(4));
                return null;
            }).when(bookKeeper).asyncOpenLedger(anyLong(), any(), any(), any(), isNull(), eq(true));
            doAnswer(invocation -> {
                AsyncCallback.DeleteCallback callback = invocation.getArgument(1);
                callback.deleteComplete(BKException.Code.OK, invocation.getArgument(2));
                return null;
            }).when(bookKeeper).asyncDeleteLedger(anyLong(), any(), isNull());
            when(bookKeeper.newCreateLedgerOp()).thenAnswer(invocation -> newWriteHandleBuilder());

            SchemaLocator locator = new SchemaLocator();
            IndexEntry latest = null;
            for (int version = 0; version < versions; version++) {
                IndexEntry index = locator.addIndex();
                index.setVersion(version);
                index.setHash(new byte[0]);
                index.setPosition().setLedgerId(version + 1L).setEntryId(0L);
                latest = index;
            }
            locator.setInfo().copyFrom(latest);
            store.put("/schemas/test", locator.toByteArray(), Optional.empty()).get(5, TimeUnit.SECONDS);

            BookKeeperClientFactory clientFactory = mock(BookKeeperClientFactory.class);
            when(clientFactory.create(any(), eq(store), isNull(), eq(Optional.empty()), isNull()))
                    .thenReturn(CompletableFuture.completedFuture(bookKeeper));
            PulsarService pulsar = mock(PulsarService.class);
            when(pulsar.getLocalMetadataStore()).thenReturn(store);
            when(pulsar.getConfiguration()).thenReturn(config);
            when(pulsar.getBookKeeperClientFactory()).thenReturn(clientFactory);
            storage = new BookkeeperSchemaStorage(pulsar);
            storage.start();
        }

        private CreateBuilder newWriteHandleBuilder() {
            long ledgerId = nextCreatedLedgerId.incrementAndGet();
            LedgerHandle writeHandle = mock(LedgerHandle.class);
            when(writeHandle.getId()).thenReturn(ledgerId);
            doAnswer(invocation -> {
                writtenEntries.put(ledgerId, invocation.getArgument(0));
                AsyncCallback.AddCallback callback = invocation.getArgument(1);
                callback.addComplete(BKException.Code.OK, writeHandle, 0L, invocation.getArgument(2));
                return null;
            }).when(writeHandle).asyncAddEntry(any(byte[].class), any(), isNull());
            when(writeHandle.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

            CreateBuilder builder = mock(CreateBuilder.class);
            when(builder.withEnsembleSize(anyInt())).thenReturn(builder);
            when(builder.withWriteQuorumSize(anyInt())).thenReturn(builder);
            when(builder.withAckQuorumSize(anyInt())).thenReturn(builder);
            when(builder.withDigestType(any())).thenReturn(builder);
            when(builder.withPassword(any())).thenReturn(builder);
            when(builder.withCustomMetadata(any())).thenReturn(builder);
            when(builder.withLoggerContext(any())).thenReturn(builder);
            when(builder.execute()).thenReturn(CompletableFuture.completedFuture(writeHandle));
            return builder;
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
            byte[] storedEntry = writtenEntries.get(pendingRead.handle().getId());
            if (storedEntry == null) {
                throw new IllegalStateException("No stored entry for ledger "
                        + pendingRead.handle().getId());
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
    public void testSchemaEntryReadHitsCache() throws Exception {
        try (Harness harness = new Harness()) {
            CompletableFuture<StoredSchema> first = harness.storage.get("test", SchemaVersion.Latest);
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(first.get(5, TimeUnit.SECONDS).data, SchemaData);

            CompletableFuture<StoredSchema> second = harness.storage.get("test", SchemaVersion.Latest);
            assertEquals(second.get(5, TimeUnit.SECONDS).data, SchemaData);

            CompletableFuture<StoredSchema> versioned =
                    harness.storage.get("test", new LongSchemaVersion(0));
            assertEquals(versioned.get(5, TimeUnit.SECONDS).data, SchemaData);

            assertEquals(harness.openCount.get(), 1);
            verify(harness.bookKeeper, times(1))
                    .asyncOpenLedger(eq(1L), any(), any(), any(), isNull(), eq(true));
            verify(harness.handle, times(1)).asyncReadEntries(anyLong(), anyLong(), any(), any());
            assertEquals(harness.storage.cachedSchemaEntryCount(), 1);
        }
    }

    @Test
    public void testGetAllCachesEveryVersion() throws Exception {
        try (Harness harness = new Harness(3)) {
            List<CompletableFuture<StoredSchema>> first = harness.storage.getAll("test")
                    .get(5, TimeUnit.SECONDS);
            assertEquals(first.size(), 3);
            for (int version = 0; version < 3; version++) {
                harness.completePendingRead(BKException.Code.OK);
            }
            for (int version = 0; version < 3; version++) {
                StoredSchema schema = first.get(version).get(5, TimeUnit.SECONDS);
                assertEquals(((LongSchemaVersion) schema.version).getVersion(), version);
                assertEquals(schema.data, dataForVersion(version));
            }
            assertEquals(harness.openCount.get(), 3);
            assertEquals(harness.storage.cachedSchemaEntryCount(), 3);

            // The second read of every version must be served from the cache without new BK reads
            List<CompletableFuture<StoredSchema>> second = harness.storage.getAll("test")
                    .get(5, TimeUnit.SECONDS);
            for (int version = 0; version < 3; version++) {
                StoredSchema schema = second.get(version).get(5, TimeUnit.SECONDS);
                assertEquals(schema.data, dataForVersion(version));
            }
            assertEquals(harness.openCount.get(), 3);
            assertEquals(harness.storage.cachedSchemaEntryCount(), 3);
        }
    }

    @Test
    public void testConcurrentReadsShareOneBookKeeperRead() throws Exception {
        try (Harness harness = new Harness()) {
            CompletableFuture<StoredSchema> first = harness.storage.get("test", SchemaVersion.Latest);
            CompletableFuture<StoredSchema> second = harness.storage.get("test", SchemaVersion.Latest);
            assertFalse(first.isDone());
            assertFalse(second.isDone());

            harness.completePendingRead(BKException.Code.OK);

            assertEquals(first.get(5, TimeUnit.SECONDS).data, SchemaData);
            assertEquals(second.get(5, TimeUnit.SECONDS).data, SchemaData);
            assertEquals(harness.openCount.get(), 1);
        }
    }

    @Test
    public void testFailedReadIsNotCached() throws Exception {
        try (Harness harness = new Harness()) {
            CompletableFuture<StoredSchema> first = harness.storage.get("test", SchemaVersion.Latest);
            harness.completePendingRead(BKException.Code.ReadException);
            Throwable error = first.handle((schema, ex) -> FutureUtil.unwrapCompletionException(ex))
                    .get(5, TimeUnit.SECONDS);
            assertEquals(error.getMessage(), bkException("Failed to read entry",
                    BKException.Code.ReadException, 1, 0, false).getMessage());
            assertEquals(harness.storage.cachedSchemaEntryCount(), 0);

            CompletableFuture<StoredSchema> second = harness.storage.get("test", SchemaVersion.Latest);
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(second.get(5, TimeUnit.SECONDS).data, SchemaData);
            assertEquals(harness.openCount.get(), 2);
            assertEquals(harness.storage.cachedSchemaEntryCount(), 1);
        }
    }

    @Test
    public void testForceDeleteInvalidatesCachedEntries() throws Exception {
        try (Harness harness = new Harness()) {
            CompletableFuture<StoredSchema> read = harness.storage.get("test", SchemaVersion.Latest);
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(read.get(5, TimeUnit.SECONDS).data, SchemaData);
            assertEquals(harness.storage.cachedSchemaEntryCount(), 1);

            harness.storage.delete("test", true).get(5, TimeUnit.SECONDS);

            assertEquals(harness.storage.cachedSchemaEntryCount(), 0);
            verify(harness.bookKeeper).asyncDeleteLedger(eq(1L), any(), isNull());
        }
    }

    @Test
    public void testForceDeleteAndRecreateServesNewEntry() throws Exception {
        try (Harness harness = new Harness()) {
            CompletableFuture<StoredSchema> before = harness.storage.get("test", SchemaVersion.Latest);
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(before.get(5, TimeUnit.SECONDS).data, SchemaData);

            harness.storage.delete("test", true).get(5, TimeUnit.SECONDS);
            // The local metadata store invalidates the locator cache synchronously on delete
            CompletableFuture<StoredSchema> deleted = harness.storage.get("test", SchemaVersion.Latest);
            assertEquals(deleted.get(5, TimeUnit.SECONDS), null);
            assertEquals(harness.storage.cachedSchemaEntryCount(), 0);

            // Re-create the schema: the new version must land in a new ledger, so the read after
            // re-creation must return the new data even though the old entry was cached before
            byte[] newData = "schema-recreated".getBytes(StandardCharsets.UTF_8);
            SchemaVersion version = harness.storage.put("test", newData, "hash".getBytes())
                    .get(5, TimeUnit.SECONDS);
            assertEquals(((LongSchemaVersion) version).getVersion(), 0);

            CompletableFuture<StoredSchema> after = harness.storage.get("test", SchemaVersion.Latest);
            harness.completePendingRead(BKException.Code.OK);
            StoredSchema recreated = after.get(5, TimeUnit.SECONDS);
            assertEquals(recreated.data, newData);
            assertEquals(harness.storage.cachedSchemaEntryCount(), 1);
        }
    }

    @Test
    public void testInFlightReadCompletingAfterDeleteIsNotServedToNewReaders() throws Exception {
        try (Harness harness = new Harness()) {
            CompletableFuture<StoredSchema> inFlight = harness.storage.get("test", SchemaVersion.Latest);
            assertFalse(inFlight.isDone());

            // Force delete completes while the BookKeeper read is still pending
            harness.storage.delete("test", true).get(5, TimeUnit.SECONDS);
            assertEquals(harness.storage.cachedSchemaEntryCount(), 0);

            // The in-flight reader still gets its answer. Caffeine drops the entry at the
            // invalidation and does not retain the value when the load completes afterwards,
            // so nothing of the deleted schema survives in the cache
            harness.completePendingRead(BKException.Code.OK);
            assertEquals(inFlight.get(5, TimeUnit.SECONDS).data, SchemaData);

            // New readers resolve the (now absent) locator and see no schema
            CompletableFuture<StoredSchema> after = harness.storage.get("test", SchemaVersion.Latest);
            assertEquals(after.get(5, TimeUnit.SECONDS), null);
        }
    }
}
