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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.OffloadReadHandleClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RangeEntryCacheOwnershipTest {
    private static final long LEDGER_ID = 1;
    private static final Position POSITION = PositionFactory.create(LEDGER_ID, 1);
    private RangeEntryCacheManagerImpl manager;

    @BeforeMethod
    public void setUp() {
        ManagedLedgerFactoryImpl factory = mock(ManagedLedgerFactoryImpl.class);
        when(factory.getConfig()).thenReturn(new ManagedLedgerFactoryConfig());
        when(factory.getMbean()).thenReturn(mock(ManagedLedgerFactoryMBeanImpl.class));
        manager = new RangeEntryCacheManagerImpl(factory, mock(OrderedScheduler.class), OpenTelemetry.noop());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        manager.clear();
        manager.getInflightReadsLimiter().close();
    }

    @DataProvider
    public Object[][] requesterTypes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "requesterTypes")
    public void testOffloadRetryUsesReaderAfterReadOnlyOwnerCloses(boolean readOnlyRequester) throws Exception {
        ReadOnlyManagedLedgerImpl readOnly = ledger(ReadOnlyManagedLedgerImpl.class);
        EntryCache cache = manager.getEntryCache(readOnly);
        ManagedLedgerImpl reader = readOnlyRequester ? ledger(ReadOnlyManagedLedgerImpl.class)
                : ledger(ManagedLedgerImpl.class);
        assertThat(manager.getEntryCache(reader)).isSameAs(cache);

        when(readOnly.reopenReadHandle(LEDGER_ID)).thenReturn(CompletableFuture.failedFuture(
                new ManagedLedgerAlreadyClosedException("Read-only cache owner closed")));
        ReadHandle expired = readHandle();
        when(expired.readUnconfirmedAsync(1, 1)).thenReturn(CompletableFuture.failedFuture(
                new OffloadReadHandleClosedException()));
        ReadHandle reopened = readHandle();
        when(reopened.readUnconfirmedAsync(1, 1)).thenAnswer(invocation ->
                CompletableFuture.completedFuture(bookKeeperEntry(1)));
        when(reader.reopenReadHandle(LEDGER_ID)).thenReturn(CompletableFuture.completedFuture(reopened));

        assertThat(read(cache, reader, expired, POSITION).get(5, TimeUnit.SECONDS)).containsExactly((byte) 42);
        verify(reader).reopenReadHandle(LEDGER_ID);
        verify(readOnly, never()).reopenReadHandle(LEDGER_ID);
    }

    @Test
    public void testOverlappingReadsAreCoalescedOnlyWithinAReader() throws Exception {
        ReadOnlyManagedLedgerImpl readOnly = ledger(ReadOnlyManagedLedgerImpl.class);
        EntryCache cache = manager.getEntryCache(readOnly);
        ManagedLedgerImpl writer = ledger(ManagedLedgerImpl.class);
        assertThat(manager.getEntryCache(writer)).isSameAs(cache);

        ReadHandle readOnlyHandle = readHandle();
        CompletableFuture<LedgerEntries> pendingRead = new CompletableFuture<>();
        when(readOnlyHandle.readUnconfirmedAsync(1, 1)).thenReturn(pendingRead);
        ReadHandle writerHandle = readHandle();
        when(writerHandle.readUnconfirmedAsync(1, 1)).thenAnswer(invocation ->
                CompletableFuture.completedFuture(bookKeeperEntry(1)));

        try {
            CompletableFuture<byte[]> firstRead = read(cache, readOnly, readOnlyHandle, POSITION);
            CompletableFuture<byte[]> sameReaderRead = read(cache, readOnly, readOnlyHandle, POSITION);
            assertThat(read(cache, writer, writerHandle, POSITION).get(5, TimeUnit.SECONDS)).containsExactly((byte) 42);
            assertThat(firstRead).isNotDone();
            assertThat(sameReaderRead).isNotDone();
            verify(readOnlyHandle).readUnconfirmedAsync(1, 1);

            pendingRead.complete(bookKeeperEntry(1));
            assertThat(firstRead.get(5, TimeUnit.SECONDS)).containsExactly((byte) 42);
            assertThat(sameReaderRead.get(5, TimeUnit.SECONDS)).containsExactly((byte) 42);
        } finally {
            pendingRead.completeExceptionally(new IllegalStateException("Test read abandoned"));
        }
    }

    @Test
    public void testReadOnlyReadUsesLiveWriterBoundary() throws Exception {
        ManagedLedgerImpl writer = ledger(ManagedLedgerImpl.class);
        EntryCache cache = manager.getEntryCache(writer);
        ReadOnlyManagedLedgerImpl readOnly = ledger(ReadOnlyManagedLedgerImpl.class);
        when(readOnly.getLastConfirmedEntry()).thenReturn(PositionFactory.create(LEDGER_ID, 0));
        assertThat(manager.getEntryCache(readOnly)).isSameAs(cache);

        ReadHandle handle = readHandle();
        when(handle.readUnconfirmedAsync(1, 1)).thenAnswer(invocation ->
                CompletableFuture.completedFuture(bookKeeperEntry(1)));
        when(handle.readUnconfirmedAsync(2, 2)).thenAnswer(invocation ->
                CompletableFuture.completedFuture(bookKeeperEntry(2)));
        when(readOnly.reopenLedgerHandleIfReplicated(handle)).thenReturn(CompletableFuture.completedFuture(null));

        assertThat(read(cache, readOnly, handle, POSITION).get(5, TimeUnit.SECONDS)).containsExactly((byte) 42);
        Position beyondWriterBoundary = PositionFactory.create(LEDGER_ID, 2);
        assertThatThrownBy(() -> read(cache, readOnly, handle, beyondWriterBoundary).get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(ManagedLedgerException.class).hasMessageContaining("LastConfirmedEntry");
        verify(handle, never()).readUnconfirmedAsync(2, 2);
    }

    @Test
    public void testWriterKeepsConfirmationBoundaryWithReadOnlyCacheOwner() {
        ReadOnlyManagedLedgerImpl readOnly = ledger(ReadOnlyManagedLedgerImpl.class);
        EntryCache cache = manager.getEntryCache(readOnly);
        ManagedLedgerImpl writer = ledger(ManagedLedgerImpl.class);
        when(writer.getLastConfirmedEntry()).thenReturn(PositionFactory.create(LEDGER_ID, 0));
        assertThat(manager.getEntryCache(writer)).isSameAs(cache);

        ReadHandle handle = readHandle();
        when(handle.readUnconfirmedAsync(1, 1)).thenAnswer(invocation ->
                CompletableFuture.completedFuture(bookKeeperEntry(1)));
        when(writer.reopenLedgerHandleIfReplicated(handle)).thenReturn(CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> read(cache, writer, handle, POSITION).get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(ManagedLedgerException.class).hasMessageContaining("LastConfirmedEntry");
        verify(handle, never()).readUnconfirmedAsync(1, 1);
    }

    private static <T extends ManagedLedgerImpl> T ledger(Class<T> ledgerType) {
        T managedLedger = mock(ledgerType);
        ThreadBoundExecutor executor = mock(ThreadBoundExecutor.class);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).executeOrRun(any(Runnable.class));
        when(managedLedger.getExecutor()).thenReturn(executor);
        when(managedLedger.getName()).thenReturn("shared-cache");
        when(managedLedger.getConfig()).thenReturn(defaultConfig());
        when(managedLedger.getMbean()).thenReturn(mock(ManagedLedgerMBeanImpl.class));
        when(managedLedger.getOptionalLedgerInfo(LEDGER_ID))
                .thenReturn(Optional.of(new LedgerInfo().setLedgerId(LEDGER_ID)));
        when(managedLedger.getLastConfirmedEntry()).thenReturn(POSITION);
        return managedLedger;
    }

    private static ReadHandle readHandle() {
        ReadHandle handle = mock(ReadHandle.class);
        when(handle.getId()).thenReturn(LEDGER_ID);
        return handle;
    }

    private static LedgerEntries bookKeeperEntry(long entryId) {
        return LedgerEntriesImpl.create(new ArrayList<>(List.of(LedgerEntryImpl.create(
                LEDGER_ID, entryId, 1, Unpooled.buffer(1).writeByte(42)))));
    }

    private static CompletableFuture<byte[]> read(EntryCache cache, ManagedLedgerImpl reader, ReadHandle handle,
                                                   Position position) {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        cache.asyncReadEntry(reader, handle, position, EntryImpl.NO_EXPECTED_READ_COUNT, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    result.complete(entry.getData());
                } finally {
                    entry.release();
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }
}
