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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.NO_MAX_SIZE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ReadOnlyManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ShadowManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReadEntryUtilsTest {

    private ManagedLedger ml;
    private LedgerHandle lh;

    @BeforeMethod
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setup() {
        ml = mock(ManagedLedger.class);
        lh = mock(LedgerHandle.class);
        when(lh.getId()).thenReturn(1L);
        Position lastConfirmedEntry = PositionFactory.create(1L, 99L);
        when(ml.getLastConfirmedEntry()).thenReturn(lastConfirmedEntry);
        when(ml.getOptionalLedgerInfo(1L)).thenReturn((Optional) Optional.of(new Object()));
    }

    @Test
    public void testReadOnlyOpenSingleEntryBeyondConfirmedFrontier() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.of(new LedgerInfo().setLedgerId(1L)));
        when(reader.getLastConfirmedEntry()).thenReturn(PositionFactory.create(1L, 0L));
        when(lh.isClosed()).thenReturn(false);
        LedgerEntries entries = createLedgerEntries(1L, 1L);
        when(lh.readUnconfirmedAsync(1L, 1L)).thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 1L, 1L, true, 1024);

        assertThat(future).isCompletedWithValue(entries);
        future.getNow(null).close();
        verify(lh).readUnconfirmedAsync(1L, 1L);
        verify(lh, never()).readAsync(anyLong(), anyLong());
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());
    }

    @Test
    public void testReadOnlyOpenSingleEntryWithStaleInventoryUsesUnconfirmedRead() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        when(lh.isClosed()).thenReturn(false);
        when(lh.getLastAddConfirmed()).thenReturn(0L);
        LedgerEntries entries = createLedgerEntries(1L, 1L);
        when(lh.readUnconfirmedAsync(1L, 1L)).thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 1L, 1L, true, 1024);

        assertThat(future).isCompletedWithValue(entries);
        future.getNow(null).close();
        verify(lh).readUnconfirmedAsync(1L, 1L);
        verify(lh, never()).readAsync(anyLong(), anyLong());
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());
    }

    @DataProvider(name = "readOnlyInventoryMembership")
    public Object[][] readOnlyInventoryMembership() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "readOnlyInventoryMembership")
    public void testReadOnlyOpenSingleEntryStorageFailureIsPropagated(boolean ownsLedger) {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(ownsLedger
                ? Optional.of(new LedgerInfo().setLedgerId(1L)) : Optional.empty());
        when(lh.isClosed()).thenReturn(false);
        when(lh.readUnconfirmedAsync(1L, 1L))
                .thenReturn(CompletableFuture.failedFuture(new BKException.BKNoSuchEntryException()));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 1L, 1L, true, 1024);

        assertThatThrownBy(future::get).hasCauseInstanceOf(BKException.BKNoSuchEntryException.class);
        verify(lh).readUnconfirmedAsync(1L, 1L);
        verify(lh, never()).readAsync(anyLong(), anyLong());
    }

    @Test
    public void testReadOnlyClosedSingleEntryWithStaleInventoryKeepsFinalHandleBound() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        when(lh.isClosed()).thenReturn(true);
        when(lh.getLastAddConfirmed()).thenReturn(2L);
        LedgerEntries entries = createLedgerEntries(1L, 2L);
        when(lh.readAsync(2L, 2L)).thenReturn(CompletableFuture.completedFuture(entries));
        when(lh.readAsync(3L, 3L)).thenReturn(CompletableFuture.failedFuture(new BKException.BKReadException()));

        CompletableFuture<LedgerEntries> found = ReadEntryUtils.readAsync(reader, lh, 2L, 2L, true, 1024);
        assertThat(found).isCompletedWithValue(entries);
        found.getNow(null).close();
        CompletableFuture<LedgerEntries> beyond = ReadEntryUtils.readAsync(reader, lh, 3L, 3L, true, 1024);
        assertThatThrownBy(beyond::get).hasCauseInstanceOf(BKException.BKReadException.class);
        verify(lh).readAsync(2L, 2L);
        verify(lh).readAsync(3L, 3L);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());
    }

    @Test
    public void testReadOnlyRangeWithStaleInventoryStillUsesConfirmedRead() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        when(lh.isClosed()).thenReturn(false);
        LedgerEntries entries = createLedgerEntries(1L, 0L, 1L);
        when(lh.readAsync(0L, 1L)).thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 0L, 1L, true, 1024);

        assertThat(future).isCompletedWithValue(entries);
        future.getNow(null).close();
        verify(lh).readAsync(0L, 1L);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());
    }

    @DataProvider(name = "capturedReadOnlyFrontiers")
    public Object[][] capturedReadOnlyFrontiers() {
        return new Object[][]{{null}, {PositionFactory.create(0L, 99L)}, {PositionFactory.create(1L, 1L)}};
    }

    @Test(dataProvider = "capturedReadOnlyFrontiers")
    public void testReadOnlyClosedSingleEntryUsesFinalHandleBound(Position capturedFrontier) {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.of(new LedgerInfo().setLedgerId(1L)));
        when(reader.getLastConfirmedEntry()).thenReturn(capturedFrontier);
        when(lh.isClosed()).thenReturn(true);
        when(lh.getLastAddConfirmed()).thenReturn(2L);
        LedgerEntries entries = createLedgerEntries(1L, 2L);
        when(lh.readAsync(2L, 2L)).thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 2L, 2L, true, 1024);

        assertThat(future).isCompletedWithValue(entries);
        future.getNow(null).close();
        verify(lh).readAsync(2L, 2L);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());
    }

    @Test
    public void testReadOnlyClosedSingleEntryPastFinalHandleBoundFails() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.of(new LedgerInfo().setLedgerId(1L)));
        when(reader.getLastConfirmedEntry()).thenReturn(PositionFactory.create(1L, 1L));
        when(lh.isClosed()).thenReturn(true);
        when(lh.getLastAddConfirmed()).thenReturn(2L);
        when(lh.readAsync(3L, 3L)).thenReturn(CompletableFuture.failedFuture(new BKException.BKReadException()));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 3L, 3L, true, 1024);

        assertThatThrownBy(future::get).hasCauseInstanceOf(BKException.BKReadException.class);
        verify(lh).readAsync(3L, 3L);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());
    }

    @DataProvider(name = "guardedReadCases")
    public Object[][] guardedReadCases() {
        List<Object[]> cases = new ArrayList<>();
        for (String kind : List.of("writable", "shadow", "read-only-range", "read-only-closed-range")) {
            for (Position frontier : new Position[]{null, PositionFactory.create(0L, 99L),
                    PositionFactory.create(1L, 0L)}) {
                cases.add(new Object[]{kind, frontier});
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "guardedReadCases")
    public void testReadOnlyPointReadDoesNotBypassOtherReadBounds(String kind, Position frontier) {
        ManagedLedger reader = switch (kind) {
            case "shadow" -> mock(ShadowManagedLedgerImpl.class);
            case "read-only-range", "read-only-closed-range" -> mock(ReadOnlyManagedLedgerImpl.class);
            default -> mock(ManagedLedger.class);
        };
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.of(new LedgerInfo().setLedgerId(1L)));
        when(reader.getLastConfirmedEntry()).thenReturn(frontier);
        when(lh.isClosed()).thenReturn(kind.equals("read-only-closed-range"));
        long first = kind.endsWith("range") ? 0L : 1L;

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, first, 1L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get).hasCauseInstanceOf(ManagedLedgerException.class);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
        verify(lh, never()).readAsync(anyLong(), anyLong());
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());
    }

    @Test
    public void testBatchReadSingleBatch() {
        LedgerEntries entries = createLedgerEntries(1L, 0, 1, 2, 3, 4);
        when(lh.batchReadUnconfirmedAsync(eq(0L), eq(5), eq(1024L)))
                .thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompleted();
        try (LedgerEntries result = future.getNow(null)) {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        }

        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testNoSizeLimitLetsBookKeeperCapTheBatch() {
        LedgerEntries entries = createLedgerEntries(1L, 0, 1, 2, 3, 4);
        when(lh.batchReadUnconfirmedAsync(eq(0L), eq(5), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, NO_MAX_SIZE_LIMIT);

        assertThat(future).isCompleted();
        future.getNow(null).close();
        verify(lh).batchReadUnconfirmedAsync(0L, 5, 0L);
    }

    @Test
    public void testBatchReadMultipleBatches() {
        LedgerEntries firstBatch = createLedgerEntries(1L, 0, 1, 2);
        when(lh.batchReadUnconfirmedAsync(eq(0L), eq(5), eq(1024L)))
                .thenReturn(CompletableFuture.completedFuture(firstBatch));
        LedgerEntries secondBatch = createLedgerEntries(1L, 3, 4);
        when(lh.batchReadUnconfirmedAsync(eq(3L), eq(2), eq(1024L)))
                .thenReturn(CompletableFuture.completedFuture(secondBatch));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompleted();
        LedgerEntries result = future.getNow(null);
        try {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        } finally {
            result.close();
        }
    }

    @Test
    public void testBatchReadReturnsEmptyEntries() {
        // An empty result is handed over as-is, like the result of a regular read, for the caller to handle
        AtomicInteger closeCount = new AtomicInteger();
        LedgerEntries emptyEntries = wrapLedgerEntries(new ArrayList<>(), closeCount);
        when(lh.batchReadUnconfirmedAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(emptyEntries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedWithValue(emptyEntries);
        assertThat(closeCount).hasValue(0);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testBatchReadFailure() {
        CompletableFuture<LedgerEntries> failedFuture =
                CompletableFuture.failedFuture(new BKException.BKBookieHandleNotAvailableException());
        when(lh.batchReadUnconfirmedAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(failedFuture);

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(BKException.class);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testSynchronousBatchReadFailureIsReturnedInFuture() {
        when(lh.batchReadUnconfirmedAsync(eq(0L), anyInt(), anyLong()))
                .thenThrow(new IllegalStateException("Batch read failed"));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(IllegalStateException.class);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testSynchronousFailureAfterPartialReadReleasesEntries() {
        AtomicInteger closeCount = new AtomicInteger();
        LedgerEntries firstBatch = createLedgerEntries(closeCount, 1L, 0, 1);
        when(lh.batchReadUnconfirmedAsync(0L, 5, 1024L))
                .thenReturn(CompletableFuture.completedFuture(firstBatch));
        when(lh.batchReadUnconfirmedAsync(2L, 3, 1024L))
                .thenThrow(new IllegalStateException("Batch read failed"));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThat(closeCount).hasValue(1);
    }

    @Test
    public void testEmptyBatchAfterPartialReadFailsAndReleasesEntries() {
        AtomicInteger firstCloseCount = new AtomicInteger();
        AtomicInteger secondCloseCount = new AtomicInteger();
        LedgerEntries firstBatch = createLedgerEntries(firstCloseCount, 1L, 0, 1);
        LedgerEntries secondBatch = wrapLedgerEntries(new ArrayList<>(), secondCloseCount);
        when(lh.batchReadUnconfirmedAsync(0L, 5, 1024L))
                .thenReturn(CompletableFuture.completedFuture(firstBatch));
        when(lh.batchReadUnconfirmedAsync(2L, 3, 1024L))
                .thenReturn(CompletableFuture.completedFuture(secondBatch));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(ManagedLedgerException.class);
        assertThat(firstCloseCount).hasValue(1);
        assertThat(secondCloseCount).hasValue(1);
    }

    @Test
    public void testNonContiguousBatchFailsAndReleasesEntries() {
        AtomicInteger closeCount = new AtomicInteger();
        LedgerEntries entries = createLedgerEntries(closeCount, 1L, 0, 2);
        when(lh.batchReadUnconfirmedAsync(0L, 3, 1024L))
                .thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 2L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(ManagedLedgerException.class)
                .hasMessageContaining("non-contiguous");
        assertThat(closeCount).hasValue(1);
    }

    @Test
    public void testExcessiveBatchFailsAndReleasesEntries() {
        AtomicInteger closeCount = new AtomicInteger();
        LedgerEntries entries = createLedgerEntries(closeCount, 1L, 0, 1, 2);
        when(lh.batchReadUnconfirmedAsync(0L, 2, 1024L))
                .thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 1L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(ManagedLedgerException.class);
        assertThat(closeCount).hasValue(1);
    }

    @Test
    public void testCancelledBatchReadReleasesResult() {
        AtomicInteger closeCount = new AtomicInteger();
        LedgerEntries entries = createLedgerEntries(closeCount, 1L, 0, 1);
        CompletableFuture<LedgerEntries> batchFuture = new CompletableFuture<>();
        when(lh.batchReadUnconfirmedAsync(0L, 2, 1024L)).thenReturn(batchFuture);

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 1L, true, 1024);

        assertThat(future.cancel(false)).isTrue();
        batchFuture.complete(entries);
        assertThat(closeCount).hasValue(1);
    }

    @Test
    public void testCancelledPartialBatchReadReleasesResult() {
        AtomicInteger closeCount = new AtomicInteger();
        LedgerEntries entries = createLedgerEntries(closeCount, 1L, 0);
        CompletableFuture<LedgerEntries> batchFuture = new CompletableFuture<>();
        when(lh.batchReadUnconfirmedAsync(0L, 3, 1024L)).thenReturn(batchFuture);

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 2L, true, 1024);

        assertThat(future.cancel(false)).isTrue();
        batchFuture.complete(entries);
        assertThat(closeCount).hasValue(1);
        verify(lh, never()).batchReadUnconfirmedAsync(eq(1L), anyInt(), anyLong());
    }

    @Test
    public void testBatchReadDisabledFallback() {
        LedgerEntries mockEntries = createLedgerEntries(1L, 0, 1, 2, 3, 4);
        when(lh.readUnconfirmedAsync(0L, 4L))
                .thenReturn(CompletableFuture.completedFuture(mockEntries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, false, 1024);

        assertThat(future).isCompleted();
        verify(lh).readUnconfirmedAsync(0L, 4L);
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());

        future.getNow(null).close();
    }

    @Test
    public void testBatchReadSingleEntryFallback() {
        LedgerEntries mockEntries = createLedgerEntries(1L, 0);
        when(lh.readUnconfirmedAsync(0L, 0L))
                .thenReturn(CompletableFuture.completedFuture(mockEntries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 0L, true, 1024);

        assertThat(future).isCompleted();
        verify(lh).readUnconfirmedAsync(0L, 0L);
        verify(lh, never()).batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong());

        future.getNow(null).close();
    }

    @Test
    public void testBatchReadWithNonLedgerHandle() {
        ReadHandle rh = mock(ReadHandle.class, CALLS_REAL_METHODS);
        when(rh.getId()).thenReturn(1L);
        LedgerEntries mockEntries = createLedgerEntries(1L, 0, 1, 2);
        // ReadHandle.batchReadUnconfirmedAsync is a default method that delegates to
        // readUnconfirmedAsync for non-LedgerHandle implementations
        when(rh.readUnconfirmedAsync(0L, 2L))
                .thenReturn(CompletableFuture.completedFuture(mockEntries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, rh, 0L, 2L, true, 1024);

        assertThat(future).isCompleted();
        try (LedgerEntries result = future.getNow(null)) {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L);
        }
        verify(rh).readUnconfirmedAsync(0L, 2L);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testReadOnlyManagedLedgerFallback() {
        when(ml.getOptionalLedgerInfo(1L)).thenReturn((Optional) Optional.empty());

        ReadHandle rh = mock(ReadHandle.class);
        when(rh.getId()).thenReturn(1L);
        LedgerEntries mockEntries = createLedgerEntries(1L, 0, 1);
        when(rh.readAsync(0L, 1L)).thenReturn(CompletableFuture.completedFuture(mockEntries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, rh, 0L, 1L, true, 1024);

        assertThat(future).isCompleted();
        verify(rh).readAsync(0L, 1L);

        future.getNow(null).close();
    }

    @Test
    public void testAutoRefillWithSizeLimitedReturns() {
        // First batch returns only entries 0-1 (size-limited)
        LedgerEntries firstBatch = createLedgerEntries(1L, 0, 1);
        when(lh.batchReadUnconfirmedAsync(eq(0L), eq(5), eq(1024L)))
                .thenReturn(CompletableFuture.completedFuture(firstBatch));
        // Second batch returns entries 2-4 to complete the read
        LedgerEntries secondBatch = createLedgerEntries(1L, 2, 3, 4);
        when(lh.batchReadUnconfirmedAsync(eq(2L), eq(3), eq(1024L)))
                .thenReturn(CompletableFuture.completedFuture(secondBatch));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompleted();
        LedgerEntries result = future.getNow(null);
        try {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        } finally {
            result.close();
        }
    }

    @Test
    public void testBatchReadFailureAfterPartialResultPreservesTheError() {
        // First batch succeeds with entries 0-2
        LedgerEntries firstBatch = createLedgerEntries(1L, 0, 1, 2);
        when(lh.batchReadUnconfirmedAsync(eq(0L), eq(5), eq(1024L)))
                .thenReturn(CompletableFuture.completedFuture(firstBatch));
        // Second batch fails
        CompletableFuture<LedgerEntries> failedFuture =
                CompletableFuture.failedFuture(new BKException.BKBookieHandleNotAvailableException());
        when(lh.batchReadUnconfirmedAsync(eq(3L), eq(2), eq(1024L)))
                .thenReturn(failedFuture);

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(BKException.class);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    // --- helpers ---

    private static LedgerEntries createLedgerEntries(long ledgerId, long... entryIds) {
        return createLedgerEntries(new AtomicInteger(), ledgerId, entryIds);
    }

    private static LedgerEntries createLedgerEntries(AtomicInteger closeCount, long ledgerId, long... entryIds) {
        List<LedgerEntry> entries = new ArrayList<>();
        for (long entryId : entryIds) {
            entries.add(LedgerEntryImpl.create(ledgerId, entryId, 1,
                    Unpooled.wrappedBuffer(new byte[]{(byte) entryId})));
        }
        return wrapLedgerEntries(entries, closeCount);
    }

    private static LedgerEntries wrapLedgerEntries(List<LedgerEntry> entries, AtomicInteger closeCount) {
        return new LedgerEntries() {
            private boolean closed;

            @Override
            public LedgerEntry getEntry(long eid) {
                for (LedgerEntry e : entries) {
                    if (e.getEntryId() == eid) {
                        return e;
                    }
                }
                throw new IndexOutOfBoundsException("Entry " + eid + " not found");
            }

            @Override
            public Iterator<LedgerEntry> iterator() {
                return entries.iterator();
            }

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    closeCount.incrementAndGet();
                    entries.forEach(LedgerEntry::close);
                }
            }
        };
    }
}
