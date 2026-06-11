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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
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
import org.testng.annotations.BeforeMethod;
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
        LedgerEntries emptyEntries = wrapLedgerEntries(new ArrayList<>());
        when(lh.batchReadUnconfirmedAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(emptyEntries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(ManagedLedgerException.class);
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
        ReadHandle rh = mock(ReadHandle.class);
        when(rh.getId()).thenReturn(1L);
        LedgerEntries mockEntries = createLedgerEntries(1L, 0, 1, 2);
        // ReadHandle.batchReadUnconfirmedAsync is a default method that delegates to
        // readUnconfirmedAsync for non-LedgerHandle implementations
        when(rh.batchReadUnconfirmedAsync(eq(0L), eq(3), eq(1024L)))
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
    public void testBatchReadFailureWithPartialDataDoesNotFallback() {
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
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testBatchReadMidBatchFailurePreservesOriginalException() {
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
    }

    // --- helpers ---

    private static LedgerEntries createLedgerEntries(long ledgerId, long... entryIds) {
        List<LedgerEntry> entries = new ArrayList<>();
        for (long entryId : entryIds) {
            entries.add(LedgerEntryImpl.create(ledgerId, entryId, 1,
                    Unpooled.wrappedBuffer(new byte[]{(byte) entryId})));
        }
        return wrapLedgerEntries(entries);
    }

    private static LedgerEntries wrapLedgerEntries(List<LedgerEntry> entries) {
        return new LedgerEntries() {
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
                entries.forEach(LedgerEntry::close);
            }
        };
    }
}
