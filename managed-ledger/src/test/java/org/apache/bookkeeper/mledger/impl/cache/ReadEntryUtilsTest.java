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
import static org.mockito.ArgumentMatchers.anyLong;
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

    private LedgerHandle lh;

    @BeforeMethod
    public void setup() {
        lh = mock(LedgerHandle.class);
        when(lh.getId()).thenReturn(1L);
    }

    @Test
    public void testReadOnlyOpenSingleEntryBeyondConfirmedFrontier() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.of(new LedgerInfo().setLedgerId(1L)));
        when(reader.getLastConfirmedEntry()).thenReturn(PositionFactory.create(1L, 0L));
        when(lh.isClosed()).thenReturn(false);
        LedgerEntries entries = createLedgerEntries(1L, 1L);
        when(lh.readUnconfirmedAsync(1L, 1L)).thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 1L, 1L);

        assertThat(future).isCompletedWithValue(entries);
        future.getNow(null).close();
        verify(lh).readUnconfirmedAsync(1L, 1L);
        verify(lh, never()).readAsync(anyLong(), anyLong());
    }

    @Test
    public void testReadOnlyOpenSingleEntryWithStaleInventoryUsesUnconfirmedRead() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        when(lh.isClosed()).thenReturn(false);
        when(lh.getLastAddConfirmed()).thenReturn(0L);
        LedgerEntries entries = createLedgerEntries(1L, 1L);
        when(lh.readUnconfirmedAsync(1L, 1L)).thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 1L, 1L);

        assertThat(future).isCompletedWithValue(entries);
        future.getNow(null).close();
        verify(lh).readUnconfirmedAsync(1L, 1L);
        verify(lh, never()).readAsync(anyLong(), anyLong());
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

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 1L, 1L);

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

        CompletableFuture<LedgerEntries> found = ReadEntryUtils.readAsync(reader, lh, 2L, 2L);
        assertThat(found).isCompletedWithValue(entries);
        found.getNow(null).close();
        CompletableFuture<LedgerEntries> beyond = ReadEntryUtils.readAsync(reader, lh, 3L, 3L);
        assertThatThrownBy(beyond::get).hasCauseInstanceOf(BKException.BKReadException.class);
        verify(lh).readAsync(2L, 2L);
        verify(lh).readAsync(3L, 3L);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testReadOnlyRangeWithStaleInventoryStillUsesConfirmedRead() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        when(lh.isClosed()).thenReturn(false);
        LedgerEntries entries = createLedgerEntries(1L, 0L, 1L);
        when(lh.readAsync(0L, 1L)).thenReturn(CompletableFuture.completedFuture(entries));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 0L, 1L);

        assertThat(future).isCompletedWithValue(entries);
        future.getNow(null).close();
        verify(lh).readAsync(0L, 1L);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
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

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 2L, 2L);

        assertThat(future).isCompletedWithValue(entries);
        future.getNow(null).close();
        verify(lh).readAsync(2L, 2L);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testReadOnlyClosedSingleEntryPastFinalHandleBoundFails() {
        ManagedLedger reader = mock(ReadOnlyManagedLedgerImpl.class);
        when(reader.getOptionalLedgerInfo(1L)).thenReturn(Optional.of(new LedgerInfo().setLedgerId(1L)));
        when(reader.getLastConfirmedEntry()).thenReturn(PositionFactory.create(1L, 1L));
        when(lh.isClosed()).thenReturn(true);
        when(lh.getLastAddConfirmed()).thenReturn(2L);
        when(lh.readAsync(3L, 3L)).thenReturn(CompletableFuture.failedFuture(new BKException.BKReadException()));

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, 3L, 3L);

        assertThatThrownBy(future::get).hasCauseInstanceOf(BKException.BKReadException.class);
        verify(lh).readAsync(3L, 3L);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
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

        CompletableFuture<LedgerEntries> future = ReadEntryUtils.readAsync(reader, lh, first, 1L);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get).hasCauseInstanceOf(ManagedLedgerException.class);
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
        verify(lh, never()).readAsync(anyLong(), anyLong());
    }

    private static LedgerEntries createLedgerEntries(long ledgerId, long... entryIds) {
        List<LedgerEntry> entries = new ArrayList<>();
        for (long entryId : entryIds) {
            entries.add(LedgerEntryImpl.create(ledgerId, entryId, 1,
                    Unpooled.wrappedBuffer(new byte[]{(byte) entryId})));
        }
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
                    entries.forEach(LedgerEntry::close);
                }
            }
        };
    }
}
