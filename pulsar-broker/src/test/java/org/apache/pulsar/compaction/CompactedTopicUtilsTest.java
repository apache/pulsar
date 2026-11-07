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
package org.apache.pulsar.compaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompactedTopicUtilsTest {

    @Test
    public void testReadCompactedEntriesWithEmptyEntries() throws ExecutionException, InterruptedException {
        Position lastCompactedPosition = PositionFactory.create(1, 100);
        TopicCompactionService service = Mockito.mock(TopicCompactionService.class);
        Mockito.doReturn(CompletableFuture.completedFuture(Collections.emptyList()))
                .when(service).readCompactedEntries(Mockito.any(), Mockito.intThat(argument -> argument > 0));
        Mockito.doReturn(CompletableFuture.completedFuture(lastCompactedPosition)).when(service)
                .getLastCompactedPosition();


        Position initPosition = PositionFactory.create(1, 90);
        AtomicReference<Position> readPositionRef = new AtomicReference<>(initPosition.getNext());
        ManagedCursorImpl cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn(readPositionRef.get()).when(cursor).getReadPosition();
        Mockito.doReturn(1).when(cursor).applyMaxSizeCap(Mockito.anyInt(), Mockito.anyLong());
        Mockito.doAnswer(invocation -> {
            readPositionRef.set(invocation.getArgument(0));
            return null;
        }).when(cursor).seek(Mockito.any());

        CompletableFuture<List<Entry>> completableFuture = new CompletableFuture<>();
        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        AsyncCallbacks.ReadEntriesCallback readEntriesCallback = new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                completableFuture.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                completableFuture.completeExceptionally(exception);
                throwableRef.set(exception);
            }
        };

        final var entries = CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 1, 100,
                PositionFactory.LATEST, false, false).get();
        Assert.assertTrue(entries.isEmpty());
        Assert.assertNull(throwableRef.get());
        Assert.assertEquals(readPositionRef.get(), lastCompactedPosition.getNext());
    }

    @Test
    public void testOriginalLedgerReaderIgnoresLaterCompaction() throws Exception {
        // The reader went on to the original ledger at 1:91, then a compaction moved the horizon to 1:100.
        TopicCompactionService service = mockService(PositionFactory.create(1, 100));
        ManagedCursorImpl cursor = mockCursor(PositionFactory.create(1, 95), PositionFactory.create(1, -1));
        Entry tombstone = mockEntry(PositionFactory.create(1, 95));
        Mockito.doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(2);
            callback.readEntriesComplete(new ArrayList<>(List.of(tombstone)), invocation.getArgument(3));
            return null;
        }).when(cursor).asyncReadEntries(Mockito.anyInt(), Mockito.anyLong(), Mockito.any(), Mockito.any(),
                Mockito.any());

        // The transaction max read position still bounds the read.
        Position maxReadPosition = PositionFactory.create(1, 97);
        List<Entry> entries = CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 10, 1000,
                maxReadPosition, false, false, PositionFactory.create(1, 91), null).get();

        Assert.assertEquals(entries, List.of(tombstone));
        Mockito.verify(cursor).asyncReadEntries(Mockito.eq(10), Mockito.eq(1000L), Mockito.any(), Mockito.any(),
                Mockito.eq(maxReadPosition));
        Mockito.verify(service, Mockito.never()).readCompactedEntries(Mockito.any(), Mockito.anyInt());
        Mockito.verify(cursor, Mockito.never()).seek(Mockito.any());
        Mockito.verify(cursor, Mockito.never()).seek(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    public void testCompactedReadStopsAtOriginalLedgerReadPosition() throws Exception {
        // The cursor was rewound below the position where the reader went on to the original ledger.
        Position originalLedgerReadPosition = PositionFactory.create(1, 91);
        TopicCompactionService service = mockService(PositionFactory.create(1, 100));
        ManagedCursorImpl cursor = mockCursor(PositionFactory.create(1, 80), PositionFactory.create(1, -1));
        Entry before = mockEntry(PositionFactory.create(1, 85));
        Entry after = mockEntry(PositionFactory.create(1, 95));
        Mockito.doReturn(CompletableFuture.completedFuture(new ArrayList<>(List.of(before, after))))
                .when(service).readCompactedEntries(Mockito.any(), Mockito.anyInt());

        List<Entry> entries = CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 10, 1000,
                PositionFactory.LATEST, false, false, originalLedgerReadPosition, null).get();

        Assert.assertEquals(entries, List.of(before));
        Mockito.verify(after).release();
        Mockito.verify(before, Mockito.never()).release();
        Mockito.verify(cursor).seek(PositionFactory.create(1, 86), true);
    }

    @Test
    public void testTrimmedOriginalLedgerFallsBackToCompactedLedger() throws Exception {
        // Retention deleted ledger 1, so the compacted ledger is the only copy of the data at 1:95.
        TopicCompactionService service = mockService(PositionFactory.create(3, 0));
        ManagedCursorImpl cursor = mockCursor(PositionFactory.create(1, 95), PositionFactory.create(2, -1));
        Entry compacted = mockEntry(PositionFactory.create(2, 5));
        Mockito.doReturn(CompletableFuture.completedFuture(new ArrayList<>(List.of(compacted))))
                .when(service).readCompactedEntries(Mockito.any(), Mockito.anyInt());

        List<Entry> entries = CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 10, 1000,
                PositionFactory.LATEST, false, false, PositionFactory.create(1, 91), null).get();

        Assert.assertEquals(entries, List.of(compacted));
        Mockito.verify(cursor).seek(PositionFactory.create(2, 6), true);
    }

    @Test
    public void testEndOfCompactedSnapshotIsReported() throws Exception {
        Position lastCompactedPosition = PositionFactory.create(1, 100);
        TopicCompactionService service = mockService(lastCompactedPosition);
        ManagedCursorImpl cursor = mockCursor(PositionFactory.create(1, 80), PositionFactory.create(1, -1));
        Entry last = mockEntry(lastCompactedPosition);
        Mockito.doReturn(CompletableFuture.completedFuture(new ArrayList<>(List.of(last))))
                .when(service).readCompactedEntries(Mockito.any(), Mockito.anyInt());

        AtomicReference<Position> reported = new AtomicReference<>();
        CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 10, 1000, PositionFactory.LATEST, false,
                false, null, reported::set).get();
        Assert.assertEquals(reported.get(), lastCompactedPosition.getNext());

        // No surviving entry left before the horizon: the cursor skips to the original ledger.
        Mockito.doReturn(CompletableFuture.completedFuture(Collections.emptyList()))
                .when(service).readCompactedEntries(Mockito.any(), Mockito.anyInt());
        reported.set(null);
        CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 10, 1000, PositionFactory.LATEST, false,
                false, null, reported::set).get();
        Assert.assertEquals(reported.get(), lastCompactedPosition.getNext());
    }

    @Test
    public void testIndividuallyAcknowledgedEntriesAreSkipped() throws Exception {
        // The cursor acknowledged 1:90 individually, and its mark-delete position is before the compacted entries.
        TopicCompactionService service = mockService(PositionFactory.create(1, 100));
        ManagedCursorImpl cursor = mockCursor(PositionFactory.create(1, 80), PositionFactory.create(1, -1));
        Mockito.doReturn(PositionFactory.create(1, 79)).when(cursor).getMarkDeletedPosition();
        Position acknowledgedPosition = PositionFactory.create(1, 90);
        Mockito.doReturn(true).when(cursor).isMessageDeleted(acknowledgedPosition);
        Entry unacknowledged = mockEntry(PositionFactory.create(1, 85));
        Entry acknowledged = mockEntry(acknowledgedPosition);
        Mockito.doReturn(CompletableFuture.completedFuture(new ArrayList<>(List.of(unacknowledged, acknowledged))))
                .when(service).readCompactedEntries(Mockito.any(), Mockito.anyInt());

        List<Entry> entries = CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 10, 1000,
                PositionFactory.LATEST, false, false).get();

        Assert.assertEquals(entries, List.of(unacknowledged));
        Mockito.verify(acknowledged).release();
        Mockito.verify(unacknowledged, Mockito.never()).release();
        // The next read starts after the skipped entry.
        Mockito.verify(cursor).seek(PositionFactory.create(1, 91), true);
    }

    @Test
    public void testEntriesUpToMarkDeletePositionAreKept() throws Exception {
        // A read from the earliest position, such as the compactor's, returns the entries up to the mark-delete
        // position, although the cursor reports them as deleted.
        TopicCompactionService service = mockService(PositionFactory.create(1, 100));
        ManagedCursorImpl cursor = mockCursor(PositionFactory.create(1, 96), PositionFactory.create(1, -1));
        Mockito.doReturn(PositionFactory.create(1, 95)).when(cursor).getMarkDeletedPosition();
        Mockito.doReturn(true).when(cursor).isMessageDeleted(Mockito.any(Position.class));
        Entry first = mockEntry(PositionFactory.create(1, 85));
        Entry last = mockEntry(PositionFactory.create(1, 95));
        Mockito.doReturn(CompletableFuture.completedFuture(new ArrayList<>(List.of(first, last))))
                .when(service).readCompactedEntries(Mockito.any(), Mockito.anyInt());

        List<Entry> entries = CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 10, 1000,
                PositionFactory.LATEST, true, false).get();

        Assert.assertEquals(entries, List.of(first, last));
        Mockito.verify(service).readCompactedEntries(Mockito.eq(PositionFactory.EARLIEST), Mockito.anyInt());
    }

    private static TopicCompactionService mockService(Position lastCompactedPosition) {
        TopicCompactionService service = Mockito.mock(TopicCompactionService.class);
        Mockito.doReturn(CompletableFuture.completedFuture(lastCompactedPosition)).when(service)
                .getLastCompactedPosition();
        return service;
    }

    private static ManagedCursorImpl mockCursor(Position readPosition, Position firstLedgerPosition) {
        ManagedLedger managedLedger = Mockito.mock(ManagedLedger.class);
        Mockito.doReturn(firstLedgerPosition).when(managedLedger).getFirstPosition();
        ManagedCursorImpl cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn(managedLedger).when(cursor).getManagedLedger();
        Mockito.doReturn(readPosition).when(cursor).getReadPosition();
        Mockito.doReturn(10).when(cursor).applyMaxSizeCap(Mockito.anyInt(), Mockito.anyLong());
        return cursor;
    }

    private static Entry mockEntry(Position position) {
        Entry entry = Mockito.mock(Entry.class);
        Mockito.doReturn(position).when(entry).getPosition();
        Mockito.doReturn(10).when(entry).getLength();
        return entry;
    }
}
