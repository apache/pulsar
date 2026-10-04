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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.Test;

public class EntryImplTest {

    @Test
    public void testFailedMetadataInitializationIsNotRetried() {
        ByteBuf bytes = Unpooled.buffer(4).writeInt(-1);
        EntryImpl entry = EntryImpl.create(1, 0, bytes);
        bytes.release();
        entry.data = spy(entry.data);
        try {
            entry.initializeMessageMetadataIfNeeded("ledger");
            entry.initializeMessageMetadataIfNeeded("ledger");
            assertThat(entry.getMessageMetadata()).isNull();
            assertThat(entry.getDataBuffer().readerIndex()).isZero();
            assertThat(entry.getDataBuffer().getInt(0)).isEqualTo(-1);
            verify(entry.data, times(1)).duplicate();
        } finally {
            entry.release();
        }
    }

    @Test
    public void testCreateWithLedgerIdEntryIdAndByteBuf() {
        // Given
        long ledgerId = 123L;
        long entryId = 456L;
        byte[] testData = "test-data".getBytes();
        ByteBuf data = Unpooled.wrappedBuffer(testData);

        // When
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, data, 1);

        try {
            // Then
            assertEntry(entry, ledgerId, entryId, testData);
        } finally {
            entry.release();
        }

        assertEquals(data.refCnt(), 1);
    }

    @Test
    public void testCreateWithPositionAndByteBuf() {
        // Given
        long ledgerId = 789L;
        long entryId = 101L;
        Position position = PositionFactory.create(ledgerId, entryId);
        byte[] testData = "position-test-data".getBytes();
        ByteBuf data = Unpooled.wrappedBuffer(testData);

        // When
        EntryImpl entry = EntryImpl.create(position, data, 1);

        try {
            // Then
            assertEntryPosition(entry, position);
            assertEntryData(entry, testData);
        } finally {
            entry.release();
        }

        assertEquals(data.refCnt(), 1);
    }

    @Test
    public void testCreateWithRetainedDuplicate() {
        // Given
        long ledgerId = 555L;
        long entryId = 666L;
        Position position = PositionFactory.create(ledgerId, entryId);
        byte[] testData = "retained-duplicate-test".getBytes();
        ByteBuf data = Unpooled.wrappedBuffer(testData);

        // When
        EntryImpl entry = EntryImpl.createWithRetainedDuplicate(position, data, 1);

        try {
            // Then
            assertEntryPosition(entry, position);
            assertEntryData(entry, testData);
            assertRetainedDuplicate(data, entry, testData);
        } finally {
            entry.release();
        }

        assertEquals(data.refCnt(), 1);
    }

    @Test
    public void testCreateFromAnotherEntryImpl() {
        // Given
        long ledgerId = 111L;
        long entryId = 222L;
        byte[] testData = "original-entry-data".getBytes();
        ByteBuf originalData = Unpooled.wrappedBuffer(testData);
        EntryImpl originalEntry = EntryImpl.create(ledgerId, entryId, originalData, 1);

        try {
            // When
            EntryImpl copiedEntry = EntryImpl.create(originalEntry);

            try {
                // Then
                assertEntryPosition(copiedEntry, originalEntry.getPosition());
                assertEntryData(copiedEntry, testData);
                assertRetainedDuplicate(originalData, copiedEntry, testData);
            } finally {
                copiedEntry.release();
            }
        } finally {
            originalEntry.release();
        }

        assertEquals(originalData.refCnt(), 1);
    }

    @Test
    public void testCreateFromGenericEntry() {
        // Given
        long ledgerId = 333L;
        long entryId = 444L;
        Position expectedPosition = PositionFactory.create(ledgerId, entryId);
        byte[] testData = "generic-entry-data".getBytes();
        ByteBuf dataBuffer = Unpooled.wrappedBuffer(testData);

        // Mock Entry interface
        Entry mockEntry = mock(Entry.class);
        when(mockEntry.getPosition()).thenReturn(expectedPosition);
        when(mockEntry.getLedgerId()).thenReturn(ledgerId);
        when(mockEntry.getEntryId()).thenReturn(entryId);
        when(mockEntry.getDataBuffer()).thenReturn(dataBuffer);

        // When
        EntryImpl entry = EntryImpl.create(mockEntry);

        try {
            // Then
            assertEntryPosition(entry, expectedPosition);
            assertEntryData(entry, testData);
            assertRetainedDuplicate(dataBuffer, entry, testData);
        } finally {
            entry.release();
        }

        assertEquals(dataBuffer.refCnt(), 1);
    }

    @Test
    public void testCreateWithEmptyData() {
        // Given
        long ledgerId = 999L;
        long entryId = 0L;
        byte[] emptyData = new byte[0];
        ByteBuf data = Unpooled.EMPTY_BUFFER;

        // When
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, data, 1);

        try {
            // Then
            assertEntry(entry, ledgerId, entryId, emptyData);
        } finally {
            entry.release();
        }
    }


    @Test
    public void testCreateFromEntryImplWhereGetPositionHasntBeenCalled() {
        // Given
        EntryImpl originalEntry = EntryImpl.create(1L, 2L, new byte[0]);
        EntryImpl newEntry = EntryImpl.create(originalEntry);

        // Expect that the position is created lazily and the instances are different
        assertNotSame(originalEntry.getPosition(), newEntry.getPosition());
        assertTrue(originalEntry.matchesPosition(newEntry.getPosition()));

        // Clean up
        originalEntry.release();
        newEntry.release();
    }

    @Test
    public void testCreateFromEntryImplWhereGetPositionHasBeenCalled() {
        // Given
        EntryImpl originalEntry = EntryImpl.create(1L, 2L, new byte[0]);
        originalEntry.getPosition();
        EntryImpl newEntry = EntryImpl.create(originalEntry);

        // Expect that the position instances are the same
        assertSame(originalEntry.getPosition(), newEntry.getPosition());

        // Clean up
        originalEntry.release();
        newEntry.release();
    }

    @Test
    public void testCreateWithPositionThatIsntImmutable() {
        // Given
        Position position = new AckSetPositionImpl(1L, 2L, new long[0]);
        EntryImpl entry = EntryImpl.create(position, Unpooled.EMPTY_BUFFER, 1);

        // Expect that the position is different since it's not immutable
        assertNotSame(entry.getPosition(), position);

        // Clean up
        entry.release();
    }

    @Test
    public void testCreateWithPositionThatIsImmutable() {
        // Given
        Position position = PositionFactory.create(1L, 2L);
        EntryImpl entry = EntryImpl.create(position, Unpooled.EMPTY_BUFFER, 1);

        // Expect that the position is same since it's immutable
        assertSame(entry.getPosition(), position);

        // Clean up
        entry.release();
    }

    @Test
    public void testRecycledObjectDoesNotInheritPoisonedPosition() throws Exception {
        // Netty's Recycler only pools instances for FastThreadLocalThreads; a plain test worker
        // thread receives a no-op handle, every create() would return a fresh object, and this
        // scenario would silently degrade to asserting fresh instances (see the review on #26707).
        // Run it on a FastThreadLocalThread and prove instance identity with assertSame.
        runOnFastThreadLocalThread(() -> {
            assertTrue(warmUpRecyclerUntilReused(),
                    "recycler never handed the same instance back; the scenario cannot exercise recycling");

            // The managed-ledger read path wraps a BookKeeper LedgerEntry.
            assertRecycledPoisonedPositionIsNotInherited("create(LedgerEntry, int)",
                    () -> createFromLedgerEntry(5L, 10L),
                    () -> createFromLedgerEntry(6L, 20L), 6L, 20L);
            // byte[] variant.
            assertRecycledPoisonedPositionIsNotInherited("byte[] variant",
                    () -> EntryImpl.create(5L, 10L, new byte[]{1, 2, 3}),
                    () -> EntryImpl.create(6L, 20L, new byte[]{4, 5, 6}), 6L, 20L);
            // ByteBuf variant.
            assertRecycledPoisonedPositionIsNotInherited("ByteBuf variant",
                    () -> createFromRetainedBuffer(5L, 10L),
                    () -> createFromRetainedBuffer(6L, 20L), 6L, 20L);
        });
    }

    private static void assertRecycledPoisonedPositionIsNotInherited(String variant,
                                                                     Supplier<EntryImpl> firstEntry,
                                                                     Supplier<EntryImpl> secondEntry,
                                                                     long expectedLedgerId,
                                                                     long expectedEntryId) {
        // Given a legitimate entry that is released normally
        EntryImpl first = firstEntry.get();
        first.release();

        // When a getPosition() call slips in AFTER the release: deallocation nulls the lazy
        // position field, so this late reader re-materializes it from the reset ids as (-1, -1)
        // and leaves the poisoned value cached inside the pooled object.
        first.getPosition();

        // Then the very next create() on this thread reuses that exact instance and must not
        // report the poisoned (-1, -1) position as its own.
        EntryImpl second = secondEntry.get();
        assertSame(second, first, variant + ": expected the recycler to reuse the same instance");
        assertTrue(second.getPosition().compareTo(PositionFactory.create(expectedLedgerId, expectedEntryId)) == 0,
                variant + ": a recycled entry must not inherit the poisoned (-1, -1) position");
        second.release();
    }

    private static EntryImpl createFromLedgerEntry(long ledgerId, long entryId) {
        ByteBuf buffer = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        LedgerEntry ledgerEntry = LedgerEntryImpl.create(ledgerId, entryId, buffer.readableBytes(), buffer);
        // LedgerEntryImpl.create adopts the caller's buffer reference (released again on close()),
        // while EntryImpl.create retains its own, so this ordering keeps the counts balanced.
        EntryImpl entry = EntryImpl.create(ledgerEntry, 0);
        ledgerEntry.close();
        return entry;
    }

    private static EntryImpl createFromRetainedBuffer(long ledgerId, long entryId) {
        ByteBuf buffer = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, buffer);
        // create(long, long, ByteBuf, int) retains the buffer; drop the caller-owned reference.
        buffer.release();
        return entry;
    }

    private static boolean warmUpRecyclerUntilReused() {
        // Absorb Netty's io.netty.recycler.ratio warm-up and prove the per-thread pool hands the
        // same instance back; without it the assertions above could be checking fresh objects.
        for (int i = 0; i < 64; i++) {
            EntryImpl warm = EntryImpl.create(0L, 0L, new byte[]{0});
            warm.release();
            EntryImpl again = EntryImpl.create(0L, 0L, new byte[]{0});
            boolean reused = again == warm;
            again.release();
            if (reused) {
                return true;
            }
        }
        return false;
    }

    private static void runOnFastThreadLocalThread(Runnable scenario) throws Exception {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        Thread thread = new FastThreadLocalThread(() -> {
            try {
                scenario.run();
                completion.complete(null);
            } catch (Throwable error) {
                completion.completeExceptionally(error);
            }
        });
        thread.start();
        // Rethrow assertion failures on the test thread.
        completion.get(30, TimeUnit.SECONDS);
    }

    private void assertEntryFields(EntryImpl entry, long expectedLedgerId, long expectedEntryId) {
        assertEquals(entry.getLedgerId(), expectedLedgerId);
        assertEquals(entry.getEntryId(), expectedEntryId);
        assertNotNull(entry.getPosition());
        assertEquals(entry.getPosition().getLedgerId(), expectedLedgerId);
        assertEquals(entry.getPosition().getEntryId(), expectedEntryId);
    }

    private void assertEntryData(EntryImpl entry, byte[] expectedData) {
        byte[] entryData = entry.getData();
        assertEquals(entryData, expectedData);
    }

    private void assertEntry(EntryImpl entry, long expectedLedgerId, long expectedEntryId,
                             byte[] expectedData) {
        assertEntryFields(entry, expectedLedgerId, expectedEntryId);
        assertEntryData(entry, expectedData);
        assertEntryPosition(entry, PositionFactory.create(expectedLedgerId, expectedEntryId));
    }

    private void assertEntryPosition(EntryImpl entry, Position expectedPosition) {
        assertEquals(entry.getLedgerId(), expectedPosition.getLedgerId());
        assertEquals(entry.getEntryId(), expectedPosition.getEntryId());
        assertTrue(entry.getPosition().compareTo(expectedPosition) == 0);
        assertTrue(entry.matchesPosition(expectedPosition));
    }

    private void assertRetainedDuplicate(ByteBuf originalDataBuffer, EntryImpl copiedEntry, byte[] testData) {
        // the new entry's readerIndex should be separate from the original buffer's readerIndex
        // since we created a retained duplicate
        originalDataBuffer.readByte();
        assertEntryData(copiedEntry, testData);
    }
}