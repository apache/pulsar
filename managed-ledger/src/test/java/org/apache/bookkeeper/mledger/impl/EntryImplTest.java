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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.Test;

public class EntryImplTest {

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