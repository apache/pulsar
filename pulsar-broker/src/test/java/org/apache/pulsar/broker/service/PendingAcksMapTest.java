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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.BrokerTestUtil.createMockConsumer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.common.util.collections.IntIntPair;
import org.testng.annotations.Test;

public class PendingAcksMapTest {
    @Test
    public void addPendingAckIfAllowed_AddsAckWhenAllowed() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);

        boolean result = pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        assertTrue(result);
        assertTrue(pendingAcksMap.contains(1L, 1L));
    }

    @Test
    public void addPendingAckIfAllowed_DoesNotAddAckWhenNotAllowed() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksAddHandler addHandler = mock(PendingAcksMap.PendingAcksAddHandler.class);
        when(addHandler.handleAdding(any(), anyLong(), anyLong(), anyInt())).thenReturn(false);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> addHandler, () -> null);

        boolean result = pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        assertFalse(result);
        assertFalse(pendingAcksMap.contains(1L, 1L));
    }

    @Test
    public void addPendingAckIfAllowed_DoesNotAddAfterClosed() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.forEachAndClose((ledgerId, entryId, batchSize, stickyKeyHash) -> {});

        boolean result = pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        assertFalse(result);
        assertFalse(pendingAcksMap.contains(1L, 1L));
    }

    @Test
    public void forEach_ProcessesAllPendingAcks() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);

        List<Long> processedEntries = new ArrayList<>();
        pendingAcksMap.forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> processedEntries.add(entryId));

        assertEquals(processedEntries.size(), 2);
        assertEquals(new HashSet<>(processedEntries), Set.of(1L, 2L));
    }

    @Test
    public void forEachAndClose_ProcessesAndClearsAllPendingAcks() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);

        List<Long> processedEntries = new ArrayList<>();
        pendingAcksMap.forEachAndClose((ledgerId, entryId, batchSize, stickyKeyHash) -> processedEntries.add(entryId));

        assertEquals(processedEntries.size(), 2);
        assertEquals(new HashSet<>(processedEntries), Set.of(1L, 2L));
        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void remove_RemovesPendingAck() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        boolean result = pendingAcksMap.remove(1L, 1L);

        assertTrue(result);
        assertFalse(pendingAcksMap.contains(1L, 1L));
    }

    @Test
    public void removeAllUpTo_RemovesAllPendingAcksUpToSpecifiedEntry() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 1, 125);

        pendingAcksMap.removeAllUpTo(1L, 2L, (ledgerId, entryId, batchSize, stickyKeyHash) -> {
        });

        assertFalse(pendingAcksMap.contains(1L, 1L));
        assertFalse(pendingAcksMap.contains(1L, 2L));
        assertTrue(pendingAcksMap.contains(2L, 1L));
    }

    @Test
    public void removeAllUpTo_RemovesAllPendingAcksUpToSpecifiedEntryAcrossMultipleLedgers() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 1, 125);
        pendingAcksMap.addPendingAckIfAllowed(2L, 2L, 1, 126);
        pendingAcksMap.addPendingAckIfAllowed(3L, 1L, 1, 127);

        pendingAcksMap.removeAllUpTo(2L, 1L, (ledgerId, entryId, batchSize, stickyKeyHash) -> {
        });

        assertFalse(pendingAcksMap.contains(1L, 1L));
        assertFalse(pendingAcksMap.contains(1L, 2L));
        assertFalse(pendingAcksMap.contains(2L, 1L));
        assertTrue(pendingAcksMap.contains(2L, 2L));
        assertTrue(pendingAcksMap.contains(3L, 1L));
    }

    @Test
    public void addPendingAckIfAllowed_InvokesAddHandler() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksAddHandler addHandler = mock(PendingAcksMap.PendingAcksAddHandler.class);
        when(addHandler.handleAdding(any(), anyLong(), anyLong(), anyInt())).thenReturn(true);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> addHandler, () -> null);

        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        verify(addHandler).handleAdding(consumer, 1L, 1L, 123);
    }

    @Test
    public void remove_InvokesRemoveHandler() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksRemoveHandler removeHandler = mock(PendingAcksMap.PendingAcksRemoveHandler.class);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> removeHandler);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        pendingAcksMap.remove(1L, 1L);

        verify(removeHandler).handleRemoving(consumer, 1L, 1L, 123, false);
    }

    @Test
    public void removeAllUpTo_InvokesRemoveHandlerForEachEntry() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksRemoveHandler removeHandler = mock(PendingAcksMap.PendingAcksRemoveHandler.class);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> removeHandler);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 1, 125);

        pendingAcksMap.removeAllUpTo(1L, 2L, (ledgerId, entryId, batchSize, stickyKeyHash) -> {
        });

        verify(removeHandler).handleRemoving(consumer, 1L, 1L, 123, false);
        verify(removeHandler).handleRemoving(consumer, 1L, 2L, 124, false);
        verify(removeHandler, never()).handleRemoving(consumer, 2L, 1L, 125, false);
    }

    @Test
    public void removeAllUpToWithCallback_InvokesCallbackForEachRemovedEntry() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 3, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 5, 124);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 7, 125);

        List<String> callbackInvocations = new ArrayList<>();
        pendingAcksMap.removeAllUpTo(1L, 2L,
                (ledgerId, entryId, batchSize, stickyKeyHash) -> {
                    callbackInvocations.add(ledgerId + ":" + entryId + ":" + batchSize + ":" + stickyKeyHash);
                });

        assertEquals(callbackInvocations.size(), 2);
        assertEquals(new HashSet<>(callbackInvocations), Set.of("1:1:3:123", "1:2:5:124"));
        assertFalse(pendingAcksMap.contains(1L, 1L));
        assertFalse(pendingAcksMap.contains(1L, 2L));
        assertTrue(pendingAcksMap.contains(2L, 1L));
    }

    @Test
    public void size_ReturnsCorrectSize() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 1, 125);

        assertEquals(pendingAcksMap.size(), 3);
    }

    @Test
    public void forEachAndClear_ProcessesAndClearsAllPendingAcks() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);

        List<Long> processedEntries = new ArrayList<>();
        pendingAcksMap.forEachAndClear((ledgerId, entryId, batchSize, stickyKeyHash) -> processedEntries.add(entryId));

        assertEquals(processedEntries.size(), 2);
        assertEquals(new HashSet<>(processedEntries), Set.of(1L, 2L));
        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void forEachAndClear_AllowsAddingAfterClear() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        pendingAcksMap.forEachAndClear((ledgerId, entryId, batchSize, stickyKeyHash) -> {});

        // Unlike forEachAndClose, forEachAndClear should allow new additions
        boolean result = pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);
        assertTrue(result);
        assertTrue(pendingAcksMap.contains(1L, 2L));
    }

    @Test
    public void forEachAndClear_InvokesRemoveHandler() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksRemoveHandler removeHandler = mock(PendingAcksMap.PendingAcksRemoveHandler.class);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> removeHandler);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);

        pendingAcksMap.forEachAndClear((ledgerId, entryId, batchSize, stickyKeyHash) -> {});

        verify(removeHandler).startBatch();
        verify(removeHandler).handleRemoving(consumer, 1L, 1L, 123, false);
        verify(removeHandler).handleRemoving(consumer, 1L, 2L, 124, false);
        verify(removeHandler).endBatch();
    }

    @Test
    public void removeAndGet_RemovesAndReturnsEntry() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 5, 123);

        IntIntPair result = pendingAcksMap.removeAndGet(1L, 1L);

        assertTrue(result != null);
        assertEquals(result.leftInt(), 5);
        assertEquals(result.rightInt(), 123);
        assertFalse(pendingAcksMap.contains(1L, 1L));
    }

    @Test
    public void removeAndGet_ReturnsNullWhenNotFound() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);

        IntIntPair result = pendingAcksMap.removeAndGet(1L, 1L);

        assertTrue(result == null);
    }

    @Test
    public void removeAndGet_InvokesRemoveHandler() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksRemoveHandler removeHandler = mock(PendingAcksMap.PendingAcksRemoveHandler.class);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> removeHandler);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 5, 123);

        pendingAcksMap.removeAndGet(1L, 1L);

        verify(removeHandler).handleRemoving(consumer, 1L, 1L, 123, false);
    }

    @Test
    public void packedPendingAckFields_RoundTripThroughPublicAccessors() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        int[][] values = new int[][] {
                {0, 0},
                {0, -1},
                {1, Integer.MIN_VALUE},
                {1, Integer.MAX_VALUE},
                {Integer.MAX_VALUE, -123456789},
                {123456789, Integer.MIN_VALUE},
                {42, 0x80000001},
        };

        Set<String> expectedEntries = new HashSet<>();
        for (int i = 0; i < values.length; i++) {
            long entryId = i + 1L;
            int remainingUnacked = values[i][0];
            int stickyKeyHash = values[i][1];
            pendingAcksMap.addPendingAckIfAllowed(1L, entryId, remainingUnacked, stickyKeyHash);
            expectedEntries.add(packedFieldsKey(entryId, remainingUnacked, stickyKeyHash));
        }

        List<String> iteratedEntries = new ArrayList<>();
        pendingAcksMap.forEach((ledgerId, entryId, remainingUnacked, stickyKeyHash) ->
                iteratedEntries.add(packedFieldsKey(entryId, remainingUnacked, stickyKeyHash)));
        assertEquals(new HashSet<>(iteratedEntries), expectedEntries);

        for (int i = 0; i < values.length; i++) {
            long entryId = i + 1L;
            int remainingUnacked = values[i][0];
            int stickyKeyHash = values[i][1];

            assertPackedFields(pendingAcksMap.get(1L, entryId), remainingUnacked, stickyKeyHash);
            assertEquals(pendingAcksMap.getRemainingUnacked(1L, entryId), remainingUnacked);

            IntIntPair removed = pendingAcksMap.removeAndGet(1L, entryId);
            assertPackedFields(removed, remainingUnacked, stickyKeyHash);
            assertFalse(pendingAcksMap.contains(1L, entryId));
        }
    }

    @Test
    public void removeAllUpTo_PreservesPackedFieldsInCallback() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        int[][] values = new int[][] {
                {7, 0},
                {8, -1},
                {9, Integer.MIN_VALUE},
                {10, Integer.MAX_VALUE},
        };
        Set<String> expectedRemovedEntries = new HashSet<>();
        for (int i = 0; i < values.length; i++) {
            long entryId = i + 1L;
            int remainingUnacked = values[i][0];
            int stickyKeyHash = values[i][1];
            pendingAcksMap.addPendingAckIfAllowed(1L, entryId, remainingUnacked, stickyKeyHash);
            expectedRemovedEntries.add(packedFieldsKey(entryId, remainingUnacked, stickyKeyHash));
        }
        pendingAcksMap.addPendingAckIfAllowed(1L, values.length + 1L, 11, -123);

        List<String> removedEntries = new ArrayList<>();
        pendingAcksMap.removeAllUpTo(1L, values.length, (ledgerId, entryId, remainingUnacked, stickyKeyHash) ->
                removedEntries.add(packedFieldsKey(entryId, remainingUnacked, stickyKeyHash)));

        assertEquals(new HashSet<>(removedEntries), expectedRemovedEntries);
        assertTrue(pendingAcksMap.contains(1L, values.length + 1L));
    }

    @Test
    public void updateRemainingUnacked_PreservesPackedStickyKeyHash() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        int stickyKeyHash = Integer.MIN_VALUE;
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 100, stickyKeyHash);

        assertTrue(pendingAcksMap.updateRemainingUnacked(1L, 1L, 7));

        assertPackedFields(pendingAcksMap.get(1L, 1L), 93, stickyKeyHash);
        assertTrue(pendingAcksMap.remove(1L, 1L, 93, stickyKeyHash));
        assertFalse(pendingAcksMap.contains(1L, 1L));
    }

    @Test
    public void getRemainingUnacked_ReturnsMinusOneWhenNotFound() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);

        assertEquals(pendingAcksMap.getRemainingUnacked(1L, 1L), -1);

        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 0, 123);

        assertEquals(pendingAcksMap.getRemainingUnacked(1L, 1L), 0);
    }

    @Test
    public void removeAndGetRemainingUnacked_InvokesRemoveHandler() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksRemoveHandler removeHandler = mock(PendingAcksMap.PendingAcksRemoveHandler.class);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> removeHandler);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 5, 123);

        int removed = pendingAcksMap.removeAndGetRemainingUnacked(1L, 1L);

        assertEquals(removed, 5);
        assertEquals(pendingAcksMap.removeAndGetRemainingUnacked(1L, 1L), -1);
        verify(removeHandler).handleRemoving(consumer, 1L, 1L, 123, false);
    }

    @Test
    public void removeAllUpTo_RemovesBoundaryEntriesWithUnorderedInnerMap() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(2L, 11L, 1, 111);
        pendingAcksMap.addPendingAckIfAllowed(2L, 3L, 1, 103);
        pendingAcksMap.addPendingAckIfAllowed(2L, 7L, 1, 107);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 1, 101);
        pendingAcksMap.addPendingAckIfAllowed(2L, 9L, 1, 109);
        pendingAcksMap.addPendingAckIfAllowed(3L, 1L, 1, 201);

        pendingAcksMap.removeAllUpTo(2L, 7L, (ledgerId, entryId, batchSize, stickyKeyHash) -> {
        });

        assertFalse(pendingAcksMap.contains(2L, 1L));
        assertFalse(pendingAcksMap.contains(2L, 3L));
        assertFalse(pendingAcksMap.contains(2L, 7L));
        assertTrue(pendingAcksMap.contains(2L, 9L));
        assertTrue(pendingAcksMap.contains(2L, 11L));
        assertTrue(pendingAcksMap.contains(3L, 1L));
    }

    @Test
    public void size_RemainsCorrectAcrossCommonOperations() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);

        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 2, 124);
        assertEquals(pendingAcksMap.size(), 1);

        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 3, 125);
        assertEquals(pendingAcksMap.size(), 2);

        assertTrue(pendingAcksMap.updateRemainingUnacked(1L, 1L, 1));
        assertEquals(pendingAcksMap.size(), 2);

        assertFalse(pendingAcksMap.remove(1L, 1L, 99, 124));
        assertEquals(pendingAcksMap.size(), 2);

        assertTrue(pendingAcksMap.remove(1L, 1L));
        assertEquals(pendingAcksMap.size(), 1);

        pendingAcksMap.removeAllUpTo(2L, 1L, (ledgerId, entryId, batchSize, stickyKeyHash) -> {
        });
        assertEquals(pendingAcksMap.size(), 0);

        pendingAcksMap.addPendingAckIfAllowed(3L, 1L, 1, 126);
        pendingAcksMap.forEachAndClear((ledgerId, entryId, batchSize, stickyKeyHash) -> {
        });
        assertEquals(pendingAcksMap.size(), 0);
    }

    private static void assertPackedFields(IntIntPair actual, int remainingUnacked, int stickyKeyHash) {
        assertTrue(actual != null);
        assertEquals(actual.leftInt(), remainingUnacked);
        assertEquals(actual.rightInt(), stickyKeyHash);
    }

    private static String packedFieldsKey(long entryId, int remainingUnacked, int stickyKeyHash) {
        return entryId + ":" + remainingUnacked + ":" + stickyKeyHash;
    }
}
