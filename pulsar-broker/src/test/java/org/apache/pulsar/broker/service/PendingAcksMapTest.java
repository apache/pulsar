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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class PendingAcksMapTest {
    @Test
    public void addPendingAckIfAllowed_AddsAckWhenAllowed() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);

        boolean result = pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        assertTrue(result);
        assertTrue(pendingAcksMap.contains(1L, 1L));
        assertEquals(pendingAcksMap.size(), 1);
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
        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void addPendingAckIfAllowed_DoesNotAddAfterClosed() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.forEachAndClose((ledgerId, entryId, batchSize, stickyKeyHash) -> {});

        boolean result = pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        assertFalse(result);
        assertFalse(pendingAcksMap.contains(1L, 1L));
        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void forEach_ProcessesAllPendingAcks() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);

        List<Long> processedEntries = new ArrayList<>();
        pendingAcksMap.forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> processedEntries.add(entryId));

        assertEquals(processedEntries, List.of(1L, 2L));
    }

    @Test
    public void forEachAndClose_ProcessesAndClearsAllPendingAcks() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);

        List<Long> processedEntries = new ArrayList<>();
        pendingAcksMap.forEachAndClose((ledgerId, entryId, batchSize, stickyKeyHash) -> processedEntries.add(entryId));

        assertEquals(processedEntries, List.of(1L, 2L));
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
        assertEquals(pendingAcksMap.size(), 0);
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
        assertEquals(pendingAcksMap.size(), 1);
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
        assertEquals(pendingAcksMap.size(), 2);
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

        List<int[]> callbackInvocations = new ArrayList<>();
        pendingAcksMap.removeAllUpTo(1L, 2L,
                (ledgerId, entryId, batchSize, stickyKeyHash) -> {
                    callbackInvocations.add(new int[]{(int) ledgerId, (int) entryId, batchSize, stickyKeyHash});
                });

        assertEquals(callbackInvocations.size(), 2);
        assertEquals(callbackInvocations.get(0), new int[]{1, 1, 3, 123});
        assertEquals(callbackInvocations.get(1), new int[]{1, 2, 5, 124});
        assertFalse(pendingAcksMap.contains(1L, 1L));
        assertFalse(pendingAcksMap.contains(1L, 2L));
        assertTrue(pendingAcksMap.contains(2L, 1L));
    }

    @Test
    public void size_ReturnsCorrectSize() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        assertEquals(pendingAcksMap.size(), 0);

        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 1, 125);

        assertEquals(pendingAcksMap.size(), 3);

        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 10, 123);
        pendingAcksMap.updateRemainingUnacked(1L, 1L, 2);

        assertEquals(pendingAcksMap.size(), 3);

        pendingAcksMap.remove(1L, 2L);

        assertEquals(pendingAcksMap.size(), 2);

        pendingAcksMap.removeAllUpTo(1L, 1L, (ledgerId, entryId, batchSize, stickyKeyHash) -> {
        });

        assertEquals(pendingAcksMap.size(), 1);

        assertFalse(pendingAcksMap.remove(2L, 1L, 10, 125));

        assertEquals(pendingAcksMap.size(), 1);

        assertTrue(pendingAcksMap.remove(2L, 1L, 1, 125));

        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void forEachAndClear_ProcessesAndClearsAllPendingAcks() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);

        List<Long> processedEntries = new ArrayList<>();
        pendingAcksMap.forEachAndClear((ledgerId, entryId, batchSize, stickyKeyHash) -> processedEntries.add(entryId));

        assertEquals(processedEntries, List.of(1L, 2L));
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
    public void removeAndGetRemainingUnacked_RemovesAndReturnsEntry() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 5, 123);

        int result = pendingAcksMap.removeAndGetRemainingUnacked(1L, 1L);

        assertEquals(result, 5);
        assertFalse(pendingAcksMap.contains(1L, 1L));
        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void removeAndGetRemainingUnacked_ReturnsNotFoundWhenNotFound() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);

        int result = pendingAcksMap.removeAndGetRemainingUnacked(1L, 1L);

        assertEquals(result, PendingAcksMap.PENDING_ACK_NOT_FOUND);
        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void removeAndGetRemainingUnacked_InvokesRemoveHandler() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksRemoveHandler removeHandler = mock(PendingAcksMap.PendingAcksRemoveHandler.class);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> removeHandler);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 5, 123);

        pendingAcksMap.removeAndGetRemainingUnacked(1L, 1L);

        verify(removeHandler).handleRemoving(consumer, 1L, 1L, 123, false);
    }

    @Test
    public void getRemainingUnacked_ReturnsStoredCount() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 0, -1);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, Integer.MAX_VALUE, Integer.MIN_VALUE);

        assertEquals(pendingAcksMap.getRemainingUnacked(1L, 1L), 0);
        assertEquals(pendingAcksMap.getRemainingUnacked(1L, 2L), Integer.MAX_VALUE);
        assertEquals(pendingAcksMap.getRemainingUnacked(1L, 3L), PendingAcksMap.PENDING_ACK_NOT_FOUND);
    }

    @Test
    public void removeAndGetRemainingUnacked_RemovesAndInvokesRemoveHandler() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap.PendingAcksRemoveHandler removeHandler = mock(PendingAcksMap.PendingAcksRemoveHandler.class);
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> removeHandler);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 0, Integer.MIN_VALUE);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, Integer.MAX_VALUE, -1);

        assertEquals(pendingAcksMap.removeAndGetRemainingUnacked(1L, 1L), 0);
        assertEquals(pendingAcksMap.removeAndGetRemainingUnacked(1L, 2L), Integer.MAX_VALUE);
        assertEquals(pendingAcksMap.removeAndGetRemainingUnacked(1L, 3L), PendingAcksMap.PENDING_ACK_NOT_FOUND);

        verify(removeHandler).handleRemoving(consumer, 1L, 1L, Integer.MIN_VALUE, false);
        verify(removeHandler).handleRemoving(consumer, 1L, 2L, -1, false);
        verifyNoMoreInteractions(removeHandler);
        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void packedPendingAckFields_RoundTripThroughPublicAccessors() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        int[][] values = new int[][] {
                {1, 123},
                {0, 0},
                {Integer.MAX_VALUE, Integer.MIN_VALUE},
                {2, Integer.MAX_VALUE},
                {42, -123456789}
        };

        for (int i = 0; i < values.length; i++) {
            pendingAcksMap.addPendingAckIfAllowed(10L + i, 100L + i, values[i][0], values[i][1]);
        }

        List<int[]> forEachValues = new ArrayList<>();
        pendingAcksMap.forEach((ledgerId, entryId, remainingUnacked, stickyKeyHash) ->
                forEachValues.add(new int[] {
                        (int) ledgerId, (int) entryId, remainingUnacked, stickyKeyHash
                }));
        assertEquals(forEachValues.size(), values.length);
        for (int i = 0; i < values.length; i++) {
            assertEquals(pendingAcksMap.getRemainingUnacked(10L + i, 100L + i), values[i][0]);
            assertEquals(forEachValues.get(i), new int[] {10 + i, 100 + i, values[i][0], values[i][1]});
        }

        for (int i = 0; i < values.length; i++) {
            assertEquals(pendingAcksMap.removeAndGetRemainingUnacked(10L + i, 100L + i), values[i][0]);
        }
        assertEquals(pendingAcksMap.size(), 0);
    }

    @Test
    public void updateRemainingUnacked_PreservesStickyKeyHash() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 10, Integer.MIN_VALUE);

        assertTrue(pendingAcksMap.updateRemainingUnacked(1L, 1L, 4));

        assertOnlyPendingAck(pendingAcksMap, 1L, 1L, 6, Integer.MIN_VALUE);
    }

    @Test
    public void updateRemainingUnacked_AllowsZeroRemainingUnacked() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, -1);

        assertTrue(pendingAcksMap.updateRemainingUnacked(1L, 1L, 1));

        assertEquals(pendingAcksMap.getRemainingUnacked(1L, 1L), 0);
        assertOnlyPendingAck(pendingAcksMap, 1L, 1L, 0, -1);
        assertEquals(pendingAcksMap.size(), 1);
    }

    @Test
    public void updateRemainingUnacked_DoesNotStoreNegativeRemainingUnacked() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);

        assertFalse(pendingAcksMap.updateRemainingUnacked(1L, 1L, 2));

        assertOnlyPendingAck(pendingAcksMap, 1L, 1L, 1, 123);
    }

    @Test
    public void removeAllUpTo_PreservesPackedFieldsInCallback() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, Integer.MAX_VALUE, Integer.MIN_VALUE);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, -1);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 2, Integer.MAX_VALUE);

        List<int[]> callbackInvocations = new ArrayList<>();
        pendingAcksMap.removeAllUpTo(1L, 2L, (ledgerId, entryId, remainingUnacked, stickyKeyHash) ->
                callbackInvocations.add(new int[] {
                        (int) ledgerId, (int) entryId, remainingUnacked, stickyKeyHash
                }));

        assertEquals(callbackInvocations.size(), 2);
        assertEquals(callbackInvocations.get(0), new int[] {1, 1, Integer.MAX_VALUE, Integer.MIN_VALUE});
        assertEquals(callbackInvocations.get(1), new int[] {1, 2, 1, -1});
        assertTrue(pendingAcksMap.contains(2L, 1L));
    }

    @Test
    public void removeWithValue_MatchesSignedPackedFields() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, Integer.MAX_VALUE, -1);

        assertFalse(pendingAcksMap.remove(1L, 1L, Integer.MAX_VALUE, 1));
        assertTrue(pendingAcksMap.remove(1L, 1L, Integer.MAX_VALUE, -1));
        assertFalse(pendingAcksMap.contains(1L, 1L));
    }

    private static void assertOnlyPendingAck(PendingAcksMap pendingAcksMap, long ledgerId, long entryId,
                                             int remainingUnacked, int stickyKeyHash) {
        List<long[]> pendingAcks = new ArrayList<>();
        pendingAcksMap.forEach((actualLedgerId, actualEntryId, actualRemainingUnacked, actualStickyKeyHash) ->
                pendingAcks.add(new long[] {
                        actualLedgerId, actualEntryId, actualRemainingUnacked, actualStickyKeyHash
                }));
        assertEquals(pendingAcks.size(), 1);
        assertEquals(pendingAcks.get(0), new long[] {ledgerId, entryId, remainingUnacked, stickyKeyHash});
    }
}
