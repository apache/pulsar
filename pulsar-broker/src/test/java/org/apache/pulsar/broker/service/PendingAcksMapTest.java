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
    }

    @Test
    public void removeAllUpTo_RemovesAllPendingAcksUpToSpecifiedEntry() {
        Consumer consumer = createMockConsumer("consumer1");
        PendingAcksMap pendingAcksMap = new PendingAcksMap(consumer, () -> null, () -> null);
        pendingAcksMap.addPendingAckIfAllowed(1L, 1L, 1, 123);
        pendingAcksMap.addPendingAckIfAllowed(1L, 2L, 1, 124);
        pendingAcksMap.addPendingAckIfAllowed(2L, 1L, 1, 125);

        pendingAcksMap.removeAllUpTo(1L, 2L);

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

        pendingAcksMap.removeAllUpTo(2L, 1L);

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

        pendingAcksMap.removeAllUpTo(1L, 2L);

        verify(removeHandler).handleRemoving(consumer, 1L, 1L, 123, false);
        verify(removeHandler).handleRemoving(consumer, 1L, 2L, 124, false);
        verify(removeHandler, never()).handleRemoving(consumer, 2L, 1L, 125, false);
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
}