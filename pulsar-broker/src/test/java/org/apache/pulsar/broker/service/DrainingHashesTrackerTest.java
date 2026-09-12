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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.broker.service.DrainingHashesTracker.UnblockingHandler;
import org.testng.annotations.Test;

public class DrainingHashesTrackerTest {
    @Test
    public void addEntry_AddsNewEntry() {
        Consumer consumer = createMockConsumer("consumer1");
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));

        tracker.addEntry(consumer, 1);

        assertNotNull(tracker.getEntry(1));
        assertSame(tracker.getEntry(1).getConsumer(), consumer);
    }

    @Test
    public void addEntry_ThrowsExceptionForZeroStickyHash() {
        Consumer consumer = createMockConsumer("consumer1");
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));

        assertThrows(IllegalArgumentException.class, () -> tracker.addEntry(consumer, 0));
    }

    @Test
    public void reduceRefCount_ReducesReferenceCount() {
        Consumer consumer = createMockConsumer("consumer1");
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));
        tracker.addEntry(consumer, 1);

        tracker.reduceRefCount(consumer, 1, false);

        assertNull(tracker.getEntry(1));
    }

    @Test
    public void reduceRefCount_DoesNotReduceForDifferentConsumer() {
        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));
        tracker.addEntry(consumer1, 1);

        assertThrows(IllegalStateException.class, () -> tracker.reduceRefCount(consumer2, 1, false));

        assertNotNull(tracker.getEntry(1));
        assertSame(tracker.getEntry(1).getConsumer(), consumer1);
    }

    @Test
    public void shouldBlockStickyKeyHash_DoesNotBlockForExistingEntryWhenSameConsumer() {
        Consumer consumer = createMockConsumer("consumer1");
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));
        tracker.addEntry(consumer, 1);

        boolean result = tracker.shouldBlockStickyKeyHash(consumer, 1);

        assertFalse(result);
    }

    @Test
    public void shouldBlockStickyKeyHash_BlocksForExistingEntryWhenDifferentConsumer() {
        Consumer consumer1 = createMockConsumer("consumer1");
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));
        tracker.addEntry(consumer1, 1);

        Consumer consumer2 = createMockConsumer("consumer2");
        boolean result = tracker.shouldBlockStickyKeyHash(consumer2, 1);

        assertTrue(result);
    }


    @Test
    public void shouldBlockStickyKeyHash_DoesNotBlockForNewEntry() {
        Consumer consumer = createMockConsumer("consumer1");
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));

        boolean result = tracker.shouldBlockStickyKeyHash(consumer, 1);

        assertFalse(result);
    }

    @Test
    public void startBatch_IncrementsBatchLevel() {
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));

        tracker.startBatch();
        assertEquals(tracker.batchLevel, 1);

        tracker.startBatch();
        assertEquals(tracker.batchLevel, 2);

        tracker.startBatch();
        assertEquals(tracker.batchLevel, 3);
    }

    @Test
    public void endBatch_DecrementsBatchLevel() {
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));
        tracker.startBatch();

        tracker.endBatch();

        assertEquals(tracker.batchLevel, 0);
    }

    @Test
    public void endBatch_InvokesUnblockingHandlerWhenUnblockedWhileBatching() {
        // given a tracker with unblocking handler
        UnblockingHandler unblockingHandler = mock(UnblockingHandler.class);
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", unblockingHandler);

        // when a hash is draining
        Consumer consumer1 = createMockConsumer("consumer1");
        tracker.addEntry(consumer1, 1);
        // and batch starts
        tracker.startBatch();

        // when hash gets blocked
        Consumer consumer2 = createMockConsumer("consumer2");
        tracker.shouldBlockStickyKeyHash(consumer2, 1);
        // and it gets unblocked
        tracker.reduceRefCount(consumer1, 1, false);

        // then no unblocking call should be done
        verify(unblockingHandler, never()).stickyKeyHashUnblocked(anyInt());

        // when batch ends
        tracker.endBatch();
        // then unblocking call should be done
        verify(unblockingHandler).stickyKeyHashUnblocked(-1);
    }

    @Test
    public void clear_RemovesAllEntries() {
        Consumer consumer = createMockConsumer("consumer1");
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", mock(UnblockingHandler.class));
        tracker.addEntry(consumer, 1);

        tracker.clear();

        assertNull(tracker.getEntry(1));
    }

    @Test
    public void unblockingHandler_InvokesStickyKeyHashUnblocked() {
        // given a tracker with unblocking handler
        UnblockingHandler unblockingHandler = mock(UnblockingHandler.class);
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", unblockingHandler);

        // when a hash is draining
        Consumer consumer = createMockConsumer("consumer1");
        tracker.addEntry(consumer, 1);
        // aand hash gets blocked
        Consumer consumer2 = createMockConsumer("consumer2");
        tracker.shouldBlockStickyKeyHash(consumer2, 1);
        // and hash gets unblocked
        tracker.reduceRefCount(consumer, 1, false);

        // then unblocking call should be done
        verify(unblockingHandler).stickyKeyHashUnblocked(1);
    }

    @Test
    public void unblockingHandler_DoesNotInvokeStickyKeyHashUnblockedWhenClosing() {
        // given a tracker with unblocking handler
        UnblockingHandler unblockingHandler = mock(UnblockingHandler.class);
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher1", unblockingHandler);

        // when a hash is draining
        Consumer consumer = createMockConsumer("consumer1");
        tracker.addEntry(consumer, 1);
        // aand hash gets blocked
        Consumer consumer2 = createMockConsumer("consumer2");
        tracker.shouldBlockStickyKeyHash(consumer2, 1);
        // and hash gets unblocked
        tracker.reduceRefCount(consumer, 1, true);

        // then unblocking call should be done
        verify(unblockingHandler, never()).stickyKeyHashUnblocked(anyInt());
    }
}