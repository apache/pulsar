/**
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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.SkipEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.testng.annotations.Test;

public class ManagedCursorContainerTest {

    private static class MockManagedCursor implements ManagedCursor {

        ManagedCursorContainer container;
        Position position;
        String name;

        public MockManagedCursor(ManagedCursorContainer container, String name, Position position) {
            this.container = container;
            this.name = name;
            this.position = position;
        }

        @Override
        public Map<String, Long> getProperties() {
            return Collections.emptyMap();
        }

        @Override
        public boolean putProperty(String key, Long value) {
            return false;
        }

        @Override
        public boolean removeProperty(String key) {
            return false;
        }

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public List<Entry> readEntries(int numberOfEntriesToRead) throws ManagedLedgerException {
            return Lists.newArrayList();
        }

        @Override
        public void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx,
                                     PositionImpl maxPosition) {
            callback.readEntriesComplete(null, ctx);
        }

        @Override
        public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes, ReadEntriesCallback callback,
                                     Object ctx, PositionImpl maxPosition) {
            callback.readEntriesComplete(null, ctx);
        }

        @Override
        public boolean hasMoreEntries() {
            return true;
        }

        @Override
        public long getNumberOfEntries() {
            return 0;
        }

        @Override
        public long getNumberOfEntriesInBacklog(boolean isPrecise) {
            return 0;
        }

        @Override
        public void markDelete(Position position) throws ManagedLedgerException {
            markDelete(position, Collections.emptyMap());
        }

        @Override
        public void markDelete(Position position, Map<String, Long> properties) throws ManagedLedgerException {
            this.position = position;
            container.cursorUpdated(this, (PositionImpl) position);
        }

        @Override
        public void asyncMarkDelete(Position position, MarkDeleteCallback callback, Object ctx) {
            fail();
        }

        @Override
        public void asyncMarkDelete(Position position, Map<String, Long> properties, MarkDeleteCallback callback,
                Object ctx) {
            fail();
        }

        @Override
        public Position getMarkDeletedPosition() {
            return position;
        }

        @Override
        public Position getPersistentMarkDeletedPosition() {
            return position;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public long getLastActive() {
            return System.currentTimeMillis();
        }

        @Override
        public void updateLastActive() {
            // no-op
        }

        public String toString() {
            return String.format("%s=%s", name, position);
        }

        @Override
        public Position getReadPosition() {
            return null;
        }

        @Override
        public void rewind() {
        }

        @Override
        public void seek(Position newReadPosition, boolean force) {
        }

        @Override
        public void close() {
        }

        @Override
        public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        }

        @Override
        public void delete(Position position) throws InterruptedException, ManagedLedgerException {
        }

        @Override
        public void asyncDelete(Position position, DeleteCallback callback, Object ctx) {
        }

        @Override
        public void delete(Iterable<Position> positions) throws InterruptedException, ManagedLedgerException {
        }

        @Override
        public void asyncDelete(Iterable<Position> position, DeleteCallback callback, Object ctx) {
        }

        @Override
        public void clearBacklog() throws InterruptedException, ManagedLedgerException {
        }

        @Override
        public void asyncClearBacklog(ClearBacklogCallback callback, Object ctx) {
        }

        @Override
        public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries)
                throws InterruptedException, ManagedLedgerException {
        }

        @Override
        public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
                final SkipEntriesCallback callback, Object ctx) {
        }

        @Override
        public Position findNewestMatching(Predicate<Entry> condition)
                throws InterruptedException, ManagedLedgerException {
            return null;
        }

        @Override
        public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
            return null;
        }

        @Override
        public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                AsyncCallbacks.FindEntryCallback callback, Object ctx) {
        }

        @Override
        public void asyncResetCursor(final Position position, AsyncCallbacks.ResetCursorCallback callback) {

        }

        @Override
        public void resetCursor(final Position position) throws ManagedLedgerException, InterruptedException {

        }

        @Override
        public Position getFirstPosition() {
            return null;
        }

        @Override
        public void setAlwaysInactive() {
        }

        @Override
        public List<Entry> replayEntries(Set<? extends Position> positions)
                throws InterruptedException, ManagedLedgerException {
            return null;
        }

        @Override
        public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions, ReadEntriesCallback callback, Object ctx) {
            return Sets.newConcurrentHashSet();
        }

        @Override
        public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions, ReadEntriesCallback callback, Object ctx, boolean sortEntries) {
            return Sets.newConcurrentHashSet();
        }

        @Override
        public List<Entry> readEntriesOrWait(int numberOfEntriesToRead)
                throws InterruptedException, ManagedLedgerException {
            return null;
        }

        @Override
        public void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx,
                                           PositionImpl maxPosition) {
        }

        @Override
        public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes, ReadEntriesCallback callback,
                                           Object ctx, PositionImpl maxPosition) {

        }

        @Override
        public boolean cancelPendingReadRequest() {
            return true;
        }

        @Override
        public Entry getNthEntry(int N, IndividualDeletedEntries deletedEntries)
                throws InterruptedException, ManagedLedgerException {
            return null;
        }

        @Override
        public void asyncGetNthEntry(int N, IndividualDeletedEntries deletedEntries, ReadEntryCallback callback,
                Object ctx) {
        }

        @Override
        public void setActive() {
        }

        @Override
        public void setInactive() {
        }

        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        public long getNumberOfEntriesSinceFirstNotAckedMessage() {
            return 0;
        }

        @Override
        public int getTotalNonContiguousDeletedMessagesRange() {
            return 0;
        }

        @Override
        public int getNonContiguousDeletedMessagesRangeSerializedSize() {
            return 0;
        }

        @Override
        public long getEstimatedSizeSinceMarkDeletePosition() {
            return 0L;
        }

        @Override
        public void setThrottleMarkDelete(double throttleMarkDelete) {
        }

        @Override
        public double getThrottleMarkDelete() {
            return -1;
        }

        @Override
        public ManagedLedger getManagedLedger() {
            return null;
        }

        @Override
        public Range<PositionImpl> getLastIndividualDeletedRange() {
            return null;
        }

        @Override
        public void trimDeletedEntries(List<Entry> entries) {

        }

        @Override
        public long[] getDeletedBatchIndexesAsLongArray(PositionImpl position) {
            return new long[0];
        }

        @Override
        public ManagedCursorMXBean getStats() {
            return null;
        }

        public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes, ReadEntriesCallback callback,
                Object ctx) {
        }

        @Override
        public List<Entry> readEntriesOrWait(int maxEntries, long maxSizeBytes)
                throws InterruptedException, ManagedLedgerException {
            return null;
        }

        @Override
        public boolean checkAndUpdateReadPositionChanged() {
            return false;
        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    @Test
    public void testSlowestReadPositionForActiveCursors() throws Exception {
        ManagedCursorContainer container =
                new ManagedCursorContainer(ManagedCursorContainer.CursorType.NonDurableCursor);
        assertNull(container.getSlowestReadPositionForActiveCursors());

        // Add no durable cursor
        PositionImpl position = PositionImpl.get(5,5);
        ManagedCursor cursor1 = spy(new MockManagedCursor(container, "test1", position));
        doReturn(false).when(cursor1).isDurable();
        doReturn(position).when(cursor1).getReadPosition();
        container.add(cursor1);
        assertEquals(container.getSlowestReadPositionForActiveCursors(), new PositionImpl(5, 5));

        // Add no durable cursor
        position = PositionImpl.get(1,1);
        ManagedCursor cursor2 = spy(new MockManagedCursor(container, "test2", position));
        doReturn(false).when(cursor2).isDurable();
        doReturn(position).when(cursor2).getReadPosition();
        container.add(cursor2);
        assertEquals(container.getSlowestReadPositionForActiveCursors(), new PositionImpl(1, 1));

        // Move forward cursor, cursor1 = 5:5 , cursor2 = 5:6, slowest is 5:5
        position = PositionImpl.get(5,6);
        container.cursorUpdated(cursor2, position);
        doReturn(position).when(cursor2).getReadPosition();
        assertEquals(container.getSlowestReadPositionForActiveCursors(), new PositionImpl(5, 5));

        // Move forward cursor, cursor1 = 5:8 , cursor2 = 5:6, slowest is 5:6
        position = PositionImpl.get(5,8);
        doReturn(position).when(cursor1).getReadPosition();
        container.cursorUpdated(cursor1, position);
        assertEquals(container.getSlowestReadPositionForActiveCursors(), new PositionImpl(5, 6));

        // Remove cursor, only cursor1 left, cursor1 = 5:8
        container.removeCursor(cursor2.getName());
        assertEquals(container.getSlowestReadPositionForActiveCursors(), new PositionImpl(5, 8));
    }

    @Test
    public void simple() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();
        assertNull(container.getSlowestReaderPosition());

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", new PositionImpl(5, 5));
        container.add(cursor1);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));

        ManagedCursor cursor2 = new MockManagedCursor(container, "test2", new PositionImpl(2, 2));
        container.add(cursor2);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        ManagedCursor cursor3 = new MockManagedCursor(container, "test3", new PositionImpl(2, 0));
        container.add(cursor3);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 0));

        assertEquals(container.toString(), "[test1=5:5, test2=2:2, test3=2:0]");

        ManagedCursor cursor4 = new MockManagedCursor(container, "test4", new PositionImpl(4, 0));
        container.add(cursor4);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 0));

        ManagedCursor cursor5 = new MockManagedCursor(container, "test5", new PositionImpl(3, 5));
        container.add(cursor5);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 0));

        cursor3.markDelete(new PositionImpl(3, 0));
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        cursor2.markDelete(new PositionImpl(10, 5));
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(3, 0));

        container.removeCursor(cursor3.getName());
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(3, 5));

        container.removeCursor(cursor2.getName());
        container.removeCursor(cursor5.getName());
        container.removeCursor(cursor1.getName());
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(4, 0));

        assertTrue(container.hasDurableCursors());

        container.removeCursor(cursor4.getName());
        assertNull(container.getSlowestReaderPosition());

        assertFalse(container.hasDurableCursors());

        ManagedCursor cursor6 = new MockManagedCursor(container, "test6", new PositionImpl(6, 5));
        container.add(cursor6);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(6, 5));

        assertEquals(container.toString(), "[test6=6:5]");
    }

    @Test
    public void updatingCursorOutsideContainer() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", new PositionImpl(5, 5));
        container.add(cursor1);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));

        MockManagedCursor cursor2 = new MockManagedCursor(container, "test2", new PositionImpl(2, 2));
        container.add(cursor2);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        cursor2.position = new PositionImpl(8, 8);

        // Until we don't update the container, the ordering will not change
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        container.cursorUpdated(cursor2, cursor2.position);

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));
    }

    @Test
    public void removingCursor() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", new PositionImpl(5, 5));
        container.add(cursor1);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));
        assertEquals(container.get("test1"), cursor1);

        MockManagedCursor cursor2 = new MockManagedCursor(container, "test2", new PositionImpl(2, 2));
        container.add(cursor2);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));
        assertEquals(container.get("test2"), cursor2);

        MockManagedCursor cursor3 = new MockManagedCursor(container, "test3", new PositionImpl(1, 1));
        container.add(cursor3);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(1, 1));
        assertEquals(container.get("test3"), cursor3);

        assertEquals(container, Lists.newArrayList(cursor1, cursor2, cursor3));

        // Remove the cursor in the middle
        container.removeCursor("test2");

        assertEquals(container, Lists.newArrayList(cursor1, cursor3));

        assertNull(container.get("test2"));

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(1, 1));

        container.removeCursor("test3");

        assertEquals(container, Lists.newArrayList(cursor1));

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));
    }

    @Test
    public void ordering() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", new PositionImpl(5, 5));
        ManagedCursor cursor2 = new MockManagedCursor(container, "test2", new PositionImpl(5, 1));
        ManagedCursor cursor3 = new MockManagedCursor(container, "test3", new PositionImpl(7, 1));
        ManagedCursor cursor4 = new MockManagedCursor(container, "test4", new PositionImpl(6, 4));
        ManagedCursor cursor5 = new MockManagedCursor(container, "test5", new PositionImpl(7, 0));

        container.add(cursor1);
        container.add(cursor2);
        container.add(cursor3);
        container.add(cursor4);
        container.add(cursor5);

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 1));
        container.removeCursor("test2");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));
        container.removeCursor("test1");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(6, 4));
        container.removeCursor("test4");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(7, 0));
        container.removeCursor("test5");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(7, 1));
        container.removeCursor("test3");

        assertFalse(container.hasDurableCursors());
    }

    @Test
    public void orderingWithUpdates() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();

        MockManagedCursor c1 = new MockManagedCursor(container, "test1", new PositionImpl(5, 5));
        MockManagedCursor c2 = new MockManagedCursor(container, "test2", new PositionImpl(5, 1));
        MockManagedCursor c3 = new MockManagedCursor(container, "test3", new PositionImpl(7, 1));
        MockManagedCursor c4 = new MockManagedCursor(container, "test4", new PositionImpl(6, 4));
        MockManagedCursor c5 = new MockManagedCursor(container, "test5", new PositionImpl(7, 0));

        container.add(c1);
        container.add(c2);
        container.add(c3);
        container.add(c4);
        container.add(c5);

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 1));

        c1.position = new PositionImpl(5, 8);
        container.cursorUpdated(c1, c1.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 1));

        c2.position = new PositionImpl(5, 6);
        container.cursorUpdated(c2, c2.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 6));

        c1.position = new PositionImpl(6, 8);
        container.cursorUpdated(c1, c1.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 6));

        c3.position = new PositionImpl(8, 5);
        container.cursorUpdated(c3, c3.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 6));

        c1.position = new PositionImpl(8, 4);
        container.cursorUpdated(c1, c1.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 6));

        c2.position = new PositionImpl(8, 4);
        container.cursorUpdated(c2, c2.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(6, 4));

        c4.position = new PositionImpl(7, 1);
        container.cursorUpdated(c4, c4.position);

        // ////

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(7, 0));
        container.removeCursor("test5");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(7, 1));
        container.removeCursor("test4");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(8, 4));
        container.removeCursor("test1");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(8, 4));
        container.removeCursor("test2");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(8, 5));
        container.removeCursor("test3");

        assertFalse(container.hasDurableCursors());
    }

    @Test
    public void orderingWithUpdatesAndReset() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();

        MockManagedCursor c1 = new MockManagedCursor(container, "test1", new PositionImpl(5, 5));
        MockManagedCursor c2 = new MockManagedCursor(container, "test2", new PositionImpl(5, 1));
        MockManagedCursor c3 = new MockManagedCursor(container, "test3", new PositionImpl(7, 1));
        MockManagedCursor c4 = new MockManagedCursor(container, "test4", new PositionImpl(6, 4));
        MockManagedCursor c5 = new MockManagedCursor(container, "test5", new PositionImpl(7, 0));

        container.add(c1);
        container.add(c2);
        container.add(c3);
        container.add(c4);
        container.add(c5);

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 1));

        c1.position = new PositionImpl(5, 8);
        container.cursorUpdated(c1, c1.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 1));

        c1.position = new PositionImpl(5, 6);
        container.cursorUpdated(c1, c1.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 1));

        c2.position = new PositionImpl(6, 8);
        container.cursorUpdated(c2, c2.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 6));

        c3.position = new PositionImpl(8, 5);
        container.cursorUpdated(c3, c3.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 6));

        c1.position = new PositionImpl(8, 4);
        container.cursorUpdated(c1, c1.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(6, 4));

        c2.position = new PositionImpl(4, 4);
        container.cursorUpdated(c2, c2.position);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(4, 4));

        c4.position = new PositionImpl(7, 1);
        container.cursorUpdated(c4, c4.position);

        // ////

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(4, 4));
        container.removeCursor("test2");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(7, 0));
        container.removeCursor("test5");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(7, 1));
        container.removeCursor("test1");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(7, 1));
        container.removeCursor("test4");

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(8, 5));
        container.removeCursor("test3");

        assertFalse(container.hasDurableCursors());
    }
}
