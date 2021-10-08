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

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.StampedLock;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Contains all the cursors for a ManagedLedger.
 *
 * <p/>The goal is to always know the slowest consumer and hence decide which is the oldest ledger we need to keep.
 *
 * <p/>This data structure maintains a list and a map of cursors. The map is used to relate a cursor name with an entry
 * in the linked-list. The list is a sorted double linked-list of cursors.
 *
 * <p/>When a cursor is markDeleted, this list is updated and the cursor is moved in its new position.
 *
 * <p/>To minimize the moving around, the order is maintained using the ledgerId, but not the entryId, since we only
 * care about ledgers to be deleted.
 *
 */
public class ManagedCursorContainer implements Iterable<ManagedCursor> {

    private static class Item {
        final ManagedCursor cursor;
        PositionImpl position;
        int idx;

        Item(ManagedCursor cursor, int idx) {
            this.cursor = cursor;
            this.position = (PositionImpl) cursor.getMarkDeletedPosition();
            this.idx = idx;
        }
    }

    public enum CursorType {
        DurableCursor,
        NonDurableCursor,
        ALL
    }

    public ManagedCursorContainer() {
        cursorType = CursorType.DurableCursor;
    }

    public ManagedCursorContainer(CursorType cursorType) {
        this.cursorType = cursorType;
    }

    private final CursorType cursorType;

    // Used to keep track of slowest cursor. Contains all of all active cursors.
    private final ArrayList<Item> heap = Lists.newArrayList();

    // Maps a cursor to its position in the heap
    private final ConcurrentMap<String, Item> cursors = new ConcurrentSkipListMap<>();

    private final StampedLock rwLock = new StampedLock();

    public void add(ManagedCursor cursor) {
        long stamp = rwLock.writeLock();
        try {
            // Append a new entry at the end of the list
            Item item = new Item(cursor, heap.size());
            cursors.put(cursor.getName(), item);

            if (shouldTrackInHeap(cursor)) {
                heap.add(item);
                siftUp(item);
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    private boolean shouldTrackInHeap(ManagedCursor cursor) {
        return CursorType.ALL.equals(cursorType)
                || (cursor.isDurable() && CursorType.DurableCursor.equals(cursorType))
                || (!cursor.isDurable() && CursorType.NonDurableCursor.equals(cursorType));
    }

    public PositionImpl getSlowestReadPositionForActiveCursors() {
        return heap.isEmpty() ? null : (PositionImpl) heap.get(0).cursor.getReadPosition();
    }

    public ManagedCursor get(String name) {
        long stamp = rwLock.readLock();
        try {
            Item item = cursors.get(name);
            return item != null ? item.cursor : null;
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    public void removeCursor(String name) {
        long stamp = rwLock.writeLock();
        try {
            Item item = cursors.remove(name);
            if (item != null && shouldTrackInHeap(item.cursor)) {
                // Move the item to the right end of the heap to be removed
                Item lastItem = heap.get(heap.size() - 1);
                swap(item, lastItem);
                heap.remove(item.idx);
                // Update the heap
                siftDown(lastItem);
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    /**
     * Signal that a cursor position has been updated and that the container must re-order the cursor list.
     *
     * @param cursor
     * @return a pair of positions, representing the previous slowest consumer and the new slowest consumer (after the
     *         update).
     */
    public Pair<PositionImpl, PositionImpl> cursorUpdated(ManagedCursor cursor, Position newPosition) {
        checkNotNull(cursor);

        long stamp = rwLock.writeLock();
        try {
            Item item = cursors.get(cursor.getName());
            if (item == null) {
                return null;
            }


            if (shouldTrackInHeap(item.cursor)) {
                PositionImpl previousSlowestConsumer = heap.get(0).position;

                // When the cursor moves forward, we need to push it toward the
                // bottom of the tree and push it up if a reset was done

                item.position = (PositionImpl) newPosition;
                if (item.idx == 0 || getParent(item).position.compareTo(item.position) <= 0) {
                    siftDown(item);
                } else {
                    siftUp(item);
                }

                PositionImpl newSlowestConsumer = heap.get(0).position;
                return Pair.of(previousSlowestConsumer, newSlowestConsumer);
            }
            return null;
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    /**
     * Get the slowest reader position, meaning older acknowledged position between all the cursors.
     *
     * @return the slowest reader position
     */
    public PositionImpl getSlowestReaderPosition() {
        long stamp = rwLock.readLock();
        try {
            return heap.isEmpty() ? null : heap.get(0).position;
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    public ManagedCursor getSlowestReader() {
        long stamp = rwLock.readLock();
        try {
            return heap.isEmpty() ? null : heap.get(0).cursor;
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    /**
     *  Check whether there are any cursors
     * @return true is there are no cursors and false if there are
     */
    public boolean isEmpty() {
        long stamp = rwLock.tryOptimisticRead();
        boolean isEmpty = cursors.isEmpty();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                isEmpty = cursors.isEmpty();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }

        return isEmpty;
    }

    /**
     * Check whether that are any durable cursors
     * @return true if there are durable cursors and false if there are not
     */
    public boolean hasDurableCursors() {
        long stamp = rwLock.tryOptimisticRead();
        boolean isEmpty = heap.isEmpty();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                isEmpty = heap.isEmpty();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }

        return !isEmpty;
    }

    @Override
    public String toString() {
        long stamp = rwLock.readLock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append('[');

            boolean first = true;
            for (Item item : cursors.values()) {
                if (!first) {
                    sb.append(", ");
                }

                first = false;
                sb.append(item.cursor);
            }

            sb.append(']');
            return sb.toString();
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    @Override
    public Iterator<ManagedCursor> iterator() {
        final Iterator<Map.Entry<String, Item>> it = cursors.entrySet().iterator();
        return new Iterator<ManagedCursor>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public ManagedCursor next() {
                return it.next().getValue().cursor;
            }

            @Override
            public void remove() {
                throw new IllegalArgumentException("Cannot remove ManagedCursor form container");
            }
        };
    }

    // //////////////////////

    /**
     * Push the item up towards the the root of the tree (lowest reading position).
     */
    private void siftUp(Item item) {
        Item parent = getParent(item);
        while (item.idx > 0 && parent.position.compareTo(item.position) > 0) {
            swap(item, parent);
            parent = getParent(item);
        }
    }

    /**
     * Push the item down towards the bottom of the tree (highest reading position).
     */
    private void siftDown(final Item item) {
        while (true) {
            Item j = null;
            Item right = getRight(item);
            if (right != null && right.position.compareTo(item.position) < 0) {
                Item left = getLeft(item);
                if (left != null && left.position.compareTo(right.position) < 0) {
                    j = left;
                } else {
                    j = right;
                }
            } else {
                Item left = getLeft(item);
                if (left != null && left.position.compareTo(item.position) < 0) {
                    j = left;
                }
            }

            if (j != null) {
                swap(item, j);
            } else {
                break;
            }
        }
    }

    /**
     * Swap two items in the heap.
     */
    private void swap(Item item1, Item item2) {
        int idx1 = item1.idx;
        int idx2 = item2.idx;

        heap.set(idx2, item1);
        heap.set(idx1, item2);

        // Update the indexes too
        item1.idx = idx2;
        item2.idx = idx1;
    }

    private Item getParent(Item item) {
        return heap.get((item.idx - 1) / 2);
    }

    private Item getLeft(Item item) {
        int i = item.idx * 2 + 1;
        return i < heap.size() ? heap.get(i) : null;
    }

    private Item getRight(Item item) {
        int i = item.idx * 2 + 2;
        return i < heap.size() ? heap.get(i) : null;
    }
}
