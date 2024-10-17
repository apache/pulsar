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

import static java.util.Objects.requireNonNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.StampedLock;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Contains cursors for a ManagedLedger.
 * <p>
 * The goal is to always know the slowest consumer and hence decide which is the oldest ledger we need to keep.
 * <p>
 * This data structure maintains a heap and a map of cursors. The map is used to relate a cursor name with
 * an entry index in the heap. The heap data structure sorts cursors in a binary tree which is represented
 * in a single array. More details about heap implementations:
 * <a href="https://en.wikipedia.org/wiki/Heap_(data_structure)#Implementation">here</a>
 * <p>
 * The heap is updated and kept sorted when a cursor is updated.
 *
 */
public class ManagedCursorContainer implements Iterable<ManagedCursor> {

    /**
     * This field is incremented everytime the cursor information is updated.
     */
    private long version;

    @Value
    public static class CursorInfo {
        ManagedCursor cursor;
        Position position;

        /**
         * Cursor info's version.
         * <p>
         * Use {@link  DataVersion#compareVersions(long, long)} to compare between two versions,
         * since it rolls over to 0 once reaching Long.MAX_VALUE
         */
        long version;
    }

    private static class Item {
        final ManagedCursor cursor;
        Position position;
        int idx;

        Item(ManagedCursor cursor, Position position, int idx) {
            this.cursor = cursor;
            this.position = position;
            this.idx = idx;
        }
    }

    /**
     * Utility class to manage a data version, which rolls over to 0 when reaching Long.MAX_VALUE.
     */
    @UtilityClass
    public class DataVersion {

        /**
         * Compares two data versions, which either rolls overs to 0 when reaching Long.MAX_VALUE.
         * <p>
         * Use {@link DataVersion#getNextVersion(long)} to increment the versions. The assumptions
         * are that metric versions are compared with close time proximity one to another, hence,
         * they are expected not close to each other in terms of distance, hence we don't
         * expect the distance ever to exceed Long.MAX_VALUE / 2, otherwise we wouldn't be able
         * to know which one is a later version in case the furthest rolls over to beyond 0. We
         * assume the shortest distance between them dictates that.
         * <p>
         * @param v1 First version to compare
         * @param v2 Second version to compare
         * @return the value {@code 0} if {@code v1 == v2};
         *         a value less than {@code 0} if {@code v1 < v2}; and
         *         a value greater than {@code 0} if {@code v1 > v2}
         */
        public static int compareVersions(long v1, long v2) {
            if (v1 == v2) {
                return 0;
            }

            // 0-------v1--------v2--------MAX_LONG
            if (v2 > v1) {
                long distance = v2 - v1;
                long wrapAroundDistance = (Long.MAX_VALUE - v2) + v1;
                if (distance < wrapAroundDistance) {
                    return -1;
                } else {
                    return 1;
                }

            // 0-------v2--------v1--------MAX_LONG
            } else {
                long distance = v1 - v2;
                long wrapAroundDistance = (Long.MAX_VALUE - v1) + v2;
                if (distance < wrapAroundDistance) {
                    return 1; // v1 is bigger
                } else {
                    return -1; // v2 is bigger
                }
            }
        }

        public static long getNextVersion(long existingVersion) {
            if (existingVersion == Long.MAX_VALUE) {
                return 0;
            } else {
                return existingVersion + 1;
            }
        }
    }

    public ManagedCursorContainer() {}

    // Used to keep track of slowest cursor.
    private final ArrayList<Item> heap = new ArrayList<>();

    // Maps a cursor to its position in the heap
    private final ConcurrentMap<String, Item> cursors = new ConcurrentSkipListMap<>();

    private final StampedLock rwLock = new StampedLock();

    private int durableCursorCount;


    /**
     * Add a cursor to the container. The cursor will be optionally tracked for the slowest reader when
     * a position is passed as the second argument. It is expected that the position is updated with
     * {@link #cursorUpdated(ManagedCursor, Position)} method when the position changes.
     *
     * @param cursor cursor to add
     * @param position position of the cursor to use for ordering, pass null if the cursor's position shouldn't be
     *                 tracked for the slowest reader.
     */
    public void add(ManagedCursor cursor, Position position) {
        long stamp = rwLock.writeLock();
        try {
            Item item = new Item(cursor, position, position != null ? heap.size() : -1);
            cursors.put(cursor.getName(), item);
            if (position != null) {
                heap.add(item);
                if (heap.size() > 1) {
                    siftUp(item);
                }
            }
            if (cursor.isDurable()) {
                durableCursorCount++;
            }
            version = DataVersion.getNextVersion(version);
        } finally {
            rwLock.unlockWrite(stamp);
        }
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

    public boolean removeCursor(String name) {
        long stamp = rwLock.writeLock();
        try {
            Item item = cursors.remove(name);
            if (item != null) {
                if (item.idx >= 0) {
                    if (heap.size() == 1) {
                        heap.clear();
                    } else {
                        // Move the item to the right end of the heap to be removed
                        Item lastItem = heap.get(heap.size() - 1);
                        swap(item, lastItem);
                        heap.remove(item.idx);
                        // Update the heap
                        siftDown(lastItem);
                    }
                }
                if (item.cursor.isDurable()) {
                    durableCursorCount--;
                }
                version = DataVersion.getNextVersion(version);
                return true;
            } else {
                return false;
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    /**
     * Signal that a cursor position has been updated and that the container must re-order the cursor heap
     * tracking the slowest reader.
     * Only those cursors are tracked and can be updated which were added to the container with the
     * {@link #add(ManagedCursor, Position)} method that specified the initial position in the position
     * parameter.
     *
     * @param cursor the cursor to update the position for
     * @param newPosition the updated position for the cursor
     * @return a pair of positions, representing the previous slowest reader and the new slowest reader (after the
     *         update).
     */
    public Pair<Position, Position> cursorUpdated(ManagedCursor cursor, Position newPosition) {
        requireNonNull(cursor);

        long stamp = rwLock.writeLock();
        try {
            Item item = cursors.get(cursor.getName());
            if (item == null || item.idx == -1) {
                return null;
            }

            Position previousSlowestConsumer = heap.get(0).position;
            item.position = newPosition;
            version = DataVersion.getNextVersion(version);

            if (heap.size() == 1) {
                return Pair.of(previousSlowestConsumer, item.position);
            }

            // When the cursor moves forward, we need to push it toward the
            // bottom of the tree and push it up if a reset was done
            if (item.idx == 0 || getParent(item).position.compareTo(item.position) <= 0) {
                siftDown(item);
            } else {
                siftUp(item);
            }
            Position newSlowestConsumer = heap.get(0).position;
            return Pair.of(previousSlowestConsumer, newSlowestConsumer);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    /**
     * Get the slowest reader position for the cursors that are ordered.
     *
     * @return the slowest reader position
     */
    public Position getSlowestReaderPosition() {
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
     * @return Returns the CursorInfo for the cursor with the oldest position,
     *         or null if there aren't any tracked cursors
     */
    public CursorInfo getCursorWithOldestPosition() {
        long stamp = rwLock.readLock();
        try {
            if (heap.isEmpty()) {
                return null;
            } else {
                Item item = heap.get(0);
                return new CursorInfo(item.cursor, item.position, version);
            }
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    /**
     *  Check whether there are any cursors.
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
     * Check whether that are any durable cursors.
     * @return true if there are durable cursors and false if there are not
     */
    public boolean hasDurableCursors() {
        long stamp = rwLock.tryOptimisticRead();
        int count = durableCursorCount;
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                count = durableCursorCount;
            } finally {
                rwLock.unlockRead(stamp);
            }
        }

        return count > 0;
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
                throw new IllegalArgumentException("Cannot remove ManagedCursor from container");
            }
        };
    }

    // //////////////////////

    /**
     * Push the item up towards the root of the tree (the lowest reading position).
     */
    private void siftUp(Item item) {
        Item parent = getParent(item);
        while (item.idx > 0 && parent.position.compareTo(item.position) > 0) {
            swap(item, parent);
            parent = getParent(item);
        }
    }

    /**
     * Push the item down towards the bottom of the tree (the highest reading position).
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
