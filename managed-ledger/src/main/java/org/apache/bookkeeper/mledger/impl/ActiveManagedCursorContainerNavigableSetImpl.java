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
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Implementation of {@link ActiveManagedCursorContainer} that uses a {@link NavigableSet} to keep track of the
 * cursors ordered by their positions. This implementation is currently used in JMH benchmarks to compare
 * performance of different cursor containers.
 */
public class ActiveManagedCursorContainerNavigableSetImpl implements ActiveManagedCursorContainer {
    // Counter to keep track of the order in which cursors are added.
    // This is used to maintain a stable order of cursors that have the same position.
    private final AtomicInteger addOrderCounter = new AtomicInteger(0);

    private static class Item implements Comparable<Item> {
        final ManagedCursor cursor;
        final int addOrder;
        Position position;

        Item(ManagedCursor cursor, Position position, int addOrder) {
            this.cursor = cursor;
            this.addOrder = addOrder;
            this.position = position;
        }

        @Override
        public int compareTo(Item o) {
            int positionComparison = this.position.compareTo(o.position);
            if (positionComparison != 0) {
                return positionComparison;
            }
            // If positions are equal, compare by add order to maintain a stable order
            // for cursors that have the same position.
            return Integer.compare(this.addOrder, o.addOrder);
        }
    }

    public ActiveManagedCursorContainerNavigableSetImpl() {}

    // Used to keep track of slowest cursor and the cursors before a given position.
    private final NavigableSet<Item> sortedByPosition = new TreeSet<>();

    // Maps a cursor to its position in the heap
    private final ConcurrentMap<String, Item> cursors = new ConcurrentHashMap<>();

    private final StampedLock rwLock = new StampedLock();

    @Override
    public void add(ManagedCursor cursor, Position position) {
        long stamp = rwLock.writeLock();
        try {
            Item item = new Item(cursor, position, addOrderCounter.getAndIncrement());
            cursors.put(cursor.getName(), item);
            if (position != null) {
                sortedByPosition.add(item);
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public ManagedCursor get(String name) {
        long stamp = rwLock.readLock();
        try {
            Item item = cursors.get(name);
            return item != null ? item.cursor : null;
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    @Override
    public boolean removeCursor(String name) {
        long stamp = rwLock.writeLock();
        try {
            Item item = cursors.remove(name);
            if (item != null) {
                if (item.position != null) {
                    sortedByPosition.remove(item);
                }
                return true;
            } else {
                return false;
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public Pair<Position, Position> cursorUpdated(ManagedCursor cursor, Position newPosition) {
        requireNonNull(cursor);

        long stamp = rwLock.writeLock();
        try {
            Item item = cursors.get(cursor.getName());
            if (item == null) {
                return null;
            }
            Position previousSlowestConsumer = internalSlowestReaderPosition();
            // it is necessary to remove the item from the sorted set
            // before updating the position, since otherwise sorting would not work correctly
            if (item.position != null) {
                sortedByPosition.remove(item);
            }
            item.position = newPosition;
            // if the new position is not null, we add it back to the sorted set
            // to maintain the order of cursors by position
            if (newPosition != null) {
                sortedByPosition.add(item);
            }
            Position newSlowestConsumer = internalSlowestReaderPosition();
            return Pair.of(previousSlowestConsumer, newSlowestConsumer);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void updateCursor(ManagedCursor cursor, Position newPosition) {
        cursorUpdated(cursor, newPosition);
    }

    private Position internalSlowestReaderPosition() {
        return !sortedByPosition.isEmpty() ? sortedByPosition.first().position : null;
    }

    @Override
    public Position getSlowestCursorPosition() {
        long stamp = rwLock.readLock();
        try {
            return internalSlowestReaderPosition();
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    @Override
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

    @Override
    public int getNumberOfCursorsAtSamePositionOrBefore(ManagedCursor cursor) {
        long stamp = rwLock.readLock();
        try {
            Item item = cursors.get(cursor.getName());
            if (item == null || item.position == null) {
                return 0;
            } else {
                int count = 0;
                for (Item o : sortedByPosition) {
                    if (o.position.compareTo(item.position) > 0) {
                        break;
                    }
                    count++;
                }
                return count;
            }
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    @Override
    public int size() {
        long stamp = rwLock.tryOptimisticRead();
        int size = cursors.size();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                size = cursors.size();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return size;
    }

    /**
     * Check the ordering and number of cursors count for each cursor.
     * This method is used for testing purposes to ensure that the internal state is consistent.
     */
    @VisibleForTesting
    void checkOrderingAndNumberOfCursorsState() {
        int currentCount = 0;
        Position lastPosition = null;
        List<String> lastPositionCursorNames = new ArrayList<>();
        Item previousItem = null;
        for (Item o : sortedByPosition) {
            currentCount++;
            if (lastPosition != null && o.position.compareTo(lastPosition) > 0) {
                int expectedCount = currentCount - 1;
                int numberOfCursorsAtSamePositionOrBefore =
                        getNumberOfCursorsAtSamePositionOrBefore(previousItem.cursor);
                if (numberOfCursorsAtSamePositionOrBefore != expectedCount) {
                    throw new IllegalStateException("Number of cursors at same position is not correct for position "
                            + previousItem.position + ": expected " + expectedCount
                            + ", but got " + numberOfCursorsAtSamePositionOrBefore + " for cursors "
                            + lastPositionCursorNames);
                }
                lastPositionCursorNames.clear();
            }
            lastPosition = o.position;
            lastPositionCursorNames.add(o.cursor.getName());
            previousItem = o;
        }
        if (lastPosition != null) {
            int expectedCount = currentCount;
            int numberOfCursorsAtSamePositionOrBefore = getNumberOfCursorsAtSamePositionOrBefore(previousItem.cursor);
            if (numberOfCursorsAtSamePositionOrBefore != expectedCount) {
                throw new IllegalStateException("Number of cursors at same position is not correct for position "
                        + previousItem.position + ": expected " + expectedCount
                        + ", but got " + numberOfCursorsAtSamePositionOrBefore + " for cursors "
                        + lastPositionCursorNames);
            }
        }
    }
}
