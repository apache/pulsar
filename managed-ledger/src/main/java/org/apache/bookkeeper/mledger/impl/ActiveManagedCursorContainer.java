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

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Contains cursors for a ManagedLedger that are actively being used.
 * <p>
 * The goal is to be able to use the container to be used for cache eviction where the tracking of the slowest cursor
 * is important for determining which cache entries can be evicted.
 * <p>
 * The usage depends on the cache eviction configuration:
 *
 * <p>When cacheEvictionByMarkDeletedPosition is enabled, the slowest cursor is the one with the oldest mark deleted
 * position. Otherwise, it is the one with the oldest read position.
 *
 * <p>When cacheEvictionByExpectedReadCount is enabled, the slowest cursor is not necessarily tracked actively for
 * every update and the position of the slowest cursor is not returned immediately. The implementation optimized
 * for cacheEvictionByExpectedReadCount will throw {@link UnsupportedOperationException} for calls to
 * {@link #cursorUpdated(ManagedCursor, Position)} method. The {@link #updateCursor(ManagedCursor, Position)} method
 * is used instead to update the cursor position.
 */
public interface ActiveManagedCursorContainer extends Iterable<ManagedCursor> {
    /**
     * Adds a cursor to the container with the specified position.
     *
     * @param cursor    The cursor to add
     * @param position  The position of the cursor, if null, the cursor won't be tracked in the slowest cursor
     *                 tracking.
     */
    void add(ManagedCursor cursor, Position position);

    /**
     * Gets a cursor by its name.
     *
     * @param name the name of the cursor
     * @return the ManagedCursor if found, otherwise null
     */
    ManagedCursor get(String name);

    /**
     * Removes a cursor from the container by its name.
     *
     * @param name the name of the cursor to remove
     * @return true if the cursor was removed, false if it was not found
     */
    boolean removeCursor(String name);

    /**
     * Signal that a cursor position has been updated and that the container must re-order the cursor heap
     * tracking the slowest cursor and return the previous position of the slowest cursor and the possibly updated
     * position of the slowest cursor.
     *
     * @param cursor the cursor to update the position for
     * @param newPosition the updated position for the cursor
     * @return a pair of positions, representing the previous slowest cursor and the new slowest cursor (after the
     *         update).
     */
    Pair<Position, Position> cursorUpdated(ManagedCursor cursor, Position newPosition);

    /**
     * Updates the cursor position without immediately returning the positions for the slowest cursor.
     * Compared to {@link #cursorUpdated(ManagedCursor, Position)}, this method does require the implementation
     * to immediately re-order the cursor heap tracking the slowest cursor allowing to optimize the performance
     * to batch updates in re-ordering.
     *
     * @param cursor       The cursor to update
     * @param newPosition  The new position to set for the cursor
     */
    void updateCursor(ManagedCursor cursor, Position newPosition);

    /**
     * Gets the position of the slowest cursor.
     *
     * @return the position of the slowest cursor
     */
    Position getSlowestCursorPosition();

    /**
     * Checks if the container is empty.
     *
     * @return true if the container has no cursors, false otherwise
     */
    boolean isEmpty();

    /**
     * Gets the number of cursors in the container.
     *
     * @return the number of cursors
     */
    int size();

    /**
     * Returns the number of cursors that are at the same position or before the given cursor.
     * This is currently used in the cacheEvictionByExpectedReadCount cache eviction algorithm implementation
     * to estimate how many reads are expected to be performed by cursors for a given position.
     * @param cursor the cursor for which to count the number of cursors at the same position or before
     * @return the number of cursors at the same position or before the given cursor, includes the cursor itself
     */
    default int getNumberOfCursorsAtSamePositionOrBefore(ManagedCursor cursor) {
        throw new UnsupportedOperationException("This method is not supported by this implementation");
    }

    /**
     * Returns true if added entries should be cached. The implementation s
     * @return
     */
    default boolean shouldCacheAddedEntry() {
        return !isEmpty();
    }
}