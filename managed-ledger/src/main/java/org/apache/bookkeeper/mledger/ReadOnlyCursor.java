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
package org.apache.bookkeeper.mledger;

import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.Range;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ReadOnlyCursor {
    /**
     * Read entries from the ManagedLedger, up to the specified number. The returned list can be smaller.
     *
     * @param numberOfEntriesToRead maximum number of entries to return
     * @return the list of entries
     * @throws ManagedLedgerException
     */
    List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronously read entries from the ManagedLedger.
     *
     * @param numberOfEntriesToRead maximum number of entries to return
     * @param callback              callback object
     * @param ctx                   opaque context
     * @param maxPosition           max position can read
     * @see #readEntries(int)
     */
    void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback,
                          Object ctx, PositionImpl maxPosition);

    /**
     * Asynchronously read entries from the ManagedLedger.
     *
     * @param numberOfEntriesToRead maximum number of entries to return
     * @param maxSizeBytes          max size in bytes of the entries to return
     * @param callback              callback object
     * @param ctx                   opaque context
     * @param maxPosition           max position can read
     */
    void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes, ReadEntriesCallback callback,
                          Object ctx, PositionImpl maxPosition);

    /**
     * Get the read position. This points to the next message to be read from the cursor.
     *
     * @return the read position
     */
    Position getReadPosition();

    /**
     * Tells whether this cursor has already consumed all the available entries.
     * <p>
     * <p/>
     * This method is not blocking.
     *
     * @return true if there are pending entries to read, false otherwise
     */
    boolean hasMoreEntries();

    /**
     * Return the number of messages that this cursor still has to read.
     * <p>
     * <p/>
     * This method has linear time complexity on the number of ledgers included in the managed ledger.
     *
     * @return the number of entries
     */
    long getNumberOfEntries();

    /**
     * Skip n entries from the read position of this cursor.
     *
     * @param numEntriesToSkip number of entries to skip
     */
    void skipEntries(int numEntriesToSkip);

    /**
     * Find the newest entry that matches the given predicate.
     *
     * @param constraint search only active entries or all entries
     * @param condition  predicate that reads an entry an applies a condition
     * @return Position of the newest entry that matches the given predicate
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    Position findNewestMatching(ManagedCursor.FindPositionConstraint constraint, Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException;

    /**
     * Return the number of messages that this cursor still has to read.
     *
     * <p/>This method has linear time complexity on the number of ledgers included in the managed ledger.
     *
     * @param range the range between two positions
     * @return the number of entries in range
     */
    long getNumberOfEntries(Range<PositionImpl> range);

    /**
     * Close the cursor and releases the associated resources.
     *
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void close() throws InterruptedException, ManagedLedgerException;

    /**
     * Close the cursor asynchronously and release the associated resources.
     *
     * @param callback callback object
     * @param ctx      opaque context
     */
    void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx);
}
