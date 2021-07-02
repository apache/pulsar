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

import com.google.common.base.Predicate;
import com.google.common.collect.Range;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.SkipEntriesCallback;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

/**
 * A ManagedCursor is a persisted cursor inside a ManagedLedger.
 *
 * <p/>The ManagedCursor is used to read from the ManagedLedger and to signal when the consumer is done with the
 * messages that it has read before.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ManagedCursor {

    @SuppressWarnings("checkstyle:javadoctype")
    enum FindPositionConstraint {
        SearchActiveEntries, SearchAllAvailableEntries
    }

    @SuppressWarnings("checkstyle:javadoctype")
    enum IndividualDeletedEntries {
        Include, Exclude
    }

    /**
     * Get the unique cursor name.
     *
     * @return the cursor name
     */
    String getName();

    /**
     * Get the last active time of the cursor.
     *
     * @return the last active time of the cursor
     */
    long getLastActive();

    /**
     * Update the last active time of the cursor
     *
     */
    void updateLastActive();

    /**
     * Return any properties that were associated with the last stored position.
     */
    Map<String, Long> getProperties();

    /**
     * Add a property associated with the last stored position.
     */
    boolean putProperty(String key, Long value);

    /**
     * Remove a property associated with the last stored position.
     */
    boolean removeProperty(String key);

    /**
     * Read entries from the ManagedLedger, up to the specified number. The returned list can be smaller.
     *
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @return the list of entries
     * @throws ManagedLedgerException
     */
    List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronously read entries from the ManagedLedger.
     *
     * @see #readEntries(int)
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     * @param maxPosition
     *            max position can read
     */
    void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx,
                          PositionImpl maxPosition);


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
     * Get 'N'th entry from the mark delete position in the cursor without updating any cursor positions.
     *
     * @param n
     *            entry position
     * @param deletedEntries
     *            skip individual deleted entries
     *
     * @return the entry
     *
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    Entry getNthEntry(int n, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronously get 'N'th entry from the mark delete position in the cursor without updating any cursor positions.
     *
     * @param n
     *            entry position
     * @param deletedEntries
     *            skip individual deleted entries
     * @param callback
     * @param ctx
     */
    void asyncGetNthEntry(int n, IndividualDeletedEntries deletedEntries, ReadEntryCallback callback,
            Object ctx);

    /**
     * Read entries from the ManagedLedger, up to the specified number. The returned list can be smaller.
     *
     * <p/>If no entries are available, the method will block until at least a new message will be persisted.
     *
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @return the list of entries
     * @throws ManagedLedgerException
     */
    List<Entry> readEntriesOrWait(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException;

    /**
     * Read entries from the ManagedLedger, up to the specified number and size.
     *
     * <p/>
     * If no entries are available, the method will block until at least a new message will be persisted.
     *
     * @param maxEntries
     *            maximum number of entries to return
     * @param maxSizeBytes
     *            max size in bytes of the entries to return
     * @return the list of entries
     * @throws ManagedLedgerException
     */
    List<Entry> readEntriesOrWait(int maxEntries, long maxSizeBytes)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronously read entries from the ManagedLedger.
     *
     * <p/>If no entries are available, the callback will not be triggered. Instead it will be registered to wait until
     * a new message will be persisted into the managed ledger
     *
     * @see #readEntriesOrWait(int)
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     * @param maxPosition
     *            max position can read
     */
    void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx,
                                PositionImpl maxPosition);

    /**
     * Asynchronously read entries from the ManagedLedger, up to the specified number and size.
     *
     * <p/>If no entries are available, the callback will not be triggered. Instead it will be registered to wait until
     * a new message will be persisted into the managed ledger
     *
     * @see #readEntriesOrWait(int, long)
     * @param maxEntries
     *            maximum number of entries to return
     * @param maxSizeBytes
     *            max size in bytes of the entries to return
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     * @param maxPosition
     *            max position can read
     */
    void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes, ReadEntriesCallback callback, Object ctx,
                                PositionImpl maxPosition);

    /**
     * Cancel a previously scheduled asyncReadEntriesOrWait operation.
     *
     * @see #asyncReadEntriesOrWait(int, ReadEntriesCallback, Object, PositionImpl)
     * @return true if the read operation was canceled or false if there was no pending operation
     */
    boolean cancelPendingReadRequest();

    /**
     * Tells whether this cursor has already consumed all the available entries.
     *
     * <p/>This method is not blocking.
     *
     * @return true if there are pending entries to read, false otherwise
     */
    boolean hasMoreEntries();

    /**
     * Return the number of messages that this cursor still has to read.
     *
     * <p/>This method has linear time complexity on the number of ledgers included in the managed ledger.
     *
     * @return the number of entries
     */
    long getNumberOfEntries();

    /**
     * Return the number of non-deleted messages on this cursor.
     *
     * <p/>This will also include messages that have already been read from the cursor but not deleted or mark-deleted
     * yet.
     *
     * <p/>This method has linear time complexity on the number of ledgers included in the managed ledger.
     *
     * @param isPrecise set to true to get precise backlog count
     * @return the number of entries
     */
    long getNumberOfEntriesInBacklog(boolean isPrecise);

    /**
     * This signals that the reader is done with all the entries up to "position" (included). This can potentially
     * trigger a ledger deletion, if all the other cursors are done too with the underlying ledger.
     *
     * @param position
     *            the last position that have been successfully consumed
     *
     * @throws ManagedLedgerException
     */
    void markDelete(Position position) throws InterruptedException, ManagedLedgerException;

    /**
     * This signals that the reader is done with all the entries up to "position" (included). This can potentially
     * trigger a ledger deletion, if all the other cursors are done too with the underlying ledger.
     *
     * @param position
     *            the last position that have been successfully consumed
     * @param properties
     *            additional user-defined properties that can be associated with a particular cursor position
     *
     * @throws ManagedLedgerException
     */
    void markDelete(Position position, Map<String, Long> properties)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronous mark delete.
     *
     * @see #markDelete(Position)
     * @param position
     *            the last position that have been successfully consumed
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncMarkDelete(Position position, MarkDeleteCallback callback, Object ctx);

    /**
     * Asynchronous mark delete.
     *
     * @see #markDelete(Position)
     * @param position
     *            the last position that have been successfully consumed
     * @param properties
     *            additional user-defined properties that can be associated with a particular cursor position
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncMarkDelete(Position position, Map<String, Long> properties, MarkDeleteCallback callback, Object ctx);

    /**
     * Delete a single message.
     *
     * <p/>Mark a single message for deletion. When all the previous messages are all deleted, then markDelete() will be
     * called internally to advance the persistent acknowledged position.
     *
     * <p/>The deletion of the message is not persisted into the durable storage and cannot be recovered upon the
     * reopening of the ManagedLedger
     *
     * @param position
     *            the position of the message to be deleted
     */
    void delete(Position position) throws InterruptedException, ManagedLedgerException;

    /**
     * Delete a single message asynchronously
     *
     * <p/>Mark a single message for deletion. When all the previous messages are all deleted, then markDelete() will be
     * called internally to advance the persistent acknowledged position.
     *
     * <p/>The deletion of the message is not persisted into the durable storage and cannot be recovered upon the
     * reopening of the ManagedLedger
     *
     * @param position
     *            the position of the message to be deleted
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncDelete(Position position, DeleteCallback callback, Object ctx);


    /**
     * Delete a group of entries.
     *
     * <p/>
     * Mark multiple single messages for deletion. When all the previous messages are all deleted, then markDelete()
     * will be called internally to advance the persistent acknowledged position.
     *
     * <p/>
     * The deletion of the message is not persisted into the durable storage and cannot be recovered upon the reopening
     * of the ManagedLedger
     *
     * @param positions
     *            positions of the messages to be deleted
     */
    void delete(Iterable<Position> positions) throws InterruptedException, ManagedLedgerException;

    /**
     * Delete a group of messages asynchronously
     *
     * <p/>
     * Mark a group of messages for deletion. When all the previous messages are all deleted, then markDelete() will be
     * called internally to advance the persistent acknowledged position.
     *
     * <p/>
     * The deletion of the messages is not persisted into the durable storage and cannot be recovered upon the reopening
     * of the ManagedLedger
     *
     * @param position
     *            the positions of the messages to be deleted
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncDelete(Iterable<Position> position, DeleteCallback callback, Object ctx);

    /**
     * Get the read position. This points to the next message to be read from the cursor.
     *
     * @return the read position
     */
    Position getReadPosition();

    /**
     * Get the newest mark deleted position on this cursor.
     *
     * @return the mark deleted position
     */
    Position getMarkDeletedPosition();

    /**
     * Get the persistent newest mark deleted position on this cursor.
     *
     * @return the persistent mark deleted position
     */
    Position getPersistentMarkDeletedPosition();

    /**
     * Rewind the cursor to the mark deleted position to replay all the already read but not yet mark deleted messages.
     *
     * <p/>The next message to be read is the one after the current mark deleted message.
     */
    void rewind();

    /**
     * Move the cursor to a different read position.
     *
     * <p/>If the new position happens to be before the already mark deleted position, it will be set to the mark
     * deleted position instead.
     *
     * @param newReadPosition
     *            the position where to move the cursor
     */
    void seek(Position newReadPosition);

    /**
     * Clear the cursor backlog.
     *
     * <p/>Consume all the entries for this cursor.
     */
    void clearBacklog() throws InterruptedException, ManagedLedgerException;

    /**
     * Clear the cursor backlog.
     *
     * <p/>Consume all the entries for this cursor.
     *
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncClearBacklog(ClearBacklogCallback callback, Object ctx);

    /**
     * Skip n entries from the read position of this cursor.
     *
     * @param numEntriesToSkip
     *            number of entries to skip
     * @param deletedEntries
     *            skip individual deleted entries
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Skip n entries from the read position of this cursor.
     *
     * @param numEntriesToSkip
     *            number of entries to skip
     * @param deletedEntries
     *            skip individual deleted entries
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
            final SkipEntriesCallback callback, Object ctx);

    /**
     * Find the newest entry that matches the given predicate.  Will only search among active entries
     *
     * @param condition
     *            predicate that reads an entry an applies a condition
     * @return Position of the newest entry that matches the given predicate
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    Position findNewestMatching(Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException;


    /**
     * Find the newest entry that matches the given predicate.
     *
     * @param constraint
     *            search only active entries or all entries
     * @param condition
     *            predicate that reads an entry an applies a condition
     * @return Position of the newest entry that matches the given predicate
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException;

    /**
     * Find the newest entry that matches the given predicate.
     *
     * @param constraint
     *            search only active entries or all entries
     * @param condition
     *            predicate that reads an entry an applies a condition
     * @param callback
     *            callback object returning the resultant position
     * @param ctx
     *            opaque context
     */
    void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
            FindEntryCallback callback, Object ctx);

    /**
     * reset the cursor to specified position to enable replay of messages.
     *
     * @param position
     *            position to move the cursor to
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void resetCursor(final Position position) throws InterruptedException, ManagedLedgerException;

    /**
     * reset the cursor to specified position to enable replay of messages.
     *
     * @param position
     *            position to move the cursor to
     * @param callback
     *            callback object
     */
    void asyncResetCursor(final Position position, AsyncCallbacks.ResetCursorCallback callback);

    /**
     * Read the specified set of positions from ManagedLedger.
     *
     * @param positions
     *            set of positions to read
     * @return the list of entries
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    List<Entry> replayEntries(Set<? extends Position> positions)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Read the specified set of positions from ManagedLedger without ordering.
     *
     * @param positions
     *            set of positions to read
     * @param callback
     *            callback object returning the list of entries
     * @param ctx
     *            opaque context
     * @return skipped positions
     *              set of positions which are already deleted/acknowledged and skipped while replaying them
     */
    Set<? extends Position> asyncReplayEntries(
        Set<? extends Position> positions, ReadEntriesCallback callback, Object ctx);

    /**
     * Read the specified set of positions from ManagedLedger.
     *
     * @param positions
     *            set of positions to read
     * @param callback
     *            callback object returning the list of entries
     * @param ctx
     *            opaque context
     * @param sortEntries
     *            callback with sorted entry list.
     * @return skipped positions
     *              set of positions which are already deleted/acknowledged and skipped while replaying them
     */
    Set<? extends Position> asyncReplayEntries(
            Set<? extends Position> positions, ReadEntriesCallback callback, Object ctx, boolean sortEntries);

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
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx);

    /**
     * Get the first position.
     *
     * @return the first position
     */
    Position getFirstPosition();

    /**
     * Activate cursor: EntryCacheManager caches entries only for activated-cursors.
     *
     */
    void setActive();

    /**
     * Deactivate cursor.
     *
     */
    void setInactive();

    /**
     * A cursor that is set as always-inactive  will never trigger the caching of
     * entries.
     */
    void setAlwaysInactive();

    /**
     * Checks if cursor is active or not.
     *
     * @return
     */
    boolean isActive();

    /**
     * Tells whether the cursor is durable or just kept in memory.
     */
    boolean isDurable();

    /**
     * Returns total number of entries from the first not-acked message to current dispatching position.
     *
     * @return
     */
    long getNumberOfEntriesSinceFirstNotAckedMessage();

    /**
     * Returns number of mark-Delete range.
     *
     * @return
     */
    int getTotalNonContiguousDeletedMessagesRange();

    /**
     * Returns the serialized size of mark-Delete ranges.
     */
    int getNonContiguousDeletedMessagesRangeSerializedSize();

    /**
     * Returns the estimated size of the unacknowledged backlog for this cursor
     *
     * @return the estimated size from the mark delete position of the cursor
     */
    long getEstimatedSizeSinceMarkDeletePosition();

    /**
     * Returns cursor throttle mark-delete rate.
     *
     * @return
     */
    double getThrottleMarkDelete();

    /**
     * Update throttle mark delete rate.
     *
     */
    void setThrottleMarkDelete(double throttleMarkDelete);

    /**
     * Get {@link ManagedLedger} attached with cursor
     *
     * @return ManagedLedger
     */
    ManagedLedger getManagedLedger();

    /**
     * Get last individual deleted range
     * @return range
     */
    Range<PositionImpl> getLastIndividualDeletedRange();

    /**
     * Trim delete entries for the given entries
     */
    void trimDeletedEntries(List<Entry> entries);

    /**
     * Get deleted batch indexes list for a batch message.
     */
    long[] getDeletedBatchIndexesAsLongArray(PositionImpl position);

    /**
     * @return the managed cursor stats MBean
     */
    ManagedCursorMXBean getStats();

    /**
     * Checks if read position changed since this method was called last time.
     *
     * @return if read position changed
     */
    boolean checkAndUpdateReadPositionChanged();
}
