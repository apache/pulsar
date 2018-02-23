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

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.SkipEntriesCallback;

/**
 * A ManangedCursor is a persisted cursor inside a ManagedLedger.
 *
 * <p/>The ManagedCursor is used to read from the ManagedLedger and to signal when the consumer is done with the
 * messages that it has read before.
 */
@Beta
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
     * Return any properties that were associated with the last stored position.
     */
    Map<String, Long> getProperties();

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
     */
    void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx);

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
     */
    void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx);

    /**
     * Cancel a previously scheduled asyncReadEntriesOrWait operation.
     *
     * @see #asyncReadEntriesOrWait(int, ReadEntriesCallback, Object)
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
     * @return the number of entries
     */
    long getNumberOfEntriesInBacklog();

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
     * Find the newest entry that matches the given predicate.
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
     * Read the specified set of positions from ManagedLedger.
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

}
