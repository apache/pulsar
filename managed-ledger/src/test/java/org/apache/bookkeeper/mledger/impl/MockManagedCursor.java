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

import static org.testng.Assert.fail;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

class MockManagedCursor implements ManagedCursor {

    ActiveManagedCursorContainer container;
    Position markDeletePosition;
    Position readPosition;
    private final boolean updateMarkDeletePosition;
    private final boolean durable;
    String name;

    MockManagedCursor(ActiveManagedCursorContainer container, String name, Position markDeletePosition) {
        this(container, name, markDeletePosition, null, true, true);
    }

    MockManagedCursor(ActiveManagedCursorContainer container, String name, Position markDeletePosition,
                      Position readPosition, boolean updateMarkDeletePosition, boolean durable) {
        this.container = container;
        this.name = name;
        this.markDeletePosition = markDeletePosition;
        this.readPosition = readPosition;
        this.updateMarkDeletePosition = updateMarkDeletePosition;
        this.durable = durable;
    }

    static MockManagedCursor createCursor(ActiveManagedCursorContainer container, String name,
                                          Position position) {
        return new MockManagedCursor(container, name, position, position, false, true);
    }

    static void addCursor(ActiveManagedCursorContainer container, String name, Position position) {
        container.add(createCursor(container, name, position), position);
    }

    @Override
    public Map<String, Long> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getCursorProperties() {
        return Collections.emptyMap();
    }

    @Override
    public CompletableFuture<Void> putCursorProperty(String key, String value) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> setCursorProperties(Map<String, String> cursorProperties) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeCursorProperty(String key) {
        return CompletableFuture.completedFuture(null);
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
        return durable;
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) {
        return new ArrayList<Entry>();
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                 Position maxPosition) {
        callback.readEntriesComplete(null, ctx);
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes,
                                 AsyncCallbacks.ReadEntriesCallback callback,
                                 Object ctx, Position maxPosition) {
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
    public void markDelete(Position position) {
        markDelete(position, Collections.emptyMap());
    }

    @Override
    public void markDelete(Position position, Map<String, Long> properties) {
        this.markDeletePosition = position;
        if (updateMarkDeletePosition) {
            container.cursorUpdated(this, position);
        }
    }

    @Override
    public void asyncMarkDelete(Position position, AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
        fail();
    }

    @Override
    public void asyncMarkDelete(Position position, Map<String, Long> properties,
                                AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
        fail();
    }

    @Override
    public Position getMarkDeletedPosition() {
        return markDeletePosition;
    }

    @Override
    public Position getPersistentMarkDeletedPosition() {
        return markDeletePosition;
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

    @Override
    public String toString() {
        return String.format("%s=%s/%s", name, markDeletePosition, readPosition);
    }

    @Override
    public Position getReadPosition() {
        return readPosition;
    }

    @Override
    public void rewind() {
    }

    @Override
    public void seek(Position newReadPosition, boolean force) {
        this.readPosition = newReadPosition;
        if (!updateMarkDeletePosition) {
            container.updateCursor(this, newReadPosition);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
    }

    @Override
    public void delete(Position position) {
    }

    @Override
    public void asyncDelete(Position position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
    }

    @Override
    public void delete(Iterable<Position> positions) {
    }

    @Override
    public void asyncDelete(Iterable<Position> position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
    }

    @Override
    public void clearBacklog() {
    }

    @Override
    public void asyncClearBacklog(AsyncCallbacks.ClearBacklogCallback callback, Object ctx) {
    }

    @Override
    public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries) {
    }

    @Override
    public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
                                 final AsyncCallbacks.SkipEntriesCallback callback, Object ctx) {
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) {
        return null;
    }

    @Override
    public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition) {
        return null;
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx) {
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx,
                                        boolean isFindFromLedger) {
    }

    @Override
    public void asyncResetCursor(final Position position, boolean forceReset,
                                 AsyncCallbacks.ResetCursorCallback callback) {

    }

    @Override
    public void resetCursor(final Position position) {

    }

    @Override
    public Position getFirstPosition() {
        return null;
    }

    @Override
    public void setAlwaysInactive() {
    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions) {
        return null;
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        return Sets.newConcurrentHashSet();
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                                      boolean sortEntries) {
        return Sets.newConcurrentHashSet();
    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead) {
        return null;
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback,
                                       Object ctx, Position maxPosition) {
    }

    @Override
    public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes,
                                       AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                       Position maxPosition) {

    }

    @Override
    public boolean cancelPendingReadRequest() {
        return true;
    }

    @Override
    public Entry getNthEntry(int num, IndividualDeletedEntries deletedEntries) {
        return null;
    }

    @Override
    public void asyncGetNthEntry(int num, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
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
    public Range<Position> getLastIndividualDeletedRange() {
        return null;
    }

    @Override
    public void trimDeletedEntries(List<Entry> entries) {

    }

    @Override
    public long[] getDeletedBatchIndexesAsLongArray(Position position) {
        return new long[0];
    }

    @Override
    public ManagedCursorMXBean getStats() {
        return null;
    }

    @Override
    public List<Entry> readEntriesOrWait(int maxEntries, long maxSizeBytes) {
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

    @Override
    public ManagedLedgerInternalStats.CursorStats getCursorStats() {
        return null;
    }

    @Override
    public boolean isMessageDeleted(Position position) {
        return false;
    }

    @Override
    public ManagedCursor duplicateNonDurableCursor(String nonDurableCursorName) throws ManagedLedgerException {
        return null;
    }

    @Override
    public long[] getBatchPositionAckSet(Position position) {
        return new long[0];
    }

    @Override
    public int applyMaxSizeCap(int maxEntries, long maxSizeBytes) {
        return 0;
    }

    @Override
    public void updateReadStats(int readEntriesCount, long readEntriesSize) {

    }


}
