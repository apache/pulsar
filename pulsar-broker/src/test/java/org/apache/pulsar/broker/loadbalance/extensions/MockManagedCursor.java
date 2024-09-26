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
package org.apache.pulsar.broker.loadbalance.extensions;

import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

@RequiredArgsConstructor
public class MockManagedCursor implements ManagedCursor {

    private final String name;
    private final MockManagedLedger managedLedger;
    private long offset = 0L;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getLastActive() {
        return 0;
    }

    @Override
    public void updateLastActive() {
    }

    @Override
    public Map<String, Long> getProperties() {
        return Map.of();
    }

    @Override
    public Map<String, String> getCursorProperties() {
        return Map.of();
    }

    @Override
    public CompletableFuture<Void> putCursorProperty(String key, String value) {
        return null;
    }

    @Override
    public CompletableFuture<Void> setCursorProperties(Map<String, String> cursorProperties) {
        return null;
    }

    @Override
    public CompletableFuture<Void> removeCursorProperty(String key) {
        return null;
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
    public synchronized List<Entry> readEntries(int numberOfEntriesToRead) {
        synchronized (managedLedger) {
            final var nextOffset = Math.min(offset + numberOfEntriesToRead, managedLedger.entries.size());
            final var entries = new ArrayList<Entry>();
            for (long i = offset; i < nextOffset; i++) {
                entries.add(EntryImpl.create(PositionFactory.create(managedLedger.ledgerId, i),
                        managedLedger.entries.get((int) i)));
            }
            offset = nextOffset;
            return entries;
        }
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                 Position maxPosition) {
        callback.readEntriesComplete(readEntries(numberOfEntriesToRead), ctx);
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes,
                                 AsyncCallbacks.ReadEntriesCallback callback, Object ctx, Position maxPosition) {
        callback.readEntriesComplete(readEntries(numberOfEntriesToRead), ctx);
    }

    @Override
    public Entry getNthEntry(int n, IndividualDeletedEntries deletedEntries) {
        throw new RuntimeException("getNthEntry is not supported");
    }

    @Override
    public void asyncGetNthEntry(int n, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        callback.readEntryFailed(new ManagedLedgerException("getNthEntry is not supported"), ctx);
    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead) {
        return List.of();
    }

    @Override
    public List<Entry> readEntriesOrWait(int maxEntries, long maxSizeBytes) {
        final var future = new CompletableFuture<List<Entry>>();
        asyncReadEntriesOrWait(maxEntries, Long.MAX_VALUE, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, PositionFactory.LATEST);
        return future.join();
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback,
                                       Object ctx, Position maxPosition) {
        asyncReadEntriesOrWait(numberOfEntriesToRead, Long.MAX_VALUE, callback, ctx, maxPosition);
    }

    @Override
    public synchronized void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes,
                                                    AsyncCallbacks.ReadEntriesCallback callback,
                                                    Object ctx, Position maxPosition) {
        final var entries = readEntries(maxEntries);
        if (entries.isEmpty()) {
            CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(() ->
                    callback.readEntriesComplete(entries, ctx));
        } else {
            callback.readEntriesComplete(entries, ctx);
        }
    }

    @Override
    public boolean cancelPendingReadRequest() {
        return false;
    }

    @Override
    public synchronized boolean hasMoreEntries() {
        synchronized (managedLedger) {
            return offset >= managedLedger.entries.size();
        }
    }

    @Override
    public long getNumberOfEntries() {
        return managedLedger.getNumberOfEntries();
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean isPrecise) {
        return 0;
    }

    @Override
    public void markDelete(Position position) {

    }

    @Override
    public void markDelete(Position position, Map<String, Long> properties) {
    }

    @Override
    public void asyncMarkDelete(Position position, AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
        callback.markDeleteComplete(ctx);
    }

    @Override
    public void asyncMarkDelete(Position position, Map<String, Long> properties,
                                AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
        callback.markDeleteComplete(ctx);
    }

    @Override
    public void delete(Position position) throws InterruptedException, ManagedLedgerException {
    }

    @Override
    public void asyncDelete(Position position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
        callback.deleteComplete(ctx);
    }

    @Override
    public void delete(Iterable<Position> positions) throws InterruptedException, ManagedLedgerException {
    }

    @Override
    public void asyncDelete(Iterable<Position> position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
        callback.deleteComplete(ctx);
    }

    @Override
    public synchronized Position getReadPosition() {
        return PositionFactory.create(managedLedger.ledgerId, offset);
    }

    @Override
    public Position getMarkDeletedPosition() {
        return getReadPosition();
    }

    @Override
    public Position getPersistentMarkDeletedPosition() {
        return getMarkDeletedPosition();
    }

    @Override
    public synchronized void rewind() {
        offset = 0L;
    }

    @Override
    public synchronized void seek(Position newReadPosition, boolean force) {
        if (newReadPosition.getLedgerId() != managedLedger.ledgerId) {
            throw new RuntimeException("Failed to seek " + newReadPosition);
        }
        offset = newReadPosition.getEntryId();
    }

    @Override
    public void clearBacklog() {
    }

    @Override
    public void asyncClearBacklog(AsyncCallbacks.ClearBacklogCallback callback, Object ctx) {
        callback.clearBacklogComplete(ctx);
    }

    @Override
    public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries) {
    }

    @Override
    public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.SkipEntriesCallback callback, Object ctx) {
        callback.skipEntriesComplete(ctx);
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) {
        return getFirstPosition();
    }

    @Override
    public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition) {
        return getFirstPosition();
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx) {
        callback.findEntryComplete(getFirstPosition(), ctx);
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx,
                                        boolean isFindFromLedger) {
        callback.findEntryComplete(getFirstPosition(), ctx);
    }

    @Override
    public void resetCursor(Position position) {
        seek(position);
    }

    @Override
    public void asyncResetCursor(Position position, boolean forceReset, AsyncCallbacks.ResetCursorCallback callback) {
        seek(position);
        callback.resetComplete(null);
    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions) {
        return List.of();
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        callback.readEntriesComplete(List.of(), ctx);
        return Set.of();
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                                      boolean sortEntries) {
        return asyncReplayEntries(positions, callback, ctx);
    }

    @Override
    public void close() {
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        callback.closeComplete(ctx);
    }

    @Override
    public Position getFirstPosition() {
        return managedLedger.getFirstPosition();
    }

    @Override
    public void setActive() {
    }

    @Override
    public void setInactive() {
    }

    @Override
    public void setAlwaysInactive() {
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public boolean isDurable() {
        return false;
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
        return 0;
    }

    @Override
    public double getThrottleMarkDelete() {
        return 0;
    }

    @Override
    public void setThrottleMarkDelete(double throttleMarkDelete) {
    }

    @Override
    public ManagedLedger getManagedLedger() {
        return managedLedger;
    }

    @Override
    public Range<Position> getLastIndividualDeletedRange() {
        throw new RuntimeException("getLastIndividualDeletedRange is not supported");
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
