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
package org.apache.pulsar.broker.delayed;

import com.google.common.collect.Range;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

public class MockManagedCursor implements ManagedCursor {

    private final String name;

    private final Map<String, String> cursorProperties;

    public MockManagedCursor(String name) {
        this.name = name;
        this.cursorProperties = new ConcurrentHashMap<>();
    }

    @Override
    public String getName() {
        return null;
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
        return null;
    }

    @Override
    public Map<String, String> getCursorProperties() {
        return this.cursorProperties;
    }

    @Override
    public CompletableFuture<Void> putCursorProperty(String key, String value) {
        cursorProperties.put(key, value);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> setCursorProperties(Map<String, String> cursorProperties) {
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> removeCursorProperty(String key) {
        cursorProperties.remove(key);
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
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                 PositionImpl maxPosition) {

    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes,
                                 AsyncCallbacks.ReadEntriesCallback callback, Object ctx, PositionImpl maxPosition) {

    }

    @Override
    public Entry getNthEntry(int n, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncGetNthEntry(int n, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.ReadEntryCallback callback, Object ctx) {

    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public List<Entry> readEntriesOrWait(int maxEntries, long maxSizeBytes)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback,
                                       Object ctx, PositionImpl maxPosition) {

    }

    @Override
    public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes, AsyncCallbacks.ReadEntriesCallback callback,
                                       Object ctx, PositionImpl maxPosition) {

    }

    @Override
    public boolean cancelPendingReadRequest() {
        return false;
    }

    @Override
    public boolean hasMoreEntries() {
        return false;
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
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void markDelete(Position position, Map<String, Long> properties)
            throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncMarkDelete(Position position, AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {

    }

    @Override
    public void asyncMarkDelete(Position position, Map<String, Long> properties,
                                AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {

    }

    @Override
    public void delete(Position position) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDelete(Position position, AsyncCallbacks.DeleteCallback callback, Object ctx) {

    }

    @Override
    public void delete(Iterable<Position> positions) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDelete(Iterable<Position> position, AsyncCallbacks.DeleteCallback callback, Object ctx) {

    }

    @Override
    public Position getReadPosition() {
        return null;
    }

    @Override
    public Position getMarkDeletedPosition() {
        return null;
    }

    @Override
    public Position getPersistentMarkDeletedPosition() {
        return null;
    }

    @Override
    public void rewind() {

    }

    @Override
    public void seek(Position newReadPosition, boolean force) {

    }

    @Override
    public void clearBacklog() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncClearBacklog(AsyncCallbacks.ClearBacklogCallback callback, Object ctx) {

    }

    @Override
    public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.SkipEntriesCallback callback, Object ctx) {

    }

    @Override
    public Position findNewestMatching(java.util.function.Predicate<Entry> condition)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public Position findNewestMatching(FindPositionConstraint constraint, java.util.function.Predicate<Entry> condition)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint,
                                        java.util.function.Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx) {

    }

    @Override
    public void resetCursor(Position position) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncResetCursor(Position position, boolean forceReset, AsyncCallbacks.ResetCursorCallback callback) {

    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        return null;
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                                      boolean sortEntries) {
        return null;
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {

    }

    @Override
    public Position getFirstPosition() {
        return null;
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

    @Override
    public boolean checkAndUpdateReadPositionChanged() {
        return false;
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
