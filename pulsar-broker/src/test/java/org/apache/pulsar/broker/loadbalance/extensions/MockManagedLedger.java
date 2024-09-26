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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

@RequiredArgsConstructor
public class MockManagedLedger implements ManagedLedger {

    private static final AtomicLong ledgerIdGenerator = new AtomicLong(0L);

    final long ledgerId = ledgerIdGenerator.getAndIncrement();
    final List<ByteBuf> entries = new ArrayList<>();
    private final Map<String, ManagedCursor> cursors = new ConcurrentHashMap<>();
    private final String name;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized Position addEntry(byte[] data) {
        final var buf = Unpooled.wrappedBuffer(data);
        entries.add(buf);
        return PositionFactory.create(ledgerId, entries.size() - 1);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages) {
        return addEntry(data);
    }

    @Override
    public synchronized void asyncAddEntry(byte[] data, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        final var position = addEntry(data);
        callback.addComplete(addEntry(data), entries.get((int) position.getEntryId()), ctx);
    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) {
        return addEntry(data);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length) {
        return addEntry(data);
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AsyncCallbacks.AddEntryCallback callback,
                              Object ctx) {
        asyncAddEntry(data, callback, ctx);
    }

    @Override
    public void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length,
                              AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        asyncAddEntry(data, callback, ctx);
    }

    @Override
    public synchronized void asyncAddEntry(ByteBuf buffer, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        final var buf = Unpooled.wrappedBuffer(buffer);
        entries.add(buf);
        final var position = PositionFactory.create(ledgerId, entries.size() - 1);
        callback.addComplete(position, buf, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AsyncCallbacks.AddEntryCallback callback,
                              Object ctx) {
        asyncAddEntry(buffer, callback, ctx);
    }

    @Override
    public ManagedCursor openCursor(String name) {
        return cursors.computeIfAbsent(name, __ -> new MockManagedCursor(name, this));
    }

    @Override
    public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition) {
        return openCursor(name);
    }

    @Override
    public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                    Map<String, Long> properties, Map<String, String> cursorProperties) {
        return openCursor(name);
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) {
        return openCursor(name);
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName) {
        return openCursor(subscriptionName);
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName,
                                             CommandSubscribe.InitialPosition initialPosition,
                                             boolean isReadCompacted) {
        return openCursor(subscriptionName);
    }

    @Override
    public void asyncDeleteCursor(String name, AsyncCallbacks.DeleteCursorCallback callback, Object ctx) {
        cursors.remove(name);
        callback.deleteCursorComplete(ctx);
    }

    @Override
    public void deleteCursor(String name) {
        cursors.remove(name);
    }

    @Override
    public void removeWaitingCursor(ManagedCursor cursor) {
    }

    @Override
    public void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        callback.openCursorComplete(openCursor(name), ctx);
    }

    @Override
    public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        callback.openCursorComplete(openCursor(name), ctx);
    }

    @Override
    public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                Map<String, Long> properties, Map<String, String> cursorProperties,
                                AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        callback.openCursorComplete(openCursor(name), ctx);
    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return this.cursors.values();
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return getCursors();
    }

    @Override
    public long getNumberOfEntries() {
        return cursors.size();
    }

    @Override
    public long getNumberOfEntries(Range<Position> range) {
        return 0;
    }

    @Override
    public long getNumberOfActiveEntries() {
        return 0;
    }

    @Override
    public long getTotalSize() {
        return 0;
    }

    @Override
    public long getEstimatedBacklogSize() {
        return 0;
    }

    @Override
    public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() {
        return CompletableFuture.completedFuture(System.currentTimeMillis());
    }

    @Override
    public long getOffloadedSize() {
        return 0;
    }

    @Override
    public long getLastOffloadedLedgerId() {
        return 0;
    }

    @Override
    public long getLastOffloadedSuccessTimestamp() {
        return 0;
    }

    @Override
    public long getLastOffloadedFailureTimestamp() {
        return 0;
    }

    @Override
    public void asyncTerminate(AsyncCallbacks.TerminateCallback callback, Object ctx) {
        callback.terminateComplete(getFirstPosition(), ctx);
    }

    @Override
    public CompletableFuture<Position> asyncMigrate() {
        return CompletableFuture.completedFuture(getFirstPosition());
    }

    @Override
    public Position terminate() {
        return getFirstPosition();
    }

    @Override
    public void close() {
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        callback.closeComplete(ctx);
    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return new MockManagedLedgerMXBean();
    }

    @Override
    public void delete() throws InterruptedException, ManagedLedgerException {
    }

    @Override
    public void asyncDelete(AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        callback.deleteLedgerComplete(ctx);
    }

    @Override
    public Position offloadPrefix(Position pos) {
        return getFirstPosition();
    }

    @Override
    public void asyncOffloadPrefix(Position pos, AsyncCallbacks.OffloadCallback callback, Object ctx) {
        callback.offloadComplete(getFirstPosition(), ctx);
    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return cursors.values().stream().findAny().orElse(null);
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean isMigrated() {
        return false;
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return new ManagedLedgerConfig();
    }

    @Override
    public void setConfig(ManagedLedgerConfig config) {
    }

    @Override
    public synchronized Position getLastConfirmedEntry() {
        return PositionFactory.create(ledgerId, entries.size() - 1);
    }

    @Override
    public void readyToCreateNewLedger() {
    }

    @Override
    public Map<String, String> getProperties() {
        return Map.of();
    }

    @Override
    public void setProperty(String key, String value) throws InterruptedException, ManagedLedgerException {
    }

    @Override
    public void asyncSetProperty(String key, String value, AsyncCallbacks.UpdatePropertiesCallback callback,
                                 Object ctx) {
        callback.updatePropertiesComplete(Map.of(), ctx);
    }

    @Override
    public void deleteProperty(String key) {
    }

    @Override
    public void asyncDeleteProperty(String key, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {

    }

    @Override
    public void setProperties(Map<String, String> properties) throws InterruptedException, ManagedLedgerException {
    }

    @Override
    public void asyncSetProperties(Map<String, String> properties, AsyncCallbacks.UpdatePropertiesCallback callback,
                                   Object ctx) {
        callback.updatePropertiesComplete(Map.of(), ctx);
    }

    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
        promise.complete(null);
    }

    @Override
    public void rollCurrentLedgerIfFull() {
    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
        return CompletableFuture.completedFuture(getFirstPosition());
    }

    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        return null;
    }

    @Override
    public CompletableFuture<MLDataFormats.ManagedLedgerInfo.LedgerInfo> getLedgerInfo(long ledgerId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Optional<MLDataFormats.ManagedLedgerInfo.LedgerInfo> getOptionalLedgerInfo(long ledgerId) {
        return Optional.empty();
    }

    @Override
    public CompletableFuture<Void> asyncTruncate() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean checkInactiveLedgerAndRollOver() {
        return false;
    }

    @Override
    public void checkCursorsToCacheEntries() {
    }

    @Override
    public synchronized void asyncReadEntry(Position position, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        if (position.getLedgerId() != ledgerId) {
            callback.readEntryFailed(new ManagedLedgerException(position.getLedgerId() + " does not exist"), ctx);
            return;
        }
        final var buf = entries.get((int) position.getEntryId());
        callback.readEntryComplete(EntryImpl.create(position, buf), ctx);
    }

    @Override
    public NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> getLedgersInfo() {
        return new TreeMap<>();
    }

    @Override
    public Position getNextValidPosition(Position position) {
        return PositionFactory.create(position.getLedgerId(), position.getEntryId() + 1);
    }

    @Override
    public Position getPreviousPosition(Position position) {
        return PositionFactory.create(position.getLedgerId(), position.getEntryId() - 1);
    }

    @Override
    public long getEstimatedBacklogSize(Position position) {
        return 0;
    }

    @Override
    public Position getPositionAfterN(Position startPosition, long n, PositionBound startRange) {
        return PositionFactory.create(startPosition.getLedgerId(), startPosition.getEntryId() + n);
    }

    @Override
    public int getPendingAddEntriesCount() {
        return 0;
    }

    @Override
    public long getCacheSize() {
        return 0;
    }

    @Override
    public Position getFirstPosition() {
        return PositionFactory.create(ledgerId, 0L);
    }
}
