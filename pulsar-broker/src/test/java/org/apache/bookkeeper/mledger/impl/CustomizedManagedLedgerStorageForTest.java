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

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.jspecify.annotations.Nullable;

public class CustomizedManagedLedgerStorageForTest extends ManagedLedgerClientFactory
        implements ManagedLedgerStorage  {

    @Override
    public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                           BookKeeperClientFactory bookkeeperProvider,
                           EventLoopGroup eventLoopGroup,
                           OpenTelemetry openTelemetry) throws Exception {
        super.initialize(conf, metadataStore, bookkeeperProvider, eventLoopGroup, openTelemetry);
        managedLedgerFactory = new CustomizedManagedLedgerFactory(managedLedgerFactory);
    }

    @AllArgsConstructor
    public static class CustomizedManagedLedgerFactory implements ManagedLedgerFactory {
        private ManagedLedgerFactory delegate;
        @Override
        public ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException {
            return new CustomizedManagedLedger((ManagedLedgerImpl) delegate.open(name));
        }

        @Override
        public ManagedLedger open(String name, ManagedLedgerConfig config) throws InterruptedException,
                ManagedLedgerException {
            return new CustomizedManagedLedger((ManagedLedgerImpl) delegate.open(name, config));
        }
        @Override
        public void asyncOpen(String name, AsyncCallbacks.OpenLedgerCallback callback, Object ctx) {
            delegate.asyncOpen(name, new AsyncCallbacks.OpenLedgerCallback() {

                @Override
                public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                    callback.openLedgerComplete(new CustomizedManagedLedger((ManagedLedgerImpl) ledger), ctx);
                }

                @Override
                public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    callback.openLedgerFailed(exception, ctx);
                }
            }, ctx);
        }

        @Override
        public void asyncOpen(String name, ManagedLedgerConfig config, AsyncCallbacks.OpenLedgerCallback callback,
                              Supplier<CompletableFuture<Boolean>> mlOwnershipChecker, Object ctx) {
            delegate.asyncOpen(name, config, new AsyncCallbacks.OpenLedgerCallback() {

                @Override
                public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                    callback.openLedgerComplete(new CustomizedManagedLedger((ManagedLedgerImpl) ledger), ctx);
                }

                @Override
                public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    callback.openLedgerFailed(exception, ctx);
                }
            }, mlOwnershipChecker, ctx);
        }

        @Override
        public ReadOnlyCursor openReadOnlyCursor(String managedLedgerName, Position startPosition,
                                                 ManagedLedgerConfig config)
                throws InterruptedException, ManagedLedgerException {
            return delegate.openReadOnlyCursor(managedLedgerName, startPosition, config);
        }

        @Override
        public void asyncOpenReadOnlyCursor(String managedLedgerName, Position startPosition,
                                            ManagedLedgerConfig config,
                                            AsyncCallbacks.OpenReadOnlyCursorCallback callback, Object ctx) {
            delegate.asyncOpenReadOnlyCursor(managedLedgerName, startPosition, config, callback, ctx);
        }

        @Override
        public void asyncOpenReadOnlyManagedLedger(String managedLedgerName,
                                                   AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback,
                                                   ManagedLedgerConfig config, Object ctx) {
            delegate.asyncOpenReadOnlyManagedLedger(managedLedgerName, callback, config, ctx);
        }

        @Override
        public ManagedLedgerInfo getManagedLedgerInfo(String name) throws InterruptedException, ManagedLedgerException {
            return delegate.getManagedLedgerInfo(name);
        }

        @Override
        public void asyncGetManagedLedgerInfo(String name, AsyncCallbacks.ManagedLedgerInfoCallback callback,
                                              Object ctx) {
            delegate.asyncGetManagedLedgerInfo(name, callback, ctx);
        }

        @Override
        public void delete(String name) throws InterruptedException, ManagedLedgerException {
            delegate.delete(name);
        }

        @Override
        public void delete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture)
                throws InterruptedException, ManagedLedgerException {
            delegate.delete(name, mlConfigFuture);
        }

        @Override
        public void asyncDelete(String name, AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
            delegate.asyncDelete(name, callback, ctx);
        }

        @Override
        public void asyncDelete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                                AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
            delegate.asyncDelete(name, mlConfigFuture, callback, ctx);
        }

        @Override
        public void shutdown() throws InterruptedException, ManagedLedgerException {
            delegate.shutdown();
        }

        @Override
        public CompletableFuture<Void> shutdownAsync() throws ManagedLedgerException, InterruptedException {
            return delegate.shutdownAsync();
        }

        @Override
        public CompletableFuture<Boolean> asyncExists(String ledgerName) {
            return delegate.asyncExists(ledgerName);
        }

        @Override
        public EntryCacheManager getEntryCacheManager() {
            return delegate.getEntryCacheManager();
        }

        @Override
        public void updateCacheEvictionTimeThreshold(long cacheEvictionTimeThresholdNanos) {
            delegate.updateCacheEvictionTimeThreshold(cacheEvictionTimeThresholdNanos);
        }

        @Override
        public long getCacheEvictionTimeThreshold() {
            return delegate.getCacheEvictionTimeThreshold();
        }

        @Override
        public CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name) {
            return delegate.getManagedLedgerPropertiesAsync(name);
        }

        @Override
        public Map<String, ManagedLedger> getManagedLedgers() {
            return delegate.getManagedLedgers();
        }

        @Override
        public ManagedLedgerFactoryMXBean getCacheStats() {
            return delegate.getCacheStats();
        }

        @Override
        public void estimateUnloadedTopicBacklog(PersistentOfflineTopicStats offlineTopicStats, TopicName topicName,
                                                 boolean accurate, Object ctx) throws Exception {
            delegate.estimateUnloadedTopicBacklog(offlineTopicStats, topicName, accurate, ctx);
        }

        @Override
        public ManagedLedgerFactoryConfig getConfig() {
            return delegate.getConfig();
        }

    }

    @AllArgsConstructor
    public static class CustomizedManagedLedger implements ManagedLedger {

        public ManagedLedgerImpl delegate;

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException {
            return delegate.addEntry(data);
        }

        @Override
        public Position addEntry(byte[] data, int numberOfMessages)
                throws InterruptedException, ManagedLedgerException {
            return delegate.addEntry(data, numberOfMessages);
        }

        @Override
        public void asyncAddEntry(byte[] data, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
            delegate.asyncAddEntry(data, callback, ctx);
        }

        @Override
        public Position addEntry(byte[] data, int offset, int length)
                throws InterruptedException, ManagedLedgerException {
            return delegate.addEntry(data, offset, length);
        }

        @Override
        public Position addEntry(byte[] data, int numberOfMessages, int offset, int length)
                throws InterruptedException, ManagedLedgerException {
            return delegate.addEntry(data, numberOfMessages, offset, length);
        }

        @Override
        public void asyncAddEntry(byte[] data, int offset, int length, AsyncCallbacks.AddEntryCallback callback,
                                  Object ctx) {
            delegate.asyncAddEntry(data, offset, length, callback, ctx);
        }

        @Override
        public void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length,
                                  AsyncCallbacks.AddEntryCallback callback, Object ctx) {
            delegate.asyncAddEntry(data, numberOfMessages, offset, length, callback, ctx);
        }

        @Override
        public void asyncAddEntry(ByteBuf buffer, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
            delegate.asyncAddEntry(buffer, callback, ctx);
        }

        @Override
        public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AsyncCallbacks.AddEntryCallback callback,
                                  Object ctx) {
            delegate.asyncAddEntry(buffer, numberOfMessages, callback, ctx);
        }

        @Override
        public ManagedCursor openCursor(String name) throws InterruptedException, ManagedLedgerException {
            ManagedCursor managedCursor = delegate.openCursor(name);
            if (managedCursor instanceof ManagedCursorImpl) {
                return new ManagedCursorDecorator((ManagedCursorImpl) managedCursor);
            }
            return  managedCursor;
        }

        @Override
        public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition)
                throws InterruptedException, ManagedLedgerException {
            ManagedCursor managedCursor = delegate.openCursor(name, initialPosition);
            if (managedCursor instanceof ManagedCursorImpl) {
                return new ManagedCursorDecorator((ManagedCursorImpl) managedCursor);
            }
            return  managedCursor;
        }

        @Override
        public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                        Map<String, Long> properties, Map<String, String> cursorProperties)
                throws InterruptedException, ManagedLedgerException {
            ManagedCursor managedCursor = delegate.openCursor(name, initialPosition, properties, cursorProperties);
            if (managedCursor instanceof ManagedCursorImpl) {
                return new ManagedCursorDecorator((ManagedCursorImpl) managedCursor);
            }
            return  managedCursor;
        }

        @Override
        public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
            return delegate.newNonDurableCursor(startCursorPosition);
        }

        @Override
        public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName)
                throws ManagedLedgerException {
            return delegate.newNonDurableCursor(startPosition, subscriptionName);
        }

        @Override
        public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName,
                                                 CommandSubscribe.InitialPosition initialPosition,
                                                 boolean isReadCompacted) throws ManagedLedgerException {
            return delegate.newNonDurableCursor(startPosition, subscriptionName, initialPosition, isReadCompacted);
        }

        @Override
        public void asyncDeleteCursor(String name, AsyncCallbacks.DeleteCursorCallback callback, Object ctx) {
            delegate.asyncDeleteCursor(name, callback, ctx);
        }

        @Override
        public void deleteCursor(String name) throws InterruptedException, ManagedLedgerException {
            delegate.deleteCursor(name);
        }

        @Override
        public void removeWaitingCursor(ManagedCursor cursor) {
            if (cursor instanceof ManagedCursorDecorator decorator) {
                delegate.removeWaitingCursor(decorator.delegate);
                return;
            }
            delegate.removeWaitingCursor(cursor);
        }

        @Override
        public void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
            delegate.asyncOpenCursor(name, callback, ctx);
        }

        @Override
        public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                    AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
            delegate.asyncOpenCursor(name, initialPosition, new AsyncCallbacks.OpenCursorCallback() {
                @Override
                public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                    callback.openCursorComplete(new ManagedCursorDecorator((ManagedCursorImpl) cursor), ctx);
                }

                @Override
                public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                    callback.openCursorComplete(null, exception);
                }
            }, ctx);
        }

        @Override
        public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                    Map<String, Long> properties, Map<String, String> cursorProperties,
                                    AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
            delegate.asyncOpenCursor(name, initialPosition, properties, cursorProperties,
                new AsyncCallbacks.OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        callback.openCursorComplete(new ManagedCursorDecorator((ManagedCursorImpl) cursor), ctx);
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        callback.openCursorComplete(null, exception);
                    }
                }, ctx);
        }

        @Override
        public Iterable<ManagedCursor> getCursors() {
            return delegate.getCursors();
        }

        @Override
        public Iterable<ManagedCursor> getActiveCursors() {
            return delegate.getActiveCursors();
        }

        @Override
        public long getNumberOfEntries() {
            return delegate.getNumberOfEntries();
        }

        @Override
        public long getNumberOfEntries(Range<Position> range) {
            return delegate.getNumberOfEntries(range);
        }

        @Override
        public long getNumberOfActiveEntries() {
            return delegate.getNumberOfActiveEntries();
        }

        @Override
        public long getTotalSize() {
            return delegate.getTotalSize();
        }

        @Override
        public long getEstimatedBacklogSize() {
            return delegate.getEstimatedBacklogSize();
        }

        @Override
        public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() {
            return delegate.getEarliestMessagePublishTimeInBacklog();
        }

        @Override
        public long getOffloadedSize() {
            return delegate.getOffloadedSize();
        }

        @Override
        public long getLastOffloadedLedgerId() {
            return delegate.getLastOffloadedLedgerId();
        }

        @Override
        public long getLastOffloadedSuccessTimestamp() {
            return delegate.getLastOffloadedSuccessTimestamp();
        }

        @Override
        public long getLastOffloadedFailureTimestamp() {
            return delegate.getLastOffloadedFailureTimestamp();
        }

        @Override
        public void asyncTerminate(AsyncCallbacks.TerminateCallback callback, Object ctx) {
            delegate.asyncTerminate(callback, ctx);
        }

        @Override
        public CompletableFuture<Position> asyncMigrate() {
            return delegate.asyncMigrate();
        }

        @Override
        public Position terminate() throws InterruptedException, ManagedLedgerException {
            return delegate.terminate();
        }

        @Override
        public void close() throws InterruptedException, ManagedLedgerException {
            delegate.close();
        }

        @Override
        public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
            delegate.asyncClose(callback, ctx);
        }

        @Override
        public ManagedLedgerMXBean getStats() {
            return delegate.getStats();
        }

        @Override
        public void delete() throws InterruptedException, ManagedLedgerException {
            delegate.delete();
        }

        @Override
        public void asyncDelete(AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
            delegate.asyncDelete(callback, ctx);
        }

        @Override
        public Position offloadPrefix(Position pos) throws InterruptedException, ManagedLedgerException {
            return delegate.offloadPrefix(pos);
        }

        @Override
        public void asyncOffloadPrefix(Position pos, AsyncCallbacks.OffloadCallback callback, Object ctx) {
            delegate.asyncOffloadPrefix(pos, callback, ctx);
        }

        @Override
        public @Nullable ManagedCursor getSlowestConsumer() {
            return delegate.getSlowestConsumer();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean isMigrated() {
            return delegate.isMigrated();
        }

        @Override
        public ManagedLedgerConfig getConfig() {
            return delegate.getConfig();
        }

        @Override
        public void setConfig(ManagedLedgerConfig config) {
            delegate.setConfig(config);
        }

        @Override
        public Position getLastConfirmedEntry() {
            return delegate.getLastConfirmedEntry();
        }

        @Override
        public void readyToCreateNewLedger() {
            delegate.readyToCreateNewLedger();
        }

        @Override
        public Map<String, String> getProperties() {
            return delegate.getProperties();
        }

        @Override
        public void setProperty(String key, String value) throws InterruptedException, ManagedLedgerException {
            delegate.setProperty(key, value);
        }

        @Override
        public void asyncSetProperty(String key, String value, AsyncCallbacks.UpdatePropertiesCallback callback,
                                     Object ctx) {
            delegate.asyncSetProperty(key, value, callback, ctx);
        }

        @Override
        public void deleteProperty(String key) throws InterruptedException, ManagedLedgerException {
            delegate.deleteProperty(key);
        }

        @Override
        public void asyncDeleteProperty(String key, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {
            delegate.asyncDeleteProperty(key, callback, ctx);
        }

        @Override
        public void setProperties(Map<String, String> properties) throws InterruptedException, ManagedLedgerException {
            delegate.setProperties(properties);
        }

        @Override
        public void asyncSetProperties(Map<String, String> properties, AsyncCallbacks.UpdatePropertiesCallback callback,
                                       Object ctx) {
            delegate.asyncSetProperties(properties, callback, ctx);
        }

        @Override
        public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
            delegate.trimConsumedLedgersInBackground(promise);
        }

        @Override
        public void rollCurrentLedgerIfFull() {
            delegate.rollCurrentLedgerIfFull();
        }

        @Override
        public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
            return delegate.asyncFindPosition(predicate);
        }

        @Override
        public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
            return delegate.getManagedLedgerInterceptor();
        }

        @Override
        public CompletableFuture<MLDataFormats.ManagedLedgerInfo.LedgerInfo> getLedgerInfo(long ledgerId) {
            return delegate.getLedgerInfo(ledgerId);
        }

        @Override
        public Optional<MLDataFormats.ManagedLedgerInfo.LedgerInfo> getOptionalLedgerInfo(long ledgerId) {
            return delegate.getOptionalLedgerInfo(ledgerId);
        }

        @Override
        public CompletableFuture<Void> asyncTruncate() {
            return delegate.asyncTruncate();
        }

        @Override
        public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(
                boolean includeLedgerMetadata) {
            return delegate.getManagedLedgerInternalStats(includeLedgerMetadata);
        }

        @Override
        public boolean checkInactiveLedgerAndRollOver() {
            return delegate.checkInactiveLedgerAndRollOver();
        }

        @Override
        public void checkCursorsToCacheEntries() {
            delegate.checkCursorsToCacheEntries();
        }

        @Override
        public void asyncReadEntry(Position position, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
            delegate.asyncReadEntry(position, callback, ctx);
        }

        @Override
        public NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> getLedgersInfo() {
            return delegate.getLedgersInfo();
        }

        @Override
        public Position getNextValidPosition(Position position) {
            return delegate.getNextValidPosition(position);
        }

        @Override
        public Position getPreviousPosition(Position position) {
            return delegate.getPreviousPosition(position);
        }

        @Override
        public long getEstimatedBacklogSize(Position position) {
            return delegate.getEstimatedBacklogSize(position);
        }

        @Override
        public Position getPositionAfterN(Position startPosition, long n, PositionBound startRange) {
            return delegate.getPositionAfterN(startPosition, n, startRange);
        }

        @Override
        public int getPendingAddEntriesCount() {
            return delegate.getPendingAddEntriesCount();
        }

        @Override
        public long getCacheSize() {
            return delegate.getCacheSize();
        }

        @Override
        public Position getFirstPosition() {
            return delegate.getFirstPosition();
        }
    }

    public static class ManagedCursorDecorator implements ManagedCursor {

        private final ManagedCursorImpl delegate;
        public Map<EntryImpl, Boolean> entryReleasedStatusMap = new ConcurrentHashMap<>();

        public ManagedCursorDecorator(ManagedCursorImpl delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public long getLastActive() {
            return delegate.getLastActive();
        }

        @Override
        public void updateLastActive() {
            delegate.updateLastActive();
        }

        @Override
        public Map<String, Long> getProperties() {
            return delegate.getProperties();
        }

        @Override
        public Map<String, String> getCursorProperties() {
            return delegate.getCursorProperties();
        }

        @Override
        public CompletableFuture<Void> putCursorProperty(String key, String value) {
            return delegate.putCursorProperty(key, value);
        }

        @Override
        public CompletableFuture<Void> setCursorProperties(Map<String, String> cursorProperties) {
            return delegate.setCursorProperties(cursorProperties);
        }

        @Override
        public CompletableFuture<Void> removeCursorProperty(String key) {
            return delegate.removeCursorProperty(key);
        }

        @Override
        public boolean putProperty(String key, Long value) {
            return delegate.putProperty(key, value);
        }

        @Override
        public boolean removeProperty(String key) {
            return delegate.removeProperty(key);
        }

        @Override
        public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
            return delegate.readEntries(numberOfEntriesToRead);
        }

        @Override
        public void asyncReadEntries(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                     Position maxPosition) {
            delegate.asyncReadEntries(numberOfEntriesToRead, callback, ctx, maxPosition);
        }

        @Override
        public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes,
                                     AsyncCallbacks.ReadEntriesCallback callback, Object ctx, Position maxPosition) {
            delegate.asyncReadEntries(numberOfEntriesToRead, maxSizeBytes, callback, ctx, maxPosition);
        }

        @Override
        public Entry getNthEntry(int n, IndividualDeletedEntries deletedEntries)
                throws InterruptedException, ManagedLedgerException {
            EntryImpl entry = (EntryImpl) delegate.getNthEntry(n, deletedEntries);
            entryReleasedStatusMap.put(entry, Boolean.FALSE);
            entry.onDeallocate(() -> {
                entryReleasedStatusMap.put(entry, Boolean.TRUE);
            });
            return entry;
        }

        @Override
        public void asyncGetNthEntry(int n, IndividualDeletedEntries deletedEntries,
                                     AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
            delegate.asyncGetNthEntry(n, deletedEntries, new AsyncCallbacks.ReadEntryCallback() {

                @Override
                public void readEntryComplete(Entry entry, Object ctx) {
                    EntryImpl e = (EntryImpl) entry;
                    entryReleasedStatusMap.put(e, Boolean.FALSE);
                    e.onDeallocate(() -> {
                        entryReleasedStatusMap.put(e, Boolean.TRUE);
                    });
                    callback.readEntryComplete(e, ctx);
                }

                @Override
                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                    callback.readEntryFailed(exception, ctx);
                }
            }, ctx);
        }

        @Override
        public List<Entry> readEntriesOrWait(int numberOfEntriesToRead)
                throws InterruptedException, ManagedLedgerException {
            return delegate.readEntriesOrWait(numberOfEntriesToRead);
        }

        @Override
        public List<Entry> readEntriesOrWait(int maxEntries, long maxSizeBytes)
                throws InterruptedException, ManagedLedgerException {
            return delegate.readEntriesOrWait(maxEntries, maxSizeBytes);
        }

        @Override
        public void asyncReadEntriesOrWait(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback,
                                           Object ctx, Position maxPosition) {
            delegate.asyncReadEntriesOrWait(numberOfEntriesToRead, callback, ctx, maxPosition);
        }

        @Override
        public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes,
                                           AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                           Position maxPosition) {
            delegate.asyncReadEntriesOrWait(maxEntries, maxSizeBytes, callback, ctx, maxPosition);
        }

        @Override
        public boolean cancelPendingReadRequest() {
            return delegate.cancelPendingReadRequest();
        }

        @Override
        public boolean hasMoreEntries() {
            return delegate.hasMoreEntries();
        }

        @Override
        public long getNumberOfEntries() {
            return delegate.getNumberOfEntries();
        }

        @Override
        public long getNumberOfEntriesInBacklog(boolean isPrecise) {
            return delegate.getNumberOfEntriesInBacklog(isPrecise);
        }

        @Override
        public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
            delegate.markDelete(position);
        }

        @Override
        public void markDelete(Position position, Map<String, Long> properties)
                throws InterruptedException, ManagedLedgerException {
            delegate.markDelete(position, properties);
        }

        @Override
        public void asyncMarkDelete(Position position, AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
            delegate.asyncMarkDelete(position, callback, ctx);
        }

        @Override
        public void asyncMarkDelete(Position position, Map<String, Long> properties,
                                    AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
            delegate.asyncMarkDelete(position, properties, callback, ctx);
        }

        @Override
        public void delete(Position position) throws InterruptedException, ManagedLedgerException {
            delegate.delete(position);
        }

        @Override
        public void asyncDelete(Position position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
            delegate.asyncDelete(position, callback, ctx);
        }

        @Override
        public void delete(Iterable<Position> positions) throws InterruptedException, ManagedLedgerException {
            delegate.delete(positions);
        }

        @Override
        public void asyncDelete(Iterable<Position> position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
            delegate.asyncDelete(position, callback, ctx);
        }

        @Override
        public Position getReadPosition() {
            return delegate.getReadPosition();
        }

        @Override
        public Position getMarkDeletedPosition() {
            return delegate.getMarkDeletedPosition();
        }

        @Override
        public Position getPersistentMarkDeletedPosition() {
            return delegate.getPersistentMarkDeletedPosition();
        }

        @Override
        public void rewind() {
            delegate.rewind();
        }

        @Override
        public void seek(Position newReadPosition, boolean force) {
            delegate.seek(newReadPosition, force);
        }

        @Override
        public void clearBacklog() throws InterruptedException, ManagedLedgerException {
            delegate.clearBacklog();
        }

        @Override
        public void asyncClearBacklog(AsyncCallbacks.ClearBacklogCallback callback, Object ctx) {
            delegate.asyncClearBacklog(callback, ctx);
        }

        @Override
        public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries)
                throws InterruptedException, ManagedLedgerException {
            delegate.skipEntries(numEntriesToSkip, deletedEntries);
        }

        @Override
        public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
                                     AsyncCallbacks.SkipEntriesCallback callback, Object ctx) {
            delegate.asyncSkipEntries(numEntriesToSkip, deletedEntries, callback, ctx);
        }

        @Override
        public Position findNewestMatching(Predicate<Entry> condition)
                throws InterruptedException, ManagedLedgerException {
            return delegate.findNewestMatching(condition);
        }

        @Override
        public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition)
                throws InterruptedException, ManagedLedgerException {
            return delegate.findNewestMatching(constraint, condition);
        }

        @Override
        public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                            AsyncCallbacks.FindEntryCallback callback, Object ctx) {
            delegate.asyncFindNewestMatching(constraint, condition, callback, ctx);
        }

        @Override
        public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                            AsyncCallbacks.FindEntryCallback callback, Object ctx,
                                            boolean isFindFromLedger) {
            delegate.asyncFindNewestMatching(constraint, condition, callback, ctx, isFindFromLedger);
        }

        @Override
        public void resetCursor(Position position) throws InterruptedException, ManagedLedgerException {
            delegate.resetCursor(position);
        }

        @Override
        public void asyncResetCursor(Position position, boolean forceReset,
                                     AsyncCallbacks.ResetCursorCallback callback) {
            delegate.asyncResetCursor(position, forceReset, callback);
        }

        @Override
        public List<Entry> replayEntries(Set<? extends Position> positions)
                throws InterruptedException, ManagedLedgerException {
            return delegate.replayEntries(positions);
        }

        @Override
        public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                          AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
            return delegate.asyncReplayEntries(positions, callback, ctx);
        }

        @Override
        public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                          AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                                          boolean sortEntries) {
            return delegate.asyncReplayEntries(positions, callback, ctx, sortEntries);
        }

        @Override
        public void close() throws InterruptedException, ManagedLedgerException {
            delegate.close();
        }

        @Override
        public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
            delegate.asyncClose(callback, ctx);
        }

        @Override
        public Position getFirstPosition() {
            return delegate.getFirstPosition();
        }

        @Override
        public void setActive() {
            delegate.setActive();
        }

        @Override
        public void setInactive() {
            delegate.setInactive();
        }

        @Override
        public void setAlwaysInactive() {
            delegate.setAlwaysInactive();
        }

        @Override
        public boolean isActive() {
            return delegate.isActive();
        }

        @Override
        public boolean isDurable() {
            return delegate.isDurable();
        }

        @Override
        public long getNumberOfEntriesSinceFirstNotAckedMessage() {
            return delegate.getNumberOfEntriesSinceFirstNotAckedMessage();
        }

        @Override
        public int getTotalNonContiguousDeletedMessagesRange() {
            return delegate.getTotalNonContiguousDeletedMessagesRange();
        }

        @Override
        public int getNonContiguousDeletedMessagesRangeSerializedSize() {
            return delegate.getNonContiguousDeletedMessagesRangeSerializedSize();
        }

        @Override
        public long getEstimatedSizeSinceMarkDeletePosition() {
            return delegate.getEstimatedSizeSinceMarkDeletePosition();
        }

        @Override
        public double getThrottleMarkDelete() {
            return delegate.getThrottleMarkDelete();
        }

        @Override
        public void setThrottleMarkDelete(double throttleMarkDelete) {
            delegate.setThrottleMarkDelete(throttleMarkDelete);
        }

        @Override
        public ManagedLedger getManagedLedger() {
            return delegate.getManagedLedger();
        }

        @Override
        public Range<Position> getLastIndividualDeletedRange() {
            return delegate.getLastIndividualDeletedRange();
        }

        @Override
        public void trimDeletedEntries(List<Entry> entries) {
            delegate.trimDeletedEntries(entries);
        }

        @Override
        public long[] getDeletedBatchIndexesAsLongArray(Position position) {
            return delegate.getDeletedBatchIndexesAsLongArray(position);
        }

        @Override
        public ManagedCursorMXBean getStats() {
            return delegate.getStats();
        }

        @Override
        public boolean checkAndUpdateReadPositionChanged() {
            return delegate.checkAndUpdateReadPositionChanged();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public ManagedLedgerInternalStats.CursorStats getCursorStats() {
            return delegate.getCursorStats();
        }

        @Override
        public boolean isMessageDeleted(Position position) {
            return delegate.isMessageDeleted(position);
        }

        @Override
        public ManagedCursor duplicateNonDurableCursor(String nonDurableCursorName) throws ManagedLedgerException {
            return delegate.duplicateNonDurableCursor(nonDurableCursorName);
        }

        @Override
        public long[] getBatchPositionAckSet(Position position) {
            return delegate.getBatchPositionAckSet(position);
        }

        @Override
        public int applyMaxSizeCap(int maxEntries, long maxSizeBytes) {
            return delegate.applyMaxSizeCap(maxEntries, maxSizeBytes);
        }

        @Override
        public void updateReadStats(int readEntriesCount, long readEntriesSize) {
            delegate.updateReadStats(readEntriesCount, readEntriesSize);
        }
    }
}
