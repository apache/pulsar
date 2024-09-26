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
package org.apache.pulsar.utils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.Setter;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.mockito.Mockito;

public class MockManagedLedgerFactory implements ManagedLedgerFactory {

    private final Map<String, MockManagedLedger> ledgers = new ConcurrentHashMap<>();
    @Setter
    private MetadataStore store;

    @Override
    public ManagedLedger open(String name) {
        return open(name, null);
    }

    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config) {
        final var future = new CompletableFuture<ManagedLedger>();
        asyncOpen(name, new AsyncCallbacks.OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                future.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future.join();
    }

    @Override
    public void asyncOpen(String name, AsyncCallbacks.OpenLedgerCallback callback, Object ctx) {
        asyncOpen(name, new ManagedLedgerConfig(), callback, null, ctx);
    }

    @Override
    public void asyncOpen(String name, ManagedLedgerConfig config, AsyncCallbacks.OpenLedgerCallback callback,
                          Supplier<CompletableFuture<Boolean>> mlOwnershipChecker, Object ctx) {
        final var ledger = ledgers.computeIfAbsent(name, __ -> {
            final String path = "/managed-ledgers/" + name;
            store.put(path, new byte[0], Optional.empty());
            return new MockManagedLedger(name);
        });
        callback.openLedgerComplete(ledger, ctx);
    }

    @Override
    public ReadOnlyCursor openReadOnlyCursor(String managedLedgerName, Position startPosition,
                                             ManagedLedgerConfig config) {
        throw new RuntimeException("openReadOnlyCursor is not supported");
    }

    @Override
    public void asyncOpenReadOnlyCursor(String managedLedgerName, Position startPosition, ManagedLedgerConfig config,
                                        AsyncCallbacks.OpenReadOnlyCursorCallback callback, Object ctx) {
        throw new RuntimeException("openReadOnlyCursor is not supported");
    }

    @Override
    public void asyncOpenReadOnlyManagedLedger(String managedLedgerName,
                                               AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback,
                                               ManagedLedgerConfig config, Object ctx) {
        throw new RuntimeException("openReadOnlyCursor is not supported");
    }

    @Override
    public ManagedLedgerInfo getManagedLedgerInfo(String name) throws InterruptedException, ManagedLedgerException {
        return new ManagedLedgerInfo();
    }

    @Override
    public void asyncGetManagedLedgerInfo(String name, AsyncCallbacks.ManagedLedgerInfoCallback callback, Object ctx) {
        callback.getInfoComplete(new ManagedLedgerInfo(), ctx);
    }

    @Override
    public void delete(String name) {
        ledgers.remove(name);
    }

    @Override
    public void delete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture) {
        delete(name);
    }

    @Override
    public void asyncDelete(String name, AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        delete(name);
        callback.deleteLedgerComplete(ctx);
    }

    @Override
    public void asyncDelete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                            AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        delete(name);
        callback.deleteLedgerComplete(ctx);
    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {
    }

    @Override
    public CompletableFuture<Void> shutdownAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> asyncExists(String ledgerName) {
        return CompletableFuture.completedFuture(ledgers.containsKey(ledgerName));
    }

    @Override
    public EntryCacheManager getEntryCacheManager() {
        return Mockito.mock(EntryCacheManager.class);
    }

    @Override
    public void updateCacheEvictionTimeThreshold(long cacheEvictionTimeThresholdNanos) {
    }

    @Override
    public long getCacheEvictionTimeThreshold() {
        return 0;
    }

    @Override
    public CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name) {
        return CompletableFuture.completedFuture(Map.of());
    }

    @Override
    public Map<String, ManagedLedger> getManagedLedgers() {
        return Map.of();
    }

    @Override
    public ManagedLedgerFactoryMXBean getCacheStats() {
        return Mockito.mock(ManagedLedgerFactoryMXBean.class);
    }

    @Override
    public void estimateUnloadedTopicBacklog(PersistentOfflineTopicStats offlineTopicStats, TopicName topicName,
                                             boolean accurate, Object ctx) throws Exception {
    }

    @Override
    public ManagedLedgerFactoryConfig getConfig() {
        return new ManagedLedgerFactoryConfig();
    }
}
