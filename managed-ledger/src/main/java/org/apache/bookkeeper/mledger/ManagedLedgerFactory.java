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
package org.apache.bookkeeper.mledger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenReadOnlyCursorCallback;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;

/**
 * A factory to open/create managed ledgers and delete them.
 *
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ManagedLedgerFactory {

    /**
     * Open a managed ledger. If the managed ledger does not exist, a new one will be automatically created. Uses the
     * default configuration parameters.
     *
     * @param name
     *            the unique name that identifies the managed ledger
     * @return the managed ledger
     * @throws ManagedLedgerException
     */
    ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException;

    /**
     * Open a managed ledger. If the managed ledger does not exist, a new one will be automatically created.
     *
     * @param name
     *            the unique name that identifies the managed ledger
     * @param config
     *            managed ledger configuration
     * @return the managed ledger
     * @throws ManagedLedgerException
     */
    ManagedLedger open(String name, ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronous open method.
     *
     * @see #open(String)
     * @param name
     *            the unique name that identifies the managed ledger
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncOpen(String name, OpenLedgerCallback callback, Object ctx);

    /**
     * Asynchronous open method.
     *
     * @see #open(String, ManagedLedgerConfig)
     * @param name
     *            the unique name that identifies the managed ledger
     * @param config
     *            managed ledger configuration
     * @param callback
     *            callback object
     * @param mlOwnershipChecker
     *            checks ml-ownership in case updating ml-metadata fails due to ownership conflict
     * @param ctx
     *            opaque context
     */
    void asyncOpen(String name, ManagedLedgerConfig config, OpenLedgerCallback callback,
            Supplier<CompletableFuture<Boolean>> mlOwnershipChecker, Object ctx);

    /**
     * Open a {@link ReadOnlyCursor} positioned to the earliest entry for the specified managed ledger.
     *
     * @param managedLedgerName
     * @param startPosition
     *            set the cursor on that particular position. If setting to `PositionImpl.earliest` it will be
     *            positioned on the first available entry.
     * @return
     */
    ReadOnlyCursor openReadOnlyCursor(String managedLedgerName, Position startPosition, ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Open a {@link ReadOnlyCursor} positioned to the earliest entry for the specified managed ledger.
     *
     * @param managedLedgerName
     * @param startPosition
     *            set the cursor on that particular position. If setting to `PositionImpl.earliest` it will be
     *            positioned on the first available entry.
     * @param callback
     * @param ctx
     */
    void asyncOpenReadOnlyCursor(String managedLedgerName, Position startPosition, ManagedLedgerConfig config,
            OpenReadOnlyCursorCallback callback, Object ctx);

    /**
     * Asynchronous open a Read-only managedLedger.
     * @param managedLedgerName the unique name that identifies the managed ledger
     * @param callback
     * @param config the managed ledger configuration.
     * @param ctx opaque context
     */
    void asyncOpenReadOnlyManagedLedger(String managedLedgerName,
                                AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback,
                                ManagedLedgerConfig config, Object ctx);

    /**
     * Get the current metadata info for a managed ledger.
     *
     * @param name
     *            the unique name that identifies the managed ledger
     */
    ManagedLedgerInfo getManagedLedgerInfo(String name) throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronously get the current metadata info for a managed ledger.
     *
     * @param name
     *            the unique name that identifies the managed ledger
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncGetManagedLedgerInfo(String name, ManagedLedgerInfoCallback callback, Object ctx);

    /**
     * Delete a managed ledger. If it's not open, it's metadata will get regardless deleted.
     *
     * @param name
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void delete(String name) throws InterruptedException, ManagedLedgerException;

    /**
     * Delete a managed ledger. If it's not open, it's metadata will get regardless deleted.
     *
     * @param name
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void delete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Delete a managed ledger. If it's not open, it's metadata will get regardless deleted.
     *
     * @param name
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void asyncDelete(String name, DeleteLedgerCallback callback, Object ctx);

    /**
     * Delete a managed ledger. If it's not open, it's metadata will get regardless deleted.
     *
     * @param name
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void asyncDelete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                             DeleteLedgerCallback callback, Object ctx);

    /**
     * Releases all the resources maintained by the ManagedLedgerFactory.
     *
     * @throws ManagedLedgerException
     */
    void shutdown() throws InterruptedException, ManagedLedgerException;

    /**
     * This method tries it's best to releases all the resources maintained by the ManagedLedgerFactory.
     * It will take longer time to shutdown than shutdown();
     *
     * @see #shutdown()
     * @throws ManagedLedgerException
     */
    CompletableFuture<Void> shutdownAsync() throws ManagedLedgerException, InterruptedException;

    /**
     * Check managed ledger has been initialized before.
     *
     * @param ledgerName {@link String}
     * @return a future represents the result of the operation.
     *         an instance of {@link Boolean} is returned
     *         if the operation succeeds.
     */
    CompletableFuture<Boolean> asyncExists(String ledgerName);

    /**
     * @return return EntryCacheManager.
     */
    EntryCacheManager getEntryCacheManager();

    /**
     * update cache evictionTimeThreshold dynamically. Similar as
     * {@link ManagedLedgerFactoryConfig#setCacheEvictionTimeThresholdMillis(long)}
     * but the value is in nanos. This inconsistency is kept for backwards compatibility.
     * @param cacheEvictionTimeThresholdNanos time threshold in nanos for eviction.
     */
    void updateCacheEvictionTimeThreshold(long cacheEvictionTimeThresholdNanos);

    /**
     * time threshold for eviction. Similar as
     * {@link ManagedLedgerFactoryConfig#getCacheEvictionTimeThresholdMillis()}
     * but the value is in nanos.
     * @return time threshold for eviction.
     */
    long getCacheEvictionTimeThreshold();

    /**
     * Update extendTTLOfEntriesWithRemainingExpectedReadsMaxTimes. Similar as
     * {@link ManagedLedgerFactoryConfig#setCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(int)}.
     * @param extendTTLOfEntriesWithRemainingExpectedReadsMaxTimes max times to extend TTL for entries with remaining
     *                                                            expected reads.
     */
    default void updateCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(
            int extendTTLOfEntriesWithRemainingExpectedReadsMaxTimes) {
        // Default implementation does nothing for backwards compatibility of the ManagedLedgerFactory interface.
        // Subclasses can override this method to provide specific behavior.
    }

    /**
     * Update cacheEvictionExtendTTLOfRecentlyAccessed. Similar as
     * {@link ManagedLedgerFactoryConfig#setCacheEvictionExtendTTLOfRecentlyAccessed(boolean)}.
     * @param cacheEvictionExtendTTLOfRecentlyAccessed whether to extend TTL of recently accessed entries.
     */
    default void updateCacheEvictionExtendTTLOfRecentlyAccessed(boolean cacheEvictionExtendTTLOfRecentlyAccessed) {
        // Default implementation does nothing for backwards compatibility of the ManagedLedgerFactory interface.
        // Subclasses can override this method to provide specific behavior.
    }

    /**
     * @return properties of this managedLedger.
     */
    CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name);

    Map<String, ManagedLedger> getManagedLedgers();

    ManagedLedgerFactoryMXBean getCacheStats();


    void estimateUnloadedTopicBacklog(PersistentOfflineTopicStats offlineTopicStats, TopicName topicName,
                                      boolean accurate, Object ctx) throws Exception;

    ManagedLedgerFactoryConfig getConfig();
}
