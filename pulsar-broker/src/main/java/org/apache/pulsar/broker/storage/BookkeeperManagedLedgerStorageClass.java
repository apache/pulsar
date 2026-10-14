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
package org.apache.pulsar.broker.storage;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;

/**
 * ManagedLedgerStorageClass represents a configured instance of ManagedLedgerFactory for managed ledgers.
 * This instance is backed by a bookkeeper storage.
 */
public interface BookkeeperManagedLedgerStorageClass extends ManagedLedgerStorageClass {
    /**
     * Return the bookkeeper client instance used by this instance.
     *
     * @return the bookkeeper client.
     */
    BookKeeper getBookKeeperClient();

    /**
     * Return the BookKeeper client for the specified ensemble placement policy.
     *
     * <p>The returned client is borrowed from the storage class and must not be closed by the caller. It must access
     * the same BookKeeper backend as {@link #getBookKeeperClient()}, so existing clients can read and delete the
     * ledgers it creates.
     *
     * <p>The default implementation delegates to {@link #getBookKeeperClient()} when the configuration or its policy
     * class is {@code null}. For a custom policy, it returns a future completed exceptionally with
     * {@link UnsupportedOperationException}.
     *
     * <p>Implementations must override this method to support custom policies. When namespace bookie affinity is
     * configured or {@code strictBookieAffinityEnabled} is enabled, auxiliary schema, compaction, and delayed bucket
     * ledgers require a custom policy. Existing implementations that inherit this default method fail those
     * operations rather than silently ignoring the policy.
     *
     * @param ensemblePlacementPolicyConfig the ensemble placement policy configuration
     * @return a future that completes with the BookKeeper client
     */
    default CompletableFuture<BookKeeper> getBookKeeperClient(
            EnsemblePlacementPolicyConfig ensemblePlacementPolicyConfig) {
        if (ensemblePlacementPolicyConfig == null
                || ensemblePlacementPolicyConfig.getPolicyClass() == null) {
            try {
                return CompletableFuture.completedFuture(getBookKeeperClient());
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Custom ensemble placement policies are not supported by this storage class"));
    }

    /**
     * Return the stats provider to expose the stats of the storage implementation.
     *
     * @return the stats provider.
     */
    StatsProvider getStatsProvider();
}
