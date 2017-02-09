/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.namespace;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.Lists;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.client.util.FutureUtil;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceBundles;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

import jersey.repackaged.com.google.common.collect.Maps;

/**
 * This {@link NamespaceOwnershipCache} helps to perform ownership-creation by creating znode on local_zk(primary-zk)
 * and data_zk(secondary-zk) atomically. It also refers both the zk in order to find ownership of bundles.
 * <p>
 * It is useful when cluster have two set of brokers (a) running with one zk instance (b) running with separate zk
 * instance for cluster-management and ledger-data.
 *
 */
public class DualOwnershipCache implements NamespaceOwnershipCache {

    private static final Logger LOG = LoggerFactory.getLogger(DualOwnershipCache.class);

    protected final OwnershipCacheImpl localZkOwnershipCache;
    protected final OwnershipCacheImpl dataZkOwnershipCache;
    private final boolean shouldWriteOnDataZk;

    /**
     * Constructor of <code>DualOwnershipCache</code>
     *
     * @param ownerUrl
     *            the local broker URL that will be set as owner for the <code>ServiceUnit</code>
     */
    public DualOwnershipCache(PulsarService pulsar, NamespaceBundleFactory bundleFactory) {
        this.localZkOwnershipCache = new OwnershipCacheImpl(pulsar, pulsar.getLocalZkCache(), bundleFactory);
        this.dataZkOwnershipCache = new OwnershipCacheImpl(pulsar, pulsar.getDataZkCache(), bundleFactory);
        this.shouldWriteOnDataZk = !pulsar.equalsDataAndLocalZk();
    }

    /**
     * Method to get the current owner of the <code>ServiceUnit</code> or set the local broker as the owner if absent
     *
     * @param suId
     *            identifier of the <code>NamespaceBundle</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * @throws Exception
     */
    public CompletableFuture<NamespaceEphemeralData> tryAcquiringOwnership(NamespaceBundle bundle) throws Exception {
        final String path = ServiceUnitZkUtils.path(bundle);

        CompletableFuture<NamespaceEphemeralData> future = new CompletableFuture<>();

        LOG.info("Trying to acquire ownership of {}", bundle);
        // Doing a get() on the ownedBundlesCache will trigger an async ZK write to acquire the lock over the
        // service unit
        localZkOwnershipCache.ownedBundlesCache.get(path).thenAccept(namespaceBundle -> {
            LOG.info("Successfully acquired ownership of {}", path);
            if (shouldWriteOnDataZk) {
                // try to acquire ownership on secondary-zk
                dataZkOwnershipCache.ownedBundlesCache.get(path).thenAccept(nsBundle -> {
                    LOG.info("Successfully acquired ownership of {} on secondary-zk", path);
                    future.complete(localZkOwnershipCache.selfOwnerInfo);
                }).exceptionally(exception -> {
                    // as failed to take ownership of secondary-zk: remove ownership on primary-zk
                    localZkOwnershipCache.removeOwnership(bundle).handle((result, ex) -> {
                        if (ex != null) {
                            LOG.warn("Failed to remove ownership from primary-zk {}", path, ex);
                        }
                        handleOwnershipFailure(exception, path, bundle, dataZkOwnershipCache.ownershipReadOnlyCache,
                                dataZkOwnershipCache.ownedBundlesCache, future);
                        return null;
                    });
                    ;
                    return null;
                });
            } else {
                future.complete(localZkOwnershipCache.selfOwnerInfo);
            }
        }).exceptionally(exception -> {
            handleOwnershipFailure(exception, path, bundle, localZkOwnershipCache.ownershipReadOnlyCache,
                    localZkOwnershipCache.ownedBundlesCache, future);
            return null;
        });

        return future;
    }

    private static void handleOwnershipFailure(Throwable exception, final String path, NamespaceBundle bundle,
            ZooKeeperDataCache<NamespaceEphemeralData> readOnlyCache,
            AsyncLoadingCache<String, OwnedBundle> ownedBundlesCache,
            CompletableFuture<NamespaceEphemeralData> resultFuture) {

        // Failed to acquire ownership
        if (exception instanceof CompletionException
                && exception.getCause() instanceof KeeperException.NodeExistsException) {
            LOG.info("Failed to acquire ownership of {} -- Already owned by other broker", path);
            // Other broker acquired ownership at the same time, let's try to read it from the read-only cache
            readOnlyCache.getAsync(path).thenAccept(ownerData -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found owner for {} at {}", bundle, ownerData);
                }

                if (ownerData.isPresent()) {
                    resultFuture.complete(ownerData.get());
                } else {
                    // Strange scenario: we couldn't create a z-node because it was already existing, but when we
                    // try to read it, it's not there anymore
                    resultFuture.completeExceptionally(exception);
                }
            }).exceptionally(ex -> {
                LOG.warn("Failed to check ownership of {}: {}", bundle, ex.getMessage(), ex);
                resultFuture.completeExceptionally(exception);
                return null;
            });
        } else {
            // Other ZK error, bailing out for now
            LOG.warn("Failed to acquire ownership of {}: {}", bundle, exception.getMessage(), exception);
            ownedBundlesCache.synchronous().invalidate(path);
            resultFuture.completeExceptionally(exception);
        }
    }

    @Override
    public CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle suname) {
        CompletableFuture<Optional<NamespaceEphemeralData>> result = new CompletableFuture<>();
        localZkOwnershipCache.getOwnerAsync(suname).thenApply(owner -> {
            if (owner.isPresent()) {
                result.complete(owner);
            } else {
                dataZkOwnershipCache.getOwnerAsync(suname).thenApply(secondary -> result.complete(secondary))
                        .exceptionally(ex -> result.completeExceptionally(ex));
            }
            return null;
        }).exceptionally(ex -> result.completeExceptionally(ex));
        return result;
    }

    @Override
    public CompletableFuture<Void> removeOwnership(NamespaceBundle bundle) {
        List<CompletableFuture<Void>> futures = Lists.newArrayList();
        futures.add(localZkOwnershipCache.removeOwnership(bundle));
        futures.add(dataZkOwnershipCache.removeOwnership(bundle));
        return FutureUtil.waitForAll(futures);
    }

    @Override
    public CompletableFuture<Void> removeOwnership(NamespaceBundles bundles) {
        List<CompletableFuture<Void>> allFutures = Lists.newArrayList();
        allFutures.add(localZkOwnershipCache.removeOwnership(bundles));
        allFutures.add(dataZkOwnershipCache.removeOwnership(bundles));
        return FutureUtil.waitForAll(allFutures);
    }

    @Override
    public Map<String, OwnedBundle> getOwnedBundles() {
        Map<String, OwnedBundle> ownedBundles = Maps
                .newHashMap(localZkOwnershipCache.ownedBundlesCache.synchronous().asMap());
        ownedBundles.putAll(this.dataZkOwnershipCache.ownedBundlesCache.synchronous().asMap());
        return ownedBundles;
    }

    @Override
    public boolean isNamespaceBundleOwned(NamespaceBundle bundle) {
        OwnedBundle primaryOwnedBundle = localZkOwnershipCache.getOwnedBundle(bundle);
        OwnedBundle secondaryOwnedBundle = dataZkOwnershipCache.getOwnedBundle(bundle);
        return (primaryOwnedBundle != null && primaryOwnedBundle.isActive())
                || (secondaryOwnedBundle != null && secondaryOwnedBundle.isActive());
    }

    @Override
    public OwnedBundle getOwnedBundle(NamespaceBundle bundle) {
        OwnedBundle ownedBundle = localZkOwnershipCache.getOwnedBundle(bundle);
        return ownedBundle != null ? ownedBundle : dataZkOwnershipCache.getOwnedBundle(bundle);
    }

    @Override
    public void disableOwnership(NamespaceBundle bundle) throws Exception {
        localZkOwnershipCache.disableOwnership(bundle);
        dataZkOwnershipCache.disableOwnership(bundle);
    }

    @Override
    public void updateBundleState(NamespaceBundle bundle, boolean isActive) throws Exception {
        localZkOwnershipCache.updateBundleState(bundle, isActive);
        dataZkOwnershipCache.updateBundleState(bundle, isActive);
    }

    @Override
    public NamespaceEphemeralData getSelfOwnerInfo() {
        return localZkOwnershipCache.getSelfOwnerInfo();
    }

}
