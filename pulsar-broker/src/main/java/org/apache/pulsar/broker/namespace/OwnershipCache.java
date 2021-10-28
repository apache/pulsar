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
package org.apache.pulsar.broker.namespace;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.stats.CacheMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a cache service for all the service unit ownership among the brokers. It provide a cache service
 * as well as MetadataStore read/write functions for a) lookup of a service unit ownership to a broker; b) take
 * ownership of a service unit by the local broker
 */
@Slf4j
public class OwnershipCache {

    private static final Logger LOG = LoggerFactory.getLogger(OwnershipCache.class);

    /**
     * The local broker URL that this <code>OwnershipCache</code> will set as owner.
     */
    private final String ownerBrokerUrl;

    /**
     * The local broker URL that this <code>OwnershipCache</code> will set as owner.
     */
    private final String ownerBrokerUrlTls;

    /**
     * The NamespaceEphemeralData objects that can be associated with the current owner.
     */
    private NamespaceEphemeralData selfOwnerInfo;

    /**
     * The NamespaceEphemeralData objects that can be associated with the current owner, when the broker is disabled.
     */
    private final NamespaceEphemeralData selfOwnerInfoDisabled;

    private final LockManager<NamespaceEphemeralData> lockManager;

    private final Map<NamespaceBundle, ResourceLock> locallyAcquiredLocks;

    /**
     * The loading cache of locally owned <code>NamespaceBundle</code> objects.
     */
    private final AsyncLoadingCache<NamespaceBundle, OwnedBundle> ownedBundlesCache;

    /**
     * The <code>NamespaceBundleFactory</code> to construct <code>NamespaceBundles</code>.
     */
    private final NamespaceBundleFactory bundleFactory;

    /**
     * The <code>NamespaceService</code> which using <code>OwnershipCache</code>.
     */
    private final NamespaceService namespaceService;

    private final PulsarService pulsar;

    private class OwnedServiceUnitCacheLoader implements AsyncCacheLoader<NamespaceBundle, OwnedBundle> {

        @Override
        public CompletableFuture<OwnedBundle> asyncLoad(NamespaceBundle namespaceBundle, Executor executor) {
            return lockManager.acquireLock(ServiceUnitUtils.path(namespaceBundle), selfOwnerInfo)
                    .thenApply(rl -> {
                        locallyAcquiredLocks.put(namespaceBundle, rl);
                        rl.getLockExpiredFuture()
                                .thenRun(() -> {
                                    log.info("Resource lock for {} has expired", rl.getPath());
                                    namespaceService.unloadNamespaceBundle(namespaceBundle);
                                    ownedBundlesCache.synchronous().invalidate(namespaceBundle);
                                    namespaceService.onNamespaceBundleUnload(namespaceBundle);
                                });
                        return new OwnedBundle(namespaceBundle);
                    });
        }
    }

    /**
     * Constructor of <code>OwnershipCache</code>.
     *
     * the local broker URL that will be set as owner for the <code>ServiceUnit</code>
     */
    public OwnershipCache(PulsarService pulsar, NamespaceBundleFactory bundleFactory,
                          NamespaceService namespaceService) {
        this.namespaceService = namespaceService;
        this.pulsar = pulsar;
        this.ownerBrokerUrl = pulsar.getBrokerServiceUrl();
        this.ownerBrokerUrlTls = pulsar.getBrokerServiceUrlTls();
        this.selfOwnerInfo = new NamespaceEphemeralData(ownerBrokerUrl, ownerBrokerUrlTls,
                pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                false, pulsar.getAdvertisedListeners());
        this.selfOwnerInfoDisabled = new NamespaceEphemeralData(ownerBrokerUrl, ownerBrokerUrlTls,
                pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                true, pulsar.getAdvertisedListeners());
        this.bundleFactory = bundleFactory;
        this.lockManager = pulsar.getCoordinationService().getLockManager(NamespaceEphemeralData.class);
        this.locallyAcquiredLocks = new ConcurrentHashMap<>();
        // ownedBundlesCache contains all namespaces that are owned by the local broker
        this.ownedBundlesCache = Caffeine.newBuilder()
                .executor(MoreExecutors.directExecutor())
                .recordStats()
                .buildAsync(new OwnedServiceUnitCacheLoader());
        CacheMetricsCollector.CAFFEINE.addCache("owned-bundles", this.ownedBundlesCache);
    }

    /**
     * Check whether this broker owns given namespace bundle.
     *
     * @param bundle namespace bundle
     * @return future that will complete with check result
     */
    public boolean checkOwnership(NamespaceBundle bundle) {
        return getOwnedBundle(bundle) != null;
    }

    /**
     * Method to get the current owner of the <code>ServiceUnit</code>.
     *
     * @param suName
     *            name of the <code>ServiceUnit</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * @throws Exception
     *             throws exception if no ownership info is found
     */
    public CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle suName) {
        CompletableFuture<OwnedBundle> ownedBundleFuture = ownedBundlesCache.getIfPresent(suName);
        if (ownedBundleFuture != null) {
            // Either we're the owners or we're trying to become the owner.
            return ownedBundleFuture.thenApply(serviceUnit -> {
                // We are the owner of the service unit
                return Optional.of(serviceUnit.isActive() ? selfOwnerInfo : selfOwnerInfoDisabled);
            });
        }

        // If we're not the owner, we need to check if anybody else is
        String path = ServiceUnitUtils.path(suName);
        return lockManager.readLock(path);
    }

    /**
     * Method to get the current owner of the <code>ServiceUnit</code> or set the local broker as the owner if absent.
     *
     * @param bundle
     *            the <code>NamespaceBundle</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * @throws Exception
     */
    public CompletableFuture<NamespaceEphemeralData> tryAcquiringOwnership(NamespaceBundle bundle) throws Exception {
        if (!refreshSelfOwnerInfo()) {
            return FutureUtil.failedFuture(
                    new RuntimeException("Namespace service does not ready for acquiring ownership"));
        }

        LOG.info("Trying to acquire ownership of {}", bundle);

        // Doing a get() on the ownedBundlesCache will trigger an async metatada write to acquire the lock over the
        // service unit
        return ownedBundlesCache.get(bundle)
                .thenApply(namespaceBundle -> {
            LOG.info("Successfully acquired ownership of {}", namespaceBundle);
            namespaceService.onNamespaceBundleOwned(bundle);
            return selfOwnerInfo;
        });
    }

    /**
     * Method to remove the ownership of local broker on the <code>NamespaceBundle</code>, if owned.
     *
     */
    public CompletableFuture<Void> removeOwnership(NamespaceBundle bundle) {
        ResourceLock<NamespaceEphemeralData> lock = locallyAcquiredLocks.get(bundle);
        if (lock == null) {
            // We don't own the specified bundle anymore
            return CompletableFuture.completedFuture(null);
        }

        return lock.release();
    }

    /**
     * Method to remove ownership of all owned bundles.
     *
     * @param bundles
     *            <code>NamespaceBundles</code> to remove from ownership cache
     */
    public CompletableFuture<Void> removeOwnership(NamespaceBundles bundles) {
        List<CompletableFuture<Void>> allFutures = Lists.newArrayList();
        for (NamespaceBundle bundle : bundles.getBundles()) {
            if (getOwnedBundle(bundle) == null) {
                // continue
                continue;
            }
            allFutures.add(this.removeOwnership(bundle));
        }
        return FutureUtil.waitForAll(allFutures);
    }


    /**
     * Method to access the map of all <code>ServiceUnit</code> objects owned by the local broker.
     *
     * @return a map of owned <code>ServiceUnit</code> objects
     */
    public Map<NamespaceBundle, OwnedBundle> getOwnedBundles() {
        return this.ownedBundlesCache.synchronous().asMap();
    }

    /**
     * Checked whether a particular bundle is currently owned by this broker.
     *
     * @param bundle
     * @return
     */
    public boolean isNamespaceBundleOwned(NamespaceBundle bundle) {
        OwnedBundle ownedBundle = getOwnedBundle(bundle);
        return ownedBundle != null && ownedBundle.isActive();
    }

    /**
     * Return the {@link OwnedBundle} instance from the local cache. Does not block.
     *
     * @param bundle
     * @return
     */
    public OwnedBundle getOwnedBundle(NamespaceBundle bundle) {
        CompletableFuture<OwnedBundle> future = ownedBundlesCache.getIfPresent(bundle);

        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            try {
                return future.get(pulsar.getConfiguration().getZooKeeperOperationTimeoutSeconds(), TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        } else {
            return null;
        }
    }

    /**
     * Disable bundle in local cache and on zk.
     *
     * @param bundle
     * @throws Exception
     */
    public CompletableFuture<Void> disableOwnership(NamespaceBundle bundle) {
        return updateBundleState(bundle, false)
                .thenCompose(__ -> {
                    ResourceLock<NamespaceEphemeralData> lock = locallyAcquiredLocks.get(bundle);
                    if (lock == null) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return lock.updateValue(selfOwnerInfoDisabled);
                    }
                });
    }

    /**
     * Update bundle state in a local cache.
     *
     * @param bundle
     * @throws Exception
     */
    public CompletableFuture<Void> updateBundleState(NamespaceBundle bundle, boolean isActive) {
        // Disable owned instance in local cache
        CompletableFuture<OwnedBundle> f = ownedBundlesCache.getIfPresent(bundle);
        if (f != null && f.isDone() && !f.isCompletedExceptionally()) {
            return f.thenAccept(ob -> ob.setActive(isActive));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public void invalidateLocalOwnerCache() {
        this.ownedBundlesCache.synchronous().invalidateAll();
    }

    public synchronized boolean refreshSelfOwnerInfo() {
        this.selfOwnerInfo = new NamespaceEphemeralData(pulsar.getBrokerServiceUrl(),
                pulsar.getBrokerServiceUrlTls(), pulsar.getSafeWebServiceAddress(),
                pulsar.getWebServiceAddressTls(), false, pulsar.getAdvertisedListeners());
        return selfOwnerInfo.getNativeUrl() != null || selfOwnerInfo.getNativeUrlTls() != null;
    }
}
