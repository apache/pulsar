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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.stats.CacheMetricsCollector;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * This class provides a cache service for all the service unit ownership among the brokers. It provide a cache service
 * as well as ZooKeeper read/write functions for a) lookup of a service unit ownership to a broker; b) take ownership of
 * a service unit by the local broker
 *
 *
 */
public class OwnershipCache {

    private static final Logger LOG = LoggerFactory.getLogger(OwnershipCache.class);

    /**
     * The local broker URL that this <code>OwnershipCache</code> will set as owner
     */
    private final String ownerBrokerUrl;

    /**
     * The local broker URL that this <code>OwnershipCache</code> will set as owner
     */
    private final String ownerBrokerUrlTls;

    /**
     * The NamespaceEphemeralData objects that can be associated with the current owner
     */
    private NamespaceEphemeralData selfOwnerInfo;

    /**
     * The NamespaceEphemeralData objects that can be associated with the current owner, when the broker is disabled.
     */
    private final NamespaceEphemeralData selfOwnerInfoDisabled;

    /**
     * Service unit ownership cache of <code>ZooKeeper</code> data of ephemeral nodes showing all known ownership of
     * service unit to active brokers
     */
    private final ZooKeeperDataCache<NamespaceEphemeralData> ownershipReadOnlyCache;

    /**
     * The loading cache of locally owned <code>NamespaceBundle</code> objects
     */
    private final AsyncLoadingCache<String, OwnedBundle> ownedBundlesCache;

    /**
     * The <code>ObjectMapper</code> to deserialize/serialize JSON objects
     */
    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    /**
     * The <code>ZooKeeperCache</code> connecting to the local ZooKeeper
     */
    private final ZooKeeperCache localZkCache;

    /**
     * The <code>NamespaceBundleFactory</code> to construct <code>NamespaceBundles</code>
     */
    private final NamespaceBundleFactory bundleFactory;

    /**
     * The <code>NamespaceService</code> which using <code>OwnershipCache</code>
     */
    private NamespaceService namespaceService;

    private final PulsarService pulsar;

    private class OwnedServiceUnitCacheLoader implements AsyncCacheLoader<String, OwnedBundle> {

        @SuppressWarnings("deprecation")
        @Override
        public CompletableFuture<OwnedBundle> asyncLoad(String namespaceBundleZNode, Executor executor) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Acquiring zk lock on namespace {}", namespaceBundleZNode);
            }

            byte[] znodeContent;
            try {
                znodeContent = jsonMapper.writeValueAsBytes(selfOwnerInfo);
            } catch (JsonProcessingException e) {
                // Failed to serialize to JSON
                return FutureUtil.failedFuture(e);
            }

            CompletableFuture<OwnedBundle> future = new CompletableFuture<>();
            ZkUtils.asyncCreateFullPathOptimistic(localZkCache.getZooKeeper(), namespaceBundleZNode, znodeContent,
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, (rc, path, ctx, name) -> {
                        if (rc == KeeperException.Code.OK.intValue()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Successfully acquired zk lock on {}", namespaceBundleZNode);
                            }
                            ownershipReadOnlyCache.invalidate(namespaceBundleZNode);
                            future.complete(new OwnedBundle(
                                    ServiceUnitZkUtils.suBundleFromPath(namespaceBundleZNode, bundleFactory)));
                        } else {
                            // Failed to acquire lock
                            future.completeExceptionally(KeeperException.create(rc));
                        }
                    }, null);

            return future;
        }
    }

    /**
     * Constructor of <code>OwnershipCache</code>
     *
     * @param ownerUrl
     *            the local broker URL that will be set as owner for the <code>ServiceUnit</code>
     */
    public OwnershipCache(PulsarService pulsar, NamespaceBundleFactory bundleFactory, NamespaceService namespaceService) {
        this.namespaceService = namespaceService;
        this.pulsar = pulsar;
        this.ownerBrokerUrl = pulsar.getSafeBrokerServiceUrl();
        this.ownerBrokerUrlTls = pulsar.getBrokerServiceUrlTls();
        this.selfOwnerInfo = new NamespaceEphemeralData(ownerBrokerUrl, ownerBrokerUrlTls,
                pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(), false);
        this.selfOwnerInfoDisabled = new NamespaceEphemeralData(ownerBrokerUrl, ownerBrokerUrlTls,
                pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(), true);
        this.bundleFactory = bundleFactory;
        this.localZkCache = pulsar.getLocalZkCache();
        this.ownershipReadOnlyCache = pulsar.getLocalZkCacheService().ownerInfoCache();
        // ownedBundlesCache contains all namespaces that are owned by the local broker
        this.ownedBundlesCache = Caffeine.newBuilder()
                .executor(MoreExecutors.directExecutor())
                .recordStats()
                .buildAsync(new OwnedServiceUnitCacheLoader());
        CacheMetricsCollector.CAFFEINE.addCache("owned-bundles", this.ownedBundlesCache);
    }

    /**
     * Method to get the current owner of the <code>ServiceUnit</code>
     *
     * @param suId
     *            identifier of the <code>ServiceUnit</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * @throws Exception
     *             throws exception if no ownership info is found
     */
    public CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle suname) {
        String path = ServiceUnitZkUtils.path(suname);

        CompletableFuture<OwnedBundle> ownedBundleFuture = ownedBundlesCache.getIfPresent(path);
        if (ownedBundleFuture != null) {
            // Either we're the owners or we're trying to become the owner.
            return ownedBundleFuture.thenApply(serviceUnit -> {
                // We are the owner of the service unit
                return Optional.of(serviceUnit.isActive() ? selfOwnerInfo : selfOwnerInfoDisabled);
            });
        }

        // If we're not the owner, we need to check if anybody else is
        return ownershipReadOnlyCache.getAsync(path);
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
        String path = ServiceUnitZkUtils.path(bundle);

        CompletableFuture<NamespaceEphemeralData> future = new CompletableFuture<>();

        if (!refreshSelfOwnerInfo()) {
            future.completeExceptionally(new RuntimeException("Namespace service does not ready for acquiring ownership"));
            return future;
        }

        LOG.info("Trying to acquire ownership of {}", bundle);

        // Doing a get() on the ownedBundlesCache will trigger an async ZK write to acquire the lock over the
        // service unit
        ownedBundlesCache.get(path).thenAccept(namespaceBundle -> {
            LOG.info("Successfully acquired ownership of {}", path);
            if (namespaceService != null) {
                namespaceService.onNamespaceBundleOwned(bundle);
            }
            future.complete(selfOwnerInfo);
        }).exceptionally(exception -> {
            // Failed to acquire ownership
            if (exception instanceof CompletionException
                    && exception.getCause() instanceof KeeperException.NodeExistsException) {
                LOG.info("Failed to acquire ownership of {} -- Already owned by other broker", path);
                // Other broker acquired ownership at the same time, let's try to read it from the read-only cache
                ownershipReadOnlyCache.getAsync(path).thenAccept(ownerData -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found owner for {} at {}", bundle, ownerData);
                    }

                    if (ownerData.isPresent()) {
                        future.complete(ownerData.get());
                    } else {
                        // Strange scenario: we couldn't create a z-node because it was already existing, but when we
                        // try to read it, it's not there anymore
                        future.completeExceptionally(exception);
                    }
                }).exceptionally(ex -> {
                    LOG.warn("Failed to check ownership of {}: {}", bundle, ex.getMessage(), ex);
                    future.completeExceptionally(exception);
                    return null;
                });
            } else {
                // Other ZK error, bailing out for now
                LOG.warn("Failed to acquire ownership of {}: {}", bundle, exception.getMessage(), exception);
                ownedBundlesCache.synchronous().invalidate(path);
                future.completeExceptionally(exception);
            }

            return null;
        });

        return future;
    }

    /**
     * Method to remove the ownership of local broker on the <code>NamespaceBundle</code>, if owned
     *
     */
    public CompletableFuture<Void> removeOwnership(NamespaceBundle bundle) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        String key = ServiceUnitZkUtils.path(bundle);
        localZkCache.getZooKeeper().delete(key, -1, (rc, path, ctx) -> {
            if (rc == KeeperException.Code.OK.intValue() || rc == KeeperException.Code.NONODE.intValue()) {
                LOG.info("[{}] Removed zk lock for service unit: {}", key, KeeperException.Code.get(rc));
                ownedBundlesCache.synchronous().invalidate(key);
                ownershipReadOnlyCache.invalidate(key);
                if (namespaceService != null) {
                    namespaceService.onNamespaceBundleUnload(bundle);
                }
                result.complete(null);
            } else {
                LOG.warn("[{}] Failed to delete the namespace ephemeral node. key={}", key,
                        KeeperException.Code.get(rc));
                result.completeExceptionally(KeeperException.create(rc));
            }
        }, null);
        return result;
    }

    /**
     * Method to remove ownership of all owned bundles
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
     * Method to access the map of all <code>ServiceUnit</code> objects owned by the local broker
     *
     * @return a map of owned <code>ServiceUnit</code> objects
     */
    public Map<String, OwnedBundle> getOwnedBundles() {
        return this.ownedBundlesCache.synchronous().asMap();
    }

    /**
     * Checked whether a particular bundle is currently owned by this broker
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
        CompletableFuture<OwnedBundle> future = ownedBundlesCache.getIfPresent(ServiceUnitZkUtils.path(bundle));
        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            return future.join();
        } else {
            return null;
        }
    }

    /**
     * Disable bundle in local cache and on zk
     *
     * @param bundle
     * @throws Exception
     */
    public void disableOwnership(NamespaceBundle bundle) throws Exception {
        String path = ServiceUnitZkUtils.path(bundle);
        updateBundleState(bundle, false);
        localZkCache.getZooKeeper().setData(path, jsonMapper.writeValueAsBytes(selfOwnerInfoDisabled), -1);
        ownershipReadOnlyCache.invalidate(path);
    }

    /**
     * Update bundle state in a local cache
     *
     * @param bundle
     * @throws Exception
     */
    public void updateBundleState(NamespaceBundle bundle, boolean isActive) throws Exception {
        String path = ServiceUnitZkUtils.path(bundle);
        // Disable owned instance in local cache
        CompletableFuture<OwnedBundle> f = ownedBundlesCache.getIfPresent(path);
        if (f != null && f.isDone() && !f.isCompletedExceptionally()) {
            f.join().setActive(isActive);
        }
    }

    public NamespaceEphemeralData getSelfOwnerInfo() {
        return selfOwnerInfo;
    }

    public synchronized boolean refreshSelfOwnerInfo() {
        if (selfOwnerInfo.getNativeUrl() == null) {
            this.selfOwnerInfo = new NamespaceEphemeralData(pulsar.getSafeBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls(),
                    pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(), false);
        }
        return selfOwnerInfo.getNativeUrl() != null;
    }
}
