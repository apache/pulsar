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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceBundles;
import com.yahoo.pulsar.common.naming.ServiceUnitId;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

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
    private final NamespaceEphemeralData[] selfOwnerInfos;

    /**
     * Service unit ownership cache of <code>ZooKeeper</code> data of ephemeral nodes showing all known ownership of
     * service unit to active brokers
     */
    private final ZooKeeperDataCache<NamespaceEphemeralData> ownershipReadOnlyCache;

    /**
     * The loading cache of locally owned <code>ServiceUnit</code> objects
     */
    private final LoadingCache<String, OwnedServiceUnit> ownedServiceUnitsCache;

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
     * The max number of retries to acquire the ownership in <code>ZooKeeper</code>
     */
    private static final int MAX_RETRIES_CREATE_ZNODE = 5; // tentatively decide to retry 5 times in acquiring the
                                                           // zookeeper node

    private class OwnedServiceUnitCacheLoader extends CacheLoader<String, OwnedServiceUnit> {
        @Override
        public OwnedServiceUnit load(String key) throws Exception {
            LOG.info("Acquiring zk lock on namespace {}", key);

            // Under the cache sync lock, acquiring the ZNode
            // If succeeded, we guaranteed that the cache entry is setup w/ ZNode acquired
            // Only enters this load function if ownedServiceUnitsCache does not have the key
            // invalidate the read-only cache to ensure loading the up-to-date znode
            int numTries = 0;
            while (numTries != MAX_RETRIES_CREATE_ZNODE) {
                try {
                    checkArgument(ServiceUnitZkUtils
                            .acquireNameSpace(localZkCache.getZooKeeper(), key, selfOwnerInfos[0]).getNativeUrl()
                            .equals(ownerBrokerUrl)
                            || ServiceUnitZkUtils.acquireNameSpace(localZkCache.getZooKeeper(), key, selfOwnerInfos[0])
                                    .getNativeUrlTls().equals(ownerBrokerUrlTls));
                    ownershipReadOnlyCache.invalidate(key);
                    LOG.info("Acquired zk lock on namespace {}", key);
                    return new OwnedServiceUnit(ServiceUnitZkUtils.suIdFromPath(key, bundleFactory));
                } catch (Exception e) {
                    // Failed to acquire the namespace, try to load the read-only cache since some other broker may have
                    // won the race
                    LOG.warn(String.format("Failed in acquiring the namespace ownership. key=%s", key));
                    try {
                        ownershipReadOnlyCache.invalidate(key);
                        ownershipReadOnlyCache.get(key);
                        // if successful, the znode has been created by someone. break out the loop
                        break;
                    } catch (NoNodeException nne) {
                        // load read-only cache failed due to no node exists, hence, we should try to acquire the
                        // namespace
                        // again
                        numTries++;
                    } catch (Exception moreErr) {
                        // Other unexpected failure
                        LOG.error(String.format(
                                "Unexpected exception while loading the read-only ZK cache for namespace. key=%s", key),
                                moreErr);
                        throw moreErr;
                    }
                }
            }
            checkArgument(numTries < MAX_RETRIES_CREATE_ZNODE,
                    "Maximum retries exceeded to acquire the namespace. key=" + key);
            // load the read-only cache w/ update-to-date entry
            checkArgument(ownershipReadOnlyCache.get(key).getNativeUrl().equals(ownerBrokerUrl),
                    "Some other broker acquired the namespace. key=" + key);
            return new OwnedServiceUnit(ServiceUnitZkUtils.suIdFromPath(key, bundleFactory));
        }
    }

    private class OwnedServiceUnitCacheRemovalListener implements RemovalListener<String, OwnedServiceUnit> {

        @Override
        public void onRemoval(RemovalNotification<String, OwnedServiceUnit> notification) {
            // Under the cache sync lock, removing the ZNode
            // If succeeded, we guaranteed that the cache entry is removed together w/ ZNode
            try {
                localZkCache.getZooKeeper().delete(notification.getKey(), -1);
                ownershipReadOnlyCache.invalidate(notification.getKey());

                LOG.info("Removed zk lock for service unit: {}", notification.getKey());
            } catch (Exception e) {
                LOG.error("Failed to delete the namespace ephemeral node. key={}", notification.getKey(), e);
            }
        }
    }

    /**
     * Constructor of <code>OwnershipCache</code>
     *
     * @param ownerUrl
     *            the local broker URL that will be set as owner for the <code>ServiceUnit</code>
     */
    public OwnershipCache(PulsarService pulsar, NamespaceBundleFactory bundleFactory) {
        ServiceConfiguration conf = pulsar.getConfiguration();
        this.ownerBrokerUrl = pulsar.getBrokerServiceUrl();
        this.ownerBrokerUrlTls = pulsar.getBrokerServiceUrlTls();
        this.selfOwnerInfos = new NamespaceEphemeralData[] {
                new NamespaceEphemeralData(ownerBrokerUrl, ownerBrokerUrlTls, pulsar.getWebServiceAddress(),
                        pulsar.getWebServiceAddressTls(), false),
                new NamespaceEphemeralData(ownerBrokerUrl, ownerBrokerUrlTls, pulsar.getWebServiceAddress(),
                        pulsar.getWebServiceAddressTls(), true) };
        this.bundleFactory = bundleFactory;
        this.localZkCache = pulsar.getLocalZkCache();
        this.ownershipReadOnlyCache = pulsar.getLocalZkCacheService().ownerInfoCache();
        // ownedServiceUnitsCache contains all namespaces that are owned by the local broker
        this.ownedServiceUnitsCache = CacheBuilder.newBuilder()
                .removalListener(new OwnedServiceUnitCacheRemovalListener()).build(new OwnedServiceUnitCacheLoader());
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
    public NamespaceEphemeralData getOwner(ServiceUnitId suname) throws Exception {
        return this.ownershipReadOnlyCache.get(ServiceUnitZkUtils.path(suname));
    }

    /**
     * Method to get the current owner of the <code>ServiceUnit</code> or set the local broker as the owner if absent
     *
     * @param suId
     *            identifier of the <code>ServiceUnit</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * @throws Exception
     */
    public NamespaceEphemeralData getOrSetOwner(ServiceUnitId suname) throws Exception {
        String path = ServiceUnitZkUtils.path(suname);

        // If the node has been deleted between two checks, we need to try again
        while (true) {
            try {
                // Trying to load the ownedServiceUnitCache by acquiring the ZNode via the loader
                this.ownedServiceUnitsCache.get(path);
            } catch (Exception e) {
                LOG.info(String.format("Failed in acquiring the ownership of service unit %s", suname), e);
            }

            try {
                return this.ownershipReadOnlyCache.get(path);
            } catch (KeeperException.NoNodeException e) {
                LOG.warn("Failed in getting service unit from read-only cache {}", suname);
            }
        }
    }

    /**
     * Method to remove the ownership of local broker on the <code>ServiceUnit</code>, if owned
     *
     * @param suId
     *            identifier of the <code>ServiceUnit</code>
     */
    public void removeOwnership(ServiceUnitId suname) {
        this.ownedServiceUnitsCache.invalidate(ServiceUnitZkUtils.path(suname));
    }

    /**
     * Method to remove ownership of all owned bundles
     *
     * @param bundles
     *            <code>NamespaceBundles</code> to remove from ownership cache
     */
    public void removeOwnership(NamespaceBundles bundles) {
        boolean hasError = false;
        for (NamespaceBundle bundle : bundles.getBundles()) {
            if (getOwnedServiceUnit(bundle) == null) {
                // continue
                continue;
            }
            try {
                this.removeOwnership(bundle);
            } catch (Exception e) {
                LOG.warn(String.format("Failed to remove ownership of a service unit: %s", bundle), e);
                hasError = true;
            }
        }
        checkState(!hasError, "Not able to remove all owned bundles");
    }

    /**
     * Method to access the map of all <code>ServiceUnit</code> objects owned by the local broker
     *
     * @return a map of owned <code>ServiceUnit</code> objects
     */
    public Map<String, OwnedServiceUnit> getOwnedServiceUnits() {
        return this.ownedServiceUnitsCache.asMap();
    }

    public OwnedServiceUnit getOwnedServiceUnit(ServiceUnitId suname) {
        return this.ownedServiceUnitsCache.getIfPresent(ServiceUnitZkUtils.path(suname));
    }

    public void disableOwnership(ServiceUnitId suName) throws Exception {
        String path = ServiceUnitZkUtils.path(suName);
        localZkCache.getZooKeeper().setData(path, jsonMapper.writeValueAsBytes(selfOwnerInfos[1]), -1);
        this.ownershipReadOnlyCache.invalidate(path);
    }
}
