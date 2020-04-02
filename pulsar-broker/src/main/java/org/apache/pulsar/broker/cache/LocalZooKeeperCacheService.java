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
package org.apache.pulsar.broker.cache;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES_ROOT;
import static org.apache.pulsar.broker.web.PulsarWebResource.joinPath;

import com.google.common.collect.Maps;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperManagedLedgerCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalZooKeeperCacheService {
    private static final Logger LOG = LoggerFactory.getLogger(LocalZooKeeperCacheService.class);

    private static final String MANAGED_LEDGER_ROOT = "/managed-ledgers";
    public static final String OWNER_INFO_ROOT = "/namespace";
    public static final String LOCAL_POLICIES_ROOT = "/admin/local-policies";

    private final ZooKeeperCache cache;

    private ZooKeeperDataCache<NamespaceEphemeralData> ownerInfoCache;
    private ZooKeeperManagedLedgerCache managedLedgerListCache;
    private ResourceQuotaCache resourceQuotaCache;
    private ZooKeeperDataCache<LocalPolicies> policiesCache;

    private ConfigurationCacheService configurationCacheService;

    public LocalZooKeeperCacheService(ZooKeeperCache cache, ConfigurationCacheService configurationCacheService)
            throws PulsarServerException {
        this.cache = cache;
        this.configurationCacheService = configurationCacheService;

        initZK();

        this.ownerInfoCache = new ZooKeeperDataCache<NamespaceEphemeralData>(cache) {
            @Override
            public NamespaceEphemeralData deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, NamespaceEphemeralData.class);
            }
        };

        this.policiesCache = new ZooKeeperDataCache<LocalPolicies>(cache) {
            @Override
            public LocalPolicies deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, LocalPolicies.class);
            }

            @Override
            public CompletableFuture<Optional<LocalPolicies>> getAsync(String path) {
                return getWithStatAsync(path).thenApply(entry -> entry.map(e -> e.getKey()));
            }

            @Override
            public CompletableFuture<Optional<Entry<LocalPolicies, Stat>>> getWithStatAsync(String path) {
                CompletableFuture<Optional<Entry<LocalPolicies, Stat>>> future = new CompletableFuture<>();

                // First check in local-zk cache
                super.getWithStatAsync(path).thenAccept(result -> {
                    Optional<LocalPolicies> localPolicies = result.map(Entry::getKey);
                    if (localPolicies.isPresent()) {
                        future.complete(result);
                    } else {
                        // create new policies node under Local ZK by coping it from Global ZK
                        createPolicies(path, true).thenAccept(p -> {
                            LOG.info("Successfully created local policies for {} -- {}", path, p);
                            // local-policies have been created but it's not part of policiesCache. so, call
                            // super.getAsync() which will load it and set the watch on local-policies path
                            super.getWithStatAsync(path);
                            Stat stat = new Stat();
                            stat.setVersion(-1);
                            future.complete(Optional.of(Maps.immutableEntry(p.orElse(null), stat)));
                        }).exceptionally(ex -> {
                            future.completeExceptionally(ex);
                            return null;
                        });
                    }
                }).exceptionally(ex -> {
                    future.completeExceptionally(ex);
                    return null;
                });

                return future;
            }
        };

        this.managedLedgerListCache = new ZooKeeperManagedLedgerCache(cache, MANAGED_LEDGER_ROOT);
        this.resourceQuotaCache = new ResourceQuotaCache(cache);
        this.resourceQuotaCache.initZK();
    }

    private void initZK() throws PulsarServerException {
        String[] paths = new String[] { MANAGED_LEDGER_ROOT, OWNER_INFO_ROOT, LOCAL_POLICIES_ROOT };
        // initialize the zk client with values
        try {
            ZooKeeper zk = cache.getZooKeeper();
            for (String path : paths) {
                if (cache.exists(path)) {
                    continue;
                }

                try {
                    ZkUtils.createFullPathOptimistic(zk, path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    // Ok
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        }

    }

    /**
     * Create LocalPolicies with bundle-data in LocalZookeeper by fetching it from GlobalZookeeper
     *
     * @param path
     *            znode path
     * @param readFromGlobal
     *            if true copy policies from global zk to local zk else create a new znode with empty {@link Policies}
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    public CompletableFuture<Optional<LocalPolicies>> createPolicies(String path, boolean readFromGlobal) {
        CompletableFuture<Optional<LocalPolicies>> future = new CompletableFuture<>();
        if (path == null || !path.startsWith(LOCAL_POLICIES_ROOT)) {
            future.completeExceptionally(new IllegalArgumentException("Invalid path of local policies " + path));
            return future;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating local namespace policies for {} - readFromGlobal: {}", path, readFromGlobal);
        }

        CompletableFuture<Optional<LocalPolicies>> readFromGlobalFuture = new CompletableFuture<>();

        if (readFromGlobal) {
            String globalPath = joinPath(POLICIES_ROOT,
                    path.substring(path.indexOf(LOCAL_POLICIES_ROOT) + LOCAL_POLICIES_ROOT.length() + 1));
            checkNotNull(configurationCacheService);
            checkNotNull(configurationCacheService.policiesCache());
            checkNotNull(configurationCacheService.policiesCache().getAsync(globalPath));
            configurationCacheService.policiesCache().getAsync(globalPath).thenAccept(policies -> {
                if (policies.isPresent()) {
                    // Copying global bundles information to local policies
                    LocalPolicies localPolicies = new LocalPolicies();
                    localPolicies.bundles = policies.get().bundles;
                    readFromGlobalFuture.complete(Optional.of(localPolicies));
                } else {
                    // Policies are not present in global zk
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Global policies not found at {}", globalPath);
                    }
                    readFromGlobalFuture.complete(Optional.empty());
                }
            }).exceptionally(ex -> {
                future.completeExceptionally(ex);
                return null;
            });
        } else {
            // Use default local policies
            readFromGlobalFuture.complete(Optional.of(new LocalPolicies()));
        }

        readFromGlobalFuture.thenAccept(localPolicies -> {
            if (!localPolicies.isPresent()) {
                future.complete(Optional.empty());
            }

            // When we have the updated localPolicies, we can write them back in local ZK
            byte[] content;
            try {
                content = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(localPolicies.get());
            } catch (Throwable t) {
                // Failed to serialize to json
                future.completeExceptionally(t);
                return;
            }

            ZkUtils.asyncCreateFullPathOptimistic(cache.getZooKeeper(), path, content, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT, (rc, path1, ctx, name) -> {
                        if (rc == KeeperException.Code.OK.intValue()
                                || rc == KeeperException.Code.NODEEXISTS.intValue()) {
                            LOG.info("Successfully copyied bundles data to local zk at {}", path);
                            future.complete(localPolicies);
                        } else {
                            LOG.error("Failed to create policies for {} in local zookeeper: {}", path,
                                    KeeperException.Code.get(rc));
                            future.completeExceptionally(new PulsarServerException(KeeperException.create(rc)));
                        }
                    }, null);
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });

        return future;
    }

    public ResourceQuotaCache getResourceQuotaCache() {
        return this.resourceQuotaCache;
    }

    public ZooKeeperDataCache<NamespaceEphemeralData> ownerInfoCache() {
        return this.ownerInfoCache;
    }

    public ZooKeeperDataCache<LocalPolicies> policiesCache() {
        return this.policiesCache;
    }

    public ZooKeeperManagedLedgerCache managedLedgerListCache() {
        return this.managedLedgerListCache;
    }

    public CompletableFuture<Boolean> managedLedgerExists(String persistentPath) {
        return cache.existsAsync(MANAGED_LEDGER_ROOT + "/" + persistentPath, cache);
    }
}
