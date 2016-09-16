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
package com.yahoo.pulsar.broker.cache;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.yahoo.pulsar.broker.cache.ConfigurationCacheService.POLICIES_ROOT;
import static com.yahoo.pulsar.broker.web.PulsarWebResource.joinPath;

import java.util.Optional;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.namespace.NamespaceEphemeralData;
import com.yahoo.pulsar.common.policies.data.LocalPolicies;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperChildrenCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

public class LocalZooKeeperCacheService {
    private static final Logger LOG = LoggerFactory.getLogger(LocalZooKeeperCacheService.class);

    private static final String MANAGED_LEDGER_ROOT = "/managed-ledgers";
    public static final String OWNER_INFO_ROOT = "/namespace";
    public static final String LOCAL_POLICIES_ROOT = "/admin/local-policies";

    private final ZooKeeperCache cache;

    private ZooKeeperDataCache<NamespaceEphemeralData> ownerInfoCache;
    private ZooKeeperChildrenCache managedLedgerListCache;
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
            public Optional<LocalPolicies> get(final String path) throws Exception {
                Optional<LocalPolicies> localPolicies = super.get(path);
                if (localPolicies.isPresent()) {
                    return localPolicies;
                } else {
                    // create new policies node under LocalZk by coping it from GlobalZk
                    createPolicies(path, true);
                    return super.get(path);
                }
            }
        };

        this.managedLedgerListCache = new ZooKeeperChildrenCache(cache, MANAGED_LEDGER_ROOT);
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
    public void createPolicies(String path, boolean readFromGlobal) throws Exception {

        try {
            checkNotNull(path, "path can't be null");
            checkArgument(path.startsWith(LOCAL_POLICIES_ROOT), "Invalid path of local policies");
            LocalPolicies localPolicies = new LocalPolicies();
            if (readFromGlobal) {
                String globalPath = joinPath(POLICIES_ROOT,
                        path.substring(path.indexOf(LOCAL_POLICIES_ROOT) + LOCAL_POLICIES_ROOT.length() + 1));
                Policies glbPolicies = configurationCacheService.policiesCache().get(globalPath)
                        .orElseThrow(() -> new IllegalStateException("Global policies not found at " + globalPath));
                localPolicies.bundles = glbPolicies.bundles;
            }
            ZooKeeper zk = cache.getZooKeeper();
            try {
                ZkUtils.createFullPathOptimistic(zk, path,
                        ObjectMapperFactory.getThreadLocal().writeValueAsBytes(localPolicies), Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Ok
            }
        } catch (Exception e) {
            LOG.error("Failed to create policies for {} in local zookeeper: {}", path, e.getMessage(), e);
            throw new PulsarServerException(e);
        }
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

    public ZooKeeperChildrenCache managedLedgerListCache() {
        return this.managedLedgerListCache;
    }
}
