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

import java.util.Map;

import org.apache.bookkeeper.util.ZkUtils;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import lombok.Getter;

/**
 * ConfigurationCacheService maintains a local in-memory cache of all the configurations and policies stored in
 * zookeeper.
 */
public class ConfigurationCacheService {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationCacheService.class);

    private final ZooKeeperCache cache;
    private ZooKeeperDataCache<TenantInfoImpl> propertiesCache;
    private ZooKeeperDataCache<Policies> policiesCache;
    private ZooKeeperDataCache<ClusterDataImpl> clustersCache;
    private ZooKeeperChildrenCache clustersListCache;
    private ZooKeeperChildrenCache failureDomainListCache;
    private ZooKeeperDataCache<NamespaceIsolationPolicies> namespaceIsolationPoliciesCache;
    private ZooKeeperDataCache<FailureDomainImpl> failureDomainCache;
    @Getter
    private PulsarResources pulsarResources;

    public static final String POLICIES = "policies";
    public static final String RESOURCEGROUPS = "resourcegroups";
    public static final String FAILURE_DOMAIN = "failureDomain";
    public final String CLUSTER_FAILURE_DOMAIN_ROOT;
    public static final String POLICIES_ROOT = "/admin/policies";
    private static final String CLUSTERS_ROOT = "/admin/clusters";

    public static final String PARTITIONED_TOPICS_ROOT = "/admin/partitioned-topics";

    public ConfigurationCacheService(ZooKeeperCache cache, String configuredClusterName) throws PulsarServerException {
        this(cache, configuredClusterName, null);
    }

    public ConfigurationCacheService(ZooKeeperCache cache, String configuredClusterName,
            PulsarResources pulsarResources) throws PulsarServerException {
        this.cache = cache;
        this.pulsarResources = pulsarResources;
        this.CLUSTER_FAILURE_DOMAIN_ROOT = CLUSTERS_ROOT + "/" + configuredClusterName + "/" + FAILURE_DOMAIN;
        if (cache == null) {
            return;
        }

        initZK();

        this.propertiesCache = new ZooKeeperDataCache<TenantInfoImpl>(cache) {
            @Override
            public TenantInfoImpl deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, TenantInfoImpl.class);
            }
        };

        this.policiesCache = new ZooKeeperDataCache<Policies>(cache) {
            @Override
            public Policies deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, Policies.class);
            }
        };

        this.clustersCache = new ZooKeeperDataCache<ClusterDataImpl>(cache) {
            @Override
            public ClusterDataImpl deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, ClusterDataImpl.class);
            }
        };

        this.clustersListCache = new ZooKeeperChildrenCache(cache, CLUSTERS_ROOT);

        if (isNotBlank(configuredClusterName)) {
            createFailureDomainRoot(cache.getZooKeeper(), CLUSTER_FAILURE_DOMAIN_ROOT);
            this.failureDomainListCache = new ZooKeeperChildrenCache(cache, CLUSTER_FAILURE_DOMAIN_ROOT);
        }

        this.namespaceIsolationPoliciesCache = new ZooKeeperDataCache<NamespaceIsolationPolicies>(cache) {
            @Override
            @SuppressWarnings("unchecked")
            public NamespaceIsolationPolicies deserialize(String path, byte[] content) throws Exception {
                return new NamespaceIsolationPolicies(ObjectMapperFactory
                        .getThreadLocal().readValue(content, new TypeReference<Map<String, NamespaceIsolationDataImpl>>() {
                        }));
            }
        };

        this.failureDomainCache = new ZooKeeperDataCache<FailureDomainImpl>(cache) {
            @Override
            public FailureDomainImpl deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, FailureDomainImpl.class);
            }
        };
    }

    private void createFailureDomainRoot(ZooKeeper zk, String path) {
        try {
            final int index = path.lastIndexOf('/');
            final String clusterZnodePath = (index > 0) ? path.substring(0, index) : null;
            if (zk.exists(clusterZnodePath, false) != null && zk.exists(path, false) == null) {
                try {
                    byte[] data = "".getBytes();
                    ZkUtils.createFullPathOptimistic(zk, path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOG.info("Successfully created failure-domain znode at {}", path);
                } catch (KeeperException.NodeExistsException e) {
                    // Ok
                }
            }
        } catch (KeeperException.NodeExistsException e) {
            // Ok
        } catch (Exception e) {
            LOG.warn("Failed to create failure-domain znode {} ", path, e);
        }
    }

    private void initZK() throws PulsarServerException {

        String[] paths = new String[] { CLUSTERS_ROOT, POLICIES_ROOT };

        // initialize the zk client with values
        try {
            ZooKeeper zk = cache.getZooKeeper();
            for (String path : paths) {
                try {
                    if (zk.exists(path, false) == null) {
                        ZkUtils.createFullPathOptimistic(zk, path, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    }
                } catch (KeeperException.NodeExistsException e) {
                    // Ok
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        }

    }

    public ZooKeeperCache cache() {
        return cache;
    }

    public ZooKeeperDataCache<TenantInfoImpl> propertiesCache() {
        return this.propertiesCache;
    }

    public ZooKeeperDataCache<Policies> policiesCache() {
        return this.policiesCache;
    }

    public ZooKeeperDataCache<ClusterDataImpl> clustersCache() {
        return this.clustersCache;
    }

    public ZooKeeperChildrenCache clustersListCache() {
        return this.clustersListCache;
    }

    public ZooKeeperChildrenCache failureDomainListCache() {
        return this.failureDomainListCache;
    }

    public ZooKeeper getZooKeeper() {
        return this.cache.getZooKeeper();
    }

    public ZooKeeperDataCache<NamespaceIsolationPolicies> namespaceIsolationPoliciesCache() {
        return this.namespaceIsolationPoliciesCache;
    }

    public ZooKeeperDataCache<FailureDomainImpl> failureDomainCache() {
        return this.failureDomainCache;
    }
}
