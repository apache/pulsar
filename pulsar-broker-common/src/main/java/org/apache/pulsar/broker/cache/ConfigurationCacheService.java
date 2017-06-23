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
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
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

/**
 * ConfigurationCacheService maintains a local in-memory cache of all the configurations and policies stored in
 * zookeeper.
 */
public class ConfigurationCacheService {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationCacheService.class);

    private final ZooKeeperCache cache;
    private ZooKeeperDataCache<PropertyAdmin> propertiesCache;
    private ZooKeeperDataCache<Policies> policiesCache;
    private ZooKeeperDataCache<ClusterData> clustersCache;
    private ZooKeeperChildrenCache clustersListCache;
    private ZooKeeperDataCache<NamespaceIsolationPolicies> namespaceIsolationPoliciesCache;

    protected static final String POLICIES_ROOT = "/admin/policies";
    private static final String CLUSTERS_ROOT = "/admin/clusters";

    public ConfigurationCacheService(ZooKeeperCache cache) throws PulsarServerException {
        this.cache = cache;

        initZK();

        this.propertiesCache = new ZooKeeperDataCache<PropertyAdmin>(cache) {
            @Override
            public PropertyAdmin deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, PropertyAdmin.class);
            }
        };

        this.policiesCache = new ZooKeeperDataCache<Policies>(cache) {
            @Override
            public Policies deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, Policies.class);
            }
        };

        this.clustersCache = new ZooKeeperDataCache<ClusterData>(cache) {
            @Override
            public ClusterData deserialize(String path, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, ClusterData.class);
            }
        };

        this.clustersListCache = new ZooKeeperChildrenCache(cache, CLUSTERS_ROOT);

        this.namespaceIsolationPoliciesCache = new ZooKeeperDataCache<NamespaceIsolationPolicies>(cache) {
            @Override
            @SuppressWarnings("unchecked")
            public NamespaceIsolationPolicies deserialize(String path, byte[] content) throws Exception {
                return new NamespaceIsolationPolicies((Map<String, NamespaceIsolationData>) ObjectMapperFactory
                        .getThreadLocal().readValue(content, new TypeReference<Map<String, NamespaceIsolationData>>() {
                        }));
            }
        };
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

    public ZooKeeperDataCache<PropertyAdmin> propertiesCache() {
        return this.propertiesCache;
    }

    public ZooKeeperDataCache<Policies> policiesCache() {
        return this.policiesCache;
    }

    public ZooKeeperDataCache<ClusterData> clustersCache() {
        return this.clustersCache;
    }

    public ZooKeeperChildrenCache clustersListCache() {
        return this.clustersListCache;
    }

    public ZooKeeper getZooKeeper() {
        return this.cache.getZooKeeper();
    }

    public ZooKeeperDataCache<NamespaceIsolationPolicies> namespaceIsolationPoliciesCache() {
        return this.namespaceIsolationPoliciesCache;
    }
}
