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
package org.apache.pulsar.zookeeper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.commons.configuration.Configuration;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ZkIsolatedBookieEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicy
        implements Deserializer<Map<String, Map<BookieSocketAddress, BookieInfo>>> {
    private static final Logger LOG = LoggerFactory.getLogger(ZkIsolatedBookieEnsemblePlacementPolicy.class);

    public static final String ISOLATION_BOOKIE_GROUPS = "isolationBookieGroups";

    private ZooKeeperCache bookieMappingCache = null;

    private final List<String> isolationGroups = new ArrayList<String>();
    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();
    private final TypeReference<Map<String, Map<BookieSocketAddress, BookieInfo>>> typeRef = new TypeReference<Map<String, Map<BookieSocketAddress, BookieInfo>>>() {
    };

    public ZkIsolatedBookieEnsemblePlacementPolicy() {
        super();
    }

    @Override
    public EnsemblePlacementPolicy initialize(Configuration conf) {
        if (conf.getProperty(ISOLATION_BOOKIE_GROUPS) != null) {
            String isolationGroupsString = (String) conf.getProperty(ISOLATION_BOOKIE_GROUPS);
            if (!isolationGroupsString.isEmpty()) {
                for (String isolationGroup : isolationGroupsString.split(",")) {
                    isolationGroups.add(isolationGroup);
                }
                bookieMappingCache = getAndSetZkCache(conf);
            }
        }
        super.initialize(conf);
        return this;
    }

    private ZooKeeperCache getAndSetZkCache(Configuration conf) {
        ZooKeeperCache zkCache = null;
        if (conf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE) != null) {
            zkCache = (ZooKeeperCache) conf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE);
        } else {
            int zkTimeout;
            String zkServers;
            if (conf instanceof ClientConfiguration) {
                zkTimeout = ((ClientConfiguration) conf).getZkTimeout();
                zkServers = ((ClientConfiguration) conf).getZkServers();
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(zkTimeout) {
                };
                try {
                    ZooKeeper zkClient = ZkUtils.createConnectedZookeeperClient(zkServers, w);
                    zkCache = new ZooKeeperCache(zkClient) {
                    };
                    conf.addProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, zkCache);
                } catch (Exception e) {
                    LOG.error("Error creating zookeeper client", e);
                }
            } else {
                LOG.error("No zk configurations available");
            }
        }
        return zkCache;
    }

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize,
            Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        Set<BookieSocketAddress> blacklistedBookies = getBlacklistedBookies();
        if (excludeBookies == null) {
            excludeBookies = new HashSet<BookieSocketAddress>();
        }
        excludeBookies.addAll(blacklistedBookies);
        return super.newEnsemble(ensembleSize, writeQuorumSize, excludeBookies);
    }

    @Override
    public BookieSocketAddress replaceBookie(BookieSocketAddress bookieToReplace,
            Set<BookieSocketAddress> currentEnsemble, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        Set<BookieSocketAddress> blacklistedBookies = getBlacklistedBookies();
        if (excludeBookies == null) {
            excludeBookies = new HashSet<BookieSocketAddress>();
        }
        excludeBookies.addAll(blacklistedBookies);
        return super.replaceBookie(bookieToReplace, currentEnsemble, excludeBookies);
    }

    private Set<BookieSocketAddress> getBlacklistedBookies() {
        Set<BookieSocketAddress> blacklistedBookies = new HashSet<BookieSocketAddress>();
        try {
            if (bookieMappingCache != null) {
                Map<String, Map<BookieSocketAddress, BookieInfo>> allGroupsBookieMapping = bookieMappingCache
                        .getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, this)
                        .orElseThrow(() -> new KeeperException.NoNodeException(
                                ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH));
                for (String group : allGroupsBookieMapping.keySet()) {
                    if (!isolationGroups.contains(group)) {
                        for (BookieSocketAddress bookieAddress : allGroupsBookieMapping.get(group).keySet()) {
                            blacklistedBookies.add(bookieAddress);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Error getting bookie isolation info from zk: {}", e.getMessage());
        }
        return blacklistedBookies;
    }

    @Override
    public Map<String, Map<BookieSocketAddress, BookieInfo>> deserialize(String key, byte[] content) throws Exception {
        LOG.info("Reloading the bookie isolation groups mapping cache.");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading the bookie mappings with bookie info data: {}", new String(content));
        }
        return jsonMapper.readValue(content, typeRef);
    }
}
