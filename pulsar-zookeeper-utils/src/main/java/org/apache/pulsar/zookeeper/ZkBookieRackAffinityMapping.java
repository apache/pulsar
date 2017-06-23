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
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.RackChangeNotifier;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.AbstractDNSToSwitchMapping;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.commons.configuration.Configuration;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * It provides the mapping of bookies to its rack from zookeeper.
 */
public class ZkBookieRackAffinityMapping extends AbstractDNSToSwitchMapping
        implements ZooKeeperCacheListener<Map<String, Map<BookieSocketAddress, BookieInfo>>>, RackChangeNotifier {
    private static final Logger LOG = LoggerFactory.getLogger(ZkBookieRackAffinityMapping.class);

    public static final String BOOKIE_INFO_ROOT_PATH = "/bookies";

    private ZooKeeperDataCache<Map<String, Map<BookieSocketAddress, BookieInfo>>> bookieMappingCache = null;
    private RackawareEnsemblePlacementPolicy rackawarePolicy = null;

    public static final ObjectMapper jsonMapper = ObjectMapperFactory.create();
    public static final TypeReference<Map<String, Map<BookieSocketAddress, BookieInfo>>> typeRef = new TypeReference<Map<String, Map<BookieSocketAddress, BookieInfo>>>() {
    };

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        bookieMappingCache = getAndSetZkCache(conf);
    }

    private ZooKeeperDataCache<Map<String, Map<BookieSocketAddress, BookieInfo>>> getAndSetZkCache(Configuration conf) {
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
        ZooKeeperDataCache<Map<String, Map<BookieSocketAddress, BookieInfo>>> zkDataCache = getZkBookieRackMappingCache(
                zkCache);
        if (zkDataCache != null) {
            zkDataCache.registerListener(this);
        }
        return zkDataCache;
    }

    public static ZooKeeperDataCache<Map<String, Map<BookieSocketAddress, BookieInfo>>> getZkBookieRackMappingCache(
            ZooKeeperCache zkCache) {
        ZooKeeperDataCache<Map<String, Map<BookieSocketAddress, BookieInfo>>> zkDataCache = new ZooKeeperDataCache<Map<String, Map<BookieSocketAddress, BookieInfo>>>(
                zkCache) {

            @Override
            public Map<String, Map<BookieSocketAddress, BookieInfo>> deserialize(String key, byte[] content)
                    throws Exception {
                LOG.info("Reloading the bookie rack affinity mapping cache.");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Loading the bookie mappings with bookie info data: {}", new String(content));
                }
                return jsonMapper.readValue(content, typeRef);
            }

        };
        return zkDataCache;
    }

    @Override
    public List<String> resolve(List<BookieSocketAddress> bookieAddressList) {
        List<String> racks = new ArrayList<String>(bookieAddressList.size());
        for (BookieSocketAddress bookieAddress : bookieAddressList) {
            racks.add(getRack(bookieAddress));
        }
        return racks;
    }

    private String getRack(BookieSocketAddress bookieAddress) {
        String rack = NetworkTopology.DEFAULT_RACK;
        try {
            if (bookieMappingCache != null) {
                Map<String, Map<BookieSocketAddress, BookieInfo>> allGroupsBookieMapping = bookieMappingCache
                        .get(BOOKIE_INFO_ROOT_PATH)
                        .orElseThrow(() -> new KeeperException.NoNodeException(BOOKIE_INFO_ROOT_PATH));
                for (Map<BookieSocketAddress, BookieInfo> bookieMapping : allGroupsBookieMapping.values()) {
                    BookieInfo bookieInfo = bookieMapping.get(bookieAddress);
                    if (bookieInfo != null) {
                        rack = bookieInfo.getRack();
                        if (!rack.startsWith("/")) {
                            rack = "/" + rack;
                        }
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Error getting bookie info from zk, using default rack node {}: {}", rack, e.getMessage());
        }
        return rack;
    }

    @Override
    public String toString() {
        return "zk based bookie rack affinity mapping";
    }

    @Override
    public void reloadCachedMappings() {
        // no-op
    }

    @Override
    public void onUpdate(String path, Map<String, Map<BookieSocketAddress, BookieInfo>> data, Stat stat) {
        if (rackawarePolicy != null) {
            LOG.info("Bookie rack info updated to {}. Notifying rackaware policy.", data.toString());
            List<BookieSocketAddress> bookieAddressList = new ArrayList<>();
            for (Map<BookieSocketAddress, BookieInfo> bookieMapping : data.values()) {
                bookieAddressList.addAll(bookieMapping.keySet());
            }
            rackawarePolicy.onBookieRackChange(bookieAddressList);
        }
    }

    @Override
    public void registerRackChangeListener(RackawareEnsemblePlacementPolicy rackawarePolicy) {
        this.rackawarePolicy = rackawarePolicy;

    }
}
