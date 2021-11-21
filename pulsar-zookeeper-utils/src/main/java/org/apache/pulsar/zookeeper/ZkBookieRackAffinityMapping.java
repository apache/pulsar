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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackChangeNotifier;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.AbstractDNSToSwitchMapping;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.configuration.Configuration;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase.getZKServersFromServiceUri;

/**
 * It provides the mapping of bookies to its rack from zookeeper.
 */
public class ZkBookieRackAffinityMapping extends AbstractDNSToSwitchMapping
        implements ZooKeeperCacheListener<BookiesRackConfiguration>, RackChangeNotifier {
    private static final Logger LOG = LoggerFactory.getLogger(ZkBookieRackAffinityMapping.class);

    public static final String BOOKIE_INFO_ROOT_PATH = "/bookies";
    public static final String ZK_DATA_CACHE_BK_RACK_CONF_INSTANCE = "zk_data_cache_bk_rack_conf_instance";
    private static final String ZK_LEDGERS_DEFAULT_ROOT_PATH = "/ledgers";

    private ZooKeeperDataCache<BookiesRackConfiguration> bookieMappingCache = null;
    private ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy = null;

    private static final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    private volatile BookiesRackConfiguration racksWithHost = new BookiesRackConfiguration();
    private volatile Map<String, BookieInfo> bookieInfoMap = new HashMap<>();

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf.getProperty(ZK_DATA_CACHE_BK_RACK_CONF_INSTANCE) != null) {
            bookieMappingCache = (ZooKeeperDataCache<BookiesRackConfiguration>) conf.getProperty(ZK_DATA_CACHE_BK_RACK_CONF_INSTANCE);
            bookieMappingCache.registerListener(this);
        } else {
            bookieMappingCache = getAndSetZkCache(conf);
            conf.setProperty(ZK_DATA_CACHE_BK_RACK_CONF_INSTANCE, bookieMappingCache);
        }

        // A previous version of this code tried to eagerly load the cache. However, this is invalid
        // in later versions of bookkeeper as when setConf is called, the bookieAddressResolver is not yet set
    }

    private void updateRacksWithHost(BookiesRackConfiguration racks) {
        // In config z-node, the bookies are added in the `ip:port` notation, while BK will ask
        // for just the IP/hostname when trying to get the rack for a bookie.
        // To work around this issue, we insert in the map the bookie ip/hostname with same rack-info
        BookiesRackConfiguration newRacksWithHost = new BookiesRackConfiguration();
        Map<String, BookieInfo> newBookieInfoMap = new HashMap<>();
        racks.forEach((group, bookies) ->
                bookies.forEach((addr, bi) -> {
                    try {
                        BookieId bookieId = BookieId.parse(addr);
                        BookieAddressResolver addressResolver = getBookieAddressResolver();
                        if (addressResolver == null) {
                            LOG.warn("Bookie address resolver not yet initialized, skipping resolution");
                        } else {
                            BookieSocketAddress bsa = addressResolver.resolve(bookieId);
                            newRacksWithHost.updateBookie(group, bsa.toString(), bi);

                            String hostname = bsa.getSocketAddress().getHostName();
                            newBookieInfoMap.put(hostname, bi);

                            InetAddress address = bsa.getSocketAddress().getAddress();
                            if (null != address) {
                                String hostIp = address.getHostAddress();
                                if (null != hostIp) {
                                    newBookieInfoMap.put(hostIp, bi);
                                }
                            } else {
                                LOG.info("Network address for {} is unresolvable yet.", addr);
                            }
                        }
                    } catch (BookieAddressResolver.BookieIdNotResolvedException e) {
                        LOG.info("Network address for {} is unresolvable yet. error is {}", addr, e);
                    }
                })
        );
        racksWithHost = newRacksWithHost;
        bookieInfoMap = newBookieInfoMap;
    }

    private ZooKeeperDataCache<BookiesRackConfiguration> getAndSetZkCache(Configuration conf) {
        ZooKeeperCache zkCache = null;
        if (conf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE) != null) {
            zkCache = (ZooKeeperCache) conf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE);
        } else {
            if (conf instanceof ClientConfiguration) {
                int zkTimeout = ((ClientConfiguration) conf).getZkTimeout();
                try {
                    final String metadataServiceUriStr = ((ClientConfiguration) conf).getMetadataServiceUri();
                    URI metadataServiceUri = URI.create(metadataServiceUriStr);
                    String ledgersRootPath = metadataServiceUri.getPath();
                    String zkServers;
                    if (ZK_LEDGERS_DEFAULT_ROOT_PATH.equals(ledgersRootPath)) {
                        zkServers = getZKServersFromServiceUri(metadataServiceUri);
                    } else {
                        int zkLedgerRootIndex = ledgersRootPath.lastIndexOf("/");
                        zkServers = getZKServersFromServiceUri(metadataServiceUri) + ledgersRootPath.substring(0, zkLedgerRootIndex);
                    }
                    ZooKeeper zkClient = ZooKeeperClient.newBuilder().connectString(zkServers)
                            .sessionTimeoutMs(zkTimeout).build();
                    zkCache = new ZooKeeperCache("bookies-racks", zkClient,
                            (int) TimeUnit.MILLISECONDS.toSeconds(zkTimeout)) {
                    };
                    conf.addProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, zkCache);
                } catch (Exception e) {
                    LOG.error("Error creating zookeeper client", e);
                }
            } else {
                LOG.error("No zk configurations available");
            }
        }
        ZooKeeperDataCache<BookiesRackConfiguration> zkDataCache = getZkBookieRackMappingCache(
                zkCache);
        zkDataCache.registerListener(this);
        return zkDataCache;
    }

    private ZooKeeperDataCache<BookiesRackConfiguration> getZkBookieRackMappingCache(
            ZooKeeperCache zkCache) {
        return new ZooKeeperDataCache<BookiesRackConfiguration>(
                zkCache) {

            @Override
            public BookiesRackConfiguration deserialize(String key, byte[] content)
                    throws Exception {
                LOG.info("Reloading the bookie rack affinity mapping cache.");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Loading the bookie mappings with bookie info data: {}", new String(content));
                }
                BookiesRackConfiguration racks = jsonMapper.readValue(content, BookiesRackConfiguration.class);
                return racks;
            }

        };
    }

    @Override
    public List<String> resolve(List<String> bookieAddressList) {
        List<String> racks = new ArrayList<>(bookieAddressList.size());
        for (String bookieAddress : bookieAddressList) {
            racks.add(getRack(bookieAddress));
        }
        return racks;
    }

    private String getRack(String bookieAddress) {
        try {
            // Trigger load of z-node in case it didn't exist
            Optional<BookiesRackConfiguration> racks = bookieMappingCache.get(BOOKIE_INFO_ROOT_PATH);
            updateRacksWithHost(racks.orElseGet(BookiesRackConfiguration::new));
            if (!racks.isPresent()) {
                // since different placement policy will have different default rack,
                // don't be smart here and just return null
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        BookieInfo bi = bookieInfoMap.get(bookieAddress);
        if (bi == null) {
            Optional<BookieInfo> biOpt = racksWithHost.getBookie(bookieAddress);
            if (biOpt.isPresent()) {
                bi = biOpt.get();
            } else {
                updateRacksWithHost(racksWithHost);
                bi = bookieInfoMap.get(bookieAddress);
            }
        }

        if (bi != null) {
            String rack = bi.getRack();
            if (!rack.startsWith("/")) {
                rack = "/" + rack;
            }
            return rack;
        } else {
            // since different placement policy will have different default rack,
            // don't be smart here and just return null
            return null;
        }
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
    public void onUpdate(String path, BookiesRackConfiguration data, Stat stat) {
        if (rackawarePolicy != null) {
            LOG.info("Bookie rack info updated to {}. Notifying rackaware policy.", data.toString());
            List<BookieId> bookieAddressList = new ArrayList<>();
            for (Map<String, BookieInfo> bookieMapping : data.values()) {
                for (String addr : bookieMapping.keySet()) {
                    bookieAddressList.add(BookieId.parse(addr));
                }
            }
            rackawarePolicy.onBookieRackChange(bookieAddressList);
        }
    }

    @Override
    public void registerRackChangeListener(ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy) {
        this.rackawarePolicy = rackawarePolicy;
    }
}
