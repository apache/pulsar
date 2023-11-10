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
package org.apache.pulsar.bookie.rackawareness;

import static org.apache.pulsar.metadata.bookkeeper.AbstractMetadataDriver.METADATA_STORE_SCHEME;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.DefaultBookieAddressResolver;
import org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackChangeNotifier;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.AbstractDNSToSwitchMapping;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It provides the mapping of bookies to its rack from zookeeper.
 */
public class BookieRackAffinityMapping extends AbstractDNSToSwitchMapping
        implements RackChangeNotifier {
    private static final Logger LOG = LoggerFactory.getLogger(BookieRackAffinityMapping.class);

    public static final String BOOKIE_INFO_ROOT_PATH = "/bookies";
    public static final String METADATA_STORE_INSTANCE = "METADATA_STORE_INSTANCE";

    private MetadataCache<BookiesRackConfiguration> bookieMappingCache = null;
    private ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy = null;
    private List<BookieId> bookieAddressListLastTime = new ArrayList<>();

    private BookiesRackConfiguration racksWithHost = new BookiesRackConfiguration();
    private Map<String, BookieInfo> bookieInfoMap = new HashMap<>();

    public static MetadataStore createMetadataStore(Configuration conf) throws MetadataException {
        MetadataStore store;
        Object storeProperty = conf.getProperty(METADATA_STORE_INSTANCE);
        if (storeProperty != null) {
            if (!(storeProperty instanceof MetadataStore)) {
                throw new RuntimeException(METADATA_STORE_INSTANCE + " is not an instance of MetadataStore");
            }
            store = (MetadataStore) storeProperty;
        } else {
            String url;
            String metadataServiceUri = ConfigurationStringUtil.castToString(conf.getProperty("metadataServiceUri"));
            if (StringUtils.isNotBlank(metadataServiceUri)) {
                try {
                    url = metadataServiceUri.replaceFirst(METADATA_STORE_SCHEME + ":", "")
                            .replace(";", ",");
                } catch (Exception e) {
                    throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
                }
            } else {
                String zkServers = ConfigurationStringUtil.castToString(conf.getProperty("zkServers"));
                if (StringUtils.isBlank(zkServers)) {
                    String errorMsg = String.format("Neither %s configuration set in the BK client configuration nor "
                            + "metadataServiceUri/zkServers set in bk server configuration", METADATA_STORE_INSTANCE);
                    throw new RuntimeException(errorMsg);
                }
                url = zkServers;
            }
            try {
                int zkTimeout = Integer.parseInt((String) conf.getProperty("zkTimeout"));
                store = MetadataStoreExtended.create(url,
                        MetadataStoreConfig.builder()
                                .sessionTimeoutMillis(zkTimeout)
                                .build());
            } catch (MetadataStoreException e) {
                throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
            }
        }
        return store;
    }

    @Override
    public synchronized void setConf(Configuration conf) {
        super.setConf(conf);
        MetadataStore store;
        try {
            store = createMetadataStore(conf);
            bookieMappingCache = store.getMetadataCache(BookiesRackConfiguration.class);
            store.registerListener(this::handleUpdates);
            racksWithHost = bookieMappingCache.get(BOOKIE_INFO_ROOT_PATH).get()
                    .orElseGet(BookiesRackConfiguration::new);
            updateRacksWithHost(racksWithHost);
            watchAvailableBookies();
            for (Map<String, BookieInfo> bookieMapping : racksWithHost.values()) {
                for (String address : bookieMapping.keySet()) {
                    bookieAddressListLastTime.add(BookieId.parse(address));
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("BookieRackAffinityMapping init, bookieAddressListLastTime {}",
                            bookieAddressListLastTime);
                }
            }
        } catch (InterruptedException | ExecutionException | MetadataException e) {
            throw new RuntimeException(METADATA_STORE_INSTANCE + " failed to init BookieId list");
        }
    }

    private void watchAvailableBookies() {
        BookieAddressResolver bookieAddressResolver = getBookieAddressResolver();
        if (bookieAddressResolver instanceof DefaultBookieAddressResolver) {
            try {
                Field field = DefaultBookieAddressResolver.class.getDeclaredField("registrationClient");
                field.setAccessible(true);
                RegistrationClient registrationClient = (RegistrationClient) field.get(bookieAddressResolver);
                registrationClient.watchWritableBookies(versioned -> {
                    try {
                        racksWithHost = bookieMappingCache.get(BOOKIE_INFO_ROOT_PATH).get()
                                .orElseGet(BookiesRackConfiguration::new);
                        updateRacksWithHost(racksWithHost);
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.error("Failed to update rack info. ", e);
                    }
                });
            } catch (NoSuchFieldException | IllegalAccessException e) {
                LOG.error("Failed watch available bookies.", e);
            }
        }
    }

    private synchronized void updateRacksWithHost(BookiesRackConfiguration racks) {
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

    @Override
    public synchronized List<String> resolve(List<String> bookieAddressList) {
        List<String> racks = new ArrayList<>(bookieAddressList.size());
        for (String bookieAddress : bookieAddressList) {
            racks.add(getRack(bookieAddress));
        }
        return racks;
    }

    private String getRack(String bookieAddress) {
        BookieInfo bi = bookieInfoMap.get(bookieAddress);
        if (bi == null) {
            bi = racksWithHost.getBookie(bookieAddress).orElse(null);
        }

        if (bi != null
                && !StringUtils.isEmpty(bi.getRack())
                && !bi.getRack().trim().equals("/")) {
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

    private void handleUpdates(Notification n) {
        if (!n.getPath().equals(BOOKIE_INFO_ROOT_PATH)) {
            return;
        }

        bookieMappingCache.get(BOOKIE_INFO_ROOT_PATH)
                .thenAccept(optVal -> {
                    Set<BookieId> bookieIdSet = new HashSet<>();
                    synchronized (this) {
                        LOG.info("Bookie rack info updated to {}. Notifying rackaware policy.", optVal);
                        this.updateRacksWithHost(optVal.orElseGet(BookiesRackConfiguration::new));
                        List<BookieId> bookieAddressList = new ArrayList<>();
                        for (Map<String, BookieInfo> bookieMapping : optVal.map(Map::values).orElse(
                                Collections.emptyList())) {
                            for (String addr : bookieMapping.keySet()) {
                                bookieAddressList.add(BookieId.parse(addr));
                            }
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Bookies with rack update from {} to {}", bookieAddressListLastTime,
                                    bookieAddressList);
                        }
                        bookieIdSet.addAll(bookieAddressList);
                        bookieIdSet.addAll(bookieAddressListLastTime);
                        bookieAddressListLastTime = bookieAddressList;
                    }
                    if (rackawarePolicy != null) {
                        rackawarePolicy.onBookieRackChange(new ArrayList<>(bookieIdSet));
                    }
                });
    }

    @Override
    public void registerRackChangeListener(ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy) {
        this.rackawarePolicy = rackawarePolicy;
    }
}
