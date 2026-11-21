/*
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

import static org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot.RACK_CONFIGURATION_SNAPSHOT;
import static org.apache.pulsar.metadata.bookkeeper.AbstractMetadataDriver.METADATA_STORE_SCHEME;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import lombok.CustomLog;
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
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * It provides the mapping of bookies to its rack from zookeeper.
 */
@CustomLog
public class BookieRackAffinityMapping extends AbstractDNSToSwitchMapping
        implements RackChangeNotifier {

    public static final String BOOKIE_INFO_ROOT_PATH = "/bookies";
    public static final String METADATA_STORE_INSTANCE = "METADATA_STORE_INSTANCE";
    /**
     * The configuration property of a {@code Consumer<Runnable>} that receives cleanup callbacks for metadata
     * resources allocated by the rack resolver and isolation policy. The BookKeeper client factory runs these
     * callbacks when it discards the client or closes.
     */
    public static final String METADATA_STORE_LISTENER_CLEANUP = "METADATA_STORE_LISTENER_CLEANUP";
    /** The configuration property of a {@code Consumer<BookieRackAffinityMapping>} for a shared resolver. */
    public static final String BOOKIE_RACK_AFFINITY_MAPPING_REGISTRATION = "bookieRackAffinityMappingRegistration";

    private final Object configurationLock = new Object();
    private volatile SharedRackConfiguration sharedRacks;
    private volatile ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy = null;
    private Set<BookieId> bookiesWithRack = new HashSet<>();

    private BookiesRackConfiguration racksWithHost = new BookiesRackConfiguration();
    private Map<String, BookieInfo> bookieInfoMap = new HashMap<>();
    private Runnable closeMetadataResources;

    static MetadataStore getMetadataStore(Configuration conf) throws MetadataException {
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
                                .metadataStoreName(MetadataStoreConfig.METADATA_STORE)
                                .sessionTimeoutMillis(zkTimeout)
                                .build());
            } catch (MetadataStoreException e) {
                throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
            }
        }
        return store;
    }

    @SuppressWarnings("unchecked")
    static void registerMetadataStoreCleanup(Configuration conf, Runnable cleanup) {
        if (conf.getProperty(METADATA_STORE_LISTENER_CLEANUP) instanceof Consumer<?> listenerCleanup) {
            ((Consumer<Runnable>) listenerCleanup).accept(cleanup);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        synchronized (configurationLock) {
            super.setConf(conf);
            if (closeMetadataResources != null) {
                closeMetadataResources.run();
            }
            if (conf.getProperty(RACK_CONFIGURATION_SNAPSHOT) instanceof BookieRackConfigurationSnapshot snapshot) {
                useSharedSnapshot(conf, snapshot);
            } else {
                sharedRacks = null;
                useLegacyMetadataCache(conf);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void useSharedSnapshot(Configuration conf, BookieRackConfigurationSnapshot snapshot) {
        var shared = new SharedRackConfiguration();
        sharedRacks = shared;
        Runnable unsubscribe = snapshot.subscribe(shared::update);
        Runnable unwatch;
        try {
            unwatch = watchAvailableBookies(() -> {
                var latest = snapshot.latest();
                if (latest != null) {
                    shared.update(latest);
                }
            });
        } catch (RuntimeException | Error e) {
            shared.close();
            unsubscribe.run();
            throw e;
        }
        closeMetadataResources = () -> {
            shared.close();
            try {
                unsubscribe.run();
            } finally {
                unwatch.run();
            }
        };
        registerMetadataStoreCleanup(conf, closeMetadataResources);
        if (conf.getProperty(BOOKIE_RACK_AFFINITY_MAPPING_REGISTRATION) instanceof Consumer<?> registration) {
            ((Consumer<BookieRackAffinityMapping>) registration).accept(this);
        }
    }

    /** Waits for the first shared configuration to reach BookKeeper's topology. */
    public CompletableFuture<Void> awaitInitialRackTopology(long timeoutMillis) {
        var shared = sharedRacks;
        if (shared == null) {
            return CompletableFuture.completedFuture(null);
        }
        return BookieRackConfigurationSnapshot.awaitInitialLoad(shared.initialTopologyApplied, timeoutMillis,
                "Timed out waiting for BookKeeper to apply the bookie rack configuration");
    }

    private void useLegacyMetadataCache(Configuration conf) {
        MetadataStore store;
        try {
            store = getMetadataStore(conf);
        } catch (MetadataException e) {
            throw new RuntimeException(METADATA_STORE_INSTANCE + " failed to init BookieId list");
        }

        MetadataCache<BookiesRackConfiguration> cache = store.getMetadataCache(BookiesRackConfiguration.class);
        Runnable unregister;
        try {
            unregister = store.registerCancellableListener(notification -> handleUpdates(cache, notification));
        } catch (RuntimeException e) {
            try {
                cache.close();
            } catch (RuntimeException closeError) {
                if (closeError != e) {
                    e.addSuppressed(closeError);
                }
            }
            throw e;
        }
        closeMetadataResources = () -> {
            try {
                unregister.run();
            } finally {
                cache.close();
            }
        };
        registerMetadataStoreCleanup(conf, closeMetadataResources);

        try {
            var racks = cache.get(BOOKIE_INFO_ROOT_PATH)
                    .thenApply(optRes -> optRes.orElseGet(BookiesRackConfiguration::new))
                    .get();
            updateRackConfiguration(racks);
        } catch (ExecutionException | InterruptedException e) {
            log.error().exception(e).log("Failed to update rack info");
            throw new RuntimeException(e);
        }

        watchAvailableBookies(() -> cache.get(BOOKIE_INFO_ROOT_PATH)
                .thenAccept(racks -> rackChangeListenerCallback(
                        updateRackConfiguration(racks.orElseGet(BookiesRackConfiguration::new))))
                .exceptionally(ex -> {
                    log.error().exception(ex).log("Failed to update rack info");
                    return null;
                }));
    }

    private Runnable watchAvailableBookies(Runnable onChange) {
        if (getBookieAddressResolver() instanceof DefaultBookieAddressResolver resolver) {
            RegistrationClient registrationClient = resolver.getRegistrationClient();
            RegistrationClient.RegistrationListener listener = versioned -> onChange.run();
            registrationClient.watchWritableBookies(listener);
            return () -> registrationClient.unwatchWritableBookies(listener);
        }
        return () -> { };
    }

    private synchronized Set<BookieId> updateRackConfiguration(Map<String, Map<String, BookieInfo>> racks) {
        // In config z-node, the bookies are added in the `ip:port` notation, while BK will ask
        // for just the IP/hostname when trying to get the rack for a bookie.
        // To work around this issue, we insert in the map the bookie ip/hostname with same rack-info
        BookiesRackConfiguration newRacksWithHost = new BookiesRackConfiguration();
        Map<String, BookieInfo> newBookieInfoMap = new HashMap<>();
        Set<BookieId> newBookiesWithRack = new HashSet<>();
        racks.forEach((group, bookies) ->
                bookies.forEach((addr, bi) -> {
                    try {
                        BookieId bookieId = BookieId.parse(addr);
                        newBookiesWithRack.add(bookieId);
                        BookieAddressResolver addressResolver = getBookieAddressResolver();
                        if (addressResolver == null) {
                            log.warn("Bookie address resolver not yet initialized, skipping resolution");
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
                                log.info().attr("address", addr).log("Network address for is unresolvable yet");
                            }
                        }
                    } catch (BookieAddressResolver.BookieIdNotResolvedException e) {
                        log.info()
                                .attr("address", addr)
                                .exceptionMessage(e)
                                .log("Network address is unresolvable yet");
                    }
                })
        );
        racksWithHost = newRacksWithHost;
        bookieInfoMap = newBookieInfoMap;
        Set<BookieId> affectedBookies = bookiesWithRack;
        bookiesWithRack = newBookiesWithRack;
        affectedBookies.addAll(newBookiesWithRack);
        return affectedBookies;
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

    private void handleUpdates(MetadataCache<BookiesRackConfiguration> cache, Notification n) {
        if (!n.getPath().equals(BOOKIE_INFO_ROOT_PATH)) {
            return;
        }

        cache.get(BOOKIE_INFO_ROOT_PATH).thenAccept(racks -> rackChangeListenerCallback(
                updateRackConfiguration(racks.orElseGet(BookiesRackConfiguration::new))));
    }

    private void rackChangeListenerCallback(Collection<BookieId> bookieIdSet) {
        if (rackawarePolicy != null) {
            rackawarePolicy.onBookieRackChange(new ArrayList<>(bookieIdSet));
        }
    }

    @Override
    public void registerRackChangeListener(ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy) {
        var shared = sharedRacks;
        if (shared == null) {
            this.rackawarePolicy = rackawarePolicy;
        } else {
            shared.registerRackChangeListener(rackawarePolicy);
        }
    }

    private final class SharedRackConfiguration {
        private final CompletableFuture<Void> initialTopologyApplied = new CompletableFuture<>();
        private long lastMappedVersion = -1;
        private boolean closed;

        private synchronized void update(BookieRackConfigurationSnapshot.VersionedConfiguration latest) {
            if (closed || latest.version() < lastMappedVersion) {
                return;
            }
            var affectedBookies = updateRackConfiguration(latest.configuration());
            // BookKeeper can resolve racks while holding its topology lock. Notify after releasing our mapping lock.
            rackChangeListenerCallback(affectedBookies);
            lastMappedVersion = latest.version();
            if (rackawarePolicy != null) {
                initialTopologyApplied.complete(null);
            }
        }

        private synchronized void registerRackChangeListener(
                ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy) {
            BookieRackAffinityMapping.this.rackawarePolicy = rackawarePolicy;
            if (!closed && lastMappedVersion >= 0) {
                // BookKeeper builds its initial topology from the existing mappings after registering the listener.
                initialTopologyApplied.complete(null);
            }
        }

        private synchronized void close() {
            closed = true;
            initialTopologyApplied.completeExceptionally(new IllegalStateException("Bookie rack resolver is closed"));
        }
    }
}
