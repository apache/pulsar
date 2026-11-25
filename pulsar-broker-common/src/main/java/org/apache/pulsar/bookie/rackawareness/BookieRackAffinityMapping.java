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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot.VersionedConfiguration;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
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
    private final Object configurationLock = new Object();
    private volatile ConfigurationBinding binding;
    private volatile ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy;
    private volatile RackMapping mapping = new RackMapping(Map.of(), Set.of());

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
            if (binding != null) {
                binding.closeForReconfiguration();
            }
            var next = new ConfigurationBinding();
            binding = next;
            try {
                if (conf.getProperty(RACK_CONFIGURATION_SNAPSHOT)
                        instanceof BookieRackConfigurationSnapshot snapshot) {
                    next.shared = true;
                    var subscription = snapshot.subscribe(next::updateShared);
                    next.unregister = subscription;
                    next.unwatch = watchAvailableBookies(subscription::reapplyOnBookieChange);
                } else {
                    useLegacyMetadataCache(conf, next);
                }
                registerMetadataStoreCleanup(conf, next::close);
            } catch (RuntimeException | Error e) {
                next.close();
                throw e;
            }
        }
    }

    private void useLegacyMetadataCache(Configuration conf, ConfigurationBinding next) {
        MetadataStore store;
        try {
            store = getMetadataStore(conf);
        } catch (MetadataException e) {
            throw new RuntimeException(METADATA_STORE_INSTANCE + " failed to initialize", e);
        }
        next.cache = store.getMetadataCache(BookiesRackConfiguration.class);
        Runnable refresh = () -> {
            var read = next.readLegacy();
            read.result().thenAccept(racks -> next.updateLegacy(read.sequence(), racks)).exceptionally(ex -> {
                log.error().exception(ex).log("Failed to update rack info");
                return null;
            });
        };
        next.unregister = store.registerCancellableListener(notification -> {
            if (BOOKIE_INFO_ROOT_PATH.equals(notification.getPath())) {
                refresh.run();
            }
        });
        try {
            var read = next.readLegacy();
            next.updateLegacy(read.sequence(), read.result().get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        next.unwatch = watchAvailableBookies(refresh);
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

    private RackMapping prepare(Map<String, Map<String, BookieInfo>> racks) {
        // DNS and bookie service-info resolution may be slow. Readers keep using the previous mapping.
        Map<String, String> byAddress = new HashMap<>();
        Set<BookieId> bookies = new HashSet<>();
        BookieAddressResolver resolver = getBookieAddressResolver();
        racks.values().forEach(group -> group.forEach((address, info) -> {
            BookieId bookie = BookieId.parse(address);
            bookies.add(bookie);
            if (resolver == null) {
                return;
            }
            try {
                BookieSocketAddress resolved = resolver.resolve(bookie);
                String rack = info.getRack();
                if (StringUtils.isEmpty(rack) || "/".equals(rack.trim())) {
                    rack = null;
                } else if (!rack.startsWith("/")) {
                    rack = "/" + rack;
                }
                byAddress.put(resolved.toString(), rack);
                var socket = resolved.getSocketAddress();
                byAddress.put(socket.getHostName(), rack);
                InetAddress ip = socket.getAddress();
                if (ip != null) {
                    byAddress.put(ip.getHostAddress(), rack);
                }
            } catch (BookieAddressResolver.BookieIdNotResolvedException e) {
                log.debug().attr("address", address).exceptionMessage(e).log("Bookie address is unavailable");
            }
        }));
        byAddress.values().removeIf(Objects::isNull);
        return new RackMapping(Map.copyOf(byAddress), Set.copyOf(bookies));
    }

    @Override
    public List<String> resolve(List<String> bookieAddressList) {
        var current = mapping;
        List<String> racks = new ArrayList<>(bookieAddressList.size());
        for (String address : bookieAddressList) {
            racks.add(current.byAddress().get(address));
        }
        return racks;
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
    public void registerRackChangeListener(ITopologyAwareEnsemblePlacementPolicy<BookieNode> policy) {
        synchronized (configurationLock) {
            if (binding == null) {
                rackawarePolicy = policy;
            } else {
                binding.register(policy);
            }
        }
    }

    private record RackMapping(Map<String, String> byAddress, Set<BookieId> bookies) {
    }

    private record LegacyRead(long sequence, CompletableFuture<Optional<BookiesRackConfiguration>> result) {
    }

    // Each configuration owns its callbacks. Retiring it also fences reads already in flight.
    private final class ConfigurationBinding {
        private final Object legacyReadLock = new Object();
        private long legacyReadSequence;
        private long appliedLegacyRead;
        private boolean shared;
        private VersionedConfiguration applied;
        private final AtomicBoolean closed = new AtomicBoolean();
        private Runnable unregister;
        private Runnable unwatch;
        private MetadataCache<BookiesRackConfiguration> cache;

        private LegacyRead readLegacy() {
            // Order read initiation without waiting for metadata or acquiring the application monitor.
            synchronized (legacyReadLock) {
                return new LegacyRead(++legacyReadSequence, cache.get(BOOKIE_INFO_ROOT_PATH));
            }
        }

        private synchronized void updateLegacy(long sequence, Optional<BookiesRackConfiguration> racks) {
            if (sequence < appliedLegacyRead) {
                return;
            }
            update(racks.orElseGet(BookiesRackConfiguration::new), null);
            appliedLegacyRead = sequence;
        }

        private void updateShared(VersionedConfiguration configuration) {
            update(configuration.configuration(), configuration);
        }

        private synchronized void update(Map<String, Map<String, BookieInfo>> racks, VersionedConfiguration revision) {
            if (closed.get() || (applied != null && revision.version() < applied.version())) {
                return;
            }
            var prepared = prepare(racks);
            if (closed.get()) {
                return;
            }
            Set<BookieId> affected = new HashSet<>(mapping.bookies());
            affected.addAll(prepared.bookies());
            var policy = rackawarePolicy;
            Runnable apply = () -> {
                mapping = prepared;
                if (policy != null) {
                    policy.onBookieRackChange(new ArrayList<>(affected));
                }
            };
            if (policy instanceof IsolatedBookieEnsemblePlacementPolicy isolated && shared) {
                isolated.applyRackConfiguration(revision, closed::get, apply);
            } else if (!closed.get()) {
                apply.run();
            }
            applied = revision;
        }

        private synchronized void register(ITopologyAwareEnsemblePlacementPolicy<BookieNode> policy) {
            if (closed.get()) {
                return;
            }
            rackawarePolicy = policy;
            if (shared && policy instanceof IsolatedBookieEnsemblePlacementPolicy isolated) {
                // The resolver may have synchronously applied its initial snapshot before BK registered the policy.
                isolated.applyRackConfiguration(applied, closed::get, () -> { });
            }
        }

        private synchronized void closeForReconfiguration() {
            // Finish applying the previous configuration before installing its replacement.
            close();
        }

        private void close() {
            // Client shutdown only retires this binding. A running resolver may be waiting on DNS or BookKeeper.
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                if (unregister != null) {
                    unregister.run();
                }
            } finally {
                try {
                    if (unwatch != null) {
                        unwatch.run();
                    }
                } finally {
                    if (cache != null) {
                        cache.close();
                    }
                }
            }
        }
    }
}
