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
package org.apache.pulsar.broker;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_VALIDATION;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_MINIMUM_REGIONS_FOR_DURABILITY;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_REGIONS_TO_WRITE;
import static org.apache.bookkeeper.net.CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY;
import static org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
import static org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot.RACK_CONFIGURATION_SNAPSHOT;
import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperClientConfigAccessor;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping;
import org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.client.internal.PropertiesUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.bookkeeper.AbstractMetadataDriver;
import org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver;

@CustomLog
public class BookKeeperClientFactoryImpl implements BookKeeperClientFactory {
    // Snapshots span all placement policies using the same metadata store. The factory owns these and the cleanup
    // registrations for DNS resolver resources, which BookKeeper itself does not close. Guarded by this.
    private final Map<MetadataStore, BookieRackConfigurationSnapshot> rackSnapshots = new IdentityHashMap<>();
    private final Set<ClientResources> clientResources = new HashSet<>();
    private final Set<CompletableFuture<BookKeeper>> pendingCreations = new HashSet<>();
    private boolean closed;

    @Override
    public CompletableFuture<BookKeeper> create(ServiceConfiguration conf, MetadataStoreExtended store,
                                                EventLoopGroup eventLoopGroup,
                                                Optional<Class<? extends EnsemblePlacementPolicy>> policyClass,
                                                Map<String, Object> properties) {
        return create(conf, store, eventLoopGroup, policyClass, properties,
                NullStatsLogger.INSTANCE);
    }

    @Override
    public CompletableFuture<BookKeeper> create(ServiceConfiguration conf, MetadataStoreExtended store,
                             EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties, StatsLogger statsLogger) {
        return FutureUtil.supplySafely(() -> createClient(conf, store, eventLoopGroup, ensemblePlacementPolicyClass,
                properties, statsLogger));
    }

    private CompletableFuture<BookKeeper> createClient(ServiceConfiguration conf, MetadataStoreExtended store,
            EventLoopGroup eventLoopGroup, Optional<Class<? extends EnsemblePlacementPolicy>> policyClass,
            Map<String, Object> properties, StatsLogger statsLogger) {
        PulsarMetadataClientDriver.init();

        ClientConfiguration bkConf = createBkClientConfiguration(store, conf);
        if (properties != null) {
            // The isolation groups of a namespace reach the placement policy through the custom metadata of each
            // ledger. They must not become the client's default groups: the client is shared and kept after the
            // namespace bookie affinity group is removed, and new ledgers would still be placed on those groups.
            properties.forEach((key, value) -> {
                if (!IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS.equals(key)
                        && !IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS.equals(key)) {
                    bkConf.setProperty(key, value);
                }
            });
        }
        if (policyClass.isPresent()) {
            setEnsemblePlacementPolicy(bkConf, conf, store, policyClass.get());
        } else {
            setDefaultEnsemblePlacementPolicy(bkConf, conf, store);
        }

        ClientResources resources = new ClientResources();
        CompletableFuture<BookKeeper> result = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("BookKeeper client factory is closed"));
            }
            clientResources.add(resources);
            pendingCreations.add(result);
        }
        result.whenComplete((client, error) -> {
            synchronized (this) {
                pendingCreations.remove(result);
            }
            if (error != null) {
                resources.run();
            }
        });
        bkConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_LISTENER_CLEANUP, resources);

        long readTimeoutMillis = TimeUnit.SECONDS.toMillis(conf.getMetadataStoreOperationTimeoutSeconds());
        CompletableFuture.completedFuture(null).thenComposeAsync(__ -> {
            try {
                return preparePlacementConfiguration(bkConf, store, readTimeoutMillis, policyClass.isEmpty());
            } catch (ConfigurationException e) {
                return CompletableFuture.failedFuture(e);
            }
        }).thenApplyAsync(__ -> {
            if (!isPendingCreation(result)) {
                throw new CancellationException("BookKeeper client creation was abandoned");
            }
            try {
                return getBookKeeperBuilder(conf, eventLoopGroup, statsLogger, bkConf).build();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            } catch (BKException | IOException e) {
                throw new CompletionException(e);
            }
        }).whenComplete((client, error) -> {
            if (error != null) {
                resources.run();
                result.completeExceptionally(error);
            } else if (!isPendingCreation(result) || !result.complete(client)) {
                // The caller owns the client once handed off. If it cancelled construction, the builder still owns it.
                CompletableFuture.runAsync(() -> closeUnclaimedClient(client, resources));
            }
        });
        return result;
    }

    private synchronized boolean isPendingCreation(CompletableFuture<BookKeeper> result) {
        return !closed && !result.isDone() && pendingCreations.contains(result);
    }

    void preloadBookieRackConfiguration(ServiceConfiguration conf, MetadataStore store) {
        // Future namespace policies can need isolation even if the default client does not use this configuration.
        // Start the owner before serving requests without gating brokers that do not use the Pulsar rack resolver.
        CompletableFuture.runAsync(() -> rackSnapshot(store,
                TimeUnit.SECONDS.toMillis(conf.getMetadataStoreOperationTimeoutSeconds())).start())
                .exceptionally(error -> {
                    log.debug().attr("path", BOOKIE_INFO_ROOT_PATH).exceptionMessage(error)
                            .log("Failed to start bookie rack configuration refresh");
                    return null;
                });
    }

    private synchronized BookieRackConfigurationSnapshot rackSnapshot(MetadataStore store, long readTimeoutMillis) {
        if (closed) {
            throw new IllegalStateException("BookKeeper client factory is closed");
        }
        return rackSnapshots.computeIfAbsent(store, key -> new BookieRackConfigurationSnapshot(key, readTimeoutMillis));
    }

    private CompletableFuture<Void> preparePlacementConfiguration(ClientConfiguration bkConf, MetadataStore store,
            long readTimeoutMillis, boolean defaultClient) throws ConfigurationException {
        var policyClass = bkConf.getEnsemblePlacementPolicy();
        boolean isolated = IsolatedBookieEnsemblePlacementPolicy.class.isAssignableFrom(policyClass);
        boolean usesRackMapping = (isolated
                || policyClass == RackawareEnsemblePlacementPolicy.class
                || policyClass == RackawareEnsemblePlacementPolicyImpl.class
                || policyClass == RegionAwareEnsemblePlacementPolicy.class)
                && usesPulsarRackMapping(bkConf);
        if (!usesRackMapping && !isolated) {
            return CompletableFuture.completedFuture(null);
        }
        var snapshot = rackSnapshot(store, readTimeoutMillis);
        snapshot.start();
        bkConf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
        if (!usesRackMapping || snapshot.hasLoaded()) {
            return CompletableFuture.completedFuture(null);
        }
        boolean enforceRacks = bkConf.getEnforceMinNumRacksPerWriteQuorum();
        if (!defaultClient) {
            // A namespace may create a client on demand. Its open does not wait for background metadata work.
            return enforceRacks ? CompletableFuture.failedFuture(new IllegalStateException(
                    "Bookie rack configuration at " + BOOKIE_INFO_ROOT_PATH + " is not loaded yet"))
                    : CompletableFuture.completedFuture(null);
        }
        // A succession of change notifications can supersede individual reads. Bound the whole bootstrap wait.
        return snapshot.initialRead().orTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS).handle((__, error) -> {
            if (error != null && !snapshot.hasLoaded()) {
                Throwable cause = FutureUtil.unwrapCompletionException(error);
                if (cause instanceof TimeoutException && cause.getMessage() == null) {
                    cause = new TimeoutException("Timed out after " + readTimeoutMillis
                            + " ms waiting for bookie rack configuration at " + BOOKIE_INFO_ROOT_PATH);
                }
                if (enforceRacks) {
                    throw new CompletionException(cause);
                }
                // Retain the subscribed resolver so it can apply the recovered configuration after startup.
                log.warn().attr("path", BOOKIE_INFO_ROOT_PATH).attr("timeoutMs", readTimeoutMillis)
                        .exceptionMessage(cause).log("Starting BookKeeper client with default rack placement");
            }
            return null;
        });
    }

    private static boolean usesPulsarRackMapping(ClientConfiguration conf) {
        String className = conf.getString(REPP_DNS_RESOLVER_CLASS, "");
        try {
            return BookieRackAffinityMapping.class.isAssignableFrom(
                    Class.forName(className, false, BookieRackAffinityMapping.class.getClassLoader()));
        } catch (ClassNotFoundException e) {
            // BookKeeper handles an unavailable resolver class using its usual fallback.
            return false;
        }
    }

    private static ClientResources resourcesOf(BookKeeper client) {
        Object resources = BookKeeperClientConfigAccessor.getConfigurationProperty(client,
                BookieRackAffinityMapping.METADATA_STORE_LISTENER_CLEANUP);
        return resources instanceof ClientResources clientResources ? clientResources : null;
    }

    static void releaseClientResources(BookKeeper client) {
        var resources = resourcesOf(client);
        if (resources != null) {
            resources.run();
        }
    }

    private static void closeUnclaimedClient(BookKeeper client, ClientResources resources) {
        try {
            client.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug().exception(e).log("Interrupted while closing a BookKeeper client");
        } catch (Exception e) {
            log.debug().exception(e).log("Failed to close a BookKeeper client");
        } finally {
            resources.run();
        }
    }

    @VisibleForTesting
    BookKeeper.Builder getBookKeeperBuilder(ServiceConfiguration conf, EventLoopGroup eventLoopGroup,
                                            StatsLogger statsLogger, ClientConfiguration bkConf) {
        BookKeeper.Builder builder = BookKeeper.forConfig(bkConf)
                .allocator(PulsarByteBufAllocator.DEFAULT)
                .statsLogger(statsLogger);
        if (!conf.isBookkeeperClientSeparatedIoThreadsEnabled()) {
            builder.eventLoopGroup(eventLoopGroup);
        }
        return builder;
    }

    @VisibleForTesting
    ClientConfiguration createBkClientConfiguration(MetadataStoreExtended store, ServiceConfiguration conf) {
        ClientConfiguration bkConf = new ClientConfiguration();
        if (conf.getBookkeeperClientAuthenticationPlugin() != null
                && conf.getBookkeeperClientAuthenticationPlugin().trim().length() > 0) {
            bkConf.setClientAuthProviderFactoryClass(conf.getBookkeeperClientAuthenticationPlugin());
            bkConf.setProperty(conf.getBookkeeperClientAuthenticationParametersName(),
                    conf.getBookkeeperClientAuthenticationParameters());
        }

        if (conf.isBookkeeperTLSClientAuthentication()) {
            bkConf.setTLSClientAuthentication(true);
            bkConf.setTLSCertificatePath(conf.getBookkeeperTLSCertificateFilePath());
            bkConf.setTLSKeyStore(conf.getBookkeeperTLSKeyFilePath());
            bkConf.setTLSKeyStoreType(conf.getBookkeeperTLSKeyFileType());
            bkConf.setTLSKeyStorePasswordPath(conf.getBookkeeperTLSKeyStorePasswordPath());
            bkConf.setTLSProviderFactoryClass(conf.getBookkeeperTLSProviderFactoryClass());
            bkConf.setTLSTrustStore(conf.getBookkeeperTLSTrustCertsFilePath());
            bkConf.setTLSTrustStoreType(conf.getBookkeeperTLSTrustCertTypes());
            bkConf.setTLSTrustStorePasswordPath(conf.getBookkeeperTLSTrustStorePasswordPath());
            bkConf.setTLSCertFilesRefreshDurationSeconds(conf.getBookkeeperTlsCertFilesRefreshDurationSeconds());
        }

        bkConf.setBusyWaitEnabled(conf.isEnableBusyWait());
        bkConf.setNumWorkerThreads(conf.getBookkeeperClientNumWorkerThreads());
        bkConf.setThrottleValue(conf.getBookkeeperClientThrottleValue());
        bkConf.setZkTimeout((int) conf.getMetadataStoreSessionTimeoutMillis());
        bkConf.setAddEntryTimeout((int) conf.getBookkeeperClientTimeoutInSeconds());
        bkConf.setReadEntryTimeout((int) conf.getBookkeeperClientTimeoutInSeconds());
        bkConf.setSpeculativeReadTimeout(conf.getBookkeeperClientSpeculativeReadTimeoutInMillis());
        bkConf.setNumChannelsPerBookie(conf.getBookkeeperNumberOfChannelsPerBookie());
        bkConf.setUseV2WireProtocol(conf.isBookkeeperUseV2WireProtocol());
        bkConf.setEnableDigestTypeAutodetection(true);
        bkConf.setStickyReadsEnabled(conf.isBookkeeperEnableStickyReads());
        bkConf.setNettyMaxFrameSizeBytes(conf.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING);
        bkConf.setDiskWeightBasedPlacementEnabled(conf.isBookkeeperDiskWeightBasedPlacementEnabled());
        bkConf.setMetadataServiceUri(conf.getBookkeeperMetadataStoreUrl());
        bkConf.setLimitStatsLogging(conf.isBookkeeperClientLimitStatsLogging());

        if (!conf.isBookkeeperMetadataStoreSeparated()) {
            // If we're connecting to the same metadata service, with same config, then
            // let's share the MetadataStore instance
            bkConf.setProperty(AbstractMetadataDriver.METADATA_STORE_INSTANCE, store);
        }

        if (conf.isBookkeeperClientHealthCheckEnabled()) {
            bkConf.enableBookieHealthCheck();
            bkConf.setBookieHealthCheckInterval((int) conf.getBookkeeperClientHealthCheckIntervalSeconds(),
                    TimeUnit.SECONDS);
            bkConf.setBookieErrorThresholdPerInterval(conf.getBookkeeperClientHealthCheckErrorThresholdPerInterval());
            bkConf.setBookieQuarantineTime((int) conf.getBookkeeperClientHealthCheckQuarantineTimeInSeconds(),
                    TimeUnit.SECONDS);
            bkConf.setBookieQuarantineRatio(conf.getBookkeeperClientQuarantineRatio());
        }

        bkConf.setReorderReadSequenceEnabled(conf.isBookkeeperClientReorderReadSequenceEnabled());
        bkConf.setExplictLacInterval(conf.getBookkeeperExplicitLacIntervalInMills());
        bkConf.setGetBookieInfoIntervalSeconds(
                conf.getBookkeeperClientGetBookieInfoIntervalSeconds(), TimeUnit.SECONDS);
        bkConf.setGetBookieInfoRetryIntervalSeconds(
                conf.getBookkeeperClientGetBookieInfoRetryIntervalSeconds(), TimeUnit.SECONDS);
        bkConf.setNumIOThreads(conf.getBookkeeperClientNumIoThreads());
        PropertiesUtils.filterAndMapProperties(conf.getProperties(), "bookkeeper_")
                .forEach((key, value) -> {
                    log.info()
                            .attr("key", key)
                            .attr("value", value)
                            .log("Applying BookKeeper client configuration setting");
                    bkConf.setProperty(key, value);
                });
        return bkConf;
    }

    static void setDefaultEnsemblePlacementPolicy(
            ClientConfiguration bkConf,
            ServiceConfiguration conf,
            MetadataStore store
    ) {
        bkConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        if (conf.isBookkeeperClientRackawarePolicyEnabled() || conf.isBookkeeperClientRegionawarePolicyEnabled()) {
            if (conf.isBookkeeperClientRegionawarePolicyEnabled()) {
                bkConf.setEnsemblePlacementPolicy(RegionAwareEnsemblePlacementPolicy.class);

                bkConf.setProperty(
                        REPP_ENABLE_VALIDATION,
                        conf.getProperties().getProperty(REPP_ENABLE_VALIDATION, "true")
                );
                bkConf.setProperty(
                        REPP_REGIONS_TO_WRITE,
                        conf.getProperties().getProperty(REPP_REGIONS_TO_WRITE, null)
                );
                bkConf.setProperty(
                        REPP_MINIMUM_REGIONS_FOR_DURABILITY,
                        conf.getProperties().getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, "2")
                );
                bkConf.setProperty(
                        REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE,
                        conf.getProperties().getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, "true")
                );
            } else {
                bkConf.setEnsemblePlacementPolicy(RackawareEnsemblePlacementPolicy.class);
            }

            bkConf.setMinNumRacksPerWriteQuorum(conf.getBookkeeperClientMinNumRacksPerWriteQuorum());
            bkConf.setEnforceMinNumRacksPerWriteQuorum(conf.isBookkeeperClientEnforceMinNumRacksPerWriteQuorum());

            bkConf.setProperty(REPP_DNS_RESOLVER_CLASS,
                    conf.getProperties().getProperty(
                            REPP_DNS_RESOLVER_CLASS,
                            BookieRackAffinityMapping.class.getName()));

            bkConf.setProperty(NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                conf.getProperties().getProperty(
                    NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                    ""));
        }

        if (conf.getBookkeeperClientIsolationGroups() != null && !conf.getBookkeeperClientIsolationGroups().isEmpty()) {
            bkConf.setEnsemblePlacementPolicy(IsolatedBookieEnsemblePlacementPolicy.class);
            bkConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientIsolationGroups());
            bkConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientSecondaryIsolationGroups());
        }
    }

    static void setEnsemblePlacementPolicy(ClientConfiguration bkConf, ServiceConfiguration conf, MetadataStore store,
                                            Class<? extends EnsemblePlacementPolicy> policyClass) {
        bkConf.setEnsemblePlacementPolicy(policyClass);
        bkConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        if (conf.isBookkeeperClientRackawarePolicyEnabled() || conf.isBookkeeperClientRegionawarePolicyEnabled()) {
            bkConf.setProperty(REPP_DNS_RESOLVER_CLASS, conf.getProperties().getProperty(REPP_DNS_RESOLVER_CLASS,
                    BookieRackAffinityMapping.class.getName()));

            bkConf.setMinNumRacksPerWriteQuorum(conf.getBookkeeperClientMinNumRacksPerWriteQuorum());
            bkConf.setEnforceMinNumRacksPerWriteQuorum(conf.isBookkeeperClientEnforceMinNumRacksPerWriteQuorum());

            bkConf.setProperty(NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                conf.getProperties().getProperty(
                    NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                    ""));
        }

        if (IsolatedBookieEnsemblePlacementPolicy.class.isAssignableFrom(policyClass)
                && conf.getBookkeeperClientIsolationGroups() != null
                && !conf.getBookkeeperClientIsolationGroups().isEmpty()) {
            bkConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientIsolationGroups());
            bkConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientSecondaryIsolationGroups());
        }
    }

    @Override
    public void close() {
        List<CompletableFuture<BookKeeper>> pending;
        List<ClientResources> resources;
        List<BookieRackConfigurationSnapshot> snapshots;
        synchronized (this) {
            closed = true;
            pending = List.copyOf(pendingCreations);
            pendingCreations.clear();
            resources = List.copyOf(clientResources);
            clientResources.clear();
            snapshots = List.copyOf(rackSnapshots.values());
            rackSnapshots.clear();
        }
        pending.forEach(result -> result.completeExceptionally(
                new IllegalStateException("BookKeeper client factory is closed")));
        resources.forEach(ClientResources::run);
        snapshots.forEach(BookieRackConfigurationSnapshot::close);
    }

    private final class ClientResources implements Consumer<Runnable>, Runnable {
        private final List<Runnable> cleanups = new ArrayList<>();
        private boolean released;

        @Override
        public void accept(Runnable cleanup) {
            synchronized (this) {
                if (!released) {
                    cleanups.add(cleanup);
                    return;
                }
            }
            runCleanup(cleanup);
        }

        @Override
        public void run() {
            List<Runnable> callbacks;
            synchronized (this) {
                if (released) {
                    return;
                }
                released = true;
                callbacks = List.copyOf(cleanups);
                cleanups.clear();
            }
            synchronized (BookKeeperClientFactoryImpl.this) {
                clientResources.remove(this);
            }
            callbacks.forEach(this::runCleanup);
        }

        private void runCleanup(Runnable cleanup) {
            try {
                cleanup.run();
            } catch (RuntimeException e) {
                log.debug().exception(e).log("Failed to release BookKeeper client metadata resources");
            }
        }
    }
}
