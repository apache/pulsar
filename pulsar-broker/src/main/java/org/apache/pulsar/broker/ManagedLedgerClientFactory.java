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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFactoryClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusMetricsProvider;
import org.apache.pulsar.broker.storage.BookkeeperManagedLedgerStorageClass;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@CustomLog
public class ManagedLedgerClientFactory implements ManagedLedgerStorage {
    private static final String DEFAULT_STORAGE_CLASS_NAME = "bookkeeper";
    private static final String CLIENT_CACHE_NAME = "managed-ledger-bk-ensemble-client-cache";
    private BookkeeperManagedLedgerStorageClass defaultStorageClass;
    @VisibleForTesting
    protected ManagedLedgerFactory managedLedgerFactory;
    private final Object clientLifecycleLock = new Object();
    private final CompletableFuture<BookKeeper> defaultBkClient = new CompletableFuture<>();
    private final AsyncCache<EnsemblePlacementPolicyConfig, BookKeeper>
            bkEnsemblePolicyToBkClientMap = Caffeine.newBuilder().recordStats().buildAsync();
    private final StatsProvider fallbackStatsProvider;
    private StatsProvider statsProvider;
    private volatile boolean closed;

    public ManagedLedgerClientFactory() {
        this(new NullStatsProvider());
    }

    @VisibleForTesting
    ManagedLedgerClientFactory(StatsProvider fallbackStatsProvider) {
        this.fallbackStatsProvider = fallbackStatsProvider;
        CacheMetricsCollector.CAFFEINE.addCache(CLIENT_CACHE_NAME, bkEnsemblePolicyToBkClientMap);
    }

    @Override
    public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                           BookKeeperClientFactory bookkeeperProvider,
                           EventLoopGroup eventLoopGroup,
                           OpenTelemetry openTelemetry) throws Exception {
        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(conf.getManagedLedgerCacheSizeMB() * 1024L * 1024L);
        managedLedgerFactoryConfig.setCacheEvictionWatermark(conf.getManagedLedgerCacheEvictionWatermark());
        managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(conf.getManagedLedgerNumSchedulerThreads());
        managedLedgerFactoryConfig.setCacheEvictionIntervalMs(conf.getManagedLedgerCacheEvictionIntervalMs());
        managedLedgerFactoryConfig.setCacheEvictionTimeThresholdMillis(
                conf.getManagedLedgerCacheEvictionTimeThresholdMillis());
        managedLedgerFactoryConfig.setCacheEvictionExtendTTLOfRecentlyAccessed(
                conf.isManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed());
        managedLedgerFactoryConfig.setCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(
                conf.getManagedLedgerCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes());
        managedLedgerFactoryConfig.setCopyEntriesInCache(conf.isManagedLedgerCacheCopyEntries());
        Long managedLedgerMaxReadsInFlightSizeInMB = conf.getManagedLedgerMaxReadsInFlightSizeInMB();
        // A single dispatcher read can retain up to dispatcherMaxReadSizeBytes bytes. The in-flight reads
        // limit must be at least this size, otherwise the limiter can block the completion of a single read.
        long dispatcherMaxReadSizeBytes = conf.getDispatcherMaxReadSizeBytes();
        final long managedLedgerMaxReadsInFlightSizeBytes;
        if (managedLedgerMaxReadsInFlightSizeInMB == null) {
            // When unset, default to 15% of the available JVM direct memory, but never below the maximum
            // size of a single read (dispatcherMaxReadSizeBytes) so that the limiter can never block the
            // completion of one read.
            long fractionOfDirectMemory = (long) (0.15d * DirectMemoryUtils.jvmMaxDirectMemory());
            managedLedgerMaxReadsInFlightSizeBytes = Math.max(fractionOfDirectMemory, dispatcherMaxReadSizeBytes);
        } else {
            // An explicit 0 disables the feature; an explicit value > 0 is used as-is.
            managedLedgerMaxReadsInFlightSizeBytes = managedLedgerMaxReadsInFlightSizeInMB * 1024L * 1024L;
            // Warn when the feature is enabled but manually configured below the size of a single read, since
            // the limiter would then be unable to admit one read. Disabled (0) is ignored.
            if (managedLedgerMaxReadsInFlightSizeBytes > 0 && dispatcherMaxReadSizeBytes > 0
                    && managedLedgerMaxReadsInFlightSizeBytes < dispatcherMaxReadSizeBytes) {
                log.warn()
                        .attr("managedLedgerMaxReadsInFlightSizeInMB", managedLedgerMaxReadsInFlightSizeInMB)
                        .attr("dispatcherMaxReadSizeBytes", dispatcherMaxReadSizeBytes)
                        .attr("minManagedLedgerMaxReadsInFlightSizeInMB",
                                (dispatcherMaxReadSizeBytes + (1024L * 1024L) - 1) / (1024L * 1024L))
                        .log("Invalid configuration:"
                                + " managedLedgerMaxReadsInFlightSizeInMB in bytes should be greater than or"
                                + " equal to dispatcherMaxReadSizeBytes, otherwise the in-flight reads limiter"
                                + " can block the completion of a single read.");
            }
        }
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightSize(managedLedgerMaxReadsInFlightSizeBytes);
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis(
                conf.getManagedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis());
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightPermitsAcquireQueueSize(
                conf.getManagedLedgerMaxReadsInFlightPermitsAcquireQueueSize());
        managedLedgerFactoryConfig.setPrometheusStatsLatencyRolloverSeconds(
                conf.getManagedLedgerPrometheusStatsLatencyRolloverSeconds());
        managedLedgerFactoryConfig.setTraceTaskExecution(conf.isManagedLedgerTraceTaskExecution());
        managedLedgerFactoryConfig.setCursorPositionFlushSeconds(conf.getManagedLedgerCursorPositionFlushSeconds());
        managedLedgerFactoryConfig.setManagedLedgerInfoCompressionType(conf.getManagedLedgerInfoCompressionType());
        managedLedgerFactoryConfig.setManagedLedgerInfoCompressionThresholdInBytes(
                conf.getManagedLedgerInfoCompressionThresholdInBytes());
        managedLedgerFactoryConfig.setStatsPeriodSeconds(conf.getManagedLedgerStatsPeriodSeconds());
        managedLedgerFactoryConfig.setManagedCursorInfoCompressionType(conf.getManagedCursorInfoCompressionType());
        managedLedgerFactoryConfig.setManagedCursorInfoCompressionThresholdInBytes(
                conf.getManagedCursorInfoCompressionThresholdInBytes());

        Configuration configuration = new ClientConfiguration();
        StatsProvider startupStatsProvider = fallbackStatsProvider;
        if (conf.isBookkeeperClientExposeStatsToPrometheus()) {
            configuration.addProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                    conf.getManagedLedgerPrometheusStatsLatencyRolloverSeconds());
            configuration.addProperty(PrometheusMetricsProvider.CLUSTER_NAME, conf.getClusterName());
            startupStatsProvider = new PrometheusMetricsProvider();
        }

        boolean registeredStatsProvider = false;
        try {
            startupStatsProvider.start(configuration);
            synchronized (clientLifecycleLock) {
                if (closed) {
                    throw new ManagedLedgerFactoryClosedException();
                }
                statsProvider = startupStatsProvider;
                registeredStatsProvider = true;
            }
            StatsLogger statsLogger = startupStatsProvider.getStatsLogger("pulsar_managedLedger_client");
            buildClient(defaultBkClient, () -> bookkeeperProvider.create(
                    conf, metadataStore, eventLoopGroup, Optional.empty(), null, statsLogger));
            borrowClient(defaultBkClient).get();

            BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory =
                    new BookkeeperFactoryForCustomEnsemblePlacementPolicy() {
                @Override
                public CompletableFuture<BookKeeper> get(EnsemblePlacementPolicyConfig policyConfig) {
                    if (policyConfig == null || policyConfig.getPolicyClass() == null) {
                        return borrowClient(defaultBkClient);
                    }

                    @SuppressWarnings("unchecked")
                    Optional<Class<? extends EnsemblePlacementPolicy>> policyClass =
                            (Optional) Optional.of(policyConfig.getPolicyClass());
                    return borrowPolicyClient(policyConfig, () ->
                            bookkeeperProvider.create(conf, metadataStore, eventLoopGroup, policyClass,
                                    policyConfig.getProperties(), statsLogger));
                }

                @Override
                public CompletableFuture<BookKeeper> getForPlacement(EnsemblePlacementPolicyConfig policyConfig) {
                    // A failed readiness check affects the request. The cache still owns the built client.
                    return FutureUtil.supplySafely(() -> get(policyConfig)).thenCompose(client -> {
                        long timeoutMillis = TimeUnit.SECONDS.toMillis(conf.getMetadataStoreOperationTimeoutSeconds());
                        var topology = BookKeeperClientFactoryImpl.awaitRackTopology(client, timeoutMillis);
                        var context = client.getClientCtx();
                        if (context != null && context.getPlacementPolicy()
                                instanceof IsolatedBookieEnsemblePlacementPolicy policy) {
                            topology = CompletableFuture.allOf(topology,
                                    policy.awaitRackConfiguration(policyConfig, timeoutMillis));
                        }
                        return topology.thenCompose(__ -> clientIfOpen(client));
                    });
                }
            };

            var ledgerFactory = createManagedLedgerFactory(
                    metadataStore, openTelemetry, bkFactory, managedLedgerFactoryConfig, statsLogger);
            synchronized (clientLifecycleLock) {
                if (!closed) {
                    managedLedgerFactory = ledgerFactory;
                    defaultStorageClass = createDefaultStorageClass();
                    return;
                }
            }
            throw cleanup(new ManagedLedgerFactoryClosedException(), ledgerFactory::shutdown);
        } catch (Exception e) {
            if (!registeredStatsProvider) {
                cleanup(e, startupStatsProvider::stop);
            }
            cleanup(e, this::close);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
    }

    private BookkeeperManagedLedgerStorageClass createDefaultStorageClass() {
        return new BookkeeperManagedLedgerStorageClass() {
            @Override
            public String getName() {
                return DEFAULT_STORAGE_CLASS_NAME;
            }

            @Override
            public ManagedLedgerFactory getManagedLedgerFactory() {
                return managedLedgerFactory;
            }

            @Override
            public StatsProvider getStatsProvider() {
                return statsProvider;
            }

            @Override
            public BookKeeper getBookKeeperClient() {
                if (closed) {
                    throw new IllegalStateException("Managed ledger storage is closed");
                }
                return defaultBkClient.resultNow();
            }
        };
    }

    private CompletableFuture<BookKeeper> borrowPolicyClient(EnsemblePlacementPolicyConfig policyConfig,
                                                             Supplier<CompletableFuture<BookKeeper>> builder) {
        var pending = new CompletableFuture<BookKeeper>();
        CompletableFuture<BookKeeper> ownedClient;
        synchronized (clientLifecycleLock) {
            if (closed) {
                return CompletableFuture.failedFuture(new ManagedLedgerFactoryClosedException());
            }
            var entries = bkEnsemblePolicyToBkClientMap.asMap();
            CompletableFuture<BookKeeper> failed = entries.get(policyConfig);
            if (failed != null && failed.isCompletedExceptionally()) {
                // Caffeine can evict a failed entry after a caller has already observed its failure.
                entries.remove(policyConfig, failed);
            }
            ownedClient = bkEnsemblePolicyToBkClientMap.get(policyConfig, (config, executor) -> pending);
        }
        if (ownedClient == pending) {
            buildClient(pending, builder);
        }
        return borrowClient(ownedClient);
    }

    private CompletableFuture<BookKeeper> borrowClient(CompletableFuture<BookKeeper> ownedClient) {
        if (closed) {
            return CompletableFuture.failedFuture(new ManagedLedgerFactoryClosedException());
        }
        // Each borrower owns a dependent future and cannot cancel the build or other borrows.
        return ownedClient.thenCompose(this::clientIfOpen);
    }

    private CompletableFuture<BookKeeper> clientIfOpen(BookKeeper client) {
        return closed ? CompletableFuture.failedFuture(new ManagedLedgerFactoryClosedException())
                : CompletableFuture.completedFuture(client);
    }

    private static void buildClient(CompletableFuture<BookKeeper> ownedClient,
                                    Supplier<CompletableFuture<BookKeeper>> builder) {
        // Run the provider outside the cache's mapping function and lifecycle lock. Retain its completion even if
        // shutdown has already failed the cache entry.
        CompletableFuture.completedFuture(null).thenComposeAsync(__ -> ownedClient.isDone()
                ? CompletableFuture.<BookKeeper>failedFuture(new ManagedLedgerFactoryClosedException())
                : FutureUtil.supplySafely(builder)).whenComplete((client, error) -> {
                    if (error != null) {
                        ownedClient.completeExceptionally(error);
                    } else if (!ownedClient.complete(client)) {
                        CompletableFuture.runAsync(() -> {
                            Exception closeError = cleanup(null, () -> closeClient(client));
                            if (closeError != null) {
                                log.warn().exception(closeError).log("Failed to close a late BookKeeper client");
                            }
                        });
                    }
                });
    }

    protected ManagedLedgerFactoryImpl createManagedLedgerFactory(MetadataStoreExtended metadataStore,
                                                                  OpenTelemetry openTelemetry,
                                                                  BookkeeperFactoryForCustomEnsemblePlacementPolicy
                                                                          bkFactory,
                                                                  ManagedLedgerFactoryConfig managedLedgerFactoryConfig,
                                                                  StatsLogger statsLogger) throws Exception {
        return new ManagedLedgerFactoryImpl(metadataStore, bkFactory, managedLedgerFactoryConfig, statsLogger,
                openTelemetry);
    }

    @Override
    public Collection<ManagedLedgerStorageClass> getStorageClasses() {
        return List.of(getDefaultStorageClass());
    }

    @Override
    public Optional<ManagedLedgerStorageClass> getManagedLedgerStorageClass(String name) {
        if (name == null || DEFAULT_STORAGE_CLASS_NAME.equals(name)) {
            return Optional.of(getDefaultStorageClass());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public ManagedLedgerStorageClass getDefaultStorageClass() {
        return defaultStorageClass;
    }

    @VisibleForTesting
    public Map<EnsemblePlacementPolicyConfig, BookKeeper> getBkEnsemblePolicyToBookKeeperMap() {
        return bkEnsemblePolicyToBkClientMap.synchronous().asMap();
    }

    @Override
    public void close() throws IOException {
        List<CompletableFuture<BookKeeper>> clients;
        ManagedLedgerFactory ledgerFactory;
        StatsProvider ownedStatsProvider;
        synchronized (clientLifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            clients = new ArrayList<>(bkEnsemblePolicyToBkClientMap.asMap().values());
            clients.add(defaultBkClient);
            ledgerFactory = managedLedgerFactory;
            ownedStatsProvider = statsProvider;
        }
        // Wake pending borrows before shutting down Managed Ledger. A build that loses this race closes its result.
        for (var client : clients) {
            client.completeExceptionally(new ManagedLedgerFactoryClosedException());
        }
        bkEnsemblePolicyToBkClientMap.asMap().clear();
        CacheMetricsCollector.removeCache(CLIENT_CACHE_NAME, bkEnsemblePolicyToBkClientMap);
        Exception error = null;
        if (ledgerFactory != null) {
            error = cleanup(error, ledgerFactory::shutdown);
        }
        for (var client : clients) {
            if (client.state() == Future.State.SUCCESS) {
                error = cleanup(error, () -> closeClient(client.resultNow()));
            }
        }
        if (ownedStatsProvider != null) {
            error = cleanup(error, ownedStatsProvider::stop);
        }
        if (error != null) {
            throw new IOException(error);
        }
    }

    private static void closeClient(BookKeeper client) throws Exception {
        try (AutoCloseable resources = () -> BookKeeperClientFactoryImpl.releaseClientResources(client)) {
            client.close();
        } catch (RejectedExecutionException e) {
            // Closing BookKeeper can complete metadata callbacks after the Managed Ledger scheduler has stopped.
            log.warn().exceptionMessage(e).log("BookKeeper close invoked a stopped scheduler");
        }
    }

    private static Exception cleanup(Exception failure, AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (failure == null) {
                return error;
            }
            if (failure != error) {
                failure.addSuppressed(error);
            }
        }
        return failure;
    }
}
