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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusMetricsProvider;
import org.apache.pulsar.broker.storage.BookkeeperManagedLedgerStorageClass;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerClientFactory implements ManagedLedgerStorage {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerClientFactory.class);
    private static final String DEFAULT_STORAGE_CLASS_NAME = "bookkeeper";
    private BookkeeperManagedLedgerStorageClass defaultStorageClass;
    private ManagedLedgerFactory managedLedgerFactory;
    private BookKeeper defaultBkClient;
    private final AsyncCache<EnsemblePlacementPolicyConfig, BookKeeper>
            bkEnsemblePolicyToBkClientMap = Caffeine.newBuilder().buildAsync();
    private StatsProvider statsProvider = new NullStatsProvider();

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
        managedLedgerFactoryConfig.setCopyEntriesInCache(conf.isManagedLedgerCacheCopyEntries());
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightSize(
                conf.getManagedLedgerMaxReadsInFlightSizeInMB() * 1024L * 1024L);
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
        if (conf.isBookkeeperClientExposeStatsToPrometheus()) {
            configuration.addProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                    conf.getManagedLedgerPrometheusStatsLatencyRolloverSeconds());
            configuration.addProperty(PrometheusMetricsProvider.CLUSTER_NAME, conf.getClusterName());
            statsProvider = new PrometheusMetricsProvider();
        }

        statsProvider.start(configuration);
        StatsLogger statsLogger = statsProvider.getStatsLogger("pulsar_managedLedger_client");

        this.defaultBkClient =
                bookkeeperProvider.create(conf, metadataStore, eventLoopGroup, Optional.empty(), null, statsLogger)
                        .get();

        BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory = (
                EnsemblePlacementPolicyConfig ensemblePlacementPolicyConfig) -> {
            if (ensemblePlacementPolicyConfig == null || ensemblePlacementPolicyConfig.getPolicyClass() == null) {
                return CompletableFuture.completedFuture(defaultBkClient);
            }

            // find or create bk-client in cache for a specific ensemblePlacementPolicy
            return bkEnsemblePolicyToBkClientMap.get(ensemblePlacementPolicyConfig,
                    (config, executor) -> bookkeeperProvider.create(conf, metadataStore, eventLoopGroup,
                            Optional.ofNullable(ensemblePlacementPolicyConfig.getPolicyClass()),
                            ensemblePlacementPolicyConfig.getProperties(), statsLogger));
        };

        try {
            this.managedLedgerFactory =
                    new ManagedLedgerFactoryImpl(metadataStore, bkFactory, managedLedgerFactoryConfig, statsLogger,
                            openTelemetry);
        } catch (Exception e) {
            statsProvider.stop();
            defaultBkClient.close();
            throw e;
        }

        defaultStorageClass = new BookkeeperManagedLedgerStorageClass() {
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
                return defaultBkClient;
            }
        };
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
        try {
            if (null != managedLedgerFactory) {
                managedLedgerFactory.shutdown();
                log.info("Closed managed ledger factory");
            }

            if (null != statsProvider) {
                statsProvider.stop();
            }
            try {
                if (null != defaultBkClient) {
                    defaultBkClient.close();
                }
            } catch (RejectedExecutionException ree) {
                // when closing bookkeeper client, it will error outs all pending metadata operations.
                // those callbacks of those operations will be triggered, and submitted to the scheduler
                // in managed ledger factory. but the managed ledger factory has been shutdown before,
                // so `RejectedExecutionException` will be thrown there. we can safely ignore this exception.
                //
                // an alternative solution is to close bookkeeper client before shutting down managed ledger
                // factory, however that might be introducing more unknowns.
                log.warn("Encountered exceptions on closing bookkeeper client", ree);
            }
            bkEnsemblePolicyToBkClientMap.synchronous().asMap().forEach((policy, bk) -> {
                try {
                    if (bk != null) {
                        bk.close();
                    }
                } catch (Exception e) {
                    log.warn("Failed to close bookkeeper-client for policy {}", policy, e);
                }
            });
            log.info("Closed BookKeeper client");
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }
}
