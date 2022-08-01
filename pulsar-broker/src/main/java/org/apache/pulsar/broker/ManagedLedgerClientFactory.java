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
package org.apache.pulsar.broker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
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
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerClientFactory implements ManagedLedgerStorage {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerClientFactory.class);

    private ManagedLedgerFactory managedLedgerFactory;
    private BookKeeper defaultBkClient;
    private final Map<EnsemblePlacementPolicyConfig, BookKeeper>
            bkEnsemblePolicyToBkClientMap = Maps.newConcurrentMap();
    private StatsProvider statsProvider = new NullStatsProvider();

    public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                           BookKeeperClientFactory bookkeeperProvider,
                           EventLoopGroup eventLoopGroup) throws Exception {
        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(conf.getManagedLedgerCacheSizeMB() * 1024L * 1024L);
        managedLedgerFactoryConfig.setCacheEvictionWatermark(conf.getManagedLedgerCacheEvictionWatermark());
        managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(conf.getManagedLedgerNumSchedulerThreads());
        managedLedgerFactoryConfig.setCacheEvictionIntervalMs(conf.getManagedLedgerCacheEvictionIntervalMs());
        managedLedgerFactoryConfig.setCacheEvictionTimeThresholdMillis(
                conf.getManagedLedgerCacheEvictionTimeThresholdMillis());
        managedLedgerFactoryConfig.setCopyEntriesInCache(conf.isManagedLedgerCacheCopyEntries());
        managedLedgerFactoryConfig.setPrometheusStatsLatencyRolloverSeconds(
                conf.getManagedLedgerPrometheusStatsLatencyRolloverSeconds());
        managedLedgerFactoryConfig.setTraceTaskExecution(conf.isManagedLedgerTraceTaskExecution());
        managedLedgerFactoryConfig.setCursorPositionFlushSeconds(conf.getManagedLedgerCursorPositionFlushSeconds());
        managedLedgerFactoryConfig.setManagedLedgerInfoCompressionType(conf.getManagedLedgerInfoCompressionType());
        managedLedgerFactoryConfig.setStatsPeriodSeconds(conf.getManagedLedgerStatsPeriodSeconds());
        managedLedgerFactoryConfig.setManagedCursorInfoCompressionType(conf.getManagedCursorInfoCompressionType());

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
                bookkeeperProvider.create(conf, metadataStore, eventLoopGroup, Optional.empty(), null, statsLogger);

        BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory = (
                EnsemblePlacementPolicyConfig ensemblePlacementPolicyConfig) -> {
            BookKeeper bkClient = null;
            // find or create bk-client in cache for a specific ensemblePlacementPolicy
            if (ensemblePlacementPolicyConfig != null && ensemblePlacementPolicyConfig.getPolicyClass() != null) {
                bkClient = bkEnsemblePolicyToBkClientMap.computeIfAbsent(ensemblePlacementPolicyConfig, (key) -> {
                    try {
                        return bookkeeperProvider.create(conf, metadataStore, eventLoopGroup,
                                Optional.ofNullable(ensemblePlacementPolicyConfig.getPolicyClass()),
                                ensemblePlacementPolicyConfig.getProperties(), statsLogger);
                    } catch (Exception e) {
                        log.error("Failed to initialize bk-client for policy {}, properties {}",
                                ensemblePlacementPolicyConfig.getPolicyClass(),
                                ensemblePlacementPolicyConfig.getProperties(), e);
                    }
                    return this.defaultBkClient;
                });
            }
            return bkClient != null ? bkClient : defaultBkClient;
        };

        this.managedLedgerFactory =
                new ManagedLedgerFactoryImpl(metadataStore, bkFactory, managedLedgerFactoryConfig, statsLogger);
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    public BookKeeper getBookKeeperClient() {
        return defaultBkClient;
    }

    public StatsProvider getStatsProvider() {
        return statsProvider;
    }

    @VisibleForTesting
    public Map<EnsemblePlacementPolicyConfig, BookKeeper> getBkEnsemblePolicyToBookKeeperMap() {
        return bkEnsemblePolicyToBkClientMap;
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
            if (bkEnsemblePolicyToBkClientMap != null) {
                bkEnsemblePolicyToBkClientMap.forEach((policy, bk) -> {
                    try {
                        if (bk != null) {
                            bk.close();
                        }
                    } catch (Exception e) {
                        log.warn("Failed to close bookkeeper-client for policy {}", policy, e);
                    }
                });
            }
            log.info("Closed BookKeeper client");
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }
}
