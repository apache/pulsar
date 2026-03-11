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
package org.apache.pulsar.broker.service;

import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;

/**
 * JVM-wide singleton that manages a lightweight Pulsar cluster for integration tests.
 *
 * <p>The cluster consists of a single bookie (with minimal memory/thread configuration and no journal
 * sync) and a single Pulsar broker, both backed by in-memory metadata stores. A default cluster
 * ({@value CLUSTER_NAME}) and tenant ({@value TENANT_NAME}) are created at startup.
 *
 * <p>The singleton is lazily initialized on first call to {@link #get()} and shut down via a JVM
 * shutdown hook, which also deletes all temporary bookie data directories.
 *
 * @see SharedPulsarBaseTest
 */
@Slf4j
public class SharedPulsarCluster {

    private static final String METADATA_STORE_URL = "memory:shared-test-cluster";
    public static final String CLUSTER_NAME = "test-cluster";
    public static final String TENANT_NAME = "test-tenant";

    private static volatile SharedPulsarCluster instance;

    private BKCluster bkCluster;
    @Getter
    private PulsarService pulsarService;
    @Getter
    private PulsarAdmin admin;
    @Getter
    private PulsarClient client;

    /**
     * Returns the singleton instance, starting the cluster on first invocation.
     * Thread-safe via double-checked locking.
     */
    public static SharedPulsarCluster get() throws Exception {
        if (instance == null) {
            synchronized (SharedPulsarCluster.class) {
                if (instance == null) {
                    SharedPulsarCluster cluster = new SharedPulsarCluster();
                    cluster.start();
                    instance = cluster;
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        try {
                            instance.close();
                        } catch (Exception e) {
                            log.error("Failed to close SharedPulsarCluster", e);
                        }
                    }));
                }
            }
        }
        return instance;
    }

    private void start() throws Exception {
        log.info("Starting SharedPulsarCluster");

        // Start a single bookie with minimal configuration
        ServerConfiguration bkConf = new ServerConfiguration();
        bkConf.setProperty("dbStorage_writeCacheMaxSizeMb", 2);
        bkConf.setProperty("dbStorage_readAheadCacheMaxSizeMb", 1);
        bkConf.setProperty("dbStorage_rocksDB_writeBufferSizeMB", 1);
        bkConf.setProperty("dbStorage_rocksDB_blockCacheSize", 1024 * 1024);
        bkConf.setJournalSyncData(false);
        bkConf.setJournalWriteData(false);
        bkConf.setProperty("journalMaxGroupWaitMSec", 0L);
        bkConf.setProperty("journalPreAllocSizeMB", 1);
        bkConf.setFlushInterval(60000);
        bkConf.setGcWaitTime(60000);
        bkConf.setAllowLoopback(true);
        bkConf.setAdvertisedAddress("127.0.0.1");
        bkConf.setAllowEphemeralPorts(true);
        bkConf.setNumAddWorkerThreads(0);
        bkConf.setNumReadWorkerThreads(0);
        bkConf.setNumHighPriorityWorkerThreads(0);
        bkConf.setNumJournalCallbackThreads(0);
        bkConf.setServerNumIOThreads(1);
        bkConf.setNumLongPollWorkerThreads(1);
        bkConf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);

        bkCluster = BKCluster.builder()
                .baseServerConfiguration(bkConf)
                .metadataServiceUri(METADATA_STORE_URL)
                .numBookies(1)
                .clearOldData(true)
                .build();

        // Configure and start the Pulsar broker
        ServiceConfiguration config = new ServiceConfiguration();
        config.setMetadataStoreUrl(METADATA_STORE_URL);
        config.setConfigurationMetadataStoreUrl(METADATA_STORE_URL);
        config.setClusterName(CLUSTER_NAME);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setDefaultNumberOfNamespaceBundles(1);
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setNumExecutorThreadPoolSize(5);
        config.setManagedLedgerCacheSizeMB(8);
        config.setActiveConsumerFailoverDelayTimeMillis(0);
        config.setAllowAutoTopicCreationType(
                org.apache.pulsar.common.policies.data.TopicType.NON_PARTITIONED);
        config.setBookkeeperNumberOfChannelsPerBookie(1);
        config.setBookkeeperClientExposeStatsToPrometheus(false);
        config.setDispatcherRetryBackoffInitialTimeInMs(0);
        config.setDispatcherRetryBackoffMaxTimeInMs(0);
        config.setForceDeleteNamespaceAllowed(true);
        config.setForceDeleteTenantAllowed(true);

        pulsarService = new PulsarService(config);
        pulsarService.start();

        // Create admin and client
        admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarService.getWebServiceAddress())
                .build();

        client = PulsarClient.builder()
                .serviceUrl(pulsarService.getBrokerServiceUrl())
                .build();

        // Set up default cluster and tenant
        admin.clusters().createCluster(CLUSTER_NAME,
                ClusterData.builder()
                        .serviceUrl(pulsarService.getWebServiceAddress())
                        .brokerServiceUrl(pulsarService.getBrokerServiceUrl())
                        .build());

        admin.tenants().createTenant(TENANT_NAME,
                TenantInfo.builder()
                        .allowedClusters(Set.of(CLUSTER_NAME))
                        .build());

        log.info("SharedPulsarCluster started. broker={} web={}",
                pulsarService.getBrokerServiceUrl(), pulsarService.getWebServiceAddress());
    }

    private void close() throws Exception {
        log.info("Closing SharedPulsarCluster");
        if (client != null) {
            client.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (pulsarService != null) {
            pulsarService.close();
        }
        if (bkCluster != null) {
            bkCluster.close();
        }
    }
}
