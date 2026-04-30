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

import io.oxia.testcontainers.OxiaContainer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.CustomLog;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
import org.apache.pulsar.tests.ThreadLeakDetectorListener;

/**
 * JVM-wide singleton that manages a lightweight multi-broker Pulsar cluster for integration tests.
 *
 * <p>Companion to {@link SharedPulsarCluster}, but spins up {@value #NUM_BROKERS} Pulsar brokers
 * sharing a single bookie and an in-memory metadata store. Use this when a test specifically
 * needs to exercise behavior that only manifests across brokers — namespace ownership transfer,
 * controller-leader failover, segment placement on different brokers, V5 client reconnect to a
 * different broker, etc.
 *
 * <p>The first broker's admin and client are exposed as the "primary" handles via
 * {@link #getAdmin()} and {@link #getClient()}; per-broker handles are available via
 * {@link #getBrokers()}, {@link #getAdmins()}, and {@link #getClients()}. Lookups against any
 * broker correctly redirect to the broker that owns the requested topic, so most tests should
 * just use the primary handles.
 *
 * <p>Lazy on first call to {@link #get()}; closed via JVM shutdown hook.
 *
 * @see SharedMultiBrokerPulsarBaseTest
 */
@CustomLog
public class SharedMultiBrokerPulsarCluster {

    public static final String CLUSTER_NAME = "multi-broker-test-cluster";
    public static final String TENANT_NAME = "multi-broker-test-tenant";

    /**
     * Number of brokers in the shared cluster. Three is the minimum that lets us exercise
     * controller-leader failover (one leader + at least two followers) and segment placement
     * across more than one broker.
     */
    public static final int NUM_BROKERS = 3;

    private static volatile SharedMultiBrokerPulsarCluster instance;

    private OxiaContainer oxiaServer;
    private String metadataStoreUrl;
    private BKCluster bkCluster;
    private final List<PulsarService> brokers = new ArrayList<>(NUM_BROKERS);
    private final List<PulsarAdmin> admins = new ArrayList<>(NUM_BROKERS);
    private final List<PulsarClient> clients = new ArrayList<>(NUM_BROKERS);

    /** Returns the singleton instance, starting the cluster on first invocation. */
    public static SharedMultiBrokerPulsarCluster get() throws Exception {
        if (instance == null) {
            synchronized (SharedMultiBrokerPulsarCluster.class) {
                if (instance == null) {
                    SharedMultiBrokerPulsarCluster cluster = new SharedMultiBrokerPulsarCluster();
                    cluster.start();
                    instance = cluster;
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        try {
                            instance.close();
                        } catch (Exception e) {
                            log.error().exception(e).log("Failed to close SharedMultiBrokerPulsarCluster");
                        }
                    }));
                }
            }
        }
        return instance;
    }

    /** All brokers in the cluster, in start order. */
    public List<PulsarService> getBrokers() {
        return Collections.unmodifiableList(brokers);
    }

    /** Per-broker {@link PulsarAdmin} handles, in the same order as {@link #getBrokers()}. */
    public List<PulsarAdmin> getAdmins() {
        return Collections.unmodifiableList(admins);
    }

    /** Per-broker {@link PulsarClient} handles, in the same order as {@link #getBrokers()}. */
    public List<PulsarClient> getClients() {
        return Collections.unmodifiableList(clients);
    }

    /** Convenience: the first broker's admin. Lookups redirect to topic-owning brokers. */
    public PulsarAdmin getAdmin() {
        return admins.get(0);
    }

    /** Convenience: the first broker's client. Lookups redirect to topic-owning brokers. */
    public PulsarClient getClient() {
        return clients.get(0);
    }

    @SuppressWarnings("deprecation")
    private void start() throws Exception {
        log.info().attr("brokers", NUM_BROKERS).log("Starting SharedMultiBrokerPulsarCluster");

        // Real Oxia server (not the in-memory metadata store). Per-topic leader election
        // (used by the ScalableTopicController) relies on per-session ephemeral nodes, and
        // the in-memory store treats every connection on the same JVM as the same session
        // — so multiple brokers all "win" the same election simultaneously. Oxia gives each
        // broker its own session and the proper ephemeral / CAS semantics. Container-based
        // because oxia ships no in-process server; tests skip cleanly when Docker isn't
        // available.
        oxiaServer = new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME);
        oxiaServer.start();
        metadataStoreUrl = "oxia://" + oxiaServer.getServiceAddress();

        // Single shared bookie. Same minimal config as SharedPulsarCluster — write quorum stays
        // at 1 across brokers because the bookie count is the limiting factor, not the brokers.
        ServerConfiguration bkConf = new ServerConfiguration();
        bkConf.setProperty("dbStorage_writeCacheMaxSizeMb", 32);
        bkConf.setProperty("dbStorage_readAheadCacheMaxSizeMb", 4);
        bkConf.setProperty("dbStorage_rocksDB_writeBufferSizeMB", 4);
        bkConf.setProperty("dbStorage_rocksDB_blockCacheSize", 4 * 1024 * 1024);
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
        bkConf.setLedgerStorageClass("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");

        bkCluster = BKCluster.builder()
                .baseServerConfiguration(bkConf)
                .metadataServiceUri(metadataStoreUrl)
                .numBookies(1)
                .clearOldData(true)
                .build();

        // Start NUM_BROKERS brokers. The first one provisions the cluster + tenant; the rest
        // discover them through the shared metadata store.
        for (int i = 0; i < NUM_BROKERS; i++) {
            PulsarService broker = startBroker(i);
            brokers.add(broker);

            PulsarAdmin admin = PulsarAdmin.builder()
                    .serviceHttpUrl(broker.getWebServiceAddress())
                    .build();
            admins.add(admin);

            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(broker.getBrokerServiceUrl())
                    .build();
            clients.add(client);

            if (i == 0) {
                admin.clusters().createCluster(CLUSTER_NAME,
                        ClusterData.builder()
                                .serviceUrl(broker.getWebServiceAddress())
                                .brokerServiceUrl(broker.getBrokerServiceUrl())
                                .build());
                admin.tenants().createTenant(TENANT_NAME,
                        TenantInfo.builder()
                                .allowedClusters(Set.of(CLUSTER_NAME))
                                .build());
            }
        }

        log.info()
                .attr("brokers", brokers.stream().map(PulsarService::getBrokerServiceUrl).toList())
                .log("SharedMultiBrokerPulsarCluster started");

        // Reset thread-leak baseline so cluster threads aren't reported against the first test.
        ThreadLeakDetectorListener.resetCapturedThreads();
    }

    private PulsarService startBroker(int index) throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setMetadataStoreUrl(metadataStoreUrl);
        config.setConfigurationMetadataStoreUrl(metadataStoreUrl);
        config.setClusterName(CLUSTER_NAME);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        // More bundles than brokers so the load balancer has room to spread ownership.
        config.setDefaultNumberOfNamespaceBundles(NUM_BROKERS * 2);
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
        config.setBrokerDeleteInactiveTopicsEnabled(false);
        config.setBrokerDeduplicationEnabled(true);

        // Reduce thread pool sizes — three brokers each spinning up the default counts is heavy.
        config.setNumIOThreads(2);
        config.setNumOrderedExecutorThreads(2);
        config.setNumHttpServerThreads(4);
        config.setBookkeeperClientNumWorkerThreads(2);
        config.setBookkeeperClientNumIoThreads(2);
        config.setNumCacheExecutorThreadPoolSize(1);
        config.setManagedLedgerNumSchedulerThreads(2);
        config.setTopicOrderedExecutorThreadNum(4);

        // Load balancer is what makes this multi-broker. Disable shedding to keep tests
        // deterministic: bundles assigned at first lookup don't move around mid-test.
        config.setLoadBalancerEnabled(true);
        config.setLoadBalancerSheddingEnabled(false);

        log.info().attr("index", index).log("Starting broker");
        PulsarService broker = new PulsarService(config);
        broker.start();
        log.info().attr("index", index)
                .attr("broker", broker.getBrokerServiceUrl())
                .attr("web", broker.getWebServiceAddress())
                .log("Broker started");
        return broker;
    }

    private void close() throws Exception {
        log.info("Closing SharedMultiBrokerPulsarCluster");
        // Tear down in reverse order: clients first so they don't interfere with broker shutdown.
        for (int i = clients.size() - 1; i >= 0; i--) {
            try {
                clients.get(i).close();
            } catch (Exception e) {
                log.warn().attr("index", i).exceptionMessage(e).log("Failed to close client");
            }
        }
        for (int i = admins.size() - 1; i >= 0; i--) {
            try {
                admins.get(i).close();
            } catch (Exception e) {
                log.warn().attr("index", i).exceptionMessage(e).log("Failed to close admin");
            }
        }
        for (int i = brokers.size() - 1; i >= 0; i--) {
            try {
                brokers.get(i).close();
            } catch (Exception e) {
                log.warn().attr("index", i).exceptionMessage(e).log("Failed to close broker");
            }
        }
        if (bkCluster != null) {
            bkCluster.close();
        }
        if (oxiaServer != null) {
            oxiaServer.close();
        }
    }
}
