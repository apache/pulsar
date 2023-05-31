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
package org.apache.pulsar;

import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.common.naming.SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN;
import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.netty.util.internal.PlatformDependent;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ShutdownUtil;
import org.apache.pulsar.functions.instance.state.PulsarMetadataStateStoreProviderImpl;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.WorkerServiceLoader;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.packages.management.storage.filesystem.FileSystemPackagesStorageProvider;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;

@Slf4j
public class PulsarStandalone implements AutoCloseable {

    private static final String PULSAR_STANDALONE_USE_ZOOKEEPER = "PULSAR_STANDALONE_USE_ZOOKEEPER";

    PulsarService broker;

    // This is used in compatibility mode
    LocalBookkeeperEnsemble bkEnsemble;

    // This is used from Pulsar 2.11 on, with new default settings
    BKCluster bkCluster;
    MetadataStoreExtended metadataStore;

    ServiceConfiguration config;
    WorkerService fnWorkerService;
    WorkerConfig workerConfig;

    public void setBroker(PulsarService broker) {
        this.broker = broker;
    }

    public void setBkEnsemble(LocalBookkeeperEnsemble bkEnsemble) {
        this.bkEnsemble = bkEnsemble;
    }

    public void setBkPort(int bkPort) {
        this.bkPort = bkPort;
    }

    public void setBkDir(String bkDir) {
        this.bkDir = bkDir;
    }

    public void setAdvertisedAddress(String advertisedAddress) {
        this.advertisedAddress = advertisedAddress;
    }

    public void setConfig(ServiceConfiguration config) {
        this.config = config;
    }

    public void setFnWorkerService(WorkerService fnWorkerService) {
        this.fnWorkerService = fnWorkerService;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public void setWipeData(boolean wipeData) {
        this.wipeData = wipeData;
    }

    public void setNumOfBk(int numOfBk) {
        this.numOfBk = numOfBk;
    }

    public void setZkPort(int zkPort) {
        this.zkPort = zkPort;
    }

    public void setZkDir(String zkDir) {
        this.zkDir = zkDir;
    }

    public void setNoBroker(boolean noBroker) {
        this.noBroker = noBroker;
    }

    public void setOnlyBroker(boolean onlyBroker) {
        this.onlyBroker = onlyBroker;
    }

    public void setNoFunctionsWorker(boolean noFunctionsWorker) {
        this.noFunctionsWorker = noFunctionsWorker;
    }

    public void setFnWorkerConfigFile(String fnWorkerConfigFile) {
        this.fnWorkerConfigFile = fnWorkerConfigFile;
    }

    public void setNoStreamStorage(boolean noStreamStorage) {
        this.noStreamStorage = noStreamStorage;
    }

    public void setStreamStoragePort(int streamStoragePort) {
        this.streamStoragePort = streamStoragePort;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public ServiceConfiguration getConfig() {
        return config;
    }

    public String getConfigFile() {
        return configFile;
    }

    public boolean isWipeData() {
        return wipeData;
    }

    public int getNumOfBk() {
        return numOfBk;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getBkPort() {
        return bkPort;
    }

    public String getZkDir() {
        return zkDir;
    }

    public String getBkDir() {
        return bkDir;
    }

    public boolean isNoBroker() {
        return noBroker;
    }

    public boolean isOnlyBroker() {
        return onlyBroker;
    }

    public boolean isNoFunctionsWorker() {
        return noFunctionsWorker;
    }

    public String getFnWorkerConfigFile() {
        return fnWorkerConfigFile;
    }

    public boolean isNoStreamStorage() {
        return noStreamStorage;
    }

    public int getStreamStoragePort() {
        return streamStoragePort;
    }

    public String getAdvertisedAddress() {
        return advertisedAddress;
    }

    public boolean isHelp() {
        return help;
    }

    @Parameter(names = { "-c", "--config" }, description = "Configuration file path")
    private String configFile;

    @Parameter(names = { "--wipe-data" }, description = "Clean up previous ZK/BK data")
    private boolean wipeData = false;

    @Parameter(names = { "--num-bookies" }, description = "Number of local Bookies")
    private int numOfBk = 1;

    @Parameter(names = { "--metadata-dir" },
            description = "Directory for storing metadata")
    private String metadataDir = "data/metadata";

    @Parameter(names = { "--metadata-url" },
            description = "Metadata store url")
    private String metadataStoreUrl = "";

    @Parameter(names = {"--zookeeper-port"}, description = "Local zookeeper's port",
            hidden = true)
    private int zkPort = 2181;

    @Parameter(names = { "--bookkeeper-port" }, description = "Local bookies base port")
    private int bkPort = 3181;

    @Parameter(names = { "--zookeeper-dir" },
            description = "Local zooKeeper's data directory",
            hidden = true)
    private String zkDir = "data/standalone/zookeeper";

    @Parameter(names = { "--bookkeeper-dir" }, description = "Local bookies base data directory")
    private String bkDir = "data/standalone/bookkeeper";

    @Parameter(names = { "--no-broker" }, description = "Only start ZK and BK services, no broker")
    private boolean noBroker = false;

    @Parameter(names = { "--only-broker" }, description = "Only start Pulsar broker service (no ZK, BK)")
    private boolean onlyBroker = false;

    @Parameter(names = {"-nfw", "--no-functions-worker"}, description = "Run functions worker with Broker")
    private boolean noFunctionsWorker = false;

    @Parameter(names = {"-fwc", "--functions-worker-conf"}, description = "Configuration file for Functions Worker")
    private String fnWorkerConfigFile = "conf/functions_worker.yml";

    @Parameter(names = {"-nss", "--no-stream-storage"}, description = "Disable stream storage")
    private boolean noStreamStorage = false;

    @Parameter(names = { "--stream-storage-port" }, description = "Local bookies stream storage port")
    private int streamStoragePort = 4181;

    @Parameter(names = { "-a", "--advertised-address" }, description = "Standalone broker advertised address")
    private String advertisedAddress = null;

    @Parameter(names = { "-h", "--help" }, description = "Show this help message")
    private boolean help = false;

    private boolean usingNewDefaultsPIP117;

    public void start() throws Exception {
        if (config == null) {
            log.error("Failed to load configuration");
            System.exit(1);
        }

        String forceUseZookeeperEnv = System.getenv(PULSAR_STANDALONE_USE_ZOOKEEPER);

        // Allow forcing to use ZK mode via an env variable. eg:
        // PULSAR_STANDALONE_USE_ZOOKEEPER=1
        if (StringUtils.equalsAnyIgnoreCase(forceUseZookeeperEnv, "1", "true")) {
            usingNewDefaultsPIP117 = false;
            log.info("Forcing to chose ZooKeeper metadata through environment variable");
        } else if (Paths.get(zkDir).toFile().exists()) {
            log.info("Found existing ZooKeeper metadata. Continuing with ZooKeeper");
            usingNewDefaultsPIP117 = false;
        } else {
            // There's no existing ZK data directory, or we're already using RocksDB for metadata
            usingNewDefaultsPIP117 = true;
        }

        log.debug("--- setup PulsarStandaloneStarter ---");

        if (!this.isOnlyBroker()) {
            if (usingNewDefaultsPIP117) {
                startBookieWithMetadataStore();
            } else {
                startBookieWithZookeeper();
            }
        }

        if (this.isNoBroker()) {
            return;
        }

        // initialize the functions worker
        if (!this.isNoFunctionsWorker()) {
            final String filepath = Path.of(getFnWorkerConfigFile()).toAbsolutePath().normalize().toString();
            workerConfig = PulsarService.initializeWorkerConfigFromBrokerConfig(config, filepath);
            if (usingNewDefaultsPIP117) {
                workerConfig.setStateStorageProviderImplementation(
                        PulsarMetadataStateStoreProviderImpl.class.getName());

                config.setEnablePackagesManagement(true);
                config.setFunctionsWorkerEnablePackageManagement(true);
                workerConfig.setFunctionsWorkerEnablePackageManagement(true);
                config.setPackagesManagementStorageProvider(FileSystemPackagesStorageProvider.class.getName());
            } else {
                // worker talks to local broker
                if (this.isNoStreamStorage()) {
                    // only set the state storage service url when state is enabled.
                    workerConfig.setStateStorageServiceUrl(null);
                } else if (workerConfig.getStateStorageServiceUrl() == null) {
                    workerConfig.setStateStorageServiceUrl("bk://127.0.0.1:" + this.getStreamStoragePort());
                }
            }
            fnWorkerService = WorkerServiceLoader.load(workerConfig);
        } else {
            workerConfig = new WorkerConfig();
        }

        config.setRunningStandalone(true);

        if (!usingNewDefaultsPIP117) {
            final String metadataStoreUrl =
                    ZKMetadataStore.ZK_SCHEME_IDENTIFIER + "localhost:" + this.getZkPort();
            config.setMetadataStoreUrl(metadataStoreUrl);
            config.setConfigurationMetadataStoreUrl(metadataStoreUrl);
            config.getProperties().setProperty("metadataStoreUrl", metadataStoreUrl);
            config.getProperties().setProperty("configurationMetadataStoreUrl", metadataStoreUrl);
        }

        // Start Broker
        broker = new PulsarService(config,
                workerConfig,
                Optional.ofNullable(fnWorkerService),
                PulsarStandalone::processTerminator);
        broker.start();

        final String cluster = config.getClusterName();

        //create default namespace
        createNameSpace(cluster, TopicName.PUBLIC_TENANT,
                NamespaceName.get(TopicName.PUBLIC_TENANT, TopicName.DEFAULT_NAMESPACE));
        //create pulsar system namespace
        createNameSpace(cluster, SYSTEM_NAMESPACE.getTenant(), SYSTEM_NAMESPACE);
        if (config.isTransactionCoordinatorEnabled()) {
            NamespaceResources.PartitionedTopicResources partitionedTopicResources =
                    broker.getPulsarResources().getNamespaceResources().getPartitionedTopicResources();
            Optional<PartitionedTopicMetadata> getResult =
                    partitionedTopicResources.getPartitionedTopicMetadataAsync(TRANSACTION_COORDINATOR_ASSIGN).get();
            if (!getResult.isPresent()) {
                partitionedTopicResources.createPartitionedTopic(TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));
            }
        }

        log.debug("--- setup completed ---");
    }

    private void createNameSpace(String cluster, String publicTenant, NamespaceName ns) throws Exception {
        PulsarAdmin admin = broker.getAdminClient();
        try {
            final List<String> clusters = admin.clusters().getClusters();
            if (!clusters.contains(cluster)) {
                admin.clusters().createCluster(cluster, ClusterData.builder()
                        .serviceUrl(broker.getWebServiceAddress())
                        .serviceUrlTls(broker.getWebServiceAddressTls())
                        .brokerServiceUrl(broker.getBrokerServiceUrl())
                        .brokerServiceUrlTls(broker.getBrokerServiceUrlTls())
                        .build());
            }
            final List<String> tenants = admin.tenants().getTenants();
            if (!tenants.contains(publicTenant)) {
                admin.tenants().createTenant(publicTenant, TenantInfo.builder()
                        .adminRoles(Sets.newHashSet(config.getSuperUserRoles()))
                        .allowedClusters(Sets.newHashSet(cluster))
                        .build());
            }
            final List<String> namespaces = admin.namespaces().getNamespaces(publicTenant);
            if (!namespaces.contains(ns.toString())) {
                admin.namespaces().createNamespace(ns.toString(), config.getDefaultNumberOfNamespaceBundles());
            }
        } catch (PulsarAdminException e) {
            log.error("Failed to create namespace {} on cluster {} and tenant {}", ns, cluster, publicTenant, e);
        }
    }

    /** This method gets a builder to build an embedded pulsar instance
     * i.e.
     * <pre>
     * <code>
     * PulsarStandalone pulsarStandalone = PulsarStandalone.builder().build();
     * pulsarStandalone.start();
     * pulsarStandalone.stop();
     * </code>
     * </pre>
     * @return PulsarStandaloneBuilder instance
     */
    public static PulsarStandaloneBuilder builder(){
        return PulsarStandaloneBuilder.instance();
    }

    @Override
    public void close() {
        try {
            if (fnWorkerService != null) {
                fnWorkerService.stop();
            }

            if (broker != null) {
                broker.close();
            }

            if (bkCluster != null) {
                bkCluster.close();
            }

            if (bkEnsemble != null) {
                bkEnsemble.stop();
            }
        } catch (Exception e) {
            log.error("Shutdown failed: {}", e.getMessage(), e);
        }
    }

    @VisibleForTesting
    void startBookieWithMetadataStore() throws Exception {
        if (StringUtils.isBlank(metadataStoreUrl)){
            log.info("Starting BK with RocksDb metadata store");
            metadataStoreUrl = "rocksdb://" + Paths.get(metadataDir).toAbsolutePath();
        } else {
            log.info("Starting BK with metadata store: {}", metadataStoreUrl);
        }

        ServerConfiguration bkServerConf = new ServerConfiguration();
        bkServerConf.loadConf(new File(configFile).toURI().toURL());
        calculateCacheSize(bkServerConf);
        bkCluster = BKCluster.builder()
                .baseServerConfiguration(bkServerConf)
                .metadataServiceUri(metadataStoreUrl)
                .bkPort(bkPort)
                .numBookies(numOfBk)
                .dataDir(bkDir)
                .clearOldData(wipeData)
                .build();
        config.setBookkeeperNumberOfChannelsPerBookie(1);
        config.setMetadataStoreUrl(metadataStoreUrl);
    }

    private void startBookieWithZookeeper() throws Exception {
        log.info("Starting BK & ZK cluster");
        ServerConfiguration bkServerConf = new ServerConfiguration();
        bkServerConf.loadConf(new File(configFile).toURI().toURL());
        calculateCacheSize(bkServerConf);
        // Start LocalBookKeeper
        bkEnsemble = new LocalBookkeeperEnsemble(
                this.getNumOfBk(), this.getZkPort(), this.getBkPort(), this.getStreamStoragePort(), this.getZkDir(),
                this.getBkDir(), this.isWipeData(), "127.0.0.1");
        bkEnsemble.startStandalone(bkServerConf, !this.isNoStreamStorage());
        config.setMetadataStoreUrl("zk:127.0.0.1:" + zkPort);
    }

    private void calculateCacheSize(ServerConfiguration bkServerConf) {
        String writeCacheMaxSizeMb = "dbStorage_writeCacheMaxSizeMb";
        String readAheadCacheMaxSizeMb = "dbStorage_readAheadCacheMaxSizeMb";
        Object writeCache = bkServerConf.getProperty(writeCacheMaxSizeMb);
        Object readCache = bkServerConf.getProperty(readAheadCacheMaxSizeMb);
        // we need to add one broker and one zk (if needed) to calculate the default cache
        int instanceCount = usingNewDefaultsPIP117 ? (1 + numOfBk) : (2 + numOfBk);
        long defaultCacheMB = PlatformDependent.maxDirectMemory() / (1024 * 1024) / instanceCount / 4;
        if (writeCache == null || writeCache.equals("")) {
            bkServerConf.setProperty(writeCacheMaxSizeMb, defaultCacheMB);
        }
        if (readCache == null || readCache.equals("")) {
            bkServerConf.setProperty(readAheadCacheMaxSizeMb, defaultCacheMB);
        }
    }

    private static void processTerminator(int exitCode) {
        log.info("Halting standalone process with code {}", exitCode);
        ShutdownUtil.triggerImmediateForcefulShutdown(exitCode);
    }


}
