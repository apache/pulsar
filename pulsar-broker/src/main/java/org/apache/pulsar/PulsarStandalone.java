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
package org.apache.pulsar;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.TRANSACTION_COORDINATOR_ASSIGN;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Sets;
import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.WorkerServiceLoader;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarStandalone implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PulsarStandalone.class);

    PulsarService broker;
    PulsarAdmin admin;
    LocalBookkeeperEnsemble bkEnsemble;
    ServiceConfiguration config;
    WorkerService fnWorkerService;
    WorkerConfig workerConfig;

    public void setBroker(PulsarService broker) {
        this.broker = broker;
    }

    public void setAdmin(PulsarAdmin admin) {
        this.admin = admin;
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

    @Parameter(names = { "-c", "--config" }, description = "Configuration file path", required = true)
    private String configFile;

    @Parameter(names = { "--wipe-data" }, description = "Clean up previous ZK/BK data")
    private boolean wipeData = false;

    @Parameter(names = { "--num-bookies" }, description = "Number of local Bookies")
    private int numOfBk = 1;

    @Parameter(names = { "--zookeeper-port" }, description = "Local zookeeper's port")
    private int zkPort = 2181;

    @Parameter(names = { "--bookkeeper-port" }, description = "Local bookies base port")
    private int bkPort = 3181;

    @Parameter(names = { "--zookeeper-dir" }, description = "Local zooKeeper's data directory")
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
    private String fnWorkerConfigFile =
            Paths.get("").toAbsolutePath().normalize().toString() + "/conf/functions_worker.yml";

    @Parameter(names = {"-nss", "--no-stream-storage"}, description = "Disable stream storage")
    private boolean noStreamStorage = false;

    @Parameter(names = { "--stream-storage-port" }, description = "Local bookies stream storage port")
    private int streamStoragePort = 4181;

    @Parameter(names = { "-a", "--advertised-address" }, description = "Standalone broker advertised address")
    private String advertisedAddress = null;

    @Parameter(names = { "-h", "--help" }, description = "Show this help message")
    private boolean help = false;

    public void start() throws Exception {

        if (config == null) {
            log.error("Failed to load configuration");
            System.exit(1);
        }

        log.debug("--- setup PulsarStandaloneStarter ---");

        if (!this.isOnlyBroker()) {
            ServerConfiguration bkServerConf = new ServerConfiguration();
            bkServerConf.loadConf(new File(configFile).toURI().toURL());

            // Start LocalBookKeeper
            bkEnsemble = new LocalBookkeeperEnsemble(
                    this.getNumOfBk(), this.getZkPort(), this.getBkPort(), this.getStreamStoragePort(), this.getZkDir(),
                    this.getBkDir(), this.isWipeData(), "127.0.0.1");
            bkEnsemble.startStandalone(bkServerConf, !this.isNoStreamStorage());
        }

        if (this.isNoBroker()) {
            return;
        }

        // initialize the functions worker
        if (!this.isNoFunctionsWorker()) {
            workerConfig = PulsarService.initializeWorkerConfigFromBrokerConfig(
                config, this.getFnWorkerConfigFile());
            // worker talks to local broker
            if (this.isNoStreamStorage()) {
                // only set the state storage service url when state is enabled.
                workerConfig.setStateStorageServiceUrl(null);
            } else if (workerConfig.getStateStorageServiceUrl() == null) {
                workerConfig.setStateStorageServiceUrl("bk://127.0.0.1:" + this.getStreamStoragePort());
            }
            fnWorkerService = WorkerServiceLoader.load(workerConfig);
        } else {
            workerConfig = new WorkerConfig();
        }

        // Start Broker
        broker = new PulsarService(config,
                                   workerConfig,
                                   Optional.ofNullable(fnWorkerService),
                                   (exitCode) -> {
                                       log.info("Halting standalone process with code {}", exitCode);
                                       LogManager.shutdown();
                                       Runtime.getRuntime().halt(exitCode);
                                   });
        broker.start();

        final String cluster = config.getClusterName();
        final AdvertisedListener internalListener = ServiceConfigurationUtils.getInternalListener(config, "pulsar");
        if (!config.isTlsEnabled()) {
            checkArgument(config.getWebServicePort().isPresent(), "webServicePort must be present");
            checkArgument(internalListener.getBrokerServiceUrl() != null,
                    "plaintext must be configured on internal listener");
            URL webServiceUrl = new URL(String.format("http://%s:%d",
                    ServiceConfigurationUtils.getWebServiceAddress(config),
                    config.getWebServicePort().get()));
            admin = PulsarAdmin.builder().serviceHttpUrl(
                    webServiceUrl.toString()).authentication(
                    config.getBrokerClientAuthenticationPlugin(),
                    config.getBrokerClientAuthenticationParameters()).build();
            ClusterData clusterData = ClusterData.builder()
                    .serviceUrl(webServiceUrl.toString())
                    .brokerServiceUrl(internalListener.getBrokerServiceUrl().toString())
                    .build();
            createSampleNameSpace(clusterData, cluster);
        } else {
            checkArgument(config.getWebServicePortTls().isPresent(), "webServicePortTls must be present");
            checkArgument(internalListener.getBrokerServiceUrlTls() != null,
                    "TLS must be configured on internal listener");
            URL webServiceUrlTls = new URL(String.format("https://%s:%d",
                    ServiceConfigurationUtils.getWebServiceAddress(config),
                    config.getWebServicePortTls().get()));
            PulsarAdminBuilder builder = PulsarAdmin.builder()
                    .serviceHttpUrl(webServiceUrlTls.toString())
                    .authentication(
                            config.getBrokerClientAuthenticationPlugin(),
                            config.getBrokerClientAuthenticationParameters());

            // set trust store if needed.
            if (config.isBrokerClientTlsEnabled()) {
                if (config.isBrokerClientTlsEnabledWithKeyStore()) {
                    builder.useKeyStoreTls(true)
                            .tlsTrustStoreType(config.getBrokerClientTlsTrustStoreType())
                            .tlsTrustStorePath(config.getBrokerClientTlsTrustStore())
                            .tlsTrustStorePassword(config.getBrokerClientTlsTrustStorePassword());
                } else {
                    builder.tlsTrustCertsFilePath(config.getBrokerClientTrustCertsFilePath());
                }
                builder.allowTlsInsecureConnection(config.isTlsAllowInsecureConnection());
            }

            admin = builder.build();
            ClusterData clusterData = ClusterData.builder()
                    .serviceUrlTls(webServiceUrlTls.toString())
                    .brokerServiceUrlTls(internalListener.getBrokerServiceUrlTls().toString())
                    .build();
            createSampleNameSpace(clusterData, cluster);
        }

        //create default namespace
        createNameSpace(cluster, TopicName.PUBLIC_TENANT, TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE);
        //create pulsar system namespace
        createNameSpace(cluster, SYSTEM_NAMESPACE.getTenant(), SYSTEM_NAMESPACE.toString());
        if (config.isTransactionCoordinatorEnabled() && !admin.namespaces()
                .getTopics(SYSTEM_NAMESPACE.toString())
                .contains(TRANSACTION_COORDINATOR_ASSIGN.getPartition(0).toString())) {
            admin.topics().createPartitionedTopic(TRANSACTION_COORDINATOR_ASSIGN.toString(), 1);
        }

        log.debug("--- setup completed ---");
    }

    private void createNameSpace(String cluster, String publicTenant, String defaultNamespace) {
        try {
            if (!admin.tenants().getTenants().contains(publicTenant)) {
                admin.tenants().createTenant(publicTenant,
                        TenantInfo.builder()
                                .adminRoles(Sets.newHashSet(config.getSuperUserRoles()))
                                .allowedClusters(Sets.newHashSet(cluster))
                                .build());
            }
            if (!admin.namespaces().getNamespaces(publicTenant).contains(defaultNamespace)) {
                admin.namespaces().createNamespace(defaultNamespace);
                admin.namespaces().setNamespaceReplicationClusters(
                        defaultNamespace, Sets.newHashSet(config.getClusterName()));
            }
        } catch (PulsarAdminException e) {
            log.info(e.getMessage(), e);
        }
    }

    private void createSampleNameSpace(ClusterData clusterData, String cluster) {
        // Create a sample namespace
        final String tenant = "sample";
        final String globalCluster = "global";
        final String namespace = tenant + "/ns1";
        try {
            List<String> clusters = admin.clusters().getClusters();
            if (!clusters.contains(cluster)) {
                admin.clusters().createCluster(cluster, clusterData);
            } else {
                admin.clusters().updateCluster(cluster, clusterData);
            }
            // Create marker for "global" cluster
            if (!clusters.contains(globalCluster)) {
                admin.clusters().createCluster(globalCluster, ClusterData.builder().build());
            }

            if (!admin.tenants().getTenants().contains(tenant)) {
                admin.tenants().createTenant(tenant,
                        new TenantInfoImpl(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }

            if (!admin.namespaces().getNamespaces(tenant).contains(namespace)) {
                admin.namespaces().createNamespace(namespace);
            }
        } catch (PulsarAdminException e) {
            log.warn(e.getMessage(), e);
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

            if (bkEnsemble != null) {
                bkEnsemble.stop();
            }
        } catch (Exception e) {
            log.error("Shutdown failed: {}", e.getMessage(), e);
        }
    }
}
