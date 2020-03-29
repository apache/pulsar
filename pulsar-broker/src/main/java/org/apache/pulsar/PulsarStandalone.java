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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.beust.jcommander.Parameter;
import com.google.common.collect.Sets;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Optional;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
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

    public void setConfig(ServiceConfiguration config) { this.config = config; }

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
    private String fnWorkerConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/functions_worker.yml";

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
            WorkerConfig workerConfig;
            if (isBlank(this.getFnWorkerConfigFile())) {
                workerConfig = new WorkerConfig();
            } else {
                workerConfig = WorkerConfig.load(this.getFnWorkerConfigFile());
            }
            // worker talks to local broker
            if (this.isNoStreamStorage()) {
                // only set the state storage service url when state is enabled.
                workerConfig.setStateStorageServiceUrl(null);
            } else if (workerConfig.getStateStorageServiceUrl() == null) {
                workerConfig.setStateStorageServiceUrl("bk://127.0.0.1:" + this.getStreamStoragePort());
            }

            String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(
                config.getAdvertisedAddress());
            workerConfig.setWorkerHostname(hostname);
            workerConfig.setWorkerPort(config.getWebServicePort().get());
            workerConfig.setWorkerId(
                "c-" + config.getClusterName()
                    + "-fw-" + hostname
                    + "-" + workerConfig.getWorkerPort());
            // inherit broker authorization setting
            workerConfig.setAuthenticationEnabled(config.isAuthenticationEnabled());
            workerConfig.setAuthenticationProviders(config.getAuthenticationProviders());

            workerConfig.setAuthorizationEnabled(config.isAuthorizationEnabled());
            workerConfig.setAuthorizationProvider(config.getAuthorizationProvider());
            workerConfig.setConfigurationStoreServers(config.getConfigurationStoreServers());
            workerConfig.setZooKeeperSessionTimeoutMillis(config.getZooKeeperSessionTimeoutMillis());
            workerConfig.setZooKeeperOperationTimeoutSeconds(config.getZooKeeperOperationTimeoutSeconds());

            workerConfig.setTlsHostnameVerificationEnable(false);

            workerConfig.setTlsAllowInsecureConnection(config.isTlsAllowInsecureConnection());
            workerConfig.setTlsTrustCertsFilePath(config.getTlsTrustCertsFilePath());

            // client in worker will use this config to authenticate with broker
            workerConfig.setClientAuthenticationPlugin(config.getBrokerClientAuthenticationPlugin());
            workerConfig.setClientAuthenticationParameters(config.getBrokerClientAuthenticationParameters());

            // inherit super users
            workerConfig.setSuperUserRoles(config.getSuperUserRoles());

            fnWorkerService = new WorkerService(workerConfig);
        }

        // Start Broker
        broker = new PulsarService(config, Optional.ofNullable(fnWorkerService));
        broker.start();

        broker.getTransactionMetadataStoreService().addTransactionMetadataStore(TransactionCoordinatorID.get(0));

        final String cluster = config.getClusterName();

        if (!config.isTlsEnabled()) {
            URL webServiceUrl = new URL(
                    String.format("http://%s:%d", config.getAdvertisedAddress(), config.getWebServicePort().get()));
            String brokerServiceUrl = String.format("pulsar://%s:%d", config.getAdvertisedAddress(),
                    config.getBrokerServicePort().get());
            admin = PulsarAdmin.builder().serviceHttpUrl(webServiceUrl.toString()).authentication(
                    config.getBrokerClientAuthenticationPlugin(), config.getBrokerClientAuthenticationParameters()).build();
            ClusterData clusterData = new ClusterData(webServiceUrl.toString(), null, brokerServiceUrl, null);
            createSampleNameSpace(clusterData, cluster);
        } else {
            URL webServiceUrlTls = new URL(
                    String.format("http://%s:%d", config.getAdvertisedAddress(), config.getWebServicePortTls().get()));
            String brokerServiceUrlTls = String.format("pulsar+ssl://%s:%d", config.getAdvertisedAddress(),
                    config.getBrokerServicePortTls().get());
            admin = PulsarAdmin.builder().serviceHttpUrl(webServiceUrlTls.toString()).authentication(
                    config.getBrokerClientAuthenticationPlugin(), config.getBrokerClientAuthenticationParameters()).build();
            ClusterData clusterData = new ClusterData(null, webServiceUrlTls.toString(), null, brokerServiceUrlTls);
            createSampleNameSpace(clusterData, cluster);
        }

        createDefaultNameSpace(cluster);

        log.debug("--- setup completed ---");
    }

    private void createDefaultNameSpace(String cluster) {
        // Create a public tenant and default namespace
        final String publicTenant = TopicName.PUBLIC_TENANT;
        final String defaultNamespace = TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE;
        try {
            if (!admin.tenants().getTenants().contains(publicTenant)) {
                admin.tenants().createTenant(publicTenant,
                        new TenantInfo(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }
            if (!admin.namespaces().getNamespaces(publicTenant).contains(defaultNamespace)) {
                admin.namespaces().createNamespace(defaultNamespace);
                admin.namespaces().setNamespaceReplicationClusters(defaultNamespace, Sets.newHashSet(config.getClusterName()));
            }
        } catch (PulsarAdminException e) {
            log.info(e.getMessage());
        }
    }

    private void createSampleNameSpace(ClusterData clusterData, String cluster) {
        // Create a sample namespace
        final String property = "sample";
        final String globalCluster = "global";
        final String namespace = property + "/" + cluster + "/ns1";
        try {
            if (!admin.clusters().getClusters().contains(cluster)) {
                admin.clusters().createCluster(cluster, clusterData);
            } else {
                admin.clusters().updateCluster(cluster, clusterData);
            }

            // Create marker for "global" cluster
            if (!admin.clusters().getClusters().contains(globalCluster)) {
                admin.clusters().createCluster(globalCluster, new ClusterData(null, null));
            }

            if (!admin.tenants().getTenants().contains(property)) {
                admin.tenants().createTenant(property,
                        new TenantInfo(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }

            if (!admin.namespaces().getNamespaces(property).contains(namespace)) {
                admin.namespaces().createNamespace(namespace);
            }
        } catch (PulsarAdminException e) {
            log.info(e.getMessage());
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
            log.error("Shutdown failed: {}", e.getMessage());
        }
    }
}
