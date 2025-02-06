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
package org.apache.pulsar.tests.integration.topologies;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_HTTPS_PORT;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_PORT_TLS;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.CS_PORT;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.PULSAR_CONTAINERS_LEAVE_RUNNING;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.ZK_PORT;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.tests.integration.containers.BKContainer;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.CSContainer;
import org.apache.pulsar.tests.integration.containers.ProxyContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.PulsarInitMetadataContainer;
import org.apache.pulsar.tests.integration.containers.WorkerContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.oxia.OxiaContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/**
 * Pulsar Cluster in containers.
 */
@Slf4j
public class PulsarCluster {

    public static final String ADMIN_SCRIPT = "/pulsar/bin/pulsar-admin";
    public static final String CLIENT_SCRIPT = "/pulsar/bin/pulsar-client";
    public static final String PULSAR_COMMAND_SCRIPT = "/pulsar/bin/pulsar";
    public static final String CURL = "/usr/bin/curl";

    /**
     * Pulsar Cluster Spec
     *
     * @param spec pulsar cluster spec.
     * @return the built pulsar cluster
     */
    public static PulsarCluster forSpec(PulsarClusterSpec spec) {
        return forSpec(spec, Network.newNetwork());
    }

    public static PulsarCluster forSpec(PulsarClusterSpec spec, Network network) {
        checkArgument(network != null, "Network should not be null");
        CSContainer csContainer = null;
        if (!spec.enableOxia) {
            csContainer = new CSContainer(spec.clusterName)
                    .withNetwork(network)
                    .withNetworkAliases(CSContainer.NAME);
        }
        return new PulsarCluster(spec, network, csContainer, false);
    }

    public static PulsarCluster forSpec(PulsarClusterSpec spec, CSContainer csContainer) {
        return new PulsarCluster(spec, csContainer.getNetwork(), csContainer, true);
    }

    @Getter
    private final PulsarClusterSpec spec;

    public boolean closeNetworkOnExit = true;
    @Getter
    private final String clusterName;
    private final Network network;
    private final ZKContainer zkContainer;

    private final OxiaContainer oxiaContainer;
    private final CSContainer csContainer;
    private final boolean sharedCsContainer;
    private final Map<String, BKContainer> bookieContainers;
    private final Map<String, BrokerContainer> brokerContainers;
    private final Map<String, WorkerContainer> workerContainers;
    private final ProxyContainer proxyContainer;
    private Map<String, GenericContainer<?>> externalServices = Collections.emptyMap();
    private Map<String, Map<String, String>> externalServiceEnvs;
    private final Map<String, String> functionWorkerEnvs;
    private final List<Integer> functionWorkerAdditionalPorts;

    private final String metadataStoreUrl;
    private final String configurationMetadataStoreUrl;

    private PulsarCluster(PulsarClusterSpec spec, Network network, CSContainer csContainer, boolean sharedCsContainer) {
        this.spec = spec;
        this.sharedCsContainer = sharedCsContainer;
        this.clusterName = spec.clusterName();
        if (network != null) {
            this.network = network;
        } else if (csContainer != null) {
            this.network = csContainer.getNetwork();
        } else {
            this.network = Network.newNetwork();
        }

        if (spec.enableOxia) {
            this.zkContainer = null;
            this.oxiaContainer = new OxiaContainer(clusterName);
            this.oxiaContainer
                    .withNetwork(network)
                    .withNetworkAliases(appendClusterName(OxiaContainer.NAME));
            metadataStoreUrl = "oxia://" + oxiaContainer.getServiceAddress();
            configurationMetadataStoreUrl = metadataStoreUrl;
        } else {
            this.oxiaContainer = null;
            this.zkContainer = new ZKContainer(clusterName);
            this.zkContainer
                    .withNetwork(network)
                    .withNetworkAliases(appendClusterName(ZKContainer.NAME))
                    .withEnv("clusterName", clusterName)
                    .withEnv("zkServers", appendClusterName(ZKContainer.NAME))
                    .withEnv("configurationStore", CSContainer.NAME + ":" + CS_PORT)
                    .withEnv("forceSync", "no")
                    .withEnv("pulsarNode", appendClusterName("pulsar-broker-0"));
            metadataStoreUrl = appendClusterName(ZKContainer.NAME);
            configurationMetadataStoreUrl = CSContainer.NAME + ":" + CS_PORT;
        }

        this.csContainer = csContainer;

        this.bookieContainers = Maps.newTreeMap();
        this.brokerContainers = Maps.newTreeMap();
        this.workerContainers = Maps.newTreeMap();

        this.proxyContainer = new ProxyContainer(clusterName, appendClusterName(ProxyContainer.NAME), spec.enableTls)
                .withNetwork(network)
                .withNetworkAliases(appendClusterName("pulsar-proxy"))
                .withEnv("metadataStoreUrl", metadataStoreUrl)
                .withEnv("configurationMetadataStoreUrl", configurationMetadataStoreUrl)
                .withEnv("clusterName", clusterName);
        // enable mTLS
        if (spec.enableTls) {
            proxyContainer
                    .withEnv("webServicePortTls", String.valueOf(BROKER_HTTPS_PORT))
                    .withEnv("servicePortTls", String.valueOf(BROKER_PORT_TLS))
                    .withEnv("forwardAuthorizationCredentials", "true")
                    .withEnv("tlsRequireTrustedClientCertOnConnect", "true")
                    .withEnv("tlsAllowInsecureConnection", "false")
                    .withEnv("tlsCertificateFilePath", "/pulsar/certificate-authority/server-keys/proxy.cert.pem")
                    .withEnv("tlsKeyFilePath", "/pulsar/certificate-authority/server-keys/proxy.key-pk8.pem")
                    .withEnv("tlsTrustCertsFilePath", "/pulsar/certificate-authority/certs/ca.cert.pem")
                    .withEnv("brokerClientAuthenticationPlugin", AuthenticationTls.class.getName())
                    .withEnv("brokerClientAuthenticationParameters", String.format("tlsCertFile:%s,tlsKeyFile:%s",
                            "/pulsar/certificate-authority/client-keys/admin.cert.pem",
                            "/pulsar/certificate-authority/client-keys/admin.key-pk8.pem"))
                    .withEnv("tlsEnabledWithBroker", "true")
                    .withEnv("brokerClientTrustCertsFilePath", "/pulsar/certificate-authority/certs/ca.cert.pem")
                    .withEnv("brokerClientCertificateFilePath",
                            "/pulsar/certificate-authority/server-keys/proxy.cert.pem")
                    .withEnv("brokerClientKeyFilePath", "/pulsar/certificate-authority/server-keys/proxy.key-pk8.pem");

        }
        if (spec.proxyEnvs != null) {
            spec.proxyEnvs.forEach(this.proxyContainer::withEnv);
        }
        if (spec.proxyMountFiles != null) {
            spec.proxyMountFiles.forEach(this.proxyContainer::withFileSystemBind);
        }
        if (spec.proxyAdditionalPorts != null) {
            spec.proxyAdditionalPorts.forEach(this.proxyContainer::addExposedPort);
        }

        // create bookies
        bookieContainers.putAll(
                runNumContainers("bookie", spec.numBookies(), (name) -> {
                    BKContainer bookieContainer = new BKContainer(clusterName, name)
                            .withNetwork(network)
                            .withNetworkAliases(appendClusterName(name))
                            .withEnv("metadataServiceUri", "metadata-store:" + metadataStoreUrl)
                            .withEnv("useHostNameAsBookieID", "true")
                            // Disable fsyncs for tests since they're slow within the containers
                            .withEnv("journalSyncData", "false")
                            .withEnv("journalMaxGroupWaitMSec", "0")
                            .withEnv("clusterName", clusterName)
                            .withEnv("PULSAR_PREFIX_diskUsageWarnThreshold", "0.95")
                            .withEnv("diskUsageThreshold", "0.99")
                            .withEnv("PULSAR_PREFIX_diskUsageLwmThreshold", "0.97")
                            .withEnv("nettyMaxFrameSizeBytes", String.valueOf(spec.maxMessageSize))
                            .withEnv("ledgerDirectories", "data/bookkeeper/" + name + "/ledgers")
                            .withEnv("journalDirectory", "data/bookkeeper/" + name + "/journal");
                    if (spec.bookkeeperEnvs != null) {
                        bookieContainer.withEnv(spec.bookkeeperEnvs);
                    }
                    if (spec.bookieAdditionalPorts != null) {
                        spec.bookieAdditionalPorts.forEach(bookieContainer::addExposedPort);
                    }
                    return bookieContainer;
                })
        );

        // create brokers
        brokerContainers.putAll(
                runNumContainers("broker", spec.numBrokers(), (name) -> {
                            BrokerContainer brokerContainer =
                                    new BrokerContainer(clusterName, appendClusterName(name), spec.enableTls)
                                            .withNetwork(network)
                                            .withNetworkAliases(appendClusterName(name))
                                            .withEnv("metadataStoreUrl", metadataStoreUrl)
                                            .withEnv("configurationMetadataStoreUrl", configurationMetadataStoreUrl)
                                            .withEnv("clusterName", clusterName)
                                            .withEnv("brokerServiceCompactionMonitorIntervalInSeconds", "1")
                                            .withEnv("loadBalancerOverrideBrokerNicSpeedGbps", "1")
                                            // used in s3 tests
                                            .withEnv("AWS_ACCESS_KEY_ID", "accesskey").withEnv("AWS_SECRET_KEY",
                                                    "secretkey")
                                            .withEnv("maxMessageSize", "" + spec.maxMessageSize);
                            if (spec.enableTls) {
                                // enable mTLS
                                brokerContainer
                                        .withEnv("webServicePortTls", String.valueOf(BROKER_HTTPS_PORT))
                                        .withEnv("brokerServicePortTls", String.valueOf(BROKER_PORT_TLS))
                                        .withEnv("authenticateOriginalAuthData", "true")
                                        .withEnv("tlsAllowInsecureConnection", "false")
                                        .withEnv("tlsRequireTrustedClientCertOnConnect", "true")
                                        .withEnv("tlsTrustCertsFilePath", "/pulsar/certificate-authority/certs/ca"
                                                + ".cert.pem")
                                        .withEnv("tlsCertificateFilePath",
                                                "/pulsar/certificate-authority/server-keys/broker.cert.pem")
                                        .withEnv("tlsKeyFilePath",
                                                "/pulsar/certificate-authority/server-keys/broker.key-pk8.pem");
                            }
                            if (spec.queryLastMessage) {
                                brokerContainer.withEnv("bookkeeperExplicitLacIntervalInMills", "10");
                                brokerContainer.withEnv("bookkeeperUseV2WireProtocol", "false");
                            }
                            if (spec.brokerEnvs != null) {
                                brokerContainer.withEnv(spec.brokerEnvs);
                            }
                            if (spec.brokerMountFiles != null) {
                                spec.brokerMountFiles.forEach(brokerContainer::withFileSystemBind);
                            }
                            if (spec.brokerAdditionalPorts() != null) {
                                spec.brokerAdditionalPorts().forEach(brokerContainer::addExposedPort);
                            }
                            return brokerContainer;
                        }
                ));

        if (spec.dataContainer != null) {
            if (!sharedCsContainer && csContainer != null) {
                csContainer.withVolumesFrom(spec.dataContainer, BindMode.READ_WRITE);
            }
            if (zkContainer != null) {
                zkContainer.withVolumesFrom(spec.dataContainer, BindMode.READ_WRITE);
            }
            proxyContainer.withVolumesFrom(spec.dataContainer, BindMode.READ_WRITE);

            bookieContainers.values().forEach(c -> c.withVolumesFrom(spec.dataContainer, BindMode.READ_WRITE));
            brokerContainers.values().forEach(c -> c.withVolumesFrom(spec.dataContainer, BindMode.READ_WRITE));
            workerContainers.values().forEach(c -> c.withVolumesFrom(spec.dataContainer, BindMode.READ_WRITE));
        }

        spec.classPathVolumeMounts.forEach((key, value) -> {
            if (zkContainer != null) {
                zkContainer.withClasspathResourceMapping(key, value, BindMode.READ_WRITE);
            }
            if (!sharedCsContainer && csContainer != null) {
                csContainer.withClasspathResourceMapping(key, value, BindMode.READ_WRITE);
            }
            proxyContainer.withClasspathResourceMapping(key, value, BindMode.READ_WRITE);

            bookieContainers.values().forEach(c -> c.withClasspathResourceMapping(key, value, BindMode.READ_WRITE));
            brokerContainers.values().forEach(c -> c.withClasspathResourceMapping(key, value, BindMode.READ_WRITE));
            workerContainers.values().forEach(c -> c.withClasspathResourceMapping(key, value, BindMode.READ_WRITE));
        });

        functionWorkerEnvs = spec.functionWorkerEnvs;
        functionWorkerAdditionalPorts = spec.functionWorkerAdditionalPorts;
    }

    public String getPlainTextServiceUrl() {
        return proxyContainer.getPlainTextServiceUrl();
    }

    public String getHttpServiceUrl() {
        return proxyContainer.getHttpServiceUrl();
    }

    public String getAnyBrokersHttpsServiceUrl() {
        return getAnyBroker().getHttpsServiceUrl();
    }

    public String getAnyBrokersServiceUrlTls() {
        return getAnyBroker().getServiceUrlTls();
    }

    public String getAllBrokersHttpServiceUrl() {
        String multiUrl = "http://";
        Iterator<BrokerContainer> brokers = getBrokers().iterator();
        while (brokers.hasNext()) {
            BrokerContainer broker = brokers.next();
            multiUrl += broker.getHost() + ":" + broker.getMappedPort(BROKER_HTTP_PORT);
            if (brokers.hasNext()) {
                multiUrl += ",";
            }
        }
        return multiUrl;
    }

    public String getZKConnString() {
        return zkContainer.getHost() + ":" + zkContainer.getMappedPort(ZK_PORT);
    }

    public String getCSConnString() {
        return csContainer.getHost() + ":" + csContainer.getMappedPort(CS_PORT);
    }

    public Network getNetwork() {
        return network;
    }

    public Map<String, GenericContainer<?>> getExternalServices() {
        return externalServices;
    }

    public void start() throws Exception {
        start(true);
    }

    public void start(boolean doInit) throws Exception {

        if (!spec.enableOxia) {
            // start the local zookeeper
            zkContainer.start();
            log.info("Successfully started local zookeeper container.");

            // start the configuration store
            if (!sharedCsContainer) {
                csContainer.start();
                log.info("Successfully started configuration store container.");
            }
        } else {
            oxiaContainer.start();
        }

        if (doInit) {
            // Run cluster metadata initialization
            @Cleanup
            PulsarInitMetadataContainer init = new PulsarInitMetadataContainer(
                    network,
                    clusterName,
                    metadataStoreUrl,
                    configurationMetadataStoreUrl,
                    appendClusterName("pulsar-broker-0")
            );
            init.initialize();
        }

        // start bookies
        bookieContainers.values().forEach(BKContainer::start);
        log.info("Successfully started {} bookie containers.", bookieContainers.size());

        // start brokers
        this.startAllBrokers();
        log.info("Successfully started {} broker containers.", brokerContainers.size());

        // create proxy
        proxyContainer.start();
        log.info("Successfully started pulsar proxy.");

        log.info("Pulsar cluster {} is up running:", clusterName);
        log.info("\tBinary Service Url : {}", getPlainTextServiceUrl());
        log.info("\tHttp Service Url : {}", getHttpServiceUrl());

        // start external services
        this.externalServices = spec.externalServices;
        this.externalServiceEnvs = spec.externalServiceEnvs;
        if (null != externalServices) {
            externalServices.entrySet().parallelStream().forEach(service -> {
                GenericContainer<?> serviceContainer = service.getValue();
                serviceContainer.withNetwork(network);
                serviceContainer.withNetworkAliases(service.getKey());
                if (null != externalServiceEnvs && null != externalServiceEnvs.get(service.getKey())) {
                    Map<String, String> env =
                            externalServiceEnvs.getOrDefault(service.getKey(), Collections.emptyMap());
                    serviceContainer.withEnv(env);
                }
                PulsarContainer.configureLeaveContainerRunning(serviceContainer);
                serviceContainer.start();
                log.info("Successfully started external service {}.", service.getKey());
            });
        }
    }

    public void startService(String networkAlias,
                             GenericContainer<?> serviceContainer) {
        log.info("Starting external service {} ...", networkAlias);
        serviceContainer.withNetwork(network);
        serviceContainer.withNetworkAliases(networkAlias);
        PulsarContainer.configureLeaveContainerRunning(serviceContainer);
        serviceContainer.start();
        log.info("Successfully start external service {}", networkAlias);
    }

    public static void stopService(String networkAlias,
                                   GenericContainer<?> serviceContainer) {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            logIgnoringStopDueToLeaveRunning();
            return;
        }
        log.info("Stopping external service {} ...", networkAlias);
        serviceContainer.stop();
        log.info("Successfully stop external service {}", networkAlias);
    }


    private static <T extends PulsarContainer> Map<String, T> runNumContainers(String serviceName,
                                                                               int numContainers,
                                                                               Function<String, T> containerCreator) {
        Map<String, T> containers = Maps.newTreeMap();
        for (int i = 0; i < numContainers; i++) {
            String name = "pulsar-" + serviceName + "-" + i;
            T container = containerCreator.apply(name);
            containers.put(name, container);
        }
        return containers;
    }

    public synchronized void stop() {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            logIgnoringStopDueToLeaveRunning();
            return;
        }

        stopInParallel(workerContainers.values());

        if (externalServices != null) {
            stopInParallel(externalServices.values());
        }

        if (null != proxyContainer) {
            proxyContainer.stop();
        }

        stopInParallel(brokerContainers.values());

        stopInParallel(bookieContainers.values());

        if (!sharedCsContainer && null != csContainer) {
            csContainer.stop();
        }

        if (null != zkContainer) {
            zkContainer.stop();
        }

        if (oxiaContainer != null) {
            oxiaContainer.stop();
        }

        if (closeNetworkOnExit) {
            try {
                network.close();
            } catch (Exception e) {
                log.info("Failed to shutdown network for pulsar cluster {}", clusterName, e);
            }
        }
    }

    private static void stopInParallel(Collection<? extends GenericContainer<?>> containers) {
        containers.parallelStream()
                .filter(Objects::nonNull)
                .forEach(GenericContainer::stop);
    }

    public synchronized void setupFunctionWorkers(String suffix, FunctionRuntimeType runtimeType,
                                                  int numFunctionWorkers) {
        switch (runtimeType) {
            case THREAD:
                startFunctionWorkersWithThreadContainerFactory(suffix, numFunctionWorkers);
                break;
            case PROCESS:
                startFunctionWorkersWithProcessContainerFactory(suffix, numFunctionWorkers);
                break;
        }
    }

    private void startFunctionWorkersWithProcessContainerFactory(String suffix, int numFunctionWorkers) {
        workerContainers.putAll(runNumContainers(
            "functions-worker-process-" + suffix,
            numFunctionWorkers,
            (name) -> createWorkerContainer(name)
        ));
        this.startWorkers();
    }

    private WorkerContainer createWorkerContainer(String name) {
        String serviceUrl = "pulsar://pulsar-broker-0:" + PulsarContainer.BROKER_PORT;
        String httpServiceUrl = "http://pulsar-broker-0:" + PulsarContainer.BROKER_HTTP_PORT;
        return new WorkerContainer(clusterName, name)
                .withNetwork(network)
                .withNetworkAliases(name)
                // worker settings
                .withEnv("PF_workerId", name)
                .withEnv("PF_workerHostname", name)
                .withEnv("PF_workerPort", "" + PulsarContainer.BROKER_HTTP_PORT)
                .withEnv("PF_pulsarFunctionsCluster", clusterName)
                .withEnv("PF_pulsarServiceUrl", serviceUrl)
                .withEnv("PF_pulsarWebServiceUrl", httpServiceUrl)
                // script
                .withEnv("clusterName", clusterName)
                .withEnv("zookeeperServers", ZKContainer.NAME)
                // bookkeeper tools
                .withEnv("zkServers", ZKContainer.NAME)
                .withEnv(functionWorkerEnvs)
                .withExposedPorts(functionWorkerAdditionalPorts.toArray(new Integer[0]));
    }

    private void startFunctionWorkersWithThreadContainerFactory(String suffix, int numFunctionWorkers) {
        workerContainers.putAll(runNumContainers(
                "functions-worker-thread-" + suffix,
                numFunctionWorkers,
                (name) -> createWorkerContainer(name)
                        .withEnv("PF_functionRuntimeFactoryClassName",
                                "org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory")
                        .withEnv("PF_functionRuntimeFactoryConfigs_threadGroupName", "pf-container-group")
        ));
        this.startWorkers();
    }

    public synchronized void startWorkers() {
        // Start workers that have been initialized
        workerContainers.values().parallelStream().forEach(WorkerContainer::start);
        log.info("Successfully started {} worker containers.", workerContainers.size());
    }

    public synchronized void stopWorker(String workerName) {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            logIgnoringStopDueToLeaveRunning();
            return;
        }
        // Stop the named worker.
        WorkerContainer worker = workerContainers.get(workerName);
        if (worker == null) {
            log.warn("Failed to find the worker to stop ({})", workerName);
            return;
        }
        worker.stop();
        workerContainers.remove(workerName);
        log.info("Worker {} stopped and removed from the map of worker containers", workerName);
    }

    public synchronized void stopWorkers() {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            logIgnoringStopDueToLeaveRunning();
            return;
        }
        // Stop workers that have been initialized
        workerContainers.values().parallelStream().forEach(WorkerContainer::stop);
        workerContainers.clear();
    }

    public void startContainers(Map<String, GenericContainer<?>> containers) {
        containers.forEach((name, container) -> {
            PulsarContainer.configureLeaveContainerRunning(container);
            container
                    .withNetwork(network)
                    .withNetworkAliases(name)
                    .start();
            log.info("Successfully start container {}.", name);
        });
    }

    public static void stopContainers(Map<String, GenericContainer<?>> containers) {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            logIgnoringStopDueToLeaveRunning();
            return;
        }
        containers.values().parallelStream().forEach(GenericContainer::stop);
        log.info("Successfully stop containers : {}", containers);
    }

    private static void logIgnoringStopDueToLeaveRunning() {
        log.warn("Ignoring stop due to PULSAR_CONTAINERS_LEAVE_RUNNING=true.");
    }

    public BrokerContainer getAnyBroker() {
        return getAnyContainer(brokerContainers, "pulsar-broker");
    }

    public synchronized WorkerContainer getAnyWorker() {
        return getAnyContainer(workerContainers, "pulsar-functions-worker");
    }

    public synchronized List<WorkerContainer> getAlWorkers() {
        return new ArrayList<WorkerContainer>(workerContainers.values());
    }

    public BrokerContainer getBroker(int index) {
        return getAnyContainer(brokerContainers, "pulsar-broker", index);
    }

    public synchronized WorkerContainer getWorker(int index) {
        return getAnyContainer(workerContainers, "pulsar-functions-worker", index);
    }

    public synchronized WorkerContainer getWorker(String workerName) {
        return workerContainers.get(workerName);
    }

    private <T> T getAnyContainer(Map<String, T> containers, String serviceName) {
        List<T> containerList = Lists.newArrayList();
        containerList.addAll(containers.values());
        Collections.shuffle(containerList);
        checkArgument(!containerList.isEmpty(), "No " + serviceName + " is alive");
        return containerList.get(0);
    }

    private <T> T getAnyContainer(Map<String, T> containers, String serviceName, int index) {
        checkArgument(!containers.isEmpty(), "No " + serviceName + " is alive");
        checkArgument((index >= 0 && index < containers.size()), "Index : " + index + " is out range");
        return containers.get(serviceName.toLowerCase() + "-" + index);
    }

    public Collection<BrokerContainer> getBrokers() {
        return brokerContainers.values();
    }

    public ProxyContainer getProxy() {
        return proxyContainer;
    }

    public Collection<BKContainer> getBookies() {
        return bookieContainers.values();
    }

    public ZKContainer getZooKeeper() {
        return zkContainer;
    }

    public ContainerExecResult runAdminCommandOnAnyBroker(String... commands) throws Exception {
        return runCommandOnAnyBrokerWithScript(ADMIN_SCRIPT, commands);
    }

    public ContainerExecResult runPulsarBaseCommandOnAnyBroker(String... commands) throws Exception {
        return runCommandOnAnyBrokerWithScript(PULSAR_COMMAND_SCRIPT, commands);
    }

    private ContainerExecResult runCommandOnAnyBrokerWithScript(String scriptType, String... commands)
            throws Exception {
        BrokerContainer container = getAnyBroker();
        String[] cmds = new String[commands.length + 1];
        cmds[0] = scriptType;
        System.arraycopy(commands, 0, cmds, 1, commands.length);
        return container.execCmd(cmds);
    }

    public void stopAllBrokers() {
        brokerContainers.values().forEach(BrokerContainer::stop);
    }

    public void startAllBrokers() {
        brokerContainers.values().forEach(BrokerContainer::start);
    }

    public void stopAllBookies() {
        bookieContainers.values().forEach(BKContainer::stop);
    }

    public void startAllBookies() {
        bookieContainers.values().forEach(BKContainer::start);
    }

    public void stopZooKeeper() {
        zkContainer.stop();
    }

    public void startZooKeeper() {
        zkContainer.start();
    }

    public ContainerExecResult createNamespace(String nsName) throws Exception {
        return runAdminCommandOnAnyBroker(
                "namespaces", "create", "public/" + nsName,
                "--clusters", clusterName);
    }

    public ContainerExecResult createPartitionedTopic(String topicName, int partitions) throws Exception {
        return runAdminCommandOnAnyBroker(
                "topics", "create-partitioned-topic", topicName,
                "-p", String.valueOf(partitions));
    }

    public ContainerExecResult enableDeduplication(String nsName, boolean enabled) throws Exception {
        return runAdminCommandOnAnyBroker(
                "namespaces", "set-deduplication", "public/" + nsName,
                enabled ? "--enable" : "--disable");
    }

    public void dumpFunctionLogs(String name) {
        for (WorkerContainer container : getAlWorkers()) {
            log.info("Trying to get function {} logs from container {}", name, container.getContainerName());
            try {
                String logFile = "/pulsar/logs/functions/public/default/" + name + "/" + name + "-0.log";
                String logs = container.<String>copyFileFromContainer(logFile, (inputStream) -> {
                    return IOUtils.toString(inputStream, "utf-8");
                });
                log.info("Function {} logs {}", name, logs);
            } catch (com.github.dockerjava.api.exception.NotFoundException notFound) {
                log.info("Cannot download {} logs from {} not found exception {}", name, container.getContainerName(),
                        notFound.toString());
            } catch (Throwable err) {
                log.info("Cannot download {} logs from {}", name, container.getContainerName(), err);
            }
        }
    }

    private String appendClusterName(String name) {
        return sharedCsContainer ? clusterName + "-" + name : name;
    }

    public BKContainer getAnyBookie() {
        return getAnyContainer(bookieContainers, "bookie");
    }
}
