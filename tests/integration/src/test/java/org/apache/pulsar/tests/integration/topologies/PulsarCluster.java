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
package org.apache.pulsar.tests.integration.topologies;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_HTTP_PORT;
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
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.tests.integration.containers.BKContainer;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.CSContainer;
import org.apache.pulsar.tests.integration.containers.PrestoWorkerContainer;
import org.apache.pulsar.tests.integration.containers.ProxyContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.WorkerContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
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

    /**
     * Pulsar Cluster Spec
     *
     * @param spec pulsar cluster spec.
     * @return the built pulsar cluster
     */
    public static PulsarCluster forSpec(PulsarClusterSpec spec) {
        CSContainer csContainer = new CSContainer(spec.clusterName)
                .withNetwork(Network.newNetwork())
                .withNetworkAliases(CSContainer.NAME);
        return new PulsarCluster(spec, csContainer, false);
    }

    public static PulsarCluster forSpec(PulsarClusterSpec spec, CSContainer csContainer) {
        return new PulsarCluster(spec, csContainer, true);
    }

    private final PulsarClusterSpec spec;

    @Getter
    private final String clusterName;
    private final Network network;
    private final ZKContainer zkContainer;
    private final CSContainer csContainer;
    private final boolean sharedCsContainer;
    private final Map<String, BKContainer> bookieContainers;
    private final Map<String, BrokerContainer> brokerContainers;
    private final Map<String, WorkerContainer> workerContainers;
    private final ProxyContainer proxyContainer;
    private PrestoWorkerContainer prestoWorkerContainer;
    @Getter
    private Map<String, PrestoWorkerContainer> sqlFollowWorkerContainers;
    private Map<String, GenericContainer<?>> externalServices = Collections.emptyMap();
    private final boolean enablePrestoWorker;

    private PulsarCluster(PulsarClusterSpec spec, CSContainer csContainer, boolean sharedCsContainer) {

        this.spec = spec;
        this.sharedCsContainer = sharedCsContainer;
        this.clusterName = spec.clusterName();
        this.network = csContainer.getNetwork();
        this.enablePrestoWorker = spec.enablePrestoWorker();

        this.sqlFollowWorkerContainers = Maps.newTreeMap();
        if (enablePrestoWorker) {
            prestoWorkerContainer = buildPrestoWorkerContainer(
                    PrestoWorkerContainer.NAME, true, null, null);
        } else {
            prestoWorkerContainer = null;
        }


        this.zkContainer = new ZKContainer(clusterName);
        this.zkContainer
            .withNetwork(network)
            .withNetworkAliases(appendClusterName(ZKContainer.NAME))
            .withEnv("clusterName", clusterName)
            .withEnv("zkServers", appendClusterName(ZKContainer.NAME))
            .withEnv("configurationStore", CSContainer.NAME + ":" + CS_PORT)
            .withEnv("forceSync", "no")
            .withEnv("pulsarNode", appendClusterName("pulsar-broker-0"));

        this.csContainer = csContainer;

        this.bookieContainers = Maps.newTreeMap();
        this.brokerContainers = Maps.newTreeMap();
        this.workerContainers = Maps.newTreeMap();

        this.proxyContainer = new ProxyContainer(appendClusterName("pulsar-proxy"), ProxyContainer.NAME)
            .withNetwork(network)
            .withNetworkAliases(appendClusterName("pulsar-proxy"))
            .withEnv("zkServers", appendClusterName(ZKContainer.NAME))
            .withEnv("zookeeperServers", appendClusterName(ZKContainer.NAME))
            .withEnv("configurationStoreServers", CSContainer.NAME + ":" + CS_PORT)
            .withEnv("clusterName", clusterName);
        if (spec.proxyEnvs != null) {
            spec.proxyEnvs.forEach(this.proxyContainer::withEnv);
        }
        if (spec.proxyMountFiles != null) {
            spec.proxyMountFiles.forEach(this.proxyContainer::withFileSystemBind);
        }

        // create bookies
        bookieContainers.putAll(
                runNumContainers("bookie", spec.numBookies(), (name) -> new BKContainer(clusterName, name)
                        .withNetwork(network)
                        .withNetworkAliases(appendClusterName(name))
                        .withEnv("zkServers", appendClusterName(ZKContainer.NAME))
                        .withEnv("useHostNameAsBookieID", "true")
                        // Disable fsyncs for tests since they're slow within the containers
                        .withEnv("journalSyncData", "false")
                        .withEnv("journalMaxGroupWaitMSec", "0")
                        .withEnv("clusterName", clusterName)
                        .withEnv("diskUsageThreshold", "0.99")
                )
        );

        // create brokers
        brokerContainers.putAll(
            runNumContainers("broker", spec.numBrokers(), (name) -> {
                    BrokerContainer brokerContainer = new BrokerContainer(clusterName, appendClusterName(name))
                        .withNetwork(network)
                        .withNetworkAliases(appendClusterName(name))
                        .withEnv("zkServers", appendClusterName(ZKContainer.NAME))
                        .withEnv("zookeeperServers", appendClusterName(ZKContainer.NAME))
                        .withEnv("configurationStoreServers", CSContainer.NAME + ":" + CS_PORT)
                        .withEnv("clusterName", clusterName)
                        .withEnv("brokerServiceCompactionMonitorIntervalInSeconds", "1")
                        // used in s3 tests
                        .withEnv("AWS_ACCESS_KEY_ID", "accesskey")
                        .withEnv("AWS_SECRET_KEY", "secretkey");
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
                    return brokerContainer;
                }
            ));

        spec.classPathVolumeMounts.forEach((key, value) -> {
            zkContainer.withClasspathResourceMapping(key, value, BindMode.READ_WRITE);
            proxyContainer.withClasspathResourceMapping(key, value, BindMode.READ_WRITE);

            bookieContainers.values().forEach(c -> c.withClasspathResourceMapping(key, value, BindMode.READ_WRITE));
            brokerContainers.values().forEach(c -> c.withClasspathResourceMapping(key, value, BindMode.READ_WRITE));
            workerContainers.values().forEach(c -> c.withClasspathResourceMapping(key, value, BindMode.READ_WRITE));
        });

    }

    public String getPlainTextServiceUrl() {
        return proxyContainer.getPlainTextServiceUrl();
    }

    public String getHttpServiceUrl() {
        return proxyContainer.getHttpServiceUrl();
    }

    public String getAllBrokersHttpServiceUrl() {
        String multiUrl = "http://";
        Iterator<BrokerContainer> brokers = getBrokers().iterator();
        while (brokers.hasNext()) {
            BrokerContainer broker = brokers.next();
            multiUrl += broker.getContainerIpAddress() + ":" + broker.getMappedPort(BROKER_HTTP_PORT);
            if (brokers.hasNext()) {
                multiUrl += ",";
            }
        }
        return multiUrl;
    }

    public String getZKConnString() {
        return zkContainer.getContainerIpAddress() + ":" + zkContainer.getMappedPort(ZK_PORT);
    }

    public String getCSConnString() {
        return csContainer.getContainerIpAddress() + ":" + csContainer.getMappedPort(CS_PORT);
    }

    public Network getNetwork() {
        return network;
    }

    public Map<String, GenericContainer<?>> getExternalServices() {
        return externalServices;
    }

    public void start() throws Exception {
        // start the local zookeeper
        zkContainer.start();
        log.info("Successfully started local zookeeper container.");

        // start the configuration store
        if (!sharedCsContainer) {
            csContainer.start();
            log.info("Successfully started configuration store container.");
        }

        // init the cluster
        zkContainer.execCmd(
            "bin/init-cluster.sh");
        log.info("Successfully initialized the cluster.");

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

        if (enablePrestoWorker) {
            log.info("Starting Presto Worker");
            prestoWorkerContainer.start();
        }

        // start external services
        this.externalServices = spec.externalServices;
        if (null != externalServices) {
            externalServices.entrySet().parallelStream().forEach(service -> {
                GenericContainer<?> serviceContainer = service.getValue();
                serviceContainer.withNetwork(network);
                serviceContainer.withNetworkAliases(service.getKey());
                PulsarContainer.configureLeaveContainerRunning(serviceContainer);
                serviceContainer.start();
                log.info("Successfully start external service {}.", service.getKey());
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

    public PrestoWorkerContainer getPrestoWorkerContainer() {
        return prestoWorkerContainer;
    }

    public synchronized void stop() {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            logIgnoringStopDueToLeaveRunning();
            return;
        }

        List<GenericContainer> containers = new ArrayList<>();

        containers.addAll(workerContainers.values());
        containers.addAll(brokerContainers.values());
        containers.addAll(bookieContainers.values());

        if (externalServices != null) {
            containers.addAll(externalServices.values());
        }

        if (null != proxyContainer) {
            containers.add(proxyContainer);
        }

        if (!sharedCsContainer && null != csContainer) {
            containers.add(csContainer);
        }

        if (null != zkContainer) {
            containers.add(zkContainer);
        }
        if (null != prestoWorkerContainer) {
            containers.add(prestoWorkerContainer);
        }

        containers = containers.parallelStream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        containers.parallelStream().forEach(GenericContainer::stop);

        try {
            network.close();
        } catch (Exception e) {
            log.info("Failed to shutdown network for pulsar cluster {}", clusterName, e);
        }
    }

    public void startPrestoWorker() {
        startPrestoWorker(null, null);
    }

    public void startPrestoWorker(String offloadDriver, String offloadProperties) {
        log.info("[startPrestoWorker] offloadDriver: {}, offloadProperties: {}", offloadDriver, offloadProperties);
        if (null == prestoWorkerContainer) {
            prestoWorkerContainer = buildPrestoWorkerContainer(
                    PrestoWorkerContainer.NAME, true, offloadDriver, offloadProperties);
        }
        prestoWorkerContainer.start();
        log.info("[{}] Presto coordinator start finished.", prestoWorkerContainer.getContainerName());
    }

    public void stopPrestoWorker() {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            logIgnoringStopDueToLeaveRunning();
            return;
        }
        if (sqlFollowWorkerContainers != null && sqlFollowWorkerContainers.size() > 0) {
            for (PrestoWorkerContainer followWorker : sqlFollowWorkerContainers.values()) {
                followWorker.stop();
                log.info("Stopped presto follow worker {}.", followWorker.getContainerName());
            }
            sqlFollowWorkerContainers.clear();
            log.info("Stopped all presto follow workers.");
        }
        if (null != prestoWorkerContainer) {
            prestoWorkerContainer.stop();
            log.info("Stopped presto coordinator.");
            prestoWorkerContainer = null;
        }
    }

    public void startPrestoFollowWorkers(int numSqlFollowWorkers, String offloadDriver, String offloadProperties) {
        log.info("start presto follow worker containers.");
        sqlFollowWorkerContainers.putAll(runNumContainers(
                "sql-follow-worker",
                numSqlFollowWorkers,
                (name) -> {
                    log.info("build presto follow worker with name {}", name);
                    return buildPrestoWorkerContainer(name, false, offloadDriver, offloadProperties);
                }
        ));
        // Start workers that have been initialized
        sqlFollowWorkerContainers.values().parallelStream().forEach(PrestoWorkerContainer::start);
        log.info("Successfully started {} presto follow worker containers.", sqlFollowWorkerContainers.size());
    }

    private PrestoWorkerContainer buildPrestoWorkerContainer(String hostName, boolean isCoordinator,
                                                             String offloadDriver, String offloadProperties) {
        String resourcePath = isCoordinator ? "presto-coordinator-config.properties"
                : "presto-follow-worker-config.properties";
        PrestoWorkerContainer container = new PrestoWorkerContainer(
                clusterName, hostName)
                .withNetwork(network)
                .withNetworkAliases(hostName)
                .withEnv("clusterName", clusterName)
                .withEnv("zkServers", ZKContainer.NAME)
                .withEnv("zookeeperServers", ZKContainer.NAME + ":" + ZKContainer.ZK_PORT)
                .withEnv("pulsar.zookeeper-uri", ZKContainer.NAME + ":" + ZKContainer.ZK_PORT)
                .withEnv("pulsar.web-service-url", "http://pulsar-broker-0:8080")
                .withClasspathResourceMapping(
                        resourcePath, "/pulsar/conf/presto/config.properties", BindMode.READ_WRITE);
        if (spec.queryLastMessage) {
            container.withEnv("pulsar.bookkeeper-use-v2-protocol", "false")
                    .withEnv("pulsar.bookkeeper-explicit-interval", "10");
        }
        if (offloadDriver != null && offloadProperties != null) {
            log.info("[startPrestoWorker] set offload env offloadDriver: {}, offloadProperties: {}",
                    offloadDriver, offloadProperties);
            // used to query from tiered storage
            container.withEnv("SQL_PREFIX_pulsar.managed-ledger-offload-driver", offloadDriver);
            container.withEnv("SQL_PREFIX_pulsar.offloader-properties", offloadProperties);
            container.withEnv("SQL_PREFIX_pulsar.offloaders-directory", "/pulsar/offloaders");
            container.withEnv("AWS_ACCESS_KEY_ID", "accesskey");
            container.withEnv("AWS_SECRET_KEY", "secretkey");
        }
        log.info("[{}] build presto worker container. isCoordinator: {}, resourcePath: {}",
                container.getContainerName(), isCoordinator, resourcePath);
        return container;
    }

    public synchronized void setupFunctionWorkers(String suffix, FunctionRuntimeType runtimeType, int numFunctionWorkers) {
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
        String serviceUrl = "pulsar://pulsar-broker-0:" + PulsarContainer.BROKER_PORT;
        String httpServiceUrl = "http://pulsar-broker-0:" + PulsarContainer.BROKER_HTTP_PORT;
        workerContainers.putAll(runNumContainers(
            "functions-worker-process-" + suffix,
            numFunctionWorkers,
            (name) -> new WorkerContainer(clusterName, name)
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
        ));
        this.startWorkers();
    }

    private void startFunctionWorkersWithThreadContainerFactory(String suffix, int numFunctionWorkers) {
        String serviceUrl = "pulsar://pulsar-broker-0:" + PulsarContainer.BROKER_PORT;
        String httpServiceUrl = "http://pulsar-broker-0:" + PulsarContainer.BROKER_HTTP_PORT;
        workerContainers.putAll(runNumContainers(
            "functions-worker-thread-" + suffix,
            numFunctionWorkers,
            (name) -> new WorkerContainer(clusterName, name)
                .withNetwork(network)
                .withNetworkAliases(name)
                // worker settings
                .withEnv("PF_workerId", name)
                .withEnv("PF_workerHostname", name)
                .withEnv("PF_workerPort", "" + PulsarContainer.BROKER_HTTP_PORT)
                .withEnv("PF_pulsarFunctionsCluster", clusterName)
                .withEnv("PF_pulsarServiceUrl", serviceUrl)
                .withEnv("PF_pulsarWebServiceUrl", httpServiceUrl)
                .withEnv("PF_functionRuntimeFactoryClassName", "org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory")
                .withEnv("PF_functionRuntimeFactoryConfigs_threadGroupName", "pf-container-group")
                // script
                .withEnv("clusterName", clusterName)
                .withEnv("zookeeperServers", ZKContainer.NAME)
                // bookkeeper tools
                .withEnv("zkServers", ZKContainer.NAME)
        ));
        this.startWorkers();
    }

    public synchronized void startWorkers() {
        // Start workers that have been initialized
        workerContainers.values().parallelStream().forEach(WorkerContainer::start);
        log.info("Successfully started {} worker containers.", workerContainers.size());
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

    public ContainerExecResult runAdminCommandOnAnyBroker(String...commands) throws Exception {
        return runCommandOnAnyBrokerWithScript(ADMIN_SCRIPT, commands);
    }

    public ContainerExecResult runPulsarBaseCommandOnAnyBroker(String...commands) throws Exception {
        return runCommandOnAnyBrokerWithScript(PULSAR_COMMAND_SCRIPT, commands);
    }

    private ContainerExecResult runCommandOnAnyBrokerWithScript(String scriptType, String...commands) throws Exception {
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
                log.info("Cannot download {} logs from {} not found exception {}", name, container.getContainerName(), notFound.toString());
            } catch (Throwable err) {
                log.info("Cannot download {} logs from {}", name, container.getContainerName(), err);
            }
        }
    }

    private String appendClusterName(String name) {
        return sharedCsContainer ? clusterName + "-" + name : name;
    }
}
