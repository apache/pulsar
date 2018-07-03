package org.apache.pulsar;

import com.beust.jcommander.Parameter;
import com.ea.agentloader.AgentLoader;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.aspectj.weaver.loadtime.Agent;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Getter
@Slf4j
public abstract class StandalonePulsarBase {

    PulsarService broker;
    PulsarAdmin admin;
    LocalBookkeeperEnsemble bkEnsemble;
    ServiceConfiguration config;
    WorkerService fnWorkerService;

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

    void start() throws Exception {

        if (config == null) {
            System.exit(1);
        }

        log.debug("--- setup PulsarStandaloneStarter ---");

        // load aspectj-weaver agent for instrumentation
        AgentLoader.loadAgentClass(Agent.class.getName(), null);

        if (!this.isOnlyBroker()) {
            // Start LocalBookKeeper
            bkEnsemble = new LocalBookkeeperEnsemble(
                this.getNumOfBk(), this.getZkPort(), this.getBkPort(), this.getStreamStoragePort(), this.getZkDir(), this.getBkDir(), this.isWipeData(), config.getAdvertisedAddress());
            bkEnsemble.startStandalone(!this.isNoStreamStorage());
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
            workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePort());
            workerConfig.setPulsarWebServiceUrl("http://127.0.0.1:" + config.getWebServicePort());
            workerConfig.setStateStorageServiceUrl("bk://127.0.0.1:" + this.getStreamStoragePort());
            String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(
                config.getAdvertisedAddress());
            workerConfig.setWorkerHostname(hostname);
            workerConfig.setWorkerId(
                "c-" + config.getClusterName()
                    + "-fw-" + hostname
                    + "-" + workerConfig.getWorkerPort());
            fnWorkerService = new WorkerService(workerConfig);
        }

        // Start Broker
        broker = new PulsarService(config, Optional.ofNullable(fnWorkerService));
        broker.start();

        URL webServiceUrl = new URL(
                String.format("http://%s:%d", config.getAdvertisedAddress(), config.getWebServicePort()));
        final String brokerServiceUrl = String.format("pulsar://%s:%d", config.getAdvertisedAddress(),
                config.getBrokerServicePort());
        admin = PulsarAdmin.builder().serviceHttpUrl(webServiceUrl.toString()).authentication(
                config.getBrokerClientAuthenticationPlugin(), config.getBrokerClientAuthenticationParameters()).build();

        final String cluster = config.getClusterName();

        createSampleNameSpace(webServiceUrl, brokerServiceUrl, cluster);
        createDeafultNameSpace(cluster);

        log.debug("--- setup completed ---");
    }

    private void createDeafultNameSpace(String cluster) {
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

    private void createSampleNameSpace(URL webServiceUrl, String brokerServiceUrl, String cluster) {
        // Create a sample namespace
        final String property = "sample";
        final String globalCluster = "global";
        final String namespace = property + "/" + cluster + "/ns1";
        try {
            ClusterData clusterData = new ClusterData(webServiceUrl.toString(), null /* serviceUrlTls */,
                    brokerServiceUrl, null /* brokerServiceUrlTls */);
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
}
