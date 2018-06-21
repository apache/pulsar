package org.apache.pulsar.tests.integration.cluster;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.pulsar.tests.DockerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.pulsar.tests.PulsarClusterUtils.waitSocketAvaialble;

public class Cluster3Bookie2Broker {

    private static final Logger LOG = LoggerFactory.getLogger(Cluster3Bookie2Broker.class);

    private static final int PULSAR_BROKER_PORT = 6650;

    public static final String ZOOKEEPER_NAME = "zookeeper";
    public static final String CONFIGURATION_STORE_NAME = "configuration-store";
    public static final String INIT_NAME = "init";
    public static final String PULSAR_BROKER1_NAME = "pulsar-broker1";
    public static final String PULSAR_BROKER2_NAME = "pulsar-broker2";
    public static final String PULSAR_BOOKIE1_NAME = "bookkeeper1";
    public static final String PULSAR_BOOKIE2_NAME = "bookkeeper2";
    public static final String PULSAR_BOOKIE3_NAME = "bookkeeper3";
    public static final String PULSAR_PROXY_NAME = "pulsar-proxy";

    private GenericContainer zookeeperContainer;
    private GenericContainer configStoreContainer;
    private GenericContainer initContainer;
    private GenericContainer broker1Container;
    private GenericContainer broker2Container;
    private GenericContainer bookie1Container;
    private GenericContainer bookie2Container;
    private GenericContainer bookie3Container;
    private GenericContainer proxyContainer;

    private Network network;

    private DockerClient dockerClient;

    private static final String NAME = "apachepulsar";
    private static final String IMG = "pulsar-test-latest-version";

    public Cluster3Bookie2Broker(final String testName) {

        network = Network.newNetwork();

        zookeeperContainer = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(ZOOKEEPER_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "zookeeper")
            .withEnv("ZOOKEEPER_SERVERS", "zookeeper")
            .withCommand("sh", "-c", "bin/run-local-zk.sh")
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(ZOOKEEPER_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(ZOOKEEPER_NAME);
                    createContainerCmd.withName(ZOOKEEPER_NAME + "-" + testName);
                }
            })
        ;

        configStoreContainer = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(CONFIGURATION_STORE_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "configuration-store")
            .withEnv("ZOOKEEPER_SERVERS", "configuration-store")
            .withCommand("sh", "-c", "bin/run-global-zk.sh")
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(CONFIGURATION_STORE_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(CONFIGURATION_STORE_NAME);
                    createContainerCmd.withName(CONFIGURATION_STORE_NAME + "-" + testName);
                }
            })
        ;

        initContainer = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(INIT_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "init")
            .withEnv("clusterName", "test")
            .withEnv("zkServers", "zookeeper")
            .withEnv("configurationStore", "configuration-store:2184")
            .withEnv("pulsarNode", "pulsar-broker1")
            .withCommand("sh", "-c", "bin/init-cluster.sh")
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(INIT_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(INIT_NAME);
                    createContainerCmd.withName(INIT_NAME + "-" + testName);
                }
            })
        ;

        bookie1Container = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(PULSAR_BOOKIE1_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "bookie")
            .withEnv("clusterName", "test")
            .withEnv("zkServers", "zookeeper")
            .withEnv("useHostNameAsBookieID", "true")
            .withCommand("sh", "-c", "bin/run-bookie.sh")
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(PULSAR_BOOKIE1_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(PULSAR_BOOKIE1_NAME);
                    createContainerCmd.withName(PULSAR_BOOKIE1_NAME + "-" + testName);
                }
            })
        ;

        bookie2Container = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(PULSAR_BOOKIE2_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "bookie")
            .withEnv("clusterName", "test")
            .withEnv("zkServers", "zookeeper")
            .withEnv("useHostNameAsBookieID", "true")
            .withCommand("sh", "-c", "bin/run-bookie.sh")
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(PULSAR_BOOKIE2_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(PULSAR_BOOKIE2_NAME);
                    createContainerCmd.withName(PULSAR_BOOKIE2_NAME + "-" + testName);
                }
            })
        ;

        bookie3Container = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(PULSAR_BOOKIE3_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "bookie")
            .withEnv("clusterName", "test")
            .withEnv("zkServers", "zookeeper")
            .withEnv("useHostNameAsBookieID", "true")
            .withCommand("sh", "-c", "bin/run-bookie.sh")
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(PULSAR_BOOKIE3_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(PULSAR_BOOKIE3_NAME);
                    createContainerCmd.withName(PULSAR_BOOKIE3_NAME + "-" + testName);
                }
            })
        ;

        broker1Container = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(PULSAR_BROKER1_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "pulsar-broker")
            .withExposedPorts(PULSAR_BROKER_PORT)
            .withEnv("clusterName", "test")
            .withEnv("zookeeperServers", "zookeeper")
            .withEnv("configurationStore", "configuration-store:2184")
            .withEnv("NO_AUTOSTART", "true")
            .withCommand("sh", "-c", "bin/run-broker.sh")
            .waitingFor(new LogMessageWaitStrategy()
                .withRegEx(".*supervisord started with pid.*\\s")
                .withTimes(1)
                .withStartupTimeout(Duration.of(100, SECONDS)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(PULSAR_BROKER1_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(PULSAR_BROKER1_NAME);
                    createContainerCmd.withName(PULSAR_BROKER1_NAME + "-" + testName);
                }
            })
        ;

        broker2Container = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(PULSAR_BROKER2_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "pulsar-broker")
            .withExposedPorts(PULSAR_BROKER_PORT)
            .withEnv("clusterName", "test")
            .withEnv("zookeeperServers", "zookeeper")
            .withEnv("configurationStore", "configuration-store:2184")
            .withEnv("NO_AUTOSTART", "true")
            .withCommand("sh", "-c", "bin/run-broker.sh")
            .waitingFor(new LogMessageWaitStrategy()
                .withRegEx(".*supervisord started with pid.*\\s")
                .withTimes(1)
                .withStartupTimeout(Duration.of(100, SECONDS)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(PULSAR_BROKER2_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(PULSAR_BROKER2_NAME);
                    createContainerCmd.withName(PULSAR_BROKER2_NAME + "-" + testName);
                }
            })
        ;

        proxyContainer = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(PULSAR_PROXY_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "pulsar-proxy")
            .withExposedPorts(PULSAR_BROKER_PORT)
            .withEnv("clusterName", "test")
            .withEnv("zookeeperServers", "zookeeper")
            .withEnv("configurationStoreServers", "configuration-store:2184")
            .withEnv("NO_AUTOSTART", "configuration-store:2184")
            .withCommand("sh", "-c", "bin/run-proxy.sh")
            .waitingFor(new LogMessageWaitStrategy()
                .withRegEx(".*supervisord started with pid.*\\s")
                .withTimes(1)
                .withStartupTimeout(Duration.of(100, SECONDS)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(PULSAR_PROXY_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(PULSAR_PROXY_NAME);
                    createContainerCmd.withName(PULSAR_PROXY_NAME + "-" + testName);
                }
            })
        ;

        dockerClient = DockerClientFactory.instance().client();
    }

    public void execInBroker(String... commands) throws IOException, InterruptedException {
        LOG.info("Executing Command in Broker : " + String.join(" ", commands));
        //DockerUtils.runCommand(dockerClient, broker1Container.getContainerName().substring(1), commands);
        Container.ExecResult execResult = broker1Container.execInContainer(commands);
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());
    }

    public String getPulsarProxyIP() {
        return proxyContainer.getContainerIpAddress();
    }

    public int getPulsarProxyPort() {
        return proxyContainer.getMappedPort(PULSAR_BROKER_PORT);
    }

    public DockerClient getDockerClient() {
        return dockerClient;
    }

    public void startAllBrokers() throws IOException, InterruptedException {
        broker1Container.execInContainer("supervisorctl", "start", "broker");
        broker2Container.execInContainer("supervisorctl", "start", "broker");
        waitSocketAvaialble(broker1Container.getContainerIpAddress(),
            broker1Container.getMappedPort(PULSAR_BROKER_PORT),
            10,
            TimeUnit.SECONDS);
        waitSocketAvaialble(broker2Container.getContainerIpAddress(),
            broker2Container.getMappedPort(PULSAR_BROKER_PORT),
            10,
            TimeUnit.SECONDS);
    }

    public void startAllProxies() throws IOException, InterruptedException {
        proxyContainer.execInContainer("supervisorctl", "start", "proxy");
        waitSocketAvaialble(proxyContainer.getContainerIpAddress(),
            proxyContainer.getMappedPort(PULSAR_BROKER_PORT),
            10,
            TimeUnit.SECONDS);
    }

    public void start() throws ExecutionException, InterruptedException {
        Stream.of(zookeeperContainer,
            configStoreContainer,
            initContainer,
            broker1Container,
            broker2Container,
            bookie1Container,
            bookie2Container,
            bookie3Container,
            proxyContainer).parallel().forEach(GenericContainer::start);
    }

    public void stop() throws Exception {
        DockerUtils.dumpContainerLogToTarget(dockerClient, zookeeperContainer.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, configStoreContainer.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, initContainer.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, broker1Container.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, broker2Container.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, bookie1Container.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, bookie2Container.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, bookie3Container.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, proxyContainer.getContainerName().substring(1));

        DockerUtils.dumpContainerLogDirToTarget(dockerClient, zookeeperContainer.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerDirToTargetCompressed(dockerClient, zookeeperContainer.getContainerName().substring(1), "/pulsar/data/zookeeper");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, configStoreContainer.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerDirToTargetCompressed(dockerClient, configStoreContainer.getContainerName().substring(1), "/pulsar/data/zookeeper");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, initContainer.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, broker1Container.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, broker2Container.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, bookie1Container.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, bookie2Container.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, bookie3Container.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, proxyContainer.getContainerName().substring(1), "/var/log/pulsar");

        Stream.of(zookeeperContainer,
            configStoreContainer,
            initContainer,
            broker1Container,
            broker2Container,
            bookie1Container,
            bookie2Container,
            bookie3Container,
            proxyContainer).parallel().forEach(GenericContainer::stop);
        network.close();
    }

}
