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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.pulsar.tests.PulsarClusterUtils.waitSocketAvailable;

public class Cluster2Bookie1BrokerWithS3 {

    private static final Logger LOG = LoggerFactory.getLogger(Cluster2Bookie1BrokerWithS3.class);

    private static final int PULSAR_BROKER_PORT = 6650;
    private static final int PULSAR_ADMIN_PORT = 8080;
    private static final int ZOOKEEPER_PORT = 2181;

    public static final String ZOOKEEPER_NAME = "zookeeper";
    public static final String CONFIGURATION_STORE_NAME = "configuration-store";
    public static final String INIT_NAME = "init";
    public static final String PULSAR_BROKER1_NAME = "pulsar-broker1";
    public static final String PULSAR_BOOKIE1_NAME = "bookkeeper1";
    public static final String PULSAR_BOOKIE2_NAME = "bookkeeper2";
    public static final String PULSAR_PROXY_NAME = "pulsar-proxy";
    public static final String S3_MOCK_NAME = "s3";

    private GenericContainer zookeeperContainer;
    private GenericContainer configStoreContainer;
    private GenericContainer initContainer;
    private GenericContainer broker1Container;
    private GenericContainer bookie1Container;
    private GenericContainer bookie2Container;
    private GenericContainer s3Container;
    private GenericContainer proxyContainer;

    private Network network;

    private DockerClient dockerClient;

    private static final String NAME = "apachepulsar";
    private static final String IMG = "pulsar-test-latest-version";

    private static final String S3_NAME = "adobe";
    private static final String S3_IMG = "s3mock";

    private List<GenericContainer> brokers;

    public Cluster2Bookie1BrokerWithS3(final String testName) {

        brokers = new ArrayList<>();

        network = Network.newNetwork();

        zookeeperContainer = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(ZOOKEEPER_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "zookeeper")
            .withExposedPorts(ZOOKEEPER_PORT)
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

        proxyContainer = new GenericContainer(NAME + "/" + IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(PULSAR_PROXY_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "pulsar-proxy")
            .withExposedPorts(PULSAR_BROKER_PORT, PULSAR_ADMIN_PORT)
            .withEnv("clusterName", "test")
            .withEnv("zookeeperServers", "zookeeper")
            .withEnv("configurationStoreServers", "configuration-store:2184")
            .withEnv("NO_AUTOSTART", "true")
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

        s3Container = new GenericContainer(S3_NAME + "/" + S3_IMG + ":" + "latest")
            .withNetwork(network)
            .withNetworkAliases(S3_MOCK_NAME)
            .withLabel("cluster", "test")
            .withLabel("service", "s3")
            .withEnv("initialBuckets", "pulsar-integtest")
            .waitingFor(new LogMessageWaitStrategy()
                .withRegEx(".*supervisord started with pid.*\\s")
                .withTimes(1)
                .withStartupTimeout(Duration.of(100, SECONDS)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(S3_NAME))
            .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(CreateContainerCmd createContainerCmd) {
                    createContainerCmd.withHostName(S3_MOCK_NAME);
                    createContainerCmd.withName(S3_MOCK_NAME + "-" + testName);
                }
            })
        ;

        brokers.add(broker1Container);

        dockerClient = DockerClientFactory.instance().client();
    }

    public String execInBroker(String... commands) throws IOException, InterruptedException {
        LOG.info("Executing Command in Broker : " + String.join(" ", commands));
        Collections.shuffle(brokers);
        GenericContainer selectedBroker = brokers.get(0);
        Container.ExecResult execResult = selectedBroker.execInContainer(commands);
        LOG.info(execResult.getStdout() != null ? execResult.getStdout() : "Stdout : N/A");
        LOG.info(execResult.getStderr() != null ? execResult.getStderr() : "Stderr : N/A");
        return execResult.getStdout() + "\n" + execResult.getStderr();
        //DockerUtils.runCommand(dockerClient, broker1Container.getContainerName().substring(1), commands);
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
        waitSocketAvailable(broker1Container.getContainerIpAddress(),
            broker1Container.getMappedPort(PULSAR_BROKER_PORT),
            10,
            TimeUnit.SECONDS);
    }

    public void stopAllBrokers() throws IOException, InterruptedException {
        broker1Container.execInContainer("supervisorctl", "stop", "broker");
        waitSocketAvailable(broker1Container.getContainerIpAddress(),
            broker1Container.getMappedPort(PULSAR_BROKER_PORT),
            10,
            TimeUnit.SECONDS);
    }

    public void startAllProxies() throws IOException, InterruptedException {
        proxyContainer.execInContainer("supervisorctl", "start", "proxy");
        waitSocketAvailable(proxyContainer.getContainerIpAddress(),
            proxyContainer.getMappedPort(PULSAR_BROKER_PORT),
            10,
            TimeUnit.SECONDS);
    }

    public void stopAllProxies() throws IOException, InterruptedException {
        proxyContainer.execInContainer("supervisorctl", "stop", "proxy");
        waitSocketAvailable(proxyContainer.getContainerIpAddress(),
            proxyContainer.getMappedPort(PULSAR_BROKER_PORT),
            10,
            TimeUnit.SECONDS);
    }

    public void start() throws ExecutionException {
        Stream.of(zookeeperContainer,
            configStoreContainer,
            initContainer,
            broker1Container,
            bookie1Container,
            bookie2Container,
            proxyContainer).parallel().forEach(GenericContainer::start);
    }

    public void updateBrokerConf(String confFile, String key, String value) throws Exception {
        String sedProgram = String.format(
            "/[[:blank:]]*%s[[:blank:]]*=/ { h; s^=.*^=%s^; }; ${x;/^$/ { s^^%s=%s^;H; }; x}",
            key, value, key, value);
        execInBroker( "sed", "-i", "-e", sedProgram, confFile);
    }

    public String getZkConnectionString(){
        return zookeeperContainer.getContainerIpAddress() + ":" + zookeeperContainer.getMappedPort(ZOOKEEPER_PORT);
    }


    public void stop() throws Exception {
        DockerUtils.dumpContainerLogToTarget(dockerClient, zookeeperContainer.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, configStoreContainer.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, initContainer.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, broker1Container.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, bookie1Container.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, bookie2Container.getContainerName().substring(1));
        DockerUtils.dumpContainerLogToTarget(dockerClient, proxyContainer.getContainerName().substring(1));

        DockerUtils.dumpContainerLogDirToTarget(dockerClient, zookeeperContainer.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerDirToTargetCompressed(dockerClient, zookeeperContainer.getContainerName().substring(1), "/pulsar/data/zookeeper");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, configStoreContainer.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerDirToTargetCompressed(dockerClient, configStoreContainer.getContainerName().substring(1), "/pulsar/data/zookeeper");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, initContainer.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, broker1Container.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, bookie1Container.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, bookie2Container.getContainerName().substring(1), "/var/log/pulsar");
        DockerUtils.dumpContainerLogDirToTarget(dockerClient, proxyContainer.getContainerName().substring(1), "/var/log/pulsar");

        Stream.of(zookeeperContainer,
            configStoreContainer,
            initContainer,
            broker1Container,
            bookie1Container,
            bookie2Container,
            proxyContainer).parallel().forEach(GenericContainer::stop);
        network.close();
    }

}
