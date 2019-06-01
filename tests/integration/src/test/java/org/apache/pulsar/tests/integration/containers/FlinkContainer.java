package org.apache.pulsar.tests.integration.containers;

import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

import java.time.Duration;

public class FlinkContainer extends ChaosContainer<FlinkContainer>  {

    public static final String NAME = "Flink";
    static final Integer[] PORTS = { 8080 };

    private static final String IMAGE_NAME = "flink:latest";

    public FlinkContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
    }

    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(NAME)
                .withExposedPorts(PORTS)
                .withCreateContainerCmdModifier(createContainerCmd -> {
                    createContainerCmd.withHostName(NAME);
                    createContainerCmd.withName(clusterName + "-" + NAME);
                })
                .withCommand("bin/start-cluster.sh")
                .waitingFor(new HostPortWaitStrategy().withStartupTimeout(Duration.ofMinutes(3)));
    }
}
