package org.apache.pulsar.tests.integration.containers;

import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

public class DebeziumMongoDbContainer extends ChaosContainer<DebeziumMongoDbContainer> {

    public static final String NAME = "debezium-mongodb-example";

    public static final Integer[] PORTS = { 27017 };
    private static final String IMAGE_NAME = "debezium/example-mongodb:latest";

    public DebeziumMongoDbContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
        this.withEnv("MONGODB_USER", "mongodb");
        this.withEnv("MONGODB_PASSWORD", "mongodb");
    }
    @Override
    public String getContainerName() {
        return clusterName;
    }

    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(NAME)
                .withExposedPorts(PORTS)
                .withCreateContainerCmdModifier(createContainerCmd -> {
                    createContainerCmd.withHostName(NAME);
                    createContainerCmd.withName(getContainerName());
                })
                .waitingFor(new HostPortWaitStrategy());
    }
}
