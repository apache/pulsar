package org.apache.pulsar.tests.integration.containers;

import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

public class ElasticSearchContainer extends ChaosContainer<ElasticSearchContainer> {
    
    public static final String NAME = "ElasticSearch";
    static final Integer[] PORTS = { 9200, 9300 };
    
    private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:6.4.0";

    protected ElasticSearchContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);       
    }
    
    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(NAME)
            .withExposedPorts(PORTS)
            .withEnv("discovery.type", "single-node")
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(clusterName + "-" + NAME);
            })
            .waitingFor(new HostPortWaitStrategy());
    }

}
