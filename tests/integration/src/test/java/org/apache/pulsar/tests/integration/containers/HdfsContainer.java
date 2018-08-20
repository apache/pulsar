package org.apache.pulsar.tests.integration.containers;

import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

public class HdfsContainer extends ChaosContainer<HdfsContainer> {

	public static final String NAME = "HDFS";
	static final Integer[] PORTS = { 8020, 8032, 8088, 9000, 10020, 19888, 50010, 50020, 50070, 50070, 50090 };
	
	private static final String IMAGE_NAME = "harisekhon/hadoop:latest";
    private final String hostname;
    
	protected HdfsContainer(String clusterName, String hostname) {
		super(clusterName, IMAGE_NAME);
		this.hostname = hostname;
	}
	
	@Override
    public String getContainerName() {
        return clusterName + "-" + hostname;
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
        .waitingFor(new HostPortWaitStrategy());
	}

}
