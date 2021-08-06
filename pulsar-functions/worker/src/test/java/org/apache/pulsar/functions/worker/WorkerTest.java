package org.apache.pulsar.functions.worker;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WorkerTest {

    @Test
    public void testWorkerFailsForMismatchedClusterNames() {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsCluster("pulsar");
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setClusterName("cluster");
        Assert.assertThrows(IllegalArgumentException.class, () -> new Worker(workerConfig, serviceConfiguration));
    }

    @Test
    public void testWorkerClassInitializesWithValidClusterName() {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsCluster("pulsar");
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setClusterName("pulsar");
        // Test only verifies that initialization does not throw an exception
        // Note that calling the start() method would fail because the configuration is incomplete
        new Worker(workerConfig, serviceConfiguration);
    }

}
