package org.apache.pulsar.tests.integration.flink;

import org.apache.pulsar.tests.integration.containers.FlinkContainer;
import org.testng.annotations.Test;

public class FlinkContainerTest {

    @Test
    public void test() throws Exception {
        FlinkContainer flinkContainer = new FlinkContainer("test");
        flinkContainer.start();
        System.out.println(flinkContainer.isRunning());
        flinkContainer.stop();
    }
}
