package org.apache.pulsar.tests.integration.presto;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestBasicPresto extends PulsarClusterTestBase {

    @BeforeSuite
    @Override
    public void setupCluster() throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .enablePrestoWorker(true)
                .clusterName(clusterName)
                .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        log.info("Cluster {} is setup with presto worker", spec.clusterName());
    }

    @Test
    public void testDefaultCatalog() throws Exception {
        ContainerExecResult containerExecResult = execQuery("show catalogs;");
        assertThat(containerExecResult.getExitCode()).isEqualTo(0);
        assertThat(containerExecResult.getStdout()).contains("pulsar", "system");
    }

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    public static ContainerExecResult execQuery(final String query) throws Exception {
        ContainerExecResult containerExecResult;

        containerExecResult = pulsarCluster.getPrestoWorkerContainer()
                .execCmd("/bin/bash", "-c", PulsarCluster.PULSAR_COMMAND_SCRIPT + " sql --execute " + "'" + query + "'");

        return containerExecResult;

    }

}
