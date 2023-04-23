package org.apache.pulsar.tests.integration.bookkeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.testng.AssertJUnit.assertEquals;

@Slf4j
public class BrokerInstallWithEntryMetadataInterceptorsTest extends PulsarClusterTestBase {
    @BeforeClass(alwaysRun = true)
    @Override
    public final void setupCluster() throws Exception {
        incrementSetupNumber();

        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> !s.isEmpty())
                .collect(joining("-"));
        brokerEnvs.put("exposingBrokerEntryMetadataToClientEnabled", "true");
        brokerEnvs.put("brokerEntryMetadataInterceptors", "org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .clusterName(clusterName)
                .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void tearDownCluster() throws Exception {
        super.tearDownCluster();
    }

    @Test
    public void testBookieHttpServerIsRunning() throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyBroker().execCmd(
                PulsarCluster.CURL,
                "-X",
                "GET",
                "http://localhost:6650/admin/v2/brokers/health");
        assertEquals(result.getExitCode(), 0);
        assertEquals(result.getStdout(), "ok\n");
    }
}
