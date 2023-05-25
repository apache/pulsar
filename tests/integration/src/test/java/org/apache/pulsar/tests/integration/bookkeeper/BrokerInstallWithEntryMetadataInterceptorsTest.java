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

/***
 * Test that verifies that regression in BookKeeper 4.16.0 is fixed.
 *
 * Anti-regression test for issue https://github.com/apache/pulsar/issues/20091.
 */
@Slf4j
public class BrokerInstallWithEntryMetadataInterceptorsTest extends PulsarClusterTestBase {
    private static final String PREFIX = "PULSAR_PREFIX_";

    @BeforeClass(alwaysRun = true)
    @Override
    public final void setupCluster() throws Exception {
        incrementSetupNumber();

        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> !s.isEmpty())
                .collect(joining("-"));
        brokerEnvs.put(PREFIX + "exposingBrokerEntryMetadataToClientEnabled", "true");
        brokerEnvs.put(PREFIX + "brokerEntryMetadataInterceptors", "org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .brokerEnvs(brokerEnvs)
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
    public void testBrokerHttpServerIsRunning() throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyBroker().execCmd(
                PulsarCluster.CURL,
                "-X",
                "GET",
                "http://localhost:8080/admin/v2/brokers/health");
        assertEquals(result.getExitCode(), 0);
        assertEquals("ok", result.getStdout());
    }
}
