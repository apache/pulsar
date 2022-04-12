package org.apache.pulsar.tests.integration.suites;

import static java.util.stream.Collectors.joining;
import java.util.stream.Stream;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class PulsarSchemaTestSuite extends PulsarClusterTestBase {

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder.schemaRegistryClassName("org.apache.pulsar.broker.service.schema.MockSchemaRegistry");
        specBuilder.schemaRegistryStorageClassName("org.apache.pulsar.tests.integration.schema.MockSchemaStorage");
        return specBuilder;
    }

    @BeforeClass(alwaysRun = true)
    @Override
    public final void setupCluster() throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .clusterName(clusterName)
                .schemaRegistryClassName("org.apache.pulsar.broker.service.schema.MockSchemaRegistry")
                .schemaRegistryStorageClassName("org.apache.pulsar.tests.integration.schema.MockSchemaStorage")
                .build();

        setupCluster(spec);
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void tearDownCluster() throws Exception {
        super.tearDownCluster();
    }
}