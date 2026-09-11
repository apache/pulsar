/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.tests.integration.topics;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testng.annotations.Test;

/**
 * Integration test for the PIP-475 regular-to-scalable topic migration, exercising the
 * {@code pulsar-admin scalable-topics migrate} command against a real multi-broker cluster
 * (real metadata store, BookKeeper, and cross-broker bundle ownership).
 *
 * <p>The V5-client transparent transition across the migration boundary is covered by the
 * in-process broker test {@code V5MigrationEndToEndTest}; this test focuses on what the
 * dockerized cluster adds: that the migration command, its CLI wiring, the resulting scalable
 * metadata, and the post-migration termination of the old topic all behave in a real
 * deployment.
 */
@CustomLog
public class TestScalableTopicMigration extends PulsarTestSuite {

    private final int numBrokers = 2;

    public void setupCluster() throws Exception {
        this.setupCluster("");
    }

    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder.numBrokers(numBrokers);
        specBuilder.enableContainerLog(true);
        return specBuilder;
    }

    @Test(dataProvider = "ServiceUrls", timeOut = 300_000)
    public void testMigrateRegularTopicToScalable(Supplier<String> serviceUrl) throws Exception {
        final String nsName = "mig-" + randomName(6);
        final String namespace = "public/" + nsName;
        final String shortTopic = namespace + "/regular";
        final String topic = "persistent://" + shortTopic;
        final int numPartitions = numBrokers * 2;

        pulsarCluster.createNamespace(nsName);
        pulsarCluster.createPartitionedTopic(topic, numPartitions);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl.get()).build()) {
            // Seed data on the regular partitioned topic (lands across the partitions).
            try (Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
                for (int i = 0; i < 50; i++) {
                    producer.newMessage().key("k-" + i).value(("v-" + i).getBytes(UTF_8)).send();
                }
            }

            // Migrate via the admin CLI.
            ContainerExecResult migrate = pulsarCluster.runAdminCommandOnAnyBroker(
                    "scalable-topics", "migrate", topic);
            assertEquals(migrate.getExitCode(), 0L, "migrate failed: " + migrate.getStderr());

            // The topic is now scalable: get-metadata returns its segment DAG.
            ContainerExecResult metadata = pulsarCluster.runAdminCommandOnAnyBroker(
                    "scalable-topics", "get-metadata", shortTopic);
            assertEquals(metadata.getExitCode(), 0L, "get-metadata failed: " + metadata.getStderr());
            assertTrue(metadata.getStdout().contains("segmentId"),
                    "scalable metadata should list segments, got: " + metadata.getStdout());

            // v4 lockout: the old partitions are terminated, so a legacy v4 producer can no
            // longer write to the topic.
            try (Producer<byte[]> blocked = client.newProducer().topic(topic).create()) {
                blocked.send("blocked".getBytes(UTF_8));
                fail("v4 produce to a migrated (terminated) topic must fail");
            } catch (PulsarClientException expected) {
                log.info().exceptionMessage(expected)
                        .log("v4 producer correctly locked out after migration");
            }
        }
    }
}
