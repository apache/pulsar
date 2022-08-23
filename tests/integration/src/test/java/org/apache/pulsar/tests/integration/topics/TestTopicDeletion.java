/**
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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

/**
 * Test cases for compaction.
 */
@Slf4j
public class TestTopicDeletion extends PulsarTestSuite {

    final private boolean unload = false;
    final private int numBrokers = 2;

    public void setupCluster() throws Exception {
        brokerEnvs.put("managedLedgerMaxEntriesPerLedger", "10");
        brokerEnvs.put("brokerDeleteInactivePartitionedTopicMetadataEnabled", "false");
        brokerEnvs.put("brokerDeleteInactiveTopicsEnabled", "false");
        this.setupCluster("");
    }

    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder.numBrokers(numBrokers);
        specBuilder.enableContainerLog(true);
        return specBuilder;
    }

    @Test(dataProvider = "ServiceUrls", timeOut=300_000)
    public void testPartitionedTopicForceDeletion(Supplier<String> serviceUrl) throws Exception {

        log.info("Creating tenant and namespace");

        final String tenant = "test-partitioned-topic-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/partitioned-topic";
        final int numPartitions = numBrokers * 3;
        final int numKeys = numPartitions * 50;
        final String subscriptionName = "sub1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-clusters", "--clusters", pulsarCluster.getClusterName(), namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-retention", "--size", "100M", "--time", "100m", namespace);

        this.createPartitionedTopic(topic, numPartitions);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl.get()).build()) {

            log.info("Creating consumer");
            Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName(subscriptionName)
                    .subscribe();

            log.info("Producing messages");
            try(Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .create()
            ) {
                for (int i = 0; i < numKeys; i++) {
                    producer.newMessage()
                        .key("" + i)
                        .value(("value-" + i).getBytes(UTF_8))
                        .sendAsync();
                }
                producer.flush();
                log.info("Successfully wrote {} values", numKeys);
            }

            log.info("Consuming half of the messages");
            for (int i = 0; i < numKeys / 2; i++) {
                Message<byte[]> m = consumer.receive(1, TimeUnit.MINUTES);
                log.info("Read value {}", m.getKey());
            }

            if (unload) {
                log.info("Unloading topic");
                pulsarCluster.runAdminCommandOnAnyBroker("topics",
                        "unload", topic);
            }

            ContainerExecResult res;
            log.info("Deleting the topic");
            try {
                res = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                        "delete-partitioned-topic", "--force", topic);
                assertNotEquals(0, res.getExitCode());
            } catch (ContainerExecException e) {
                log.info("Second delete failed with ContainerExecException, could be ok", e);
                if (!e.getMessage().contains("with error code 1")) {
                    fail("Expected different error code");
                }
            }

            log.info("Close the consumer and delete the topic again");
            consumer.close();

            res = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "delete-partitioned-topic", "--force", topic);
            assertNotEquals(0, res.getExitCode());

            Thread.sleep(5000);
            // should succeed
            log.info("Creating the topic again");
            this.createPartitionedTopic(topic, numBrokers * 2);
        }
    }


    private ContainerExecResult createTenantName(final String tenantName,
                                                 final String allowedClusterName,
                                                 final String adminRoleName) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
            "tenants", "create", "--allowed-clusters", allowedClusterName,
            "--admin-roles", adminRoleName, tenantName);
        assertEquals(0, result.getExitCode());
        return result;
    }

    private ContainerExecResult createNamespace(final String Ns) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces",
                "create",
                "--clusters",
                pulsarCluster.getClusterName(), Ns);
        assertEquals(0, result.getExitCode());
        return result;
    }

    private ContainerExecResult createPartitionedTopic(final String partitionedTopicName, int numPartitions)
            throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
            "topics",
            "create-partitioned-topic",
            "--partitions", "" + numPartitions,
            partitionedTopicName);
        assertEquals(0, result.getExitCode());
        return result;
    }


}
