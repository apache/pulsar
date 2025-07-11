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
package org.apache.pulsar.tests.integration.upgrade;

import com.github.dockerjava.api.model.Bind;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testng.annotations.Test;
import java.util.stream.Stream;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;

/**
 * Test upgrading/downgrading Pulsar cluster from major releases.
 */
@Slf4j
public class PulsarUpgradeDowngradeTest extends PulsarClusterTestBase {

    @Test(timeOut=600_000)
    public void upgradeTest() throws Exception {
        testUpgradeDowngrade(PulsarContainer.LAST_RELEASE_IMAGE_NAME, PulsarContainer.UPGRADE_TEST_IMAGE_NAME);
    }

    private void testUpgradeDowngrade(String imageOld, String imageNew) throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> !s.isEmpty())
                .collect(joining("-"));
        String topicName = generateTopicName("testupdown", true);

        @Cleanup
        Network network = Network.newNetwork();
        @Cleanup
        GenericContainer<?> alpine = new GenericContainer<>(PulsarContainer.ALPINE_IMAGE_NAME)
                .withExposedPorts(80)
                .withNetwork(network)
                .withNetworkAliases("shared-storage")
                .withEnv("MAGIC_NUMBER", "42")
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                    .getHostConfig()
                    .withBinds(Bind.parse("/pulsar/data:/pulsar/data")))
                .withCommand("/bin/sh", "-c",
                        "mkdir -p /pulsar/data && "
                                + "chmod -R ug+rwx /pulsar/data && "
                                + "chown -R 10000:0 /pulsar/data && "
                                + "rm -rf /pulsar/data/* && "
                                + "while true; do echo \"$MAGIC_NUMBER\" | nc -l -p 80; done");
        alpine.start();

        PulsarClusterSpec specOld = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .clusterName(clusterName)
                .dataContainer(alpine)
                .pulsarTestImage(imageOld)
                .build();

        PulsarClusterSpec specNew = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .clusterName(clusterName)
                .dataContainer(alpine)
                .pulsarTestImage(imageNew)
                .build();

        log.info("Setting up OLD cluster {} with {} bookies, {} brokers using {}",
                specOld.clusterName(), specOld.numBookies(), specOld.numBrokers(), imageOld);

        pulsarCluster = PulsarCluster.forSpec(specNew, network);
        pulsarCluster.closeNetworkOnExit = false;
        pulsarCluster.start(true);

        try {
            log.info("setting retention");
            pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-retention", "--size", "100M", "--time", "100m", "public/default");

            publishAndConsume(topicName, pulsarCluster.getPlainTextServiceUrl(), 10, 10);
        } finally {
            pulsarCluster.stop();
        }

        log.info("Upgrading to NEW cluster {} with {} bookies, {} brokers using {}",
                specNew.clusterName(), specNew.numBookies(), specNew.numBrokers(), imageNew);

        pulsarCluster = PulsarCluster.forSpec(specNew, network);
        pulsarCluster.closeNetworkOnExit = false;
        pulsarCluster.start(false);

        try {
            publishAndConsume(topicName, pulsarCluster.getPlainTextServiceUrl(), 10, 20);
        } finally {
            pulsarCluster.stop();
        }

        log.info("Downgrading to OLD cluster {} with {} bookies, {} brokers using {}",
                specOld.clusterName(), specOld.numBookies(), specOld.numBrokers(), imageOld);

        pulsarCluster = PulsarCluster.forSpec(specOld, network);
        pulsarCluster.closeNetworkOnExit = false;
        pulsarCluster.start(false);

        try {
            publishAndConsume(topicName, pulsarCluster.getPlainTextServiceUrl(), 10, 30);
        } finally {
            pulsarCluster.stop();
            alpine.stop();
            network.close();
        }
    }

    private void publishAndConsume(String topicName, String serviceUrl, int numProduce, int numConsume) throws Exception {
        log.info("publishAndConsume: topic name: {}", topicName);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .create();

        log.info("Publishing {} messages", numProduce);
        for (int i = numConsume - numProduce; i < numConsume; i++) {
            log.info("Publishing message: {}", "smoke-message-" + i);
            producer.send("smoke-message-" + i);
        }

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        consumer.seek(MessageId.earliest);

        log.info("Consuming {} messages", numConsume);
        for (int i = 0; i < numConsume; i++) {
            log.info("Waiting for message: {}", i);
            Message<String> m = consumer.receive();
            log.info("Received message: {}", m.getValue());
            assertEquals("smoke-message-" + i, m.getValue());
        }
    }
}
