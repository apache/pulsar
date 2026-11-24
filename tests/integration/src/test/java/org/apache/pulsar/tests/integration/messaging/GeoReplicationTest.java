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
package org.apache.pulsar.tests.integration.messaging;

import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.TOPIC_FIELDS;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.assertInstance;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.get;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarGeoClusterTestBase;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Geo replication test.
 */
@CustomLog
public class GeoReplicationTest extends PulsarGeoClusterTestBase {

    private static final int PARTITION_COUNT = 10;
    private static final int MESSAGES = 10;

    @BeforeClass(alwaysRun = true)
    public final void setupBeforeClass() throws Exception {
        setup();
    }

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder[] beforeSetupCluster (
            PulsarClusterSpec.PulsarClusterSpecBuilder... specBuilder) {
        if (specBuilder != null) {
            Map<String, String> brokerEnvs = new HashMap<>();
            brokerEnvs.put("systemTopicEnabled", "false");
            brokerEnvs.put("topicLevelPoliciesEnabled", "false");
            for (PulsarClusterSpec.PulsarClusterSpecBuilder builder : specBuilder) {
                builder.brokerEnvs(brokerEnvs);
            }
        }
        return specBuilder;
    }

    @AfterClass(alwaysRun = true)
    public final void tearDownAfterClass() throws Exception {
        cleanup();
    }

    @Test(timeOut = 120_000, dataProvider = "TopicDomain")
    public void testTopicReplication(String domain) throws Exception {
        String cluster1 = getGeoCluster().getClusters()[0].getClusterName();
        String cluster2 = getGeoCluster().getClusters()[1].getClusterName();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(getGeoCluster().getClusters()[0].getHttpServiceUrl())
                .requestTimeout(30, TimeUnit.SECONDS)
                .build();

        String topic = domain + "://public/default/testTopicReplication-" + UUID.randomUUID();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try {
                admin.topics().createPartitionedTopic(topic, PARTITION_COUNT);
            } catch (Exception e) {
                log.error().attr("topic", topic).exception(e).log("Failed to create partitioned topic .");
                Assert.fail("Failed to create partitioned topic " + topic);
            }
            Assert.assertEquals(admin.topics().getPartitionedTopicMetadata(topic).partitions, PARTITION_COUNT);
        });
        log.info().attr("topic", topic).log("Test geo-replication produce and consume for topic .");

        @Cleanup
        PulsarClient client1 = PulsarClient.builder()
                .serviceUrl(getGeoCluster().getClusters()[0].getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarClient client2 = PulsarClient.builder()
                .serviceUrl(getGeoCluster().getClusters()[1].getPlainTextServiceUrl())
                .build();

        @Cleanup
        Producer<byte[]> p = client1.newProducer()
                .topic(topic)
                .create();
        log.info()
                .attr("cluster", cluster1)
                .attr("topic", topic)
                .log("Successfully create producer in cluster for topic .");

        @Cleanup
        Consumer<byte[]> c = client2.newConsumer()
                .topic(topic)
                .subscriptionName("geo-sub")
                .subscribe();
        log.info()
                .attr("cluster", cluster2)
                .attr("topic", topic)
                .log("Successfully create consumer in cluster for topic .");

        if ("non-persistent".equals(domain)) {
            waitForNonPersistentReplicators(admin, topic, cluster2);
        }

        Set<Integer> publishedPartitions = new HashSet<>();
        for (int i = 0; i < MESSAGES; i++) {
            MessageIdAdv id = (MessageIdAdv) p.send(String.format("Message [%d]", i).getBytes(StandardCharsets.UTF_8));
            publishedPartitions.add(id.getPartitionIndex());
        }
        log.info()
                .attr("cluster", cluster1)
                .attr("topic", topic)
                .log("Successfully produce message to cluster for topic .");

        for (int i = 0; i < MESSAGES; i++) {
            Message<byte[]> message = c.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
        }
        log.info()
                .attr("cluster", cluster2)
                .attr("topic", topic)
                .log("Successfully consume message from cluster for topic .");

        String partition = TopicName.get(topic).getPartition(0).toString();
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            JsonNode detailed = get(admin, topic, "partitioned-stats?perPartition=true");
            JsonNode aggregate = get(admin, topic, "partitioned-stats?perPartition=false");
            JsonNode partitionStats = detailed.path("partitions").path(partition).path("replication").path(cluster2);
            JsonNode direct = get(admin, partition, "stats?getPreciseBacklog=true&excludePublishers=true")
                    .path("replication").path(cluster2);
            Assert.assertEquals(aggregate.path("partitions").size(), 0);
            for (JsonNode top : new JsonNode[]{aggregate, detailed}) {
                JsonNode row = top.path("replication").path(cluster2);
                Assert.assertTrue(row.isObject(), top.toString());
                TOPIC_FIELDS.forEach(field -> Assert.assertFalse(row.has(field), field));
            }
            Assert.assertEquals(detailed.path("partitions").size(), PARTITION_COUNT);
            Assert.assertTrue(direct.isObject() && partitionStats.isObject());
            JsonNode internal = get(admin, partition, "internalStats");
            if ("persistent".equals(domain)) {
                Assert.assertEquals(direct.path("replicatorState").asText(), "STARTED");
                for (int i = 0; i < PARTITION_COUNT; i++) {
                    String name = TopicName.get(topic).getPartition(i).toString();
                    JsonNode row = detailed.path("partitions").path(name).path("replication").path(cluster2);
                    Assert.assertEquals(row.path("replicatorState").asText(), "STARTED", name);
                    Assert.assertEquals(row.path("producerState").asText(), "READY", name);
                    Assert.assertTrue(row.path("replicatorHealth").path("healthy").asBoolean(), name);
                    if (publishedPartitions.contains(i)) {
                        Assert.assertTrue(row.path("lastSendStartedAt").isIntegralNumber(), name);
                        Assert.assertTrue(row.path("lastReplicationSucceededAt").isIntegralNumber(), name);
                    }
                    GeoReplicationAdminStats.assertUnavailable(row.path("lastCursorReadStartedAt"));
                }
                JsonNode partitionedInternal = get(admin, topic, "partitioned-internalStats");
                Assert.assertFalse(partitionedInternal.has("replicators"));
                Assert.assertEquals(partitionedInternal.path("partitions").size(), PARTITION_COUNT);
                assertInstance(internal.path("replicators").path(cluster2));
                for (JsonNode perPartition : partitionedInternal.path("partitions")) {
                    assertInstance(perPartition.path("replicators").path(cluster2));
                }
            } else {
                for (String field : TOPIC_FIELDS) {
                    Assert.assertFalse(direct.has(field), field);
                    for (JsonNode perPartition : detailed.path("partitions")) {
                        Assert.assertFalse(perPartition.path("replication").path(cluster2).has(field), field);
                    }
                }
                Assert.assertFalse(internal.has("replicators"));
            }
        });
        if ("persistent".equals(domain)) {
            JsonNode cli = ObjectMapperFactory.getMapper().reader().readTree(getGeoCluster().getClusters()[0]
                    .runAdminCommandOnAnyBroker("topics", "partitioned-stats-internal", topic).getStdout());
            Assert.assertFalse(cli.has("replicators"));
            JsonNode instance = cli.path("partitions").path(partition).path("replicators").path(cluster2);
            Assert.assertTrue(instance.path("producerQueueMaxSize").isIntegralNumber(), instance.toString());
        }
    }

    private void waitForNonPersistentReplicators(PulsarAdmin admin, String topic, String remoteCluster)
            throws Exception {
        TopicName topicName = TopicName.get(topic);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            for (int i = 0; i < PARTITION_COUNT; i++) {
                String partitionName = topicName.getPartition(i).toString();
                ReplicatorStats replicatorStats = admin.topics()
                        .getStats(partitionName)
                        .getReplication()
                        .get(remoteCluster);
                Assert.assertNotNull(replicatorStats);
                Assert.assertTrue(replicatorStats.isConnected());
            }
        });
    }
}
