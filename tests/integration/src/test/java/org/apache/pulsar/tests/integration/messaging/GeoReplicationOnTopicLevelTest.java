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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarGeoClusterTestBase;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Geo replication test on topic level.
 */
@Slf4j
public class GeoReplicationOnTopicLevelTest extends PulsarGeoClusterTestBase {

    @BeforeMethod(alwaysRun = true)
    public final void setupBeforeMethod() throws Exception {
        setup();
    }

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder[] beforeSetupCluster (
            PulsarClusterSpec.PulsarClusterSpecBuilder... specBuilder) {
        if (specBuilder != null) {
            Map<String, String> brokerEnvs = new HashMap<>();
            brokerEnvs.put("systemTopicEnabled", "true");
            brokerEnvs.put("topicLevelPoliciesEnabled", "true");
            for(PulsarClusterSpec.PulsarClusterSpecBuilder builder : specBuilder) {
                builder.brokerEnvs(brokerEnvs);
            }
        }
        return specBuilder;
    }

    @AfterMethod(alwaysRun = true)
    public final void tearDownAfterMethod() throws Exception {
        cleanup();
    }

    @Test(timeOut = 1000 * 60)
    public void testTopicReplication() throws Exception {
        // TODO: Disable non-persistent test due to `admin1.topics().setReplicationClusters` throws TopicNotFound.

        String domain = "persistent";
        String cluster1Name = getGeoCluster().getClusters()[0].getClusterName();
        PulsarCluster cluster2 = getGeoCluster().getClusters()[1];
        String cluster2Name = cluster2.getClusterName();

        @Cleanup
        PulsarAdmin admin1 = PulsarAdmin.builder()
                .serviceHttpUrl(getGeoCluster().getClusters()[0].getHttpServiceUrl())
                .requestTimeout(30, TimeUnit.SECONDS)
                .build();

        ClusterData cluster2Data = ClusterData.builder()
                .serviceUrl(cluster2.getInternalHttpServiceUrl())
                .brokerServiceUrl(cluster2.getInternalBrokerServiceUrl()).build();
        admin1.clusters().createCluster(cluster2Name, cluster2Data);

        String tenant = "public";
        TenantInfo tenantInfo = admin1.tenants().getTenantInfo(tenant);
        tenantInfo.getAllowedClusters().add(cluster2Name);
        admin1.tenants().updateTenant(tenant, tenantInfo);

        String topic = domain + "://public/default/testTopicReplication-" + UUID.randomUUID();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try {
                admin1.topics().createPartitionedTopic(topic, 10);
            } catch (Exception e) {
                log.error("Failed to create partitioned topic {}.", topic, e);
                Assert.fail("Failed to create partitioned topic " + topic);
            }
            Assert.assertEquals(admin1.topics().getPartitionedTopicMetadata(topic).partitions, 10);
        });

        admin1.topics().setReplicationClusters(topic, new ArrayList<>(tenantInfo.getAllowedClusters()));

        log.info("Test geo-replication produce and consume for topic {}.", topic);
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
        log.info("Successfully create producer in cluster {} for topic {}.", cluster1Name, topic);

        @Cleanup
        Consumer<byte[]> c = client2.newConsumer()
                .topic(topic)
                .subscriptionName("geo-sub")
                .subscribe();
        log.info("Successfully create consumer in cluster {} for topic {}.", cluster2Name, topic);

        for (int i = 0; i < 10; i++) {
            p.send(String.format("Message [%d]", i).getBytes(StandardCharsets.UTF_8));
        }
        log.info("Successfully produce message to cluster {} for topic {}.", cluster1Name, topic);

        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = c.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
        }

        log.info("Successfully consume message from cluster {} for topic {}.", cluster2, topic);

        @Cleanup
        PulsarAdmin admin2 = PulsarAdmin.builder()
                .serviceHttpUrl(getGeoCluster().getClusters()[1].getHttpServiceUrl())
                .requestTimeout(30, TimeUnit.SECONDS)
                .build();
        List<String> partitionedTopicList = admin2.topics().getPartitionedTopicList("public/default");
        Assert.assertTrue(partitionedTopicList.contains(topic));
    }
}
