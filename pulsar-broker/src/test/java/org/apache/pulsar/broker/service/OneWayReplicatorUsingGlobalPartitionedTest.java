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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.service.TopicPoliciesService.GetType.GLOBAL_ONLY;
import static org.apache.pulsar.broker.service.TopicPoliciesService.GetType.LOCAL_ONLY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
// TODO: This test is in flaky group until CI is fixed.
// To be addressed as part of https://github.com/apache/pulsar/pull/24154
@Test(groups = "flaky")
public class OneWayReplicatorUsingGlobalPartitionedTest extends OneWayReplicatorTest {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.usingGlobalZK = true;
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Override
    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        config.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        config.setDefaultNumPartitions(1);
    }

    @Override
    @Test(enabled = false)
    public void testReplicatorProducerStatInTopic() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Override
    @Test(enabled = false)
    public void testDeleteRemoteTopicByGlobalPolicy() throws Exception {
        super.testDeleteRemoteTopicByGlobalPolicy();
    }

    @Override
    @Test(enabled = false)
    public void testPoliciesOverWrite() throws Exception {
        super.testPoliciesOverWrite();
    }

    @Override
    @Test(enabled = false)
    public void testCreateRemoteConsumerFirst() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Override
    @Test(enabled = false)
    public void testTopicCloseWhenInternalProducerCloseErrorOnce() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Override
    @Test(enabled = false)
    public void testConcurrencyOfUnloadBundleAndRecreateProducer() throws Exception {
        super.testConcurrencyOfUnloadBundleAndRecreateProducer();
    }

    @Override
    @Test(enabled = false)
    public void testPartitionedTopicLevelReplication() throws Exception {
        super.testPartitionedTopicLevelReplication();
    }

    @Override
    @Test(enabled = false)
    public void testPartitionedTopicLevelReplicationRemoteTopicExist() throws Exception {
        super.testPartitionedTopicLevelReplicationRemoteTopicExist();
    }

    @Override
    @Test(enabled = false)
    public void testPartitionedTopicLevelReplicationRemoteConflictTopicExist() throws Exception {
        super.testPartitionedTopicLevelReplicationRemoteConflictTopicExist();
    }

    @Override
    @Test(enabled = false)
    public void testConcurrencyOfUnloadBundleAndRecreateProducer2() throws Exception {
        super.testConcurrencyOfUnloadBundleAndRecreateProducer2();
    }

    @Override
    @Test(enabled = false)
    public void testUnFenceTopicToReuse() throws Exception {
        super.testUnFenceTopicToReuse();
    }

    @Override
    @Test(enabled = false)
    public void testDeleteNonPartitionedTopic() throws Exception {
        super.testDeleteNonPartitionedTopic();
    }

    @Override
    @Test(enabled = false)
    public void testDeletePartitionedTopic() throws Exception {
        super.testDeletePartitionedTopic();
    }

    @Override
    @Test(enabled = false)
    public void testNoExpandTopicPartitionsWhenDisableTopicLevelReplication() throws Exception {
        super.testNoExpandTopicPartitionsWhenDisableTopicLevelReplication();
    }

    @Override
    @Test(enabled = false)
    public void testExpandTopicPartitionsOnNamespaceLevelReplication() throws Exception {
        super.testExpandTopicPartitionsOnNamespaceLevelReplication();
    }

    @Override
    @Test(enabled = false)
    public void testReloadWithTopicLevelGeoReplication(ReplicationLevel replicationLevel) throws Exception {
        super.testReloadWithTopicLevelGeoReplication(replicationLevel);
    }

    @Test(enabled = false)
    @Override
    public void testConfigReplicationStartAt() throws Exception {
        super.testConfigReplicationStartAt();
    }

    @Test(enabled = false)
    @Override
    public void testDifferentTopicCreationRule(ReplicationMode replicationMode) throws Exception {
        super.testDifferentTopicCreationRule(replicationMode);
    }

    @Test(enabled = false)
    @Override
    public void testReplicationCountMetrics() throws Exception {
        super.testReplicationCountMetrics();
    }

    @Test(enabled = false)
    @Override
    public void testNonPersistentReplicatorQueueSize() throws Exception {
        super.testNonPersistentReplicatorQueueSize();
    }

    @DataProvider
    public Object[][] removeClusterLevels() {
        return new Object[][] {
            {"namespace"},
            {"topic"}
        };
    }

    @Test(timeOut = 60_000, dataProvider = "removeClusterLevels")
    public void testRemoveCluster(String removeClusterLevel) throws Exception {
        // Initialize.
        final String ns1 = defaultTenant + "/" + "ns_73b1a31afce34671a5ddc48fe5ad7fc8";
        final String topic = "persistent://" + ns1 + "/___tp-5dd50794-7af8-4a34-8a0b-06188052c66a";
        final String topicP0 = TopicName.get(topic).getPartition(0).toString();
        final String topicP1 = TopicName.get(topic).getPartition(1).toString();
        final String topicChangeEvents = "persistent://" + ns1 + "/__change_events-partition-0";
        admin1.namespaces().createNamespace(ns1);
        admin1.namespaces().setNamespaceReplicationClusters(ns1, new HashSet<>(Arrays.asList(cluster1, cluster2)));
        admin1.topics().createPartitionedTopic(topic, 2);
        PublishRate publishRateAddGlobal = new PublishRate(100, 10000);
        admin1.topicPolicies(true).setPublishRate(topic, publishRateAddGlobal);
        PublishRate publishRateAddLocal1 = new PublishRate(200, 20000);
        admin1.topicPolicies(false).setPublishRate(topic, publishRateAddLocal1);
        PublishRate publishRateAddLocal2 = new PublishRate(300, 30000);
        admin2.topicPolicies(false).setPublishRate(topic, publishRateAddLocal2);
        Awaitility.await().untilAsserted(() -> {
           assertTrue(pulsar2.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                   .partitionedTopicExists(TopicName.get(topic)));
            List<CompletableFuture<StoredSchema>> schemaList11 =
                    pulsar1.getSchemaStorage().getAll(TopicName.get(topic).getSchemaName()).get();
            assertEquals(schemaList11.size(), 0);
            List<CompletableFuture<StoredSchema>> schemaList21 =
                    pulsar2.getSchemaStorage().getAll(TopicName.get(topic).getSchemaName()).get();
            assertEquals(schemaList21.size(), 0);
            PublishRate valueGlobal = admin2.topicPolicies(true).getPublishRate(topic);
            assertEquals(valueGlobal, publishRateAddGlobal);
            PublishRate valueLocal = admin2.topicPolicies(false).getPublishRate(topic);
            assertEquals(valueLocal, publishRateAddLocal2);
        });

        // Wait for copying messages.
        Producer<String> p = client1.newProducer(Schema.STRING).topic(topic).create();
        p.send("msg-1");
        p.close();
        Awaitility.await().untilAsserted(() -> {
            Map<String, CompletableFuture<Optional<Topic>>> tps = pulsar1.getBrokerService().getTopics();
            assertTrue(tps.containsKey(topicP0));
            assertTrue(tps.containsKey(topicP1));
            assertTrue(tps.containsKey(topicChangeEvents));
            Map<String, CompletableFuture<Optional<Topic>>> tps2 = pulsar2.getBrokerService().getTopics();
            assertTrue(tps2.containsKey(topicP0));
            assertTrue(tps2.containsKey(topicP1));
            assertTrue(tps2.containsKey(topicChangeEvents));
            List<CompletableFuture<StoredSchema>> schemaList12 =
                    pulsar1.getSchemaStorage().getAll(TopicName.get(topic).getSchemaName()).get();
            assertEquals(schemaList12.size(), 1);
            List<CompletableFuture<StoredSchema>> schemaList22 =
                    pulsar2.getSchemaStorage().getAll(TopicName.get(topic).getSchemaName()).get();
            assertEquals(schemaList12.size(), 1);
        });

        // The topics under the namespace of the cluster-1 will be deleted.
        // Verify the result.
        if ("namespace".equals(removeClusterLevel)) {
            admin1.namespaces().setNamespaceReplicationClusters(ns1, new HashSet<>(Arrays.asList(cluster2)));
        } else {
            admin1.topicPolicies(true).setReplicationClusters(topic, Arrays.asList(cluster2));
            admin2.topicPolicies(true).setReplicationClusters(topic, Arrays.asList(cluster2));
        }
        Awaitility.await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(() -> {
            Map<String, CompletableFuture<Optional<Topic>>> tps = pulsar1.getBrokerService().getTopics();
            assertFalse(tps.containsKey(topicP0));
            assertFalse(tps.containsKey(topicP1));
            if ("namespace".equals(removeClusterLevel)) {
                assertFalse(tps.containsKey(topicChangeEvents));
                assertFalse(pulsar1.getNamespaceService()
                        .checkTopicExistsAsync(TopicName.get(topicChangeEvents))
                        .get(5, TimeUnit.SECONDS).isExists());
            } else {
                assertTrue(tps.containsKey(topicChangeEvents));
                assertTrue(pulsar1.getNamespaceService()
                        .checkTopicExistsAsync(TopicName.get(topicChangeEvents))
                        .get(5, TimeUnit.SECONDS).isExists());
            }
            // Verify: schema will be removed in local cluster, and remote cluster will not.
            List<CompletableFuture<StoredSchema>> schemaList13 =
                    pulsar1.getSchemaStorage().getAll(TopicName.get(topic).getSchemaName()).get();
            assertEquals(schemaList13.size(), 0);
            List<CompletableFuture<StoredSchema>> schemaList23 =
                    pulsar2.getSchemaStorage().getAll(TopicName.get(topic).getSchemaName()).get();
            assertEquals(schemaList23.size(), 1);
            // Verify: the topic policies will be removed in local cluster, but remote cluster will not.
            if ("topic".equals(removeClusterLevel)) {
                Optional<TopicPolicies> localPolicies1 = pulsar1.getTopicPoliciesService()
                        .getTopicPoliciesAsync(TopicName.get(topic), LOCAL_ONLY).join();
                assertTrue(localPolicies1.isEmpty(), "Local cluster should have deleted local policies.");
                Optional<TopicPolicies> globalPolicies1 = pulsar1.getTopicPoliciesService()
                        .getTopicPoliciesAsync(TopicName.get(topic), GLOBAL_ONLY).join();
                assertTrue(globalPolicies1.isPresent(), "Local cluster should have global policies.");
                assertEquals(globalPolicies1.get().getPublishRate(), publishRateAddGlobal,
                        "Remote cluster should have global policies: publish rate.");
            }
            Optional<TopicPolicies> globalPolicies2 = pulsar2.getTopicPoliciesService()
                    .getTopicPoliciesAsync(TopicName.get(topic), GLOBAL_ONLY).join();
            assertTrue(globalPolicies2.isPresent(), "Remote cluster should have global policies.");
            assertEquals(globalPolicies2.get().getPublishRate(), publishRateAddGlobal,
                "Remote cluster should have global policies: publish rate.");
            Optional<TopicPolicies> localPolicies2 = pulsar2.getTopicPoliciesService()
                    .getTopicPoliciesAsync(TopicName.get(topic), LOCAL_ONLY).join();
            assertTrue(localPolicies2.isPresent(), "Remote cluster should have local policies.");
            assertEquals(localPolicies2.get().getPublishRate(), publishRateAddLocal2,
                "Remote cluster should have local policies: publish rate.");
        });

        // cleanup.
        if ("topic".equals(removeClusterLevel)) {
            admin1.namespaces().setNamespaceReplicationClusters(ns1, new HashSet<>(Arrays.asList(cluster2)));
        }
        admin2.topics().deletePartitionedTopic(topic);
        assertEquals(admin2.topics().getList(ns1).size(), 0);
        admin2.namespaces().deleteNamespace(ns1);
    }

    @Override
    @Test(dataProvider = "enableDeduplication", enabled = false)
    public void testIncompatibleMultiVersionSchema(boolean enableDeduplication) throws Exception {
        super.testIncompatibleMultiVersionSchema(enableDeduplication);
    }

    @Override
    @Test
    public void testTopicPoliciesReplicationRule() throws Exception {
        super.testTopicPoliciesReplicationRule();
    }

    @Override
    @Test
    public void testReplicatorsInflightTaskListIsEmptyAfterReplicationFinished() throws Exception {
        super.testReplicatorsInflightTaskListIsEmptyAfterReplicationFinished();
    }

    @Override
    @Test(enabled = false)
    public void testConcurrencyReplicationReadEntries() throws Exception {
        super.testConcurrencyReplicationReadEntries();
    }

    @Test(enabled = false)
    public void testCloseTopicAfterStartReplicationFailed() throws Exception {
        super.testCloseTopicAfterStartReplicationFailed();
    }

    @Override
    @Test
    public void testPartitionedTopicWithTopicPolicyAndNoReplicationClusters() throws Exception {
        super.testPartitionedTopicWithTopicPolicyAndNoReplicationClusters();
    }
}
