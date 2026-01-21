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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for validation when setting namespace replication clusters.
 * These tests verify that partition compatibility and auto-topic creation policy
 * compatibility are properly validated when enabling namespace-level replication.
 */
@Slf4j
@Test(groups = "broker-admin")
public class SetReplicationClustersValidationTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }
    /**
     * Helper method to clean up a namespace properly.
     * First removes replication clusters (if any), then deletes the namespace.
     */
    private void clearReplicationPolicies(String namespace) throws PulsarAdminException {
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1));
        admin2.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster2));
    }

    @Test
    public void testSetReplicationClustersSuccess() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testSetReplicationClustersSuccess");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        // Set replication clusters should succeed when no topics exist
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
        Set<String> clusters = new HashSet<>(admin1.namespaces().getNamespaceReplicationClusters(namespace));
        assertEquals(clusters, Sets.newHashSet(cluster1, cluster2));
        // cleanup
        clearReplicationPolicies(namespace);
    }

    @Test
    public void testSetReplicationClustersWithMatchingPartitionedTopics() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testMatchingPartitions");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        String topic = "persistent://" + namespace + "/partitioned-topic";
        // Create partitioned topic with same partition count on both clusters
        admin1.topics().createPartitionedTopic(topic, 4);
        admin2.topics().createPartitionedTopic(topic, 4);
        // Set replication clusters should succeed
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
        Set<String> clusters = new HashSet<>(admin1.namespaces().getNamespaceReplicationClusters(namespace));
        assertEquals(clusters, Sets.newHashSet(cluster1, cluster2));
        // cleanup
        clearReplicationPolicies(namespace);
        admin1.topics().deletePartitionedTopic(topic);
        admin2.topics().deletePartitionedTopic(topic);
    }

    @Test
    public void testSetReplicationClustersWithMismatchedPartitionedTopics() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testMismatchedPartitions");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        String topic = "persistent://" + namespace + "/partitioned-topic";
        // Create partitioned topic with different partition counts
        admin1.topics().createPartitionedTopic(topic, 4);
        admin2.topics().createPartitionedTopic(topic, 8);
        // Set replication clusters should fail due to partition mismatch
        try {
            admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
            fail("Should have failed due to partition count mismatch");
        } catch (PulsarAdminException.ConflictException e) {
            assertTrue(e.getMessage().contains("Partition count mismatch"));
            assertTrue(e.getMessage().contains("local cluster has 4 partitions"));
            assertTrue(e.getMessage().contains("has 8 partitions"));
        }
        // cleanup
        clearReplicationPolicies(namespace);
        admin1.topics().deletePartitionedTopic(topic);
        admin2.topics().deletePartitionedTopic(topic);
    }

    @Test
    public void testSetReplicationClustersTopicExistsOnlyOnLocal() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testTopicOnlyOnLocal");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        String topic = "persistent://" + namespace + "/local-only-topic";
        // Create partitioned topic only on local cluster
        admin1.topics().createPartitionedTopic(topic, 4);
        // Set replication clusters should succeed (topic doesn't exist on remote)
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
        Set<String> clusters = new HashSet<>(admin1.namespaces().getNamespaceReplicationClusters(namespace));
        assertEquals(clusters, Sets.newHashSet(cluster1, cluster2));
        // cleanup
        clearReplicationPolicies(namespace);
        admin1.topics().deletePartitionedTopic(topic);
        admin2.namespaces().unload(namespace);
    }

    @Test
    public void testSetReplicationClustersNonPartitionedVsPartitioned() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testNonPartitionedVsPartitioned");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        String topic = "persistent://" + namespace + "/topic-type-mismatch";
        // Create non-partitioned topic on local, partitioned topic on remote
        admin1.topics().createNonPartitionedTopic(topic);
        admin2.topics().createPartitionedTopic(topic, 4);
        // Set replication clusters should fail due to topic type mismatch
        try {
            admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
            fail("Should have failed due to topic type mismatch");
        } catch (PulsarAdminException.ConflictException e) {
            assertTrue(e.getMessage().contains("Topic type mismatch"));
            assertTrue(e.getMessage().contains("non-partitioned topic"));
            assertTrue(e.getMessage().contains("partitioned topic"));
        }
        // cleanup
        clearReplicationPolicies(namespace);
        admin1.topics().delete(topic);
        admin2.topics().deletePartitionedTopic(topic);
    }

    @Test
    public void testSetReplicationClustersWithMatchingAutoTopicCreationPolicy() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testMatchingAutoTopicPolicy");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        // Set same auto-topic creation policy on both namespaces
        AutoTopicCreationOverride policy = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .defaultNumPartitions(4)
                .build();
        admin1.namespaces().setAutoTopicCreation(namespace, policy);
        admin2.namespaces().setAutoTopicCreation(namespace, policy);
        // Set replication clusters should succeed
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
        Set<String> clusters = new HashSet<>(admin1.namespaces().getNamespaceReplicationClusters(namespace));
        assertEquals(clusters, Sets.newHashSet(cluster1, cluster2));
        // cleanup
        clearReplicationPolicies(namespace);
    }

    @Test
    public void testSetReplicationClustersWithMismatchedAutoTopicCreationType() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testMismatchedAutoTopicType");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        // Set different auto-topic creation types
        AutoTopicCreationOverride policy1 = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .defaultNumPartitions(4)
                .build();
        AutoTopicCreationOverride policy2 = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.NON_PARTITIONED.toString())
                .build();
        admin1.namespaces().setAutoTopicCreation(namespace, policy1);
        admin2.namespaces().setAutoTopicCreation(namespace, policy2);
        // Set replication clusters should fail due to policy mismatch
        try {
            admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
            fail("Should have failed due to auto-topic creation policy mismatch");
        } catch (PulsarAdminException.ConflictException e) {
            assertTrue(e.getMessage().contains("auto-topic creation policy mismatch"));
            assertTrue(e.getMessage().contains("topicType"));
        }
    }

    @Test
    public void testSetReplicationClustersWithMismatchedDefaultNumPartitions() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testMismatchedNumPartitions");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        // Set different default partition counts
        AutoTopicCreationOverride policy1 = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .defaultNumPartitions(4)
                .build();
        AutoTopicCreationOverride policy2 = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .defaultNumPartitions(8)
                .build();
        admin1.namespaces().setAutoTopicCreation(namespace, policy1);
        admin2.namespaces().setAutoTopicCreation(namespace, policy2);

        // Set replication clusters should fail due to partition count mismatch
        try {
            admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
            fail("Should have failed due to defaultNumPartitions mismatch");
        } catch (PulsarAdminException.ConflictException e) {
            assertTrue(e.getMessage().contains("auto-topic creation policy mismatch"));
            assertTrue(e.getMessage().contains("defaultNumPartitions"));
        }
    }

    @Test
    public void testSetReplicationClustersSkipsValidationWhenLocalClusterNotInSet() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("public/testSkipValidationNoLocalCluster");
        admin1.namespaces().createNamespace(namespace);
        admin2.namespaces().createNamespace(namespace);
        String topic = "persistent://" + namespace + "/partitioned-topic";
        // Create partitioned topic with different counts.
        admin1.topics().createPartitionedTopic(topic, 1);
        admin2.topics().createPartitionedTopic(topic, 4);
        // This should succeed because validation is skipped when local cluster is not in the set
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster2));
        Set<String> clusters = new HashSet<>(admin1.namespaces().getNamespaceReplicationClusters(namespace));
        assertEquals(clusters, Sets.newHashSet(cluster2));
        // cleanup
        clearReplicationPolicies(namespace);
    }

}

