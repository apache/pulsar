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

import lombok.extern.slf4j.Slf4j;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class OneWayReplicatorUsingGlobalZKTest extends OneWayReplicatorTest {

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

    @Test(enabled = false)
    public void testReplicatorProducerStatInTopic() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Test(enabled = false)
    public void testCreateRemoteConsumerFirst() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Test(enabled = false)
    public void testTopicCloseWhenInternalProducerCloseErrorOnce() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Test(enabled = false)
    public void testConcurrencyOfUnloadBundleAndRecreateProducer() throws Exception {
        super.testConcurrencyOfUnloadBundleAndRecreateProducer();
    }

    @Test(enabled = false)
    public void testPartitionedTopicLevelReplication() throws Exception {
        super.testPartitionedTopicLevelReplication();
    }

    @Test(enabled = false)
    public void testPartitionedTopicLevelReplicationRemoteTopicExist() throws Exception {
        super.testPartitionedTopicLevelReplicationRemoteTopicExist();
    }

    @Test(enabled = false)
    public void testPartitionedTopicLevelReplicationRemoteConflictTopicExist() throws Exception {
        super.testPartitionedTopicLevelReplicationRemoteConflictTopicExist();
    }

    @Test(enabled = false)
    public void testConcurrencyOfUnloadBundleAndRecreateProducer2() throws Exception {
        super.testConcurrencyOfUnloadBundleAndRecreateProducer2();
    }

    @Test(enabled = false)
    public void testUnFenceTopicToReuse() throws Exception {
        super.testUnFenceTopicToReuse();
    }

    @Test
    public void testDeleteNonPartitionedTopic() throws Exception {
        super.testDeleteNonPartitionedTopic();
    }

    @Test
    public void testDeletePartitionedTopic() throws Exception {
        super.testDeletePartitionedTopic();
    }

    @Test(enabled = false)
    public void testNoExpandTopicPartitionsWhenDisableTopicLevelReplication() throws Exception {
        super.testNoExpandTopicPartitionsWhenDisableTopicLevelReplication();
    }

    @Test(enabled = false)
    public void testExpandTopicPartitionsOnNamespaceLevelReplication() throws Exception {
        super.testExpandTopicPartitionsOnNamespaceLevelReplication();
    }

    @Test(enabled = false)
    public void testReloadWithTopicLevelGeoReplication(ReplicationLevel replicationLevel) throws Exception {
        super.testReloadWithTopicLevelGeoReplication(replicationLevel);
    }

    @Test
    public void testRemoveCluster() throws Exception {
        // Initialize.
        final String ns1 = defaultTenant + "/" + "ns_73b1a31afce34671a5ddc48fe5ad7fc8";
        final String topic = "persistent://" + ns1 + "/___tp-5dd50794-7af8-4a34-8a0b-06188052c66a";
        final String topicChangeEvents = "persistent://" + ns1 + "/__change_events";
        admin1.namespaces().createNamespace(ns1);
        admin1.namespaces().setNamespaceReplicationClusters(ns1, new HashSet<>(Arrays.asList(cluster1, cluster2)));
        admin1.topics().createNonPartitionedTopic(topic);

        // Wait for loading topic up.
        Producer<String> p = client1.newProducer(Schema.STRING).topic(topic).create();
        Awaitility.await().untilAsserted(() -> {
            ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> tps = pulsar1.getBrokerService().getTopics();
            assertTrue(tps.containsKey(topic));
            assertTrue(tps.containsKey(topicChangeEvents));
        });

        // The topics under the namespace of the cluster-1 will be deleted.
        // Verify the result.
        admin1.namespaces().setNamespaceReplicationClusters(ns1, new HashSet<>(Arrays.asList(cluster2)));
        Awaitility.await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> tps = pulsar1.getBrokerService().getTopics();
            assertFalse(tps.containsKey(topic));
            assertFalse(tps.containsKey(topicChangeEvents));
            assertFalse(pulsar1.getNamespaceService().checkTopicExists(TopicName.get(topic)).join());
            assertFalse(pulsar1.getNamespaceService()
                    .checkTopicExists(TopicName.get(topicChangeEvents)).join());
        });

        // cleanup.
        p.close();
        admin2.topics().delete(topic);
        admin2.namespaces().deleteNamespace(ns1);
    }
}
