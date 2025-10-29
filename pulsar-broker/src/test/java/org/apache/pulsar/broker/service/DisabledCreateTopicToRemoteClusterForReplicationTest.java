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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-replication")
public class DisabledCreateTopicToRemoteClusterForReplicationTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
        admin1.namespaces().setRetention(replicatedNamespace, new RetentionPolicies(300, 1024));
        admin2.namespaces().setRetention(replicatedNamespace, new RetentionPolicies(300, 1024));
        admin1.namespaces().setRetention(nonReplicatedNamespace, new RetentionPolicies(300, 1024));
        admin2.namespaces().setRetention(nonReplicatedNamespace, new RetentionPolicies(300, 1024));
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
        config.setCreateTopicToRemoteClusterForReplication(false);
        config.setReplicationStartAt("earliest");
    }

    @Test
    public void testCreatePartitionedTopicWithNsReplication() throws Exception {
        String ns = defaultTenant + "/" + UUID.randomUUID().toString().replace("-", "");
        admin1.namespaces().createNamespace(ns);
        admin2.namespaces().createNamespace(ns);
        admin1.namespaces().setRetention(ns, new RetentionPolicies(3600, -1));
        admin2.namespaces().setRetention(ns, new RetentionPolicies(3600, -1));

        // Create non-partitioned topic.
        // Enable replication.
        final String tp = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp_");
        final String part1 = TopicName.get(tp).getPartition(0).toString();
        admin1.topics().createPartitionedTopic(tp, 1);
        admin1.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1, cluster2)));

        // Trigger and wait for replicator starts.
        String msgValue = "msg-1";
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(tp).create();
        producer1.send(msgValue);
        producer1.close();
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic topicPart1 = (PersistentTopic) broker1.getTopic(part1, false).join().get();
            assertFalse(topicPart1.getReplicators().isEmpty());
        });

        // Verify: there is no topic with the same name on the remote cluster.
        try {
            admin2.topics().getPartitionedTopicMetadata(tp);
            fail("Expected a not found ex");
        } catch (PulsarAdminException.NotFoundException ex) {
            // expected.
        }

        // Verify: after creating the topic on the remote cluster, all things are fine.
        admin2.topics().createPartitionedTopic(tp, 1);
        Consumer<String> consumer2 = client2.newConsumer(Schema.STRING).topic(tp).isAckReceiptEnabled(true)
                .subscriptionName("s1").subscribe();
        assertEquals(consumer2.receive(10, TimeUnit.SECONDS).getValue(), msgValue);
        consumer2.close();

        // cleanup.
        admin1.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1)));
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic topicPart1 = (PersistentTopic) broker1.getTopic(part1, false).join().get();
            assertTrue(topicPart1.getReplicators().isEmpty());
        });
        admin1.topics().deletePartitionedTopic(tp, false);
        admin2.topics().deletePartitionedTopic(tp, false);
        admin1.namespaces().deleteNamespace(ns);
        admin2.namespaces().deleteNamespace(ns);
    }

    @Test
    public void testEnableTopicReplication() throws Exception {
        String ns = nonReplicatedNamespace;

        // Create non-partitioned topic.
        // Enable replication.
        final String tp = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp_");
        final String part1 = TopicName.get(tp).getPartition(0).toString();
        admin1.topics().createPartitionedTopic(tp, 1);
        admin1.topics().setReplicationClusters(tp, Arrays.asList(cluster1, cluster2));

        // Trigger and wait for replicator starts.
        Producer<String> p1 = client1.newProducer(Schema.STRING).topic(tp).create();
        p1.send("msg-1");
        p1.close();
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic topicPart1 = (PersistentTopic) broker1.getTopic(part1, false).join().get();
            assertFalse(topicPart1.getReplicators().isEmpty());
        });

        // Verify: there is no topic with the same name on the remote cluster.
        try {
            admin2.topics().getPartitionedTopicMetadata(tp);
            fail("Expected a not found ex");
        } catch (PulsarAdminException.NotFoundException ex) {
            // expected.
        }

        // Verify: after creating the topic on the remote cluster, all things are fine.
        admin2.topics().createPartitionedTopic(tp, 1);
        waitReplicatorStarted(part1);

        // cleanup.
        admin1.topics().setReplicationClusters(tp, Arrays.asList(cluster1));
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic topicPart1 = (PersistentTopic) broker1.getTopic(part1, false).join().get();
            assertTrue(topicPart1.getReplicators().isEmpty());
        });
        admin1.topics().deletePartitionedTopic(tp, false);
        admin2.topics().deletePartitionedTopic(tp, false);
    }

    @Test
    public void testNonPartitionedTopic() throws Exception {
        String ns = nonReplicatedNamespace;

        // Create non-partitioned topic.
        // Enable replication.
        final String tp = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp_");
        admin1.topics().createNonPartitionedTopic(tp);
        admin1.topics().setReplicationClusters(tp, Arrays.asList(cluster1, cluster2));

        // Trigger and wait for replicator starts.
        Producer<String> p1 = client1.newProducer(Schema.STRING).topic(tp).create();
        p1.send("msg-1");
        p1.close();
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic topicPart1 = (PersistentTopic) broker1.getTopic(tp, false).join().get();
            assertFalse(topicPart1.getReplicators().isEmpty());
        });

        // Verify: there is no topic with the same name on the remote cluster.
        try {
            admin2.topics().getPartitionedTopicMetadata(tp);
            fail("Expected a not found ex");
        } catch (PulsarAdminException.NotFoundException ex) {
            // expected.
        }

        // Verify: after creating the topic on the remote cluster, all things are fine.
        admin2.topics().createNonPartitionedTopic(tp);
        waitReplicatorStarted(tp);

        // cleanup.
        admin1.topics().setReplicationClusters(tp, Arrays.asList(cluster1));
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic topicPart1 = (PersistentTopic) broker1.getTopic(tp, false).join().get();
            assertTrue(topicPart1.getReplicators().isEmpty());
        });
        admin1.topics().delete(tp, false);
        admin2.topics().delete(tp, false);
    }
}
