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
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class OneWayReplicatorUsingGlobalPartitionedTest extends OneWayReplicatorTestBase {

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

    @Test(timeOut = 60_000)
    public void testRemoveCluster() throws Exception {
        // Initialize.
        final String ns1 = defaultTenant + "/" + "ns_73b1a31afce34671a5ddc48fe5ad7fc8";
        final String topic = "persistent://" + ns1 + "/___tp-5dd50794-7af8-4a34-8a0b-06188052c66a";
        final String topicChangeEvents = "persistent://" + ns1 + "/__change_events-partition-0";
        admin1.namespaces().createNamespace(ns1);
        admin1.namespaces().setNamespaceReplicationClusters(ns1, new HashSet<>(Arrays.asList(cluster1, cluster2)));
        admin1.topics().createNonPartitionedTopic(topic);

        // Wait for loading topic up.
        Producer<String> p = client1.newProducer(Schema.STRING).topic(topic).create();
        Awaitility.await().untilAsserted(() -> {
            Map<String, CompletableFuture<Optional<Topic>>> tps = pulsar1.getBrokerService().getTopics();
            assertTrue(tps.containsKey(topic));
            assertTrue(tps.containsKey(topicChangeEvents));
        });

        // The topics under the namespace of the cluster-1 will be deleted.
        // Verify the result.
        admin1.namespaces().setNamespaceReplicationClusters(ns1, new HashSet<>(Arrays.asList(cluster2)));
        Awaitility.await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(() -> {
            Map<String, CompletableFuture<Optional<Topic>>> tps = pulsar1.getBrokerService().getTopics();
            assertFalse(tps.containsKey(topic));
            assertFalse(tps.containsKey(topicChangeEvents));
            assertFalse(pulsar1.getNamespaceService().checkTopicExists(TopicName.get(topic))
                    .get(5, TimeUnit.SECONDS).isExists());
            assertFalse(pulsar1.getNamespaceService()
                    .checkTopicExists(TopicName.get(topicChangeEvents))
                    .get(5, TimeUnit.SECONDS).isExists());
        });

        // cleanup.
        p.close();
        admin2.topics().delete(topic);
        admin2.namespaces().deleteNamespace(ns1);
    }

    /**
     * This test used to confirm the "start replicator retry task" will be skipped after the topic is closed.
     */
    @Test
    public void testCloseTopicAfterStartReplicationFailed() throws Exception {
        Field fieldTopicNameCache = TopicName.class.getDeclaredField("cache");
        fieldTopicNameCache.setAccessible(true);
        ConcurrentHashMap<String, TopicName> topicNameCache =
                (ConcurrentHashMap<String, TopicName>) fieldTopicNameCache.get(null);
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        // 1.Create topic, does not enable replication now.
        admin1.topics().createNonPartitionedTopic(topicName);
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();

        // We inject an error to make "start replicator" to fail.
        AsyncLoadingCache<String, Boolean> existsCache =
                WhiteboxImpl.getInternalState(pulsar1.getConfigurationMetadataStore(), "existsCache");
        String path = "/admin/partitioned-topics/" + TopicName.get(topicName).getPersistenceNamingEncoding();
        existsCache.put(path, CompletableFuture.completedFuture(true));

        // 2.Enable replication and unload topic after failed to start replicator.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        Thread.sleep(3000);
        producer1.close();
        existsCache.synchronous().invalidate(path);
        admin1.topics().unload(topicName);
        // Verify: the "start replicator retry task" will be skipped after the topic is closed.
        // - Retry delay is "PersistentTopic.POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS": 60s, so wait for 70s.
        // - Since the topic should not be touched anymore, we use "TopicName" to confirm whether it be used by
        //   Replication again.
        Thread.sleep(10 * 1000);
        topicNameCache.remove(topicName);
        Thread.sleep(60 * 1000);
        assertTrue(!topicNameCache.containsKey(topicName));

        // cleanup.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        admin1.topics().delete(topicName, false);
    }

    // https://github.com/apache/pulsar/issues/22967
    @Test
    public void testPartitionedTopicWithTopicPolicyAndNoReplicationClusters() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createPartitionedTopic(topicName, 2);
        try {
            admin1.topicPolicies().setMessageTTL(topicName, 5);
            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                assertEquals(admin2.topics().getPartitionedTopicMetadata(topicName).partitions, 2);
            });
            admin1.topics().updatePartitionedTopic(topicName, 3, false);
            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                assertEquals(admin2.topics().getPartitionedTopicMetadata(topicName).partitions, 3);
            });
        } finally {
            // cleanup.
            admin1.topics().deletePartitionedTopic(topicName, true);
        }
    }

    @Test(timeOut = 30 * 1000)
    public void testCreateRemoteAdminFailed() throws Exception {
        final TenantInfo tenantInfo = admin1.tenants().getTenantInfo(defaultTenant);
        final String ns1 = defaultTenant + "/ns_" + UUID.randomUUID().toString().replace("-", "");
        final String randomClusterName = "c_" + UUID.randomUUID().toString().replace("-", "");
        final String topic = BrokerTestUtil.newUniqueName(ns1 + "/tp");
        admin1.namespaces().createNamespace(ns1);
        admin1.topics().createPartitionedTopic(topic, 2);

        // Inject a wrong cluster data which with empty fields.
        ClusterResources clusterResources = broker1.getPulsar().getPulsarResources().getClusterResources();
        clusterResources.createCluster(randomClusterName, ClusterData.builder().build());
        Set<String> allowedClusters = new HashSet<>(tenantInfo.getAllowedClusters());
        allowedClusters.add(randomClusterName);
        admin1.tenants().updateTenant(defaultTenant, TenantInfo.builder().adminRoles(tenantInfo.getAdminRoles())
                .allowedClusters(allowedClusters).build());

        // Verify.
        try {
            admin1.topics().setReplicationClusters(topic, Arrays.asList(cluster1, randomClusterName));
            fail("Expected a error due to empty fields");
        } catch (Exception ex) {
            // Expected an error.
        }

        // cleanup.
        admin1.topics().deletePartitionedTopic(topic);
        admin1.tenants().updateTenant(defaultTenant, tenantInfo);
    }
}
