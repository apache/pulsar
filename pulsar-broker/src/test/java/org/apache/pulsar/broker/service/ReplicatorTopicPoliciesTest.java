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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Starts 3 brokers that are in 3 different clusters
 */
@Test(groups = "broker")
public class ReplicatorTopicPoliciesTest extends ReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        config1.setSystemTopicEnabled(true);
        config1.setDefaultNumberOfNamespaceBundles(1);
        config1.setTopicLevelPoliciesEnabled(true);
        config2.setSystemTopicEnabled(true);
        config2.setTopicLevelPoliciesEnabled(true);
        config2.setDefaultNumberOfNamespaceBundles(1);
        config3.setSystemTopicEnabled(true);
        config3.setTopicLevelPoliciesEnabled(true);
        config3.setDefaultNumberOfNamespaceBundles(1);
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testReplicateQuotaTopicPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // set BacklogQuota
        BacklogQuotaImpl backlogQuota = new BacklogQuotaImpl();
        backlogQuota.setLimitSize(1);
        backlogQuota.setLimitTime(2);
        admin1.topicPolicies(true).setBacklogQuota(topic, backlogQuota);

        Awaitility.await().untilAsserted(() ->
                assertTrue(admin2.topicPolicies(true).getBacklogQuotaMap(topic).containsValue(backlogQuota)));
        Awaitility.await().untilAsserted(() ->
                assertTrue(admin3.topicPolicies(true).getBacklogQuotaMap(topic).containsValue(backlogQuota)));
        //remove BacklogQuota
        admin1.topicPolicies(true).removeBacklogQuota(topic);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getBacklogQuotaMap(topic).size(), 0));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getBacklogQuotaMap(topic).size(), 0));
    }

    @Test
    public void testReplicateMessageTTLPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // set message ttl
        admin1.topicPolicies(true).setMessageTTL(topic, 10);
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getMessageTTL(topic).intValue(), 10));
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getMessageTTL(topic).intValue(), 10));
        //remove message ttl
        admin1.topicPolicies(true).removeMessageTTL(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getMessageTTL(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getMessageTTL(topic)));
    }

    @Test
    public void testReplicatePersistentPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // set PersistencePolicies
        PersistencePolicies policies = new PersistencePolicies(5, 3, 2, 1000);
        admin1.topicPolicies(true).setPersistence(topic, policies);

        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getPersistence(topic), policies));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getPersistence(topic), policies));
        //remove PersistencePolicies
        admin1.topicPolicies(true).removePersistence(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getPersistence(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getPersistence(topic)));
    }

    @Test
    public void testReplicateDeduplicationStatusPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // set subscription types policies
        admin1.topicPolicies(true).setDeduplicationStatus(topic, true);
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertTrue(admin2.topicPolicies(true).getDeduplicationStatus(topic)));
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertTrue(admin3.topicPolicies(true).getDeduplicationStatus(topic)));
        // remove subscription types policies
        admin1.topicPolicies(true).removeDeduplicationStatus(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getDeduplicationStatus(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getDeduplicationStatus(topic)));

    }

    @Test
    public void testReplicatorTopicPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();

        init(namespace, persistentTopicName);
        // set retention
        RetentionPolicies retentionPolicies = new RetentionPolicies(1, 1);
        admin1.topicPolicies(true).setRetention(persistentTopicName, retentionPolicies);

        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies));

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin1.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies);
            assertNull(admin1.topicPolicies().getRetention(persistentTopicName));
        });

        //remove retention
        admin1.topicPolicies(true).removeRetention(persistentTopicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getRetention(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getRetention(persistentTopicName)));
    }
    @Test
    public void testReplicateSubscriptionTypesPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        Set<SubscriptionType> subscriptionTypes = new HashSet<>();
        subscriptionTypes.add(SubscriptionType.Shared);
        // set subscription types policies
        admin1.topicPolicies(true).setSubscriptionTypesEnabled(topic, subscriptionTypes);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getSubscriptionTypesEnabled(topic), subscriptionTypes));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getSubscriptionTypesEnabled(topic), subscriptionTypes));
        // remove subscription types policies
        admin1.topicPolicies(true).removeSubscriptionTypesEnabled(topic);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getSubscriptionTypesEnabled(topic), Collections.emptySet()));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getSubscriptionTypesEnabled(topic), Collections.emptySet()));

    }

    private void init(String namespace, String topic)
            throws PulsarAdminException, PulsarClientException, PulsarServerException {
        final String cluster2 = pulsar2.getConfig().getClusterName();
        final String cluster1 = pulsar1.getConfig().getClusterName();
        final String cluster3 = pulsar3.getConfig().getClusterName();

        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(cluster1, cluster2, cluster3));
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));
        // Create partitioned-topic from R1
        admin1.topics().createPartitionedTopic(topic, 3);
        // List partitioned topics from R2
        Awaitility.await().untilAsserted(() -> assertNotNull(admin2.topics().getPartitionedTopicList(namespace)));
        Awaitility.await().untilAsserted(() -> assertEquals(
                admin2.topics().getPartitionedTopicList(namespace).get(0), topic));
        assertEquals(admin1.topics().getList(namespace).size(), 3);
        // List partitioned topics from R3
        Awaitility.await().untilAsserted(() -> assertNotNull(admin3.topics().getPartitionedTopicList(namespace)));
        Awaitility.await().untilAsserted(() -> assertEquals(
                admin3.topics().getPartitionedTopicList(namespace).get(0), topic));

        pulsar1.getClient().newProducer().topic(topic).create().close();
        pulsar2.getClient().newProducer().topic(topic).create().close();
        pulsar3.getClient().newProducer().topic(topic).create().close();

        //init topic policies server
        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            assertNull(pulsar1.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)));
            assertNull(pulsar2.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)));
            assertNull(pulsar3.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)));
        });
    }

}
