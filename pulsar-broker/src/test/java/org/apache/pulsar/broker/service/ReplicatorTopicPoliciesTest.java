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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
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
        // local
        admin1.topicPolicies().setBacklogQuota(topic, backlogQuota);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies().getBacklogQuotaMap(topic).size(), 0));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies().getBacklogQuotaMap(topic).size(), 0));
        // global
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
        // local
        admin1.topicPolicies().setMessageTTL(topic, 10);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getMessageTTL(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getMessageTTL(topic)));
        // global
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
    public void testReplicateSubscribeRatePolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // set global topic policy
        SubscribeRate subscribeRate = new SubscribeRate(100, 10000);
        // local
        admin1.topicPolicies().setSubscribeRate(topic, subscribeRate);
        untilRemoteClustersAsserted(admin -> assertNull(admin.topicPolicies().getSubscribeRate(topic)));
        // global
        admin1.topicPolicies(true).setSubscribeRate(topic, subscribeRate);

        // get global topic policy
        untilRemoteClustersAsserted(
                admin -> assertEquals(admin.topicPolicies(true).getSubscribeRate(topic), subscribeRate));

        // remove global topic policy
        admin1.topicPolicies(true).removeSubscribeRate(topic);
        untilRemoteClustersAsserted(admin -> assertNull(admin.topicPolicies(true).getSubscribeRate(topic)));
    }

    @Test
    public void testReplicateMaxMessageSizePolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // local
        admin1.topicPolicies().setMaxMessageSize(topic, 1000);
        untilRemoteClustersAsserted(admin -> assertNull(admin.topicPolicies().getMaxMessageSize(topic)));
        // global
        admin1.topicPolicies(true).setMaxMessageSize(topic, 1000);

        // get global topic policy
        untilRemoteClustersAsserted(
                admin -> assertEquals(admin.topicPolicies(true).getMaxMessageSize(topic), Integer.valueOf(1000)));

        // remove global topic policy
        admin1.topicPolicies(true).removeMaxMessageSize(topic);
        untilRemoteClustersAsserted(admin -> assertNull(admin.topicPolicies(true).getMaxMessageSize(topic)));
    }

    @Test
    public void testReplicatePublishRatePolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // set global topic policy
        PublishRate publishRate = new PublishRate(100, 10000);
        // local
        admin1.topicPolicies().setPublishRate(topic, publishRate);
        untilRemoteClustersAsserted(admin -> assertNull(admin.topicPolicies().getPublishRate(topic)));
        // global
        admin1.topicPolicies(true).setPublishRate(topic, publishRate);

        // get global topic policy
        untilRemoteClustersAsserted(
                admin -> assertEquals(admin.topicPolicies(true).getPublishRate(topic), publishRate));

        // remove global topic policy
        admin1.topicPolicies(true).removePublishRate(topic);
        untilRemoteClustersAsserted(admin -> assertNull(admin.topicPolicies(true).getPublishRate(topic)));
    }

    @Test
    public void testReplicateDeduplicationSnapshotIntervalPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // local
        admin1.topicPolicies().setDeduplicationSnapshotInterval(topic, 100);
        untilRemoteClustersAsserted(
                admin -> assertNull(admin.topicPolicies().getDeduplicationSnapshotInterval(topic)));
        // global
        admin1.topicPolicies(true).setDeduplicationSnapshotInterval(topic, 100);

        // get global topic policy
        untilRemoteClustersAsserted(
                admin -> assertEquals(admin.topicPolicies(true).getDeduplicationSnapshotInterval(topic),
                        Integer.valueOf(100)));

        // remove global topic policy
        admin1.topicPolicies(true).removeDeduplicationSnapshotInterval(topic);
        untilRemoteClustersAsserted(
                admin -> assertNull(admin.topicPolicies(true).getDeduplicationSnapshotInterval(topic)));
    }

    private void untilRemoteClustersAsserted(ThrowingConsumer<PulsarAdmin> condition) {
        Awaitility.await().untilAsserted(() -> condition.apply(admin2));
        Awaitility.await().untilAsserted(() -> condition.apply(admin3));
    }

    private interface ThrowingConsumer<I> {
        void apply(I input) throws Throwable;
    }


    @Test
    public void testReplicatePersistentPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // set PersistencePolicies
        PersistencePolicies policies = new PersistencePolicies(5, 3, 2, 1000);
        // local
        admin1.topicPolicies().setPersistence(topic, policies);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getPersistence(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getPersistence(topic)));
        // global
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
        // local
        admin1.topicPolicies().setDeduplicationStatus(topic, true);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getDeduplicationStatus(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getDeduplicationStatus(topic)));
        // global
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
    public void testReplicatorMaxProducer() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // local
        admin1.topicPolicies().setMaxProducers(topic, 100);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getMaxProducers(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getMaxProducers(topic)));
        // global
        admin1.topicPolicies(true).setMaxProducers(topic, 100);
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getMaxProducers(topic).intValue(), 100));
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getMaxProducers(topic).intValue(), 100));

        // remove max producer policies
        admin1.topicPolicies(true).removeMaxProducers(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getMaxProducers(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getMaxProducers(topic)));
    }

    @Test
    public void testReplicatorMaxConsumerPerSubPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();

        init(namespace, topic);
        // local
        admin1.topicPolicies().setMaxConsumersPerSubscription(topic, 100);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getMaxConsumersPerSubscription(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getMaxConsumersPerSubscription(topic)));
        // global
        admin1.topicPolicies(true).setMaxConsumersPerSubscription(topic, 100);
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getMaxConsumersPerSubscription(topic).intValue(), 100));
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getMaxConsumersPerSubscription(topic).intValue(), 100));

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin1.topicPolicies(true).getMaxConsumersPerSubscription(topic).intValue(), 100);
        });

        //remove max consumer per sub
        admin1.topicPolicies(true).removeMaxConsumersPerSubscription(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getMaxConsumersPerSubscription(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getMaxConsumersPerSubscription(topic)));
    }

    @Test
    public void testReplicateMaxUnackedMsgPerConsumer() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // local
        admin1.topicPolicies().setMaxUnackedMessagesOnConsumer(topic, 100);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getMaxUnackedMessagesOnConsumer(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getMaxUnackedMessagesOnConsumer(topic)));
        // global
        admin1.topicPolicies(true).setMaxUnackedMessagesOnConsumer(topic, 100);
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getMaxUnackedMessagesOnConsumer(topic).intValue(), 100));
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getMaxUnackedMessagesOnConsumer(topic).intValue(), 100));
        // remove max unacked msgs per consumers
        admin1.topicPolicies(true).removeMaxUnackedMessagesOnConsumer(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getMaxUnackedMessagesOnConsumer(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getMaxUnackedMessagesOnConsumer(topic)));
    }

    @Test
    public void testReplicatorTopicPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();

        init(namespace, persistentTopicName);
        // set retention
        RetentionPolicies retentionPolicies = new RetentionPolicies(1, 1);
        // local
        admin1.topicPolicies().setRetention(persistentTopicName, retentionPolicies);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getRetention(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getRetention(persistentTopicName)));
        // global
        admin1.topicPolicies(true).setRetention(persistentTopicName, retentionPolicies);

        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies));

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin1.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies);
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
        // local
        admin1.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypes);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getSubscriptionTypesEnabled(topic), null));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getSubscriptionTypesEnabled(topic), null));

        // global
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


    @Test
    public void testReplicateMaxConsumers() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // local
        admin1.topicPolicies().setMaxConsumers(topic, 100);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getMaxConsumers(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getMaxConsumers(topic)));
        // global
        admin1.topicPolicies(true).setMaxConsumers(topic, 100);
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getMaxConsumers(topic).intValue(), 100));
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getMaxConsumers(topic).intValue(), 100));
        // remove max consumers
        admin1.topicPolicies(true).removeMaxConsumers(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getMaxConsumers(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getMaxConsumers(topic)));
    }

    @Test
    public void testReplicatorMessageDispatchRatePolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();

        init(namespace, persistentTopicName);
        // set dispatchRate
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1)
                .dispatchThrottlingRateInMsg(2)
                .ratePeriodInSecond(3)
                .relativeToPublishRate(true)
                .build();
        // local
        admin1.topicPolicies().setDispatchRate(persistentTopicName, dispatchRate);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getDispatchRate(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getDispatchRate(persistentTopicName)));
        // global
        admin1.topicPolicies(true).setDispatchRate(persistentTopicName, dispatchRate);

        // get dispatchRate
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getDispatchRate(persistentTopicName), dispatchRate));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getDispatchRate(persistentTopicName), dispatchRate));

        //remove dispatchRate
        admin1.topicPolicies(true).removeDispatchRate(persistentTopicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getDispatchRate(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getDispatchRate(persistentTopicName)));
    }

    @Test
    public void testReplicateDelayedDelivery() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        DelayedDeliveryPolicies policies = DelayedDeliveryPolicies.builder().active(true).tickTime(10000L).build();
        // local
        admin1.topicPolicies().setDelayedDeliveryPolicy(topic, policies);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getDelayedDeliveryPolicy(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getDelayedDeliveryPolicy(topic)));
        // global
        admin1.topicPolicies(true).setDelayedDeliveryPolicy(topic, policies);
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getDelayedDeliveryPolicy(topic), policies));
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getDelayedDeliveryPolicy(topic), policies));
        // remove delayed delivery
        admin1.topicPolicies(true).removeDelayedDeliveryPolicy(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getDelayedDeliveryPolicy(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getDelayedDeliveryPolicy(topic)));
    }

    @Test
    public void testReplicatorInactiveTopicPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, persistentTopicName);

        // set InactiveTopicPolicies
        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true);
        // local
        admin1.topicPolicies().setInactiveTopicPolicies(persistentTopicName, inactiveTopicPolicies);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getInactiveTopicPolicies(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getInactiveTopicPolicies(persistentTopicName)));
        // global
        admin1.topicPolicies(true).setInactiveTopicPolicies(persistentTopicName, inactiveTopicPolicies);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true)
                        .getInactiveTopicPolicies(persistentTopicName), inactiveTopicPolicies));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true)
                        .getInactiveTopicPolicies(persistentTopicName), inactiveTopicPolicies));
        // remove InactiveTopicPolicies
        admin1.topicPolicies(true).removeInactiveTopicPolicies(persistentTopicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getInactiveTopicPolicies(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getInactiveTopicPolicies(persistentTopicName)));
    }

    @Test
    public void testReplicatorSubscriptionDispatchRatePolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();

        init(namespace, persistentTopicName);
        // set subscription dispatch rate
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1)
                .ratePeriodInSecond(1)
                .dispatchThrottlingRateInByte(1)
                .relativeToPublishRate(true)
                .build();
        // local
        admin1.topicPolicies().setSubscriptionDispatchRate(persistentTopicName, dispatchRate);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getSubscriptionDispatchRate(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getSubscriptionDispatchRate(persistentTopicName)));
        // global
        admin1.topicPolicies(true).setSubscriptionDispatchRate(persistentTopicName, dispatchRate);
        // get subscription dispatch rate
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true)
                        .getSubscriptionDispatchRate(persistentTopicName), dispatchRate));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true)
                        .getSubscriptionDispatchRate(persistentTopicName), dispatchRate));

        //remove subscription dispatch rate
        admin1.topicPolicies(true).removeSubscriptionDispatchRate(persistentTopicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getSubscriptionDispatchRate(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getSubscriptionDispatchRate(persistentTopicName)));
    }

    @Test
    public void testReplicateReplicatorDispatchRatePolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();

        init(namespace, persistentTopicName);
        // set replicator dispatch rate
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1)
                .ratePeriodInSecond(1)
                .dispatchThrottlingRateInByte(1)
                .relativeToPublishRate(true)
                .build();
        // local
        admin1.topicPolicies().setReplicatorDispatchRate(persistentTopicName, dispatchRate);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getReplicatorDispatchRate(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getReplicatorDispatchRate(persistentTopicName)));
        // global
        admin1.topicPolicies(true).setReplicatorDispatchRate(persistentTopicName, dispatchRate);
        // get replicator dispatch rate
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true)
                        .getReplicatorDispatchRate(persistentTopicName), dispatchRate));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true)
                        .getReplicatorDispatchRate(persistentTopicName), dispatchRate));

        //remove replicator dispatch rate
        admin1.topicPolicies(true).removeReplicatorDispatchRate(persistentTopicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getReplicatorDispatchRate(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getReplicatorDispatchRate(persistentTopicName)));
    }

    @Test
    public void testReplicateMaxUnackedMsgPerSub() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, topic);
        // local
        admin1.topicPolicies().setMaxUnackedMessagesOnSubscription(topic, 100);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getMaxUnackedMessagesOnSubscription(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getMaxUnackedMessagesOnSubscription(topic)));
        // global
        admin1.topicPolicies(true).setMaxUnackedMessagesOnSubscription(topic, 100);
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getMaxUnackedMessagesOnSubscription(topic).intValue(), 100));
        Awaitility.await().ignoreExceptions().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getMaxUnackedMessagesOnSubscription(topic).intValue(), 100));
        // remove max unacked msgs per sub
        admin1.topicPolicies(true).removeMaxUnackedMessagesOnSubscription(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getMaxUnackedMessagesOnSubscription(topic)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getMaxUnackedMessagesOnSubscription(topic)));
    }

    @Test
    public void testReplicatorCompactionThresholdPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();

        init(namespace, persistentTopicName);
        // local
        admin1.topicPolicies().setCompactionThreshold(persistentTopicName, 1);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getCompactionThreshold(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getCompactionThreshold(persistentTopicName)));
        // global
        admin1.topicPolicies(true).setCompactionThreshold(persistentTopicName, 1);
        // get compaction threshold
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true)
                        .getCompactionThreshold(persistentTopicName), Long.valueOf(1)));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true)
                        .getCompactionThreshold(persistentTopicName), Long.valueOf(1)));

        //remove compaction threshold
        admin1.topicPolicies(true).removeCompactionThreshold(persistentTopicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getCompactionThreshold(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getCompactionThreshold(persistentTopicName)));
    }

    @Test
    public void testReplicateMaxSubscriptionsPerTopic() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        init(namespace, persistentTopicName);
        // local
        admin1.topicPolicies().setMaxSubscriptionsPerTopic(persistentTopicName, 1024);
        untilRemoteClustersAsserted(
                admin -> assertNull(admin.topicPolicies().getMaxSubscriptionsPerTopic(persistentTopicName)));

        // global
        admin1.topicPolicies(true).setMaxSubscriptionsPerTopic(persistentTopicName, 1024);

        //get max subscriptions per topic
        untilRemoteClustersAsserted(
                admin -> assertEquals(admin.topicPolicies(true).getMaxSubscriptionsPerTopic(persistentTopicName),
                        Integer.valueOf(1024)));

        //remove
        admin1.topicPolicies(true).removeMaxSubscriptionsPerTopic(persistentTopicName);
        untilRemoteClustersAsserted(
                admin -> assertNull(admin.topicPolicies(true).getMaxSubscriptionsPerTopic(persistentTopicName)));
    }

    @Test
    public void testReplicatorOffloadPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/topic" + UUID.randomUUID();

        init(namespace, persistentTopicName);
        OffloadPoliciesImpl offloadPolicies =
                OffloadPoliciesImpl.create("s3", "region", "bucket", "endpoint", null, null, null, null,
                8, 9, 10L, null, OffloadedReadPriority.BOOKKEEPER_FIRST);
        // local
        try {
            admin1.topicPolicies().setOffloadPolicies(persistentTopicName, offloadPolicies);
        } catch (Exception exception){
            // driver not found exception.
            assertTrue(exception instanceof PulsarAdminException.ServerSideErrorException);
        }
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies().getOffloadPolicies(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies().getOffloadPolicies(persistentTopicName)));
        // global
        try{
            admin1.topicPolicies(true).setOffloadPolicies(persistentTopicName, offloadPolicies);
        }catch (Exception exception){
            // driver not found exception.
            assertTrue(exception instanceof PulsarAdminException.ServerSideErrorException);
        }
        // get offload policies
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin2.topicPolicies(true).getOffloadPolicies(persistentTopicName), offloadPolicies));
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin3.topicPolicies(true).getOffloadPolicies(persistentTopicName), offloadPolicies));

        //remove offload policies
        admin1.topicPolicies(true).removeOffloadPolicies(persistentTopicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin2.topicPolicies(true).getOffloadPolicies(persistentTopicName)));
        Awaitility.await().untilAsserted(() ->
                assertNull(admin3.topicPolicies(true).getOffloadPolicies(persistentTopicName)));
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
