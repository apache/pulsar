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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class InactiveTopicDeleteTest extends BrokerTestBase {

    @BeforeMethod
    protected void setup() throws Exception {
        resetConfig();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testDeleteWhenNoSubscriptions() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testDeleteWhenNoSubscriptions";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("sub")
            .subscribe();

        consumer.close();
        producer.close();

        Awaitility.await().untilAsserted(() -> Assert.assertTrue(admin.topics().getList("prop/ns-abc")
                .contains(topic)));

        admin.topics().deleteSubscription(topic, "sub");
        Awaitility.await().untilAsserted(() -> Assert.assertFalse(admin.topics().getList("prop/ns-abc")
                .contains(topic)));
    }

    @Test
    public void testDeleteAndCleanZkNode() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        conf.setBrokerDeleteInactivePartitionedTopicMetadataEnabled(true);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testDeleteWhenNoSubscriptions";
        admin.topics().createPartitionedTopic(topic, 5);
        pulsarClient.newProducer().topic(topic).create().close();
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();

        Awaitility.await()
                .untilAsserted(() -> Assert.assertTrue(admin.topics().getPartitionedTopicList("prop/ns-abc")
                .contains(topic)));

        admin.topics().deleteSubscription(topic, "sub");
        Awaitility.await()
                .untilAsserted(() -> Assert.assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc")
                .contains(topic)));
    }

    @Test
    public void testWhenSubPartitionNotDelete() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        conf.setBrokerDeleteInactivePartitionedTopicMetadataEnabled(true);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testDeleteWhenNoSubscriptions";
        final TopicName topicName = TopicName.get(topic);
        admin.topics().createPartitionedTopic(topic, 5);
        pulsarClient.newProducer().topic(topic).create().close();
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();

        Thread.sleep(2000);

        // Topic should not be deleted
        Assert.assertTrue(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topic));

        admin.topics().deleteSubscription(topic, "sub");
        Awaitility.await().untilAsserted(() -> {
            // Now the topic should be deleted
            Assert.assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topic));
        });
    }

    @Test
    public void testNotEnabledDeleteZkNode() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        conf.setBrokerDeleteInactiveTopicsEnabled(true);
        super.baseSetup();
        final String namespace = "prop/ns-abc";
        final String topic = "persistent://prop/ns-abc/testNotEnabledDeleteZkNode1";
        final String topic2 = "persistent://prop/ns-abc/testNotEnabledDeleteZkNode2";

        admin.topics().createPartitionedTopic(topic, 5);
        admin.topics().createNonPartitionedTopic(topic2);
        pulsarClient.newProducer().topic(topic).create().close();
        pulsarClient.newProducer().topic(topic2).create().close();
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();
        pulsarClient.newConsumer().topic(topic2).subscriptionName("sub2").subscribe().close();

        Awaitility.await()
                .untilAsserted(() -> Assert.assertTrue(admin.topics().getList(namespace).contains(topic2)));
        Assert.assertTrue(admin.topics().getPartitionedTopicList(namespace).contains(topic));

        admin.topics().deleteSubscription(topic, "sub");
        admin.topics().deleteSubscription(topic2, "sub2");
        Awaitility.await()
                .untilAsserted(() -> Assert.assertFalse(admin.topics().getList(namespace).contains(topic2)));
        Assert.assertTrue(admin.topics().getPartitionedTopicList(namespace).contains(topic));
        // BrokerDeleteInactivePartitionedTopicMetaDataEnabled is not enabled, so only NonPartitionedTopic will be cleaned
        Assert.assertFalse(admin.topics().getList(namespace).contains(topic2));
    }

    @Test(timeOut = 20000)
    public void testTopicPolicyUpdateAndClean() throws Exception {
        final String namespace = "prop/ns-abc";
        final String namespace2 = "prop/ns-abc2";
        final String namespace3 = "prop/ns-abc3";
        List<String> namespaceList = Arrays.asList(namespace2, namespace3);

        conf.setBrokerDeleteInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(1000);
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        InactiveTopicPolicies defaultPolicy = new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions
                , 1000, true);

        super.baseSetup();

        for (String ns : namespaceList) {
            admin.namespaces().createNamespace(ns);
            admin.namespaces().setNamespaceReplicationClusters(ns, Sets.newHashSet("test"));
        }

        final String topic = "persistent://prop/ns-abc/testDeletePolicyUpdate";
        final String topic2 = "persistent://prop/ns-abc2/testDeletePolicyUpdate";
        final String topic3 = "persistent://prop/ns-abc3/testDeletePolicyUpdate";
        List<String> topics = Arrays.asList(topic, topic2, topic3);

        for (String tp : topics) {
            admin.topics().createNonPartitionedTopic(tp);
        }

        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true);
        admin.namespaces().setInactiveTopicPolicies(namespace, inactiveTopicPolicies);
        inactiveTopicPolicies.setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        admin.namespaces().setInactiveTopicPolicies(namespace2, inactiveTopicPolicies);
        inactiveTopicPolicies.setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        admin.namespaces().setInactiveTopicPolicies(namespace3, inactiveTopicPolicies);

        InactiveTopicPolicies policies;
        //wait for zk
        Awaitility.await().until(() -> {
            InactiveTopicPolicies temp = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get()
                    .get()).getInactiveTopicPolicies();
            return temp.isDeleteWhileInactive();
        });
        policies = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get()
                .get()).getInactiveTopicPolicies();

        Assert.assertTrue(policies.isDeleteWhileInactive());
        assertEquals(policies.getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_no_subscriptions);
        assertEquals(policies.getMaxInactiveDurationSeconds(), 1);
        assertEquals(policies, admin.namespaces().getInactiveTopicPolicies(namespace));

        admin.namespaces().removeInactiveTopicPolicies(namespace);
        Awaitility.await().until(() -> {
            InactiveTopicPolicies temp = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get()
                    .get()).getInactiveTopicPolicies();
            return temp.getMaxInactiveDurationSeconds() == 1000;
        });
        assertEquals(((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get().get()).getInactiveTopicPolicies(), defaultPolicy);

        policies = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic2, false).get()
                .get()).getInactiveTopicPolicies();
        Assert.assertTrue(policies.isDeleteWhileInactive());
        assertEquals(policies.getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        assertEquals(policies.getMaxInactiveDurationSeconds(), 1);
        assertEquals(policies, admin.namespaces().getInactiveTopicPolicies(namespace2));

        admin.namespaces().removeInactiveTopicPolicies(namespace2);
        Awaitility.await().until(() -> {
            InactiveTopicPolicies temp = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get()
                    .get()).getInactiveTopicPolicies();
            return temp.getMaxInactiveDurationSeconds() == 1000;
        });
        assertEquals(((PersistentTopic) pulsar.getBrokerService().getTopic(topic2, false).get().get()).getInactiveTopicPolicies()
                , defaultPolicy);
    }

    @Test(timeOut = 20000)
    public void testDeleteWhenNoSubscriptionsWithMultiConfig() throws Exception {
        final String namespace = "prop/ns-abc";
        final String namespace2 = "prop/ns-abc2";
        final String namespace3 = "prop/ns-abc3";
        List<String> namespaceList = Arrays.asList(namespace2, namespace3);
        conf.setBrokerDeleteInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();

        for (String ns : namespaceList) {
            admin.namespaces().createNamespace(ns);
            admin.namespaces().setNamespaceReplicationClusters(ns, Sets.newHashSet("test"));
        }

        final String topic = "persistent://prop/ns-abc/testDeleteWhenNoSubscriptionsWithMultiConfig";
        final String topic2 = "persistent://prop/ns-abc2/testDeleteWhenNoSubscriptionsWithMultiConfig";
        final String topic3 = "persistent://prop/ns-abc3/testDeleteWhenNoSubscriptionsWithMultiConfig";
        List<String> topics = Arrays.asList(topic, topic2, topic3);
        //create producer/consumer and close
        Map<String, String> topicToSub = new HashMap<>();
        for (String tp : topics) {
            Producer<byte[]> producer = pulsarClient.newProducer().topic(tp).create();
            String subName = "sub" + System.currentTimeMillis();
            topicToSub.put(tp, subName);
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(tp).subscriptionName(subName).subscribe();
            for (int i = 0; i < 10; i++) {
                producer.send("Pulsar".getBytes());
            }
            consumer.close();
            producer.close();
            Thread.sleep(1);
        }
        // namespace use delete_when_no_subscriptions, namespace2 use delete_when_subscriptions_caught_up
        // namespace3 use default:delete_when_no_subscriptions
        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,1,true);
        admin.namespaces().setInactiveTopicPolicies(namespace, inactiveTopicPolicies);
        inactiveTopicPolicies.setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        admin.namespaces().setInactiveTopicPolicies(namespace2, inactiveTopicPolicies);

        //wait for zk
        Awaitility.await().until(() -> {
            InactiveTopicPolicies temp = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get()
                    .get()).getInactiveTopicPolicies();
            return temp.isDeleteWhileInactive();
        });

        // topic should still exist
        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList(namespace).contains(topic));
        Assert.assertTrue(admin.topics().getList(namespace2).contains(topic2));
        Assert.assertTrue(admin.topics().getList(namespace3).contains(topic3));

        // no backlog, trigger delete_when_subscriptions_caught_up
        admin.topics().skipAllMessages(topic2, topicToSub.remove(topic2));
        Awaitility.await().untilAsserted(()
                -> Assert.assertFalse(admin.topics().getList(namespace2).contains(topic2)));
        // delete subscription, trigger delete_when_no_subscriptions
        for (Map.Entry<String, String> entry : topicToSub.entrySet()) {
            admin.topics().deleteSubscription(entry.getKey(), entry.getValue());
        }
        Awaitility.await()
                .untilAsserted(() -> Assert.assertFalse(admin.topics().getList(namespace).contains(topic)));
        Assert.assertFalse(admin.topics().getList(namespace3).contains(topic3));
    }

    @Test
    public void testDeleteWhenNoBacklogs() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testDeleteWhenNoBacklogs";
        final String topic2 = "persistent://prop/ns-abc/testDeleteWhenNoBacklogsB";
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(topic2)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("sub")
            .subscribe();

        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topicsPattern("persistent://prop/ns-abc/test.*")
                .subscriptionName("sub2")
                .subscribe();

        int producedCount = 10;
        for (int i = 0; i < producedCount; i++) {
            producer.send("Pulsar".getBytes());
            producer2.send("Pulsar".getBytes());
        }

        producer.close();
        producer2.close();
        int receivedCount = 0;
        Message<byte[]> msg;
        while((msg = consumer2.receive(1, TimeUnit.SECONDS)) != null) {
            consumer2.acknowledge(msg);
            receivedCount ++;
        }
        assertEquals(producedCount * 2, receivedCount);

        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList("prop/ns-abc").contains(topic));

        admin.topics().skipAllMessages(topic, "sub");
        Awaitility.await().untilAsserted(() -> {
            Assert.assertFalse(consumer.isConnected());
            final List<ConsumerImpl> consumers = ((MultiTopicsConsumerImpl) consumer2).getConsumers();
            consumers.forEach(c -> Assert.assertFalse(c.isConnected()));
            Assert.assertFalse(consumer2.isConnected());
            Assert.assertFalse(admin.topics().getList("prop/ns-abc").contains(topic));
            Assert.assertFalse(admin.topics().getList("prop/ns-abc").contains(topic2));
        });
        consumer.close();
        consumer2.close();
    }

    @Test
    public void testMaxInactiveDuration() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        conf.setBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(5);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testMaxInactiveDuration";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .create();

        producer.close();
        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        Thread.sleep(4000);
        Assert.assertFalse(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testTopicLevelInActiveTopicApi() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        super.baseSetup();
        final String topicName = "persistent://prop/ns-abc/testMaxInactiveDuration-" + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe().close();
        TopicName topic = TopicName.get(topicName);

        InactiveTopicPolicies inactiveTopicPolicies = admin.topics().getInactiveTopicPolicies(topicName);
        assertNull(inactiveTopicPolicies);

        InactiveTopicPolicies policies = new InactiveTopicPolicies();
        policies.setDeleteWhileInactive(true);
        policies.setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        policies.setMaxInactiveDurationSeconds(10);
        admin.topics().setInactiveTopicPolicies(topicName, policies);

        Awaitility.await().until(()
                -> admin.topics().getInactiveTopicPolicies(topicName) != null);
        assertEquals(admin.topics().getInactiveTopicPolicies(topicName), policies);
        admin.topics().removeInactiveTopicPolicies(topicName);

        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topics().getInactiveTopicPolicies(topicName)));
    }

    @Test(timeOut = 30000)
    public void testTopicLevelInactivePolicyUpdateAndClean() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setBrokerDeleteInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(1000);
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        InactiveTopicPolicies defaultPolicy = new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions
                , 1000, true);

        super.baseSetup();
        final String namespace = "prop/ns-abc";
        final String topic = "persistent://prop/ns-abc/testTopicLevelInactivePolicy" + UUID.randomUUID().toString();
        final String topic2 = "persistent://prop/ns-abc/testTopicLevelInactivePolicy" + UUID.randomUUID().toString();
        final String topic3 = "persistent://prop/ns-abc/testTopicLevelInactivePolicy" + UUID.randomUUID().toString();
        List<String> topics = Arrays.asList(topic, topic2, topic3);

        for (String tp : topics) {
            admin.topics().createNonPartitionedTopic(tp);
        }
        for (String tp : topics) {
            //wait for cache
            pulsarClient.newConsumer().topic(tp).subscriptionName("my-sub").subscribe().close();
            TopicName topicName = TopicName.get(tp);
        }

        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true);
        admin.topics().setInactiveTopicPolicies(topic, inactiveTopicPolicies);
        inactiveTopicPolicies.setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        admin.topics().setInactiveTopicPolicies(topic2, inactiveTopicPolicies);
        inactiveTopicPolicies.setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        admin.topics().setInactiveTopicPolicies(topic3, inactiveTopicPolicies);

        //wait for cache
        Awaitility.await().until(()
                -> admin.topics().getInactiveTopicPolicies(topic) != null);
        InactiveTopicPolicies policies = ((PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic, false).get().get()).getInactiveTopicPolicies();
        Assert.assertTrue(policies.isDeleteWhileInactive());
        assertEquals(policies.getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_no_subscriptions);
        assertEquals(policies.getMaxInactiveDurationSeconds(), 1);
        assertEquals(policies, admin.topics().getInactiveTopicPolicies(topic));

        admin.topics().removeInactiveTopicPolicies(topic);
        //Only the broker-level policies is set, so after removing the topic-level policies
        //, the topic will use the broker-level policies
        Awaitility.await().untilAsserted(()
                -> assertEquals(((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get().get()).getInactiveTopicPolicies()
                , defaultPolicy));

        policies = ((PersistentTopic)pulsar.getBrokerService().getTopic(topic2,false).get().get()).getInactiveTopicPolicies();
        Assert.assertTrue(policies.isDeleteWhileInactive());
        assertEquals(policies.getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        assertEquals(policies.getMaxInactiveDurationSeconds(), 1);
        assertEquals(policies, admin.topics().getInactiveTopicPolicies(topic2));
        inactiveTopicPolicies.setMaxInactiveDurationSeconds(999);
        //Both broker level and namespace level policies are set, so after removing the topic level policies
        //, the topic will use the namespace level policies
        admin.namespaces().setInactiveTopicPolicies(namespace, inactiveTopicPolicies);
        //wait for zk
        Awaitility.await().until(() -> {
            InactiveTopicPolicies tempPolicies = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false)
                    .get().get()).getInactiveTopicPolicies();
            return inactiveTopicPolicies.equals(tempPolicies);
        });
        admin.topics().removeInactiveTopicPolicies(topic2);
        // The cache has been updated, but the system-event may not be consumed yet
        // ，so wait for topic-policies update event
        Awaitility.await().untilAsserted(() -> {
            InactiveTopicPolicies nsPolicies = ((PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topic2, false).get().get()).getInactiveTopicPolicies();
            assertEquals(nsPolicies.getMaxInactiveDurationSeconds(), 999);
        });

    }

    @Test(timeOut = 30000)
    public void testDeleteWhenNoSubscriptionsWithTopicLevelPolicies() throws Exception {
        final String namespace = "prop/ns-abc";
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setBrokerDeleteInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();
        final String topic = "persistent://prop/ns-abc/test-" + UUID.randomUUID();
        final String topic2 = "persistent://prop/ns-abc/test-" + UUID.randomUUID();
        final String topic3 = "persistent://prop/ns-abc/test-" + UUID.randomUUID();
        List<String> topics = Arrays.asList(topic, topic2, topic3);
        //create producer/consumer and close
        Map<String, String> topicToSub = new HashMap<>();
        for (String tp : topics) {
            Producer<byte[]> producer = pulsarClient.newProducer().topic(tp).create();
            String subName = "sub" + System.currentTimeMillis();
            topicToSub.put(tp, subName);
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(tp).subscriptionName(subName).subscribe();
            for (int i = 0; i < 10; i++) {
                producer.send("Pulsar".getBytes());
            }
            consumer.close();
            producer.close();
            Thread.sleep(1);
        }
        // "topic" use delete_when_no_subscriptions, "topic2" use delete_when_subscriptions_caught_up
        // "topic3" use default:delete_when_no_subscriptions
        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,1,true);
        admin.topics().setInactiveTopicPolicies(topic, inactiveTopicPolicies);
        inactiveTopicPolicies.setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        admin.topics().setInactiveTopicPolicies(topic2, inactiveTopicPolicies);

        //wait for update
        Awaitility.await().until(()
                -> admin.topics().getInactiveTopicPolicies(topic2) != null);
        // topic should still exist
        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList(namespace).contains(topic));
        Assert.assertTrue(admin.topics().getList(namespace).contains(topic2));
        Assert.assertTrue(admin.topics().getList(namespace).contains(topic3));

        // no backlog, trigger delete_when_subscriptions_caught_up
        admin.topics().skipAllMessages(topic2, topicToSub.remove(topic2));
        Awaitility.await().untilAsserted(()
                -> Assert.assertFalse(admin.topics().getList(namespace).contains(topic2)));
        // delete subscription, trigger delete_when_no_subscriptions
        for (Map.Entry<String, String> entry : topicToSub.entrySet()) {
            admin.topics().deleteSubscription(entry.getKey(), entry.getValue());
        }
        Awaitility.await().untilAsserted(()
                -> Assert.assertFalse(admin.topics().getList(namespace).contains(topic3)));
        Assert.assertFalse(admin.topics().getList(namespace).contains(topic));
    }

    @Test(timeOut = 30000)
    public void testInactiveTopicApplied() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        super.baseSetup();

        final String namespace = "prop/ns-abc";
        final String topic = "persistent://prop/ns-abc/test-" + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        //namespace-level default value is null
        assertNull(admin.namespaces().getInactiveTopicPolicies(namespace));
        //topic-level default value is null
        assertNull(admin.topics().getInactiveTopicPolicies(topic));
        //use broker-level by default
        InactiveTopicPolicies brokerLevelPolicy =
                new InactiveTopicPolicies(conf.getBrokerDeleteInactiveTopicsMode(),
                        conf.getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                        conf.isBrokerDeleteInactiveTopicsEnabled());
        Assert.assertEquals(admin.topics().getInactiveTopicPolicies(topic, true), brokerLevelPolicy);
        //set namespace-level policy
        InactiveTopicPolicies namespaceLevelPolicy =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,
                20, false);
        admin.namespaces().setInactiveTopicPolicies(namespace, namespaceLevelPolicy);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getInactiveTopicPolicies(namespace)));
        InactiveTopicPolicies policyFromBroker = admin.topics().getInactiveTopicPolicies(topic, true);
        assertEquals(policyFromBroker.getMaxInactiveDurationSeconds(), 20);
        assertFalse(policyFromBroker.isDeleteWhileInactive());
        assertEquals(policyFromBroker.getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_no_subscriptions);
        // set topic-level policy
        InactiveTopicPolicies topicLevelPolicy =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up,
                        30, false);
        admin.topics().setInactiveTopicPolicies(topic, topicLevelPolicy);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getInactiveTopicPolicies(topic)));
        policyFromBroker = admin.topics().getInactiveTopicPolicies(topic, true);
        assertEquals(policyFromBroker.getMaxInactiveDurationSeconds(), 30);
        assertFalse(policyFromBroker.isDeleteWhileInactive());
        assertEquals(policyFromBroker.getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        //remove topic-level policy
        admin.topics().removeInactiveTopicPolicies(topic);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getInactiveTopicPolicies(topic, true), namespaceLevelPolicy));
        //remove namespace-level policy
        admin.namespaces().removeInactiveTopicPolicies(namespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getInactiveTopicPolicies(topic, true), brokerLevelPolicy));
    }

    @Test(timeOut = 30000)
    public void testHealthTopicInactiveNotClean() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();
        // init topic
        NamespaceName heartbeatNamespaceV1 = NamespaceService.getHeartbeatNamespace(pulsar.getAdvertisedAddress(), pulsar.getConfig());
        final String healthCheckTopicV1 = "persistent://" + heartbeatNamespaceV1 + "/healthcheck";

        NamespaceName heartbeatNamespaceV2 = NamespaceService.getHeartbeatNamespaceV2(pulsar.getAdvertisedAddress(), pulsar.getConfig());
        final String healthCheckTopicV2 = "persistent://" + heartbeatNamespaceV2 + "/healthcheck";

        admin.brokers().healthcheck(TopicVersion.V1);
        admin.brokers().healthcheck(TopicVersion.V2);

        List<String> V1Partitions = pulsar
                .getPulsarResources()
                .getTopicResources()
                .getExistingPartitions(TopicName.get(healthCheckTopicV1))
                .get(10, TimeUnit.SECONDS);
        List<String> V2Partitions = pulsar
                .getPulsarResources()
                .getTopicResources()
                .getExistingPartitions(TopicName.get(healthCheckTopicV2))
                .get(10, TimeUnit.SECONDS);
        Assert.assertTrue(V1Partitions.contains(healthCheckTopicV1));
        Assert.assertTrue(V2Partitions.contains(healthCheckTopicV2));
    }
}
