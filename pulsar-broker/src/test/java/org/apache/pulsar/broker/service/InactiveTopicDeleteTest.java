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


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Consumer;

import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InactiveTopicDeleteTest extends BrokerTestBase {

    protected void setup() throws Exception {
        // No-op
    }

    protected void cleanup() throws Exception {
        // No-op
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

        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        admin.topics().deleteSubscription(topic, "sub");
        Thread.sleep(2000);
        Assert.assertFalse(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testTopicPolicyUpdateAndClean() throws Exception {
        final String namespace = "prop/ns-abc";
        final String namespace2 = "prop/ns-abc2";
        final String namespace3 = "prop/ns-abc3";
        List<String> namespaceList = Arrays.asList(namespace2, namespace3);

        super.resetConfig();
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
        while (true) {
            policies = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get().get()).inactiveTopicPolicies;
            if (policies.isDeleteWhileInactive()) {
                break;
            }
            Thread.sleep(1000);
        }

        Assert.assertTrue(policies.isDeleteWhileInactive());
        Assert.assertEquals(policies.getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_no_subscriptions);
        Assert.assertEquals(policies.getMaxInactiveDurationSeconds(), 1);
        Assert.assertEquals(policies, admin.namespaces().getInactiveTopicPolicies(namespace));

        admin.namespaces().removeInactiveTopicPolicies(namespace);
        while (true) {
            Thread.sleep(500);
            policies = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get().get()).inactiveTopicPolicies;
            if (policies.getMaxInactiveDurationSeconds() == 1000) {
                break;
            }
        }
        Assert.assertEquals(((PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).get().get()).inactiveTopicPolicies
                , defaultPolicy);

        policies = ((PersistentTopic)pulsar.getBrokerService().getTopic(topic2,false).get().get()).inactiveTopicPolicies;
        Assert.assertTrue(policies.isDeleteWhileInactive());
        Assert.assertEquals(policies.getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        Assert.assertEquals(policies.getMaxInactiveDurationSeconds(), 1);
        Assert.assertEquals(policies, admin.namespaces().getInactiveTopicPolicies(namespace2));

        admin.namespaces().removeInactiveTopicPolicies(namespace2);
        while (true) {
            Thread.sleep(500);
            policies = ((PersistentTopic) pulsar.getBrokerService().getTopic(topic2, false).get().get()).inactiveTopicPolicies;
            if (policies.getMaxInactiveDurationSeconds() == 1000) {
                break;
            }
        }
        Assert.assertEquals(((PersistentTopic) pulsar.getBrokerService().getTopic(topic2, false).get().get()).inactiveTopicPolicies
                , defaultPolicy);

        super.internalCleanup();
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
        while (true) {
            InactiveTopicPolicies policies = ((PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topic, false).get().get()).inactiveTopicPolicies;
            if (policies.isDeleteWhileInactive()) {
                break;
            }
            Thread.sleep(100);
        }

        // topic should still exist
        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList(namespace).contains(topic));
        Assert.assertTrue(admin.topics().getList(namespace2).contains(topic2));
        Assert.assertTrue(admin.topics().getList(namespace3).contains(topic3));

        // no backlog, trigger delete_when_subscriptions_caught_up
        admin.topics().skipAllMessages(topic2, topicToSub.remove(topic2));
        Thread.sleep(2000);
        Assert.assertFalse(admin.topics().getList(namespace2).contains(topic2));
        // delete subscription, trigger delete_when_no_subscriptions
        for (Map.Entry<String, String> entry : topicToSub.entrySet()) {
            admin.topics().deleteSubscription(entry.getKey(), entry.getValue());
        }
        Thread.sleep(2000);
        Assert.assertFalse(admin.topics().getList(namespace).contains(topic));
        Assert.assertFalse(admin.topics().getList(namespace3).contains(topic3));

        super.internalCleanup();
    }

    @Test
    public void testDeleteWhenNoBacklogs() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testDeleteWhenNoBacklogs";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("sub")
            .subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("Pulsar".getBytes());
        }

        consumer.close();
        producer.close();

        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        admin.topics().skipAllMessages(topic, "sub");
        Thread.sleep(2000);
        Assert.assertFalse(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        super.internalCleanup();
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
}
