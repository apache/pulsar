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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class LegacyAwareTopicPoliciesServiceTest extends MetadataStoreTopicPoliciesServiceTest{

    @Override
    protected boolean isLegacyTopicPoliciesService() {
        return true;
    }

    @Test
    public void testLegacyNamespaceKeepsSystemTopicBackendAfterRestart() throws Exception {
        restartWithSystemTopicPoliciesService();

        final var namespace1 = "public/legacy-aware-ns1";
        final var topic1 = "persistent://" + namespace1 + "/topic";
        final var eventTopic1 = NamespaceEventsSystemTopicFactory
                .getEventsTopicName(NamespaceName.get(namespace1))
                .toString();
        admin.namespaces().createNamespace(namespace1);
        admin.topics().createNonPartitionedTopic(topic1);

        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(topic1).create()) {
            producer.send("warmup");
        }
        final var namespace1TopicBeforeRestart = getPersistentTopic(topic1);

        assertNull(admin.topicPolicies().getCompactionThreshold(topic1));

        admin.topicPolicies().setCompactionThreshold(topic1, 1000);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getCompactionThreshold(topic1), Long.valueOf(1000));
            assertEquals(namespace1TopicBeforeRestart.getHierarchyTopicPolicies().getCompactionThreshold().get(),
                    Long.valueOf(1000));
            assertTrue(pulsar.getPulsarResources().getTopicResources().persistentTopicExists(TopicName.get(eventTopic1))
                    .join());
        });

        restartBroker(configuration ->
                configuration.setTopicPoliciesServiceClassName(MetadataStoreTopicPoliciesService.class.getName()));
        assertTrue(pulsar.getTopicPoliciesService() instanceof LegacyAwareTopicPoliciesService);

        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.topicPolicies().getCompactionThreshold(topic1), Long.valueOf(1000)));

        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(topic1).create()) {
            producer.send("after-restart");
        }
        final var namespace1TopicAfterRestart = getPersistentTopic(topic1);
        final var namespace1LacBeforeUpdate = admin.topics().getInternalStats(eventTopic1).lastConfirmedEntry;

        admin.topicPolicies().setCompactionThreshold(topic1, 2000);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getCompactionThreshold(topic1), Long.valueOf(2000));
            assertEquals(namespace1TopicAfterRestart.getHierarchyTopicPolicies().getCompactionThreshold().get(),
                    Long.valueOf(2000));
            Assert.assertNotEquals(admin.topics().getInternalStats(eventTopic1).lastConfirmedEntry,
                    namespace1LacBeforeUpdate);
        });
        assertFalse(pulsar.getLocalMetadataStore()
                .exists(MetadataStoreTopicPoliciesService.pathFor(TopicName.get(topic1), false))
                .join());

        final var namespace2 = "public/legacy-aware-ns2";
        final var topic2 = "persistent://" + namespace2 + "/topic";
        final var eventTopic2 = NamespaceEventsSystemTopicFactory
                .getEventsTopicName(NamespaceName.get(namespace2))
                .toString();
        admin.namespaces().createNamespace(namespace2);
        admin.topics().createNonPartitionedTopic(topic2);

        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(topic2).create()) {
            producer.send("warmup");
        }
        final var namespace2Topic = getPersistentTopic(topic2);

        assertNull(admin.topicPolicies().getCompactionThreshold(topic2));
        assertFalse(pulsar.getPulsarResources().getTopicResources().persistentTopicExists(TopicName.get(eventTopic2))
                .join());

        admin.topicPolicies().setCompactionThreshold(topic2, 3000);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getCompactionThreshold(topic2), Long.valueOf(3000));
            assertEquals(namespace2Topic.getHierarchyTopicPolicies().getCompactionThreshold().get(),
                    Long.valueOf(3000));
            assertFalse(pulsar.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(eventTopic2)).join());
            assertTrue(pulsar.getLocalMetadataStore()
                    .exists(MetadataStoreTopicPoliciesService.pathFor(TopicName.get(topic2), false))
                    .join());
        });
    }

    @Test
    public void testOwnedBundleCanSwitchToLegacyBackendAfterNamespaceBecomesLegacy() throws Exception {
        restartWithLegacyAwareMetadataStoreService();

        final var namespace = "public/legacy-aware-flip";
        final var topic = "persistent://" + namespace + "/topic";
        final var eventTopic = NamespaceEventsSystemTopicFactory
                .getEventsTopicName(NamespaceName.get(namespace))
                .toString();
        admin.namespaces().createNamespace(namespace);
        admin.topics().createNonPartitionedTopic(topic);

        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create()) {
            producer.send("warmup");
        }
        final var persistentTopic = getPersistentTopic(topic);

        assertNull(admin.topicPolicies().getCompactionThreshold(topic));
        assertFalse(pulsar.getPulsarResources().getTopicResources().persistentTopicExists(TopicName.get(eventTopic))
                .join());
        assertFalse(pulsar.getLocalMetadataStore()
                .exists(MetadataStoreTopicPoliciesService.pathFor(TopicName.get(topic), false))
                .join());

        admin.topics().createNonPartitionedTopic(eventTopic);
        Awaitility.await().untilAsserted(() -> assertTrue(pulsar.getPulsarResources().getTopicResources()
                .persistentTopicExists(TopicName.get(eventTopic)).join()));

        final var eventTopicLastConfirmedEntryBeforeUpdate = admin.topics().getInternalStats(eventTopic)
                .lastConfirmedEntry;
        admin.topicPolicies().setCompactionThreshold(topic, 1000);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getCompactionThreshold(topic), Long.valueOf(1000));
            assertEquals(persistentTopic.getHierarchyTopicPolicies().getCompactionThreshold().get(),
                    Long.valueOf(1000));
            Assert.assertNotEquals(admin.topics().getInternalStats(eventTopic).lastConfirmedEntry,
                    eventTopicLastConfirmedEntryBeforeUpdate);
            assertFalse(pulsar.getLocalMetadataStore()
                    .exists(MetadataStoreTopicPoliciesService.pathFor(TopicName.get(topic), false))
                    .join());
        });
    }

    private void restartWithSystemTopicPoliciesService() throws Exception {
        restartBroker(configuration ->
                configuration.setTopicPoliciesServiceClassName(SystemTopicBasedTopicPoliciesService.class.getName()));
        assertTrue(pulsar.getTopicPoliciesService() instanceof SystemTopicBasedTopicPoliciesService);
    }

    private void restartWithLegacyAwareMetadataStoreService() throws Exception {
        restartBroker(configuration ->
                configuration.setTopicPoliciesServiceClassName(MetadataStoreTopicPoliciesService.class.getName()));
        assertTrue(pulsar.getTopicPoliciesService() instanceof LegacyAwareTopicPoliciesService);
    }

    private PersistentTopic getPersistentTopic(String topic) throws Exception {
        return (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().orElseThrow();
    }
}
