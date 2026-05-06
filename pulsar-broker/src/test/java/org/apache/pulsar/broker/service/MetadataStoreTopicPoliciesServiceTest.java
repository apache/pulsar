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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MetadataStoreTopicPoliciesServiceTest extends MockedPulsarServiceBaseTest {

    protected boolean isLegacyTopicPoliciesService() {
        return false;
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setTopicPoliciesServiceClassName(MetadataStoreTopicPoliciesService.class.getName());
        conf.setSystemTopicEnabled(isLegacyTopicPoliciesService());
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
        if (isLegacyTopicPoliciesService()) {
            assertTrue(pulsar.getTopicPoliciesService() instanceof LegacyAwareTopicPoliciesService);
        } else {
            assertTrue(pulsar.getTopicPoliciesService() instanceof MetadataStoreTopicPoliciesService);
        }
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTopicPoliciesAdminPersistsAndUpdatesLoadedTopic() throws Exception {
        final var topic = TopicName.get("test-metadata-store-topic-policies").toString();
        admin.topics().createNonPartitionedTopic(topic);

        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create()) {
            producer.send("warmup");
        }
        final var persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get()
                .orElseThrow();

        assertNull(admin.topicPolicies().getCompactionThreshold(topic));
        Assert.assertNotEquals(persistentTopic.getHierarchyTopicPolicies().getCompactionThreshold().get(),
                Long.valueOf(1000));

        admin.topicPolicies().setCompactionThreshold(topic, 1000);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getCompactionThreshold(topic), Long.valueOf(1000));
            assertEquals(persistentTopic.getHierarchyTopicPolicies().getCompactionThreshold().get(), Long.valueOf(1000));
        });

        restartBroker();

        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.topicPolicies().getCompactionThreshold(topic), Long.valueOf(1000)));
        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create()) {
            producer.send("after-restart");
        }
        final var reloadedTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get()
                .orElseThrow();
        Awaitility.await().untilAsserted(() ->
                assertEquals(reloadedTopic.getHierarchyTopicPolicies().getCompactionThreshold().get(),
                        Long.valueOf(1000)));
    }

    @Test
    public void testTopicPoliciesDeletedWithTopic() throws Exception {
        final var topic = TopicName.get("test-metadata-store-topic-policies-delete").toString();
        admin.topics().createNonPartitionedTopic(topic);

        assertNull(admin.topicPolicies().getCompactionThreshold(topic));
        admin.topicPolicies().setCompactionThreshold(topic, 1000);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.topicPolicies().getCompactionThreshold(topic), Long.valueOf(1000)));

        admin.topics().delete(topic);
        admin.topics().createNonPartitionedTopic(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies().getCompactionThreshold(topic)));
    }

    @Test
    public void testShadowReplicator() throws Exception {
        final var sourceTopic = TopicName.get("test-metadata-shadow-replicator").toString();
        final var shadowTopic = sourceTopic + "-shadow";

        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, List.of(shadowTopic));

        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
             var consumer = pulsarClient.newConsumer(Schema.STRING).topic(shadowTopic)
                     .subscriptionName("sub").subscribe()) {
            producer.send("msg");
            final var msg = consumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(msg.getValue(), "msg");
        }

        final var persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopic).get()
                .orElseThrow();
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(persistentTopic.getShadowReplicators().size(), 1));
    }
}
