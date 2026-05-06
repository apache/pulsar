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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MetadataStoreTopicPoliciesServiceTest {

    private MetadataStoreExtended localStore;
    private MetadataStoreExtended configurationStore;
    private MetadataStoreTopicPoliciesService service;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        localStore = MetadataStoreExtended.create("memory:local-" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build());
        configurationStore = MetadataStoreExtended.create("memory:configuration-" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build());
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(localStore);
        when(pulsar.getConfigurationMetadataStore()).thenReturn(configurationStore);
        service = new MetadataStoreTopicPoliciesService();
        service.start(pulsar);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (service != null) {
            service.close();
        }
        if (localStore != null) {
            localStore.close();
        }
        if (configurationStore != null) {
            configurationStore.close();
        }
    }

    @Test
    public void testLocalAndGlobalPoliciesUseSeparateStoresAndNormalizePartitions() throws Exception {
        TopicName partition = TopicName.get("persistent://tenant/ns/topic-partition-0");
        TopicName topic = TopicName.get("persistent://tenant/ns/topic");

        service.updateTopicPoliciesAsync(partition, false, false,
                policies -> policies.setMaxConsumerPerTopic(3)).get();
        service.updateTopicPoliciesAsync(partition, true, false,
                policies -> policies.setMessageTTLInSeconds(10)).get();

        Optional<TopicPolicies> localPolicies =
                service.getTopicPoliciesAsync(topic, TopicPoliciesService.GetType.LOCAL_ONLY).get();
        Optional<TopicPolicies> globalPolicies =
                service.getTopicPoliciesAsync(topic, TopicPoliciesService.GetType.GLOBAL_ONLY).get();

        assertTrue(localPolicies.isPresent());
        assertFalse(localPolicies.get().isGlobalPolicies());
        assertEquals(localPolicies.get().getMaxConsumerPerTopic(), Integer.valueOf(3));
        assertTrue(globalPolicies.isPresent());
        assertTrue(globalPolicies.get().isGlobalPolicies());
        assertEquals(globalPolicies.get().getMessageTTLInSeconds(), Integer.valueOf(10));
        assertTrue(localStore.exists(MetadataStoreTopicPoliciesService.pathFor(topic, false)).get());
        assertTrue(configurationStore.exists(MetadataStoreTopicPoliciesService.pathFor(topic, true)).get());
    }

    @Test
    public void testDeleteIsIdempotentAndCanKeepGlobalPolicies() throws Exception {
        TopicName topic = TopicName.get("persistent://tenant/ns/delete-topic");

        service.updateTopicPoliciesAsync(topic, true, false,
                policies -> policies.setMessageTTLInSeconds(10)).get();
        service.deleteTopicPoliciesAsync(topic, false).get();
        assertTrue(service.getTopicPoliciesAsync(topic, TopicPoliciesService.GetType.GLOBAL_ONLY).get().isEmpty());
        // second delete should be idempotent
        service.deleteTopicPoliciesAsync(topic, false).get();
        assertTrue(service.getTopicPoliciesAsync(topic, TopicPoliciesService.GetType.GLOBAL_ONLY).get().isEmpty());

        service.updateTopicPoliciesAsync(topic, false, false,
                policies -> policies.setMaxConsumerPerTopic(3)).get();
        service.updateTopicPoliciesAsync(topic, true, false,
                policies -> policies.setMessageTTLInSeconds(20)).get();
        service.deleteTopicPoliciesAsync(topic, true).get();

        assertTrue(service.getTopicPoliciesAsync(topic, TopicPoliciesService.GetType.LOCAL_ONLY).get().isEmpty());
        Optional<TopicPolicies> globalPolicies =
                service.getTopicPoliciesAsync(topic, TopicPoliciesService.GetType.GLOBAL_ONLY).get();
        assertTrue(globalPolicies.isPresent());
        assertEquals(globalPolicies.get().getMessageTTLInSeconds(), Integer.valueOf(20));
    }

    @Test
    public void testListenerReceivesMetadataUpdatesAndDeletes() throws Exception {
        TopicName topic = TopicName.get("persistent://tenant/ns/listener-topic");
        List<TopicPolicies> updates = new CopyOnWriteArrayList<>();
        service.registerListener(topic, updates::add);

        service.updateTopicPoliciesAsync(topic, false, false,
                policies -> policies.setMaxProducerPerTopic(2)).get();
        Awaitility.await().untilAsserted(() -> {
            assertFalse(updates.isEmpty());
            assertEquals(updates.get(updates.size() - 1).getMaxProducerPerTopic(), Integer.valueOf(2));
        });

        service.deleteTopicPoliciesAsync(topic, true).get();
        Awaitility.await().untilAsserted(() -> assertTrue(updates.contains(null)));
    }

    @Test
    public void testSkipUpdateWhenTopicPolicyDoesntExist() throws Exception {
        TopicName topic = TopicName.get("persistent://tenant/ns/skip-update-topic");
        // Should not throw when skip=true and policy doesn't exist
        service.updateTopicPoliciesAsync(topic, false, true,
                policies -> policies.setMaxConsumerPerTopic(5)).get();
        assertTrue(service.getTopicPoliciesAsync(topic, TopicPoliciesService.GetType.LOCAL_ONLY).get().isEmpty());

        // Normal update creates the policy
        service.updateTopicPoliciesAsync(topic, false, false,
                policies -> policies.setMaxConsumerPerTopic(5)).get();
        Optional<TopicPolicies> result =
                service.getTopicPoliciesAsync(topic, TopicPoliciesService.GetType.LOCAL_ONLY).get();
        assertTrue(result.isPresent());
        assertEquals(result.get().getMaxConsumerPerTopic(), Integer.valueOf(5));
    }

    @Test
    public void testCloseStopsReadsAndWrites() throws Exception {
        TopicName existingTopic = TopicName.get("persistent://tenant/ns/closed-topic");
        TopicName newTopic = TopicName.get("persistent://tenant/ns/closed-topic-new");

        service.updateTopicPoliciesAsync(existingTopic, false, false,
                policies -> policies.setMaxConsumerPerTopic(7)).get();
        service.close();

        assertTrue(service.getTopicPoliciesAsync(existingTopic, TopicPoliciesService.GetType.LOCAL_ONLY)
                .get().isEmpty());

        try {
            service.updateTopicPoliciesAsync(newTopic, false, false,
                    policies -> policies.setMaxConsumerPerTopic(9)).get();
            fail("Expected update after close to fail");
        } catch (ExecutionException error) {
            assertTrue(error.getCause() instanceof BrokerServiceException);
        }
        assertFalse(localStore.exists(MetadataStoreTopicPoliciesService.pathFor(newTopic, false)).get());

        try {
            service.deleteTopicPoliciesAsync(existingTopic).get();
            fail("Expected delete after close to fail");
        } catch (ExecutionException error) {
            assertTrue(error.getCause() instanceof BrokerServiceException);
        }
        assertTrue(localStore.exists(MetadataStoreTopicPoliciesService.pathFor(existingTopic, false)).get());
    }

    @Test
    public void testPathFor() {
        TopicName topic = TopicName.get("persistent://tenant/ns/topic");
        String globalPath = MetadataStoreTopicPoliciesService.pathFor(topic, true);
        String localPath = MetadataStoreTopicPoliciesService.pathFor(topic, false);

        assertTrue(globalPath.startsWith(MetadataStoreTopicPoliciesService.GLOBAL_POLICIES_ROOT));
        assertTrue(localPath.startsWith(MetadataStoreTopicPoliciesService.LOCAL_POLICIES_ROOT));
        assertTrue(globalPath.contains("tenant/ns/"));
        assertTrue(localPath.contains("tenant/ns/"));
    }
}
