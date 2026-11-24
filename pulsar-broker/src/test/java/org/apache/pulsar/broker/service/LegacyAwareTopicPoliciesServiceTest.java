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
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test order: testUpgrade() -> other tests (with MetadataStoreTopicPoliciesService configured) -> testDowngrade().
 */
@Test(groups = "broker")
public class LegacyAwareTopicPoliciesServiceTest extends MockedPulsarServiceBaseTest {

    private static final String metaNamespace = "public/meta-ns";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(priority = -1)
    public void testUpgrade() throws Exception {
        final var topic = "test-upgrade";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setCompactionThreshold(topic, 100);
        waitUntilAssert(() -> assertEquals(admin.topicPolicies().getCompactionThreshold(topic), 100));

        restartBroker(conf -> {
            conf.setSystemTopicEnabled(false);
            conf.setTopicPoliciesServiceClassName(MetadataStoreTopicPoliciesService.class.getName());
        });
        // The policies will be lost because when system topic is disabled, it will not try to read policies from the
        // __change_events topic
        assertNull(admin.topicPolicies().getCompactionThreshold(topic));

        restartBroker(conf -> conf.setSystemTopicEnabled(true));
        // The default namespace still read policies from the __change_events topic if it exists
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic), 100);
        assertFalse(pulsar.getLocalMetadataStore().exists(MetadataStoreTopicPoliciesService.LOCAL_POLICIES_ROOT).get());

        // The global policies are still stored in the __change_events topic
        admin.topicPolicies(true).setCompactionThreshold(topic, 200);
        waitUntilAssert(() -> assertEquals(admin.topicPolicies(true).getCompactionThreshold(topic), 200));
        assertFalse(pulsar.getConfigurationMetadataStore()
                .exists(MetadataStoreTopicPoliciesService.GLOBAL_POLICIES_ROOT).get());

        admin.topicPolicies().deleteTopicPolicies(topic);
        waitUntilAssert(() -> assertNull(admin.topicPolicies().getCompactionThreshold(topic)));

        admin.namespaces().createNamespace(metaNamespace);
    }

    @Test(priority = 1)
    public void testDowngrade() throws Exception {
        final var topic1 = "downgrade"; // in default namespace
        admin.topics().createNonPartitionedTopic(topic1);
        admin.topicPolicies().setCompactionThreshold(topic1, 1);
        waitUntilAssert(() -> assertEquals(admin.topicPolicies().getCompactionThreshold(topic1), 1));

        final var topic2 = metaNamespace + "/downgrade";
        admin.topics().createNonPartitionedTopic(topic2);
        admin.topicPolicies().setCompactionThreshold(topic2, 2);
        waitUntilAssert(() -> assertEquals(admin.topicPolicies().getCompactionThreshold(topic2), 2));

        restartBroker(conf ->
                conf.setTopicPoliciesServiceClassName(SystemTopicBasedTopicPoliciesService.class.getName()));
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic1), 1);
        // The policies will be lost because they are not stored in the __change_events topic
        assertNull(admin.topicPolicies().getCompactionThreshold(topic2));
    }

    @DataProvider
    public Object[][] namespaces() {
        return new Object[][] {
                { "public/default" },
                { metaNamespace }
        };
    }

    @Test(dataProvider = "namespaces")
    public void testPoliciesOperations(String namespace) throws Exception {
        final var topicName = TopicName.get(namespace + "/test-policies-operations");
        final var topic = topicName.toString();
        admin.topics().createNonPartitionedTopic(topic);

        final var compactionThreshold = new AtomicLong(0);
        // Verify the exception thrown from one listener does not affect other listeners
        pulsar.getTopicPoliciesService().registerListenerAsync(topicName, __ -> {
            throw new RuntimeException("injected failure");
        }).get();
        pulsar.getTopicPoliciesService().registerListenerAsync(topicName, policies ->
                Optional.ofNullable(policies).map(TopicPolicies::getCompactionThreshold).ifPresentOrElse(
                        compactionThreshold::set, () -> compactionThreshold.set(-1))).get();

        // Verify Created events are handled
        admin.topicPolicies(false).setCompactionThreshold(topic, 100);
        waitUntilAssert(() -> assertEquals(compactionThreshold.get(), 100));
        final var localStore = pulsar.getLocalMetadataStore();
        final var configurationStore = pulsar.getConfigurationMetadataStore();

        if (namespace.equals(metaNamespace)) {
            assertTrue(localStore.exists(MetadataStoreTopicPoliciesService.pathFor(topicName, false)).get());
            assertFalse(configurationStore.exists(MetadataStoreTopicPoliciesService.pathFor(topicName, true)).get());
        }

        admin.topicPolicies(true).setCompactionThreshold(topic, 200);
        waitUntilAssert(() -> assertEquals(compactionThreshold.get(), 200));
        if (namespace.equals(metaNamespace)) {
            assertTrue(configurationStore.exists(MetadataStoreTopicPoliciesService.pathFor(topicName, true)).get());
        }

        // Verify Modified events are handled
        admin.topicPolicies(false).setCompactionThreshold(topic, 300);
        waitUntilAssert(() -> assertEquals(compactionThreshold.get(), 300));

        admin.topicPolicies(true).setCompactionThreshold(topic, 400);
        waitUntilAssert(() -> assertEquals(compactionThreshold.get(), 400));

        final var readerNamespaces = ((LegacyAwareTopicPoliciesService) pulsar.getTopicPoliciesService())
                .systemTopicService.getReaderCaches().keySet();
        assertFalse(readerNamespaces.contains(NamespaceName.get(metaNamespace)));

        // Verify Deleted events are handled
        admin.topicPolicies(false).deleteTopicPolicies(topic);
        waitUntilAssert(() -> assertEquals(compactionThreshold.get(), -1));
        if (namespace.equals(metaNamespace)) {
            assertFalse(localStore.exists(MetadataStoreTopicPoliciesService.pathFor(topicName, false)).get());
            assertFalse(configurationStore.exists(MetadataStoreTopicPoliciesService.pathFor(topicName, true)).get());
        }
    }

    @Test
    public void testUserCreatedEventsTopicAreIgnored() throws Exception {
        final var topic = TopicName.get(metaNamespace + "/" + System.currentTimeMillis()).toString();
        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setCompactionThreshold(topic, 1);
        waitUntilAssert(() -> assertEquals(admin.topicPolicies().getCompactionThreshold(topic), 1));

        final var eventsTopic = metaNamespace + "/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
        admin.topics().createNonPartitionedTopic(eventsTopic);
        // Even if the __change_events topic is created, since it has detected the namespace didn't have the events
        // topic before, it will be ignored and the policies are still read from metadata store.
        waitUntilAssert(() -> assertEquals(admin.topicPolicies().getCompactionThreshold(topic), 1));
        admin.topics().delete(eventsTopic);
    }

    private static void waitUntilAssert(ThrowingRunnable assertion) {
        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(assertion);
    }
}
