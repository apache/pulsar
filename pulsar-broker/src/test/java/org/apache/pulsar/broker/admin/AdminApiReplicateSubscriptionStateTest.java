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
package org.apache.pulsar.broker.admin;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class AdminApiReplicateSubscriptionStateTest extends MockedPulsarServiceBaseTest {
    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testReplicateSubscriptionStateOnNamespaceLevel() throws PulsarAdminException {
        String nsName = "public/testReplicateSubscriptionState" + System.nanoTime();
        admin.namespaces().createNamespace(nsName);
        assertNull(admin.namespaces().getReplicateSubscriptionState(nsName));

        String topicName = nsName + "/topic" + System.nanoTime();
        admin.topics().createNonPartitionedTopic(topicName);
        assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));

        admin.namespaces().setReplicateSubscriptionState(nsName, true);
        assertTrue(admin.namespaces().getReplicateSubscriptionState(nsName));
        await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, false));
            assertTrue(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));
        });

        admin.namespaces().setReplicateSubscriptionState(nsName, false);
        assertFalse(admin.namespaces().getReplicateSubscriptionState(nsName));
        await().untilAsserted(() -> {
            assertFalse(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));
            assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, false));
        });

        admin.namespaces().setReplicateSubscriptionState(nsName, null);
        assertNull(admin.namespaces().getReplicateSubscriptionState(nsName));
        await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));
            assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, false));
        });
    }

    @Test
    public void testReplicateSubscriptionStateOnTopicLevel() throws PulsarAdminException {
        String nsName = "public/testReplicateSubscriptionState" + System.nanoTime();
        admin.namespaces().createNamespace(nsName);
        assertNull(admin.namespaces().getReplicateSubscriptionState(nsName));

        String topicName = nsName + "/topic" + System.nanoTime();
        admin.topics().createNonPartitionedTopic(topicName);
        assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));
        assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, false));

        admin.topicPolicies().setReplicateSubscriptionState(topicName, true);
        await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicateSubscriptionState(topicName, true), Boolean.TRUE);
            assertEquals(admin.topicPolicies().getReplicateSubscriptionState(topicName, false), Boolean.TRUE);
        });

        admin.topicPolicies().setReplicateSubscriptionState(topicName, false);
        await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicateSubscriptionState(topicName, true), Boolean.FALSE);
            assertEquals(admin.topicPolicies().getReplicateSubscriptionState(topicName, false), Boolean.FALSE);
        });

        admin.topicPolicies().setReplicateSubscriptionState(topicName, null);
        await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));
            assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));
        });
    }

    @DataProvider
    Object[] replicateSubscriptionStatePriorityLevelDataProvider() {
        return new Object[]{
                true,
                false,
                null,
        };
    }

    @Test(dataProvider = "replicateSubscriptionStatePriorityLevelDataProvider")
    public void testReplicateSubscriptionStatePriorityLevel(Boolean enabledOnNamespace)
            throws PulsarAdminException {
        String nsName = "public/testReplicateSubscriptionState" + System.nanoTime();
        admin.namespaces().createNamespace(nsName);
        assertNull(admin.namespaces().getReplicateSubscriptionState(nsName));
        admin.namespaces().setReplicateSubscriptionState(nsName, enabledOnNamespace);

        String topicName = nsName + "/topic" + System.nanoTime();
        admin.topics().createNonPartitionedTopic(topicName);

        admin.topicPolicies().setReplicateSubscriptionState(topicName, false);
        await().untilAsserted(() -> {
            assertFalse(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));
            assertFalse(admin.topicPolicies().getReplicateSubscriptionState(topicName, false));
        });

        admin.topicPolicies().setReplicateSubscriptionState(topicName, true);
        await().untilAsserted(() -> {
            assertTrue(admin.topicPolicies().getReplicateSubscriptionState(topicName, true));
            assertTrue(admin.topicPolicies().getReplicateSubscriptionState(topicName, false));
        });

        admin.topicPolicies().setReplicateSubscriptionState(topicName, null);
        await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicateSubscriptionState(topicName, false));
            assertEquals(admin.topicPolicies().getReplicateSubscriptionState(topicName, true), enabledOnNamespace);
        });
    }
}
