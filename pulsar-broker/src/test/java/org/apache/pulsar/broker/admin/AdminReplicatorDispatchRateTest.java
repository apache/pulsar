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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AdminReplicatorDispatchRateTest extends MockedPulsarServiceBaseTest {
    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
    }

    @Test
    public void testReplicatorDispatchRateOnTopicLevel() throws Exception {
        TopicName topicName = TopicName.get("test-replicator-dispatch-rate");
        String topic = topicName.toString();

        admin.topics().createNonPartitionedTopic(topic);

        Awaitility.await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic, false));
            // The broker config is used when the replicator dispatch rate is not set.
            assertReplicatorDispatchRateByBrokerConfig(admin.topicPolicies().getReplicatorDispatchRate(topic, true));
        });

        // Set the default replicator dispatch rate on the topic level.
        DispatchRate defaultDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(200)
                .ratePeriodInSecond(1)
                .build();
        admin.topicPolicies().setReplicatorDispatchRate(topic, defaultDispatchRate);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, false), defaultDispatchRate);
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), defaultDispatchRate);
        });

        // Set the replicator dispatch rate for the r1 cluster on the topic leve.
        String r1Cluster = "r1";
        DispatchRate r1DispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(200)
                .dispatchThrottlingRateInByte(400)
                .ratePeriodInSecond(1)
                .build();
        admin.topicPolicies().setReplicatorDispatchRate(topic, r1Cluster, r1DispatchRate);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), defaultDispatchRate);
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, false), defaultDispatchRate);
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, true), r1DispatchRate);
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, false), r1DispatchRate);
        });

        // Remove the replicator dispatch rate for the r1 cluster on the topic level.
        admin.topicPolicies().removeReplicatorDispatchRate(topic, r1Cluster);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), defaultDispatchRate);
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, false), defaultDispatchRate);
            assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, false));
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, true), defaultDispatchRate);
        });

        // Remove the default replicator dispatch rate on the topic level.
        admin.topicPolicies().removeReplicatorDispatchRate(topic);
        Awaitility.await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic, false));
            assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, false));
            // The broker config is used when the replicator dispatch rate is not set on any level.
            assertReplicatorDispatchRateByBrokerConfig(
                    admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, true));
            assertReplicatorDispatchRateByBrokerConfig(admin.topicPolicies().getReplicatorDispatchRate(topic, true));
        });
    }

    @Test
    public void testReplicatorDispatchRateOnNamespaceAndTopicLevels() throws Exception {
        String namespace = "public/test-replicator-dispatch-rate-ns";
        admin.namespaces().createNamespace(namespace);

        // Set the default replicator dispatch rate.
        DispatchRate defaultDispatchRateOnNamespace = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(200)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().setReplicatorDispatchRate(namespace, defaultDispatchRateOnNamespace);
        assertEquals(admin.namespaces().getReplicatorDispatchRate(namespace), defaultDispatchRateOnNamespace);

        // Set the replicator dispatch rate for the r1 cluster.
        String r1Cluster = "r1";
        DispatchRate r1DispatchRateOnNamespace = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(200)
                .dispatchThrottlingRateInByte(400)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().setReplicatorDispatchRate(namespace, r1Cluster, r1DispatchRateOnNamespace);
        assertEquals(admin.namespaces().getReplicatorDispatchRate(namespace), defaultDispatchRateOnNamespace);
        assertEquals(admin.namespaces().getReplicatorDispatchRate(namespace, r1Cluster), r1DispatchRateOnNamespace);

        // Topic inherits the namespace level replicator dispatch rate.
        String topic = TopicName.get(namespace + "/test-replicator-dispatch-rate").toString();
        admin.topics().createNonPartitionedTopic(topic);
        assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic));
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), defaultDispatchRateOnNamespace);
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, true),
                r1DispatchRateOnNamespace);

        // Topic overrides the namespace level replicator dispatch rate.
        DispatchRate defaultDispatchRateOnTopic = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(300)
                .dispatchThrottlingRateInByte(600)
                .ratePeriodInSecond(1)
                .build();
        admin.topicPolicies().setReplicatorDispatchRate(topic, defaultDispatchRateOnTopic);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, false), defaultDispatchRateOnTopic);
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), defaultDispatchRateOnTopic);
        });

        DispatchRate topicDispatchRateForR1 = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(400)
                .dispatchThrottlingRateInByte(800)
                .ratePeriodInSecond(1)
                .build();
        admin.topicPolicies().setReplicatorDispatchRate(topic, r1Cluster, topicDispatchRateForR1);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, false),
                    topicDispatchRateForR1);
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, true),
                    topicDispatchRateForR1);
        });

        // r1 cluster's dispatch rate is removed.
        // If applied is true, return default dispatch rate, otherwise, return null.
        admin.topicPolicies().removeReplicatorDispatchRate(topic, r1Cluster);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, true), defaultDispatchRateOnTopic);
            assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, false));
        });

        // The default dispatch rate is removed.
        // If applied is true, return namespace level config, otherwise, return null.
        admin.topicPolicies().removeReplicatorDispatchRate(topic);
        Awaitility.await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic, false));
            assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), defaultDispatchRateOnNamespace);
        });

        // Remove the replicator dispatch rate for the r1 cluster.
        admin.namespaces().removeReplicatorDispatchRate(namespace, r1Cluster);
        assertEquals(admin.namespaces().getReplicatorDispatchRate(namespace), defaultDispatchRateOnNamespace);
        assertNull(admin.namespaces().getReplicatorDispatchRate(namespace, r1Cluster));

        // Remove the default replicator dispatch rate.
        admin.namespaces().removeReplicatorDispatchRate(namespace);
        assertNull(admin.namespaces().getReplicatorDispatchRate(namespace));

        // The broker config is used when the replicator dispatch rate is not set on any level.
        Awaitility.await().untilAsserted(() -> assertReplicatorDispatchRateByBrokerConfig(
                admin.topicPolicies().getReplicatorDispatchRate(topic, r1Cluster, true)));
    }

    private void assertReplicatorDispatchRateByBrokerConfig(DispatchRate replicatorDispatchRate) {
        assertNotNull(replicatorDispatchRate);
        assertEquals(replicatorDispatchRate.getDispatchThrottlingRateInByte(),
                conf.getDispatchThrottlingRatePerReplicatorInByte());
        assertEquals(replicatorDispatchRate.getDispatchThrottlingRateInByte(),
                conf.getDispatchThrottlingRatePerReplicatorInMsg());
    }
}