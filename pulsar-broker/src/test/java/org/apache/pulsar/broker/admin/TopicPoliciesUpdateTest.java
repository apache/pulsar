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

import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.testng.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.SystemTopicBasedTopicPoliciesService;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class TopicPoliciesUpdateTest extends MockedPulsarServiceBaseTest {
    private final boolean partitionedSystemTopic;
    private final String testTenant = "my-tenant";
    private final String testNamespace = "my-namespace";
    private final String myNamespace = testTenant + "/" + testNamespace;

    // comment out the @Factory annotation to run this test individually in IDE
    @Factory
    public static Object[] createTestInstances() {
        return new Object[]{
                new TopicPoliciesUpdateTest(false),
                new TopicPoliciesUpdateTest(true) // test with partitioned system topic
        };
    }

    public TopicPoliciesUpdateTest() {
        partitionedSystemTopic = false;
    }

    private TopicPoliciesUpdateTest(boolean partitionedSystemTopic) {
        this.partitionedSystemTopic = partitionedSystemTopic;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        if (partitionedSystemTopic) {
            // A partitioned system topic will get created when allowAutoTopicCreationType is set to PARTITIONED
            conf.setDefaultNumPartitions(4);
            conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        }
        this.conf.setDefaultNumberOfNamespaceBundles(1);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Set.of("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public Object[][] topicTypes() {
        return new Object[][]{
            {TopicType.PARTITIONED},
            {TopicType.NON_PARTITIONED}
        };
    }

    @Test(dataProvider = "topicTypes")
    public void testMultipleUpdates(TopicType topicType) throws Exception {
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String topic = newUniqueName("persistent://" + myNamespace + "/testtopic" + i);
            if (TopicType.PARTITIONED.equals(topicType)) {
                admin.topics().createNonPartitionedTopic(topic);
            } else {
                admin.topics().createPartitionedTopic(topic, 2);
            }
            topics.add(topic);
        }

        // test data
        InactiveTopicPolicies inactiveTopicPolicies = new InactiveTopicPolicies();
        inactiveTopicPolicies.setDeleteWhileInactive(true);
        inactiveTopicPolicies.setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        inactiveTopicPolicies.setMaxInactiveDurationSeconds(3600);
        DispatchRate dispatchRate = DispatchRate
            .builder()
            .dispatchThrottlingRateInMsg(1000)
            .dispatchThrottlingRateInByte(1000000)
            .build();
        String clusterId = "test";

        // test multiple updates
        for (String topic : topics) {
            for (int i = 0; i < 10; i++) {
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                futures.add(admin.topicPolicies().setInactiveTopicPoliciesAsync(topic, inactiveTopicPolicies));
                futures.add(admin.topicPolicies().setReplicatorDispatchRateAsync(topic, dispatchRate));
                futures.add(admin.topics().setReplicationClustersAsync(topic, List.of(clusterId)));

                // wait for all futures to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                assertEquals(admin.topicPolicies().getInactiveTopicPolicies(topic), inactiveTopicPolicies);
                assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic), dispatchRate);
                assertEquals(admin.topics().getReplicationClusters(topic, true), Set.of(clusterId));
            }
        }

        // verify that there aren't any pending updates in the sequencer
        SystemTopicBasedTopicPoliciesService policyService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        assertEquals(policyService.getTopicPolicyUpdateSequencerSize(), 0,
                "There should be no pending updates after completing all updates");
    }
}
