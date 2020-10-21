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

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicPoliciesCacheNotInitException;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

public class SystemTopicBasedTopicPoliciesServiceTest extends MockedPulsarServiceBaseTest {

    private static final String NAMESPACE1 = "system-topic/namespace-1";
    private static final String NAMESPACE2 = "system-topic/namespace-2";
    private static final String NAMESPACE3 = "system-topic/namespace-3";

    private static final TopicName TOPIC1 = TopicName.get("persistent", NamespaceName.get(NAMESPACE1), "topic-1");
    private static final TopicName TOPIC2 = TopicName.get("persistent", NamespaceName.get(NAMESPACE1), "topic-2");
    private static final TopicName TOPIC3 = TopicName.get("persistent", NamespaceName.get(NAMESPACE2), "topic-1");
    private static final TopicName TOPIC4 = TopicName.get("persistent", NamespaceName.get(NAMESPACE2), "topic-2");
    private static final TopicName TOPIC5 = TopicName.get("persistent", NamespaceName.get(NAMESPACE3), "topic-1");
    private static final TopicName TOPIC6 = TopicName.get("persistent", NamespaceName.get(NAMESPACE3), "topic-2");

    private NamespaceEventsSystemTopicFactory systemTopicFactory;
    private SystemTopicBasedTopicPoliciesService systemTopicBasedTopicPoliciesService;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        super.internalSetup();
        prepareData();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetPolicy() throws ExecutionException, InterruptedException, TopicPoliciesCacheNotInitException {

        // Init topic policies
        TopicPolicies initPolicy = TopicPolicies.builder()
                .maxConsumerPerTopic(10)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, initPolicy).get();

        // Wait for all topic policies updated.
        Thread.sleep(3000);

        Assert.assertTrue(systemTopicBasedTopicPoliciesService.getPoliciesCacheInit(TOPIC1.getNamespaceObject()));
        // Assert broker is cache all topic policies
        Assert.assertEquals(systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC1).getMaxConsumerPerTopic().intValue(), 10);

        // Update policy for TOPIC1
        TopicPolicies policies1 = TopicPolicies.builder()
                .maxProducerPerTopic(1)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1).get();

        // Update policy for TOPIC2
        TopicPolicies policies2 = TopicPolicies.builder()
                .maxProducerPerTopic(2)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2).get();

        // Update policy for TOPIC3
        TopicPolicies policies3 = TopicPolicies.builder()
                .maxProducerPerTopic(3)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC3, policies3).get();

        // Update policy for TOPIC4
        TopicPolicies policies4 = TopicPolicies.builder()
                .maxProducerPerTopic(4)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC4, policies4).get();

        // Update policy for TOPIC5
        TopicPolicies policies5 = TopicPolicies.builder()
                .maxProducerPerTopic(5)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC5, policies5).get();

        // Update policy for TOPIC6
        TopicPolicies policies6 = TopicPolicies.builder()
                .maxProducerPerTopic(6)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC6, policies6).get();

        Thread.sleep(1000);

        TopicPolicies policiesGet1 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC1);
        TopicPolicies policiesGet2 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC2);
        TopicPolicies policiesGet3 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC3);
        TopicPolicies policiesGet4 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC4);
        TopicPolicies policiesGet5 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC5);
        TopicPolicies policiesGet6 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC6);

        Assert.assertEquals(policiesGet1, policies1);
        Assert.assertEquals(policiesGet2, policies2);
        Assert.assertEquals(policiesGet3, policies3);
        Assert.assertEquals(policiesGet4, policies4);
        Assert.assertEquals(policiesGet5, policies5);
        Assert.assertEquals(policiesGet6, policies6);

        // Remove reader cache will remove policies cache
        Assert.assertEquals(systemTopicBasedTopicPoliciesService.getPoliciesCacheSize(), 6);

        // Check reader cache is correct.
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE1)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE2)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE3)));

        policies1.setMaxProducerPerTopic(101);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);
        policies2.setMaxProducerPerTopic(102);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies2.setMaxProducerPerTopic(103);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies1.setMaxProducerPerTopic(104);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);
        policies2.setMaxProducerPerTopic(105);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies1.setMaxProducerPerTopic(106);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);

        Thread.sleep(1000);

        // reader for NAMESPACE1 will back fill the reader cache
        policiesGet1 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC1);
        policiesGet2 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC2);
        Assert.assertEquals(policies1, policiesGet1);
        Assert.assertEquals(policies2, policiesGet2);

        // Check reader cache is correct.
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE2)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE1)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE3)));

        // Check get without cache
        policiesGet1 = systemTopicBasedTopicPoliciesService.getTopicPoliciesBypassCacheAsync(TOPIC1).get();
        Assert.assertEquals(policies1, policiesGet1);
    }

    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster("test", new ClusterData(pulsar.getBrokerServiceUrl()));
        admin.tenants().createTenant("system-topic",
                new TenantInfo(Sets.newHashSet(), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.namespaces().createNamespace(NAMESPACE2);
        admin.namespaces().createNamespace(NAMESPACE3);
        admin.lookups().lookupTopic(TOPIC1.toString());
        admin.lookups().lookupTopic(TOPIC2.toString());
        admin.lookups().lookupTopic(TOPIC3.toString());
        admin.lookups().lookupTopic(TOPIC4.toString());
        admin.lookups().lookupTopic(TOPIC5.toString());
        admin.lookups().lookupTopic(TOPIC6.toString());
        systemTopicFactory = new NamespaceEventsSystemTopicFactory(pulsarClient);
        systemTopicBasedTopicPoliciesService = (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
    }
}
