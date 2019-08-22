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

import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
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
import java.util.concurrent.TimeUnit;

public class TopicPoliciesServiceTest extends MockedPulsarServiceBaseTest {

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
    private TopicPoliciesService topicPoliciesService;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        prepareData();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetPolicy() throws PulsarClientException, ExecutionException, InterruptedException {

        // Update policy for TOPIC1
        TopicPolicies policies1 = TopicPolicies.builder()
                .maxProducerPerTopic(1)
                .build();
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1).get();

        // Update policy for TOPIC2
        TopicPolicies policies2 = TopicPolicies.builder()
                .maxProducerPerTopic(2)
                .build();
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2).get();

        // Update policy for TOPIC3
        TopicPolicies policies3 = TopicPolicies.builder()
                .maxProducerPerTopic(3)
                .build();
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC3, policies3).get();

        // Update policy for TOPIC4
        TopicPolicies policies4 = TopicPolicies.builder()
                .maxProducerPerTopic(4)
                .build();
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC4, policies4).get();

        // Update policy for TOPIC5
        TopicPolicies policies5 = TopicPolicies.builder()
                .maxProducerPerTopic(5)
                .build();
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC5, policies5).get();

        // Update policy for TOPIC6
        TopicPolicies policies6 = TopicPolicies.builder()
                .maxProducerPerTopic(6)
                .build();
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC6, policies6).get();

        TopicPolicies policiesGet1 = topicPoliciesService.getTopicPoliciesAsync(TOPIC1).get();
        TopicPolicies policiesGet2 = topicPoliciesService.getTopicPoliciesAsync(TOPIC2).get();
        TopicPolicies policiesGet3 = topicPoliciesService.getTopicPoliciesAsync(TOPIC3).get();
        TopicPolicies policiesGet4 = topicPoliciesService.getTopicPoliciesAsync(TOPIC4).get();
        TopicPolicies policiesGet5 = topicPoliciesService.getTopicPoliciesAsync(TOPIC5).get();
        TopicPolicies policiesGet6 = topicPoliciesService.getTopicPoliciesAsync(TOPIC6).get();

        Assert.assertEquals(policiesGet1, policies1);
        Assert.assertEquals(policiesGet2, policies2);
        Assert.assertEquals(policiesGet3, policies3);
        Assert.assertEquals(policiesGet4, policies4);
        Assert.assertEquals(policiesGet5, policies5);
        Assert.assertEquals(policiesGet6, policies6);

        // Only cache 2 readers, reader for NAMESPACE1 is evicted
        Assert.assertEquals(topicPoliciesService.getReaderCacheCount(), 3 - 1);

        // Remove reader cache will remove policies cache
        Assert.assertEquals(topicPoliciesService.getPoliciesCacheSize(), 6 - 2);

        // Check reader cache is correct.
        Assert.assertFalse(topicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE1)));
        Assert.assertTrue(topicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE2)));
        Assert.assertTrue(topicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE3)));

        policies1.setMaxProducerPerTopic(101);
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);
        policies2.setMaxProducerPerTopic(102);
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies2.setMaxProducerPerTopic(103);
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies1.setMaxProducerPerTopic(104);
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);
        policies2.setMaxProducerPerTopic(105);
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies1.setMaxProducerPerTopic(106);
        topicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);

        // reader for NAMESPACE1 will back fill the reader cache
        policiesGet1 = topicPoliciesService.getTopicPoliciesAsync(TOPIC1).get();
        policiesGet2 = topicPoliciesService.getTopicPoliciesAsync(TOPIC2).get();
        Assert.assertEquals(policies1, policiesGet1);
        Assert.assertEquals(policies2, policiesGet2);

        // Check reader cache is correct.
        Assert.assertFalse(topicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE2)));
        Assert.assertTrue(topicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE1)));
        Assert.assertTrue(topicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE3)));

        // Check get without cache
        policiesGet1 = topicPoliciesService.getTopicPoliciesWithoutCacheAsync(TOPIC1).get();
        Assert.assertEquals(policies1, policiesGet1);
    }

    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster("test", new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
        admin.tenants().createTenant("system-topic",
                new TenantInfo(Sets.newHashSet(), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.namespaces().createNamespace(NAMESPACE2);
        admin.namespaces().createNamespace(NAMESPACE3);
        systemTopicFactory = new NamespaceEventsSystemTopicFactory(pulsarClient);
        topicPoliciesService = new TopicPoliciesService(pulsar, 2, 1, TimeUnit.MINUTES);
    }
}
