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
import org.apache.pulsar.broker.systopic.ActionType;
import org.apache.pulsar.broker.systopic.EventType;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.PulsarEvent;
import org.apache.pulsar.broker.systopic.SystemTopic;
import org.apache.pulsar.broker.systopic.TopicEvent;
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

        SystemTopic systemTopicForNamespace1 = systemTopicFactory.createSystemTopic(NamespaceName.get(NAMESPACE1), EventType.TOPIC_POLICY);

        // Update policy for TOPIC1
        TopicPolicies policies1 = TopicPolicies.builder()
                .maxProducerPerTopic(1)
                .build();
        systemTopicForNamespace1.newWriter().write(buildEvent(TOPIC1, policies1));

        // Update policy for TOPIC2
        TopicPolicies policies2 = TopicPolicies.builder()
                .maxProducerPerTopic(2)
                .build();
        systemTopicForNamespace1.newWriter().write(buildEvent(TOPIC2, policies2));

        SystemTopic systemTopicForNamespace2 = systemTopicFactory.createSystemTopic(NamespaceName.get(NAMESPACE2), EventType.TOPIC_POLICY);

        // Update policy for TOPIC3
        TopicPolicies policies3 = TopicPolicies.builder()
                .maxProducerPerTopic(3)
                .build();
        systemTopicForNamespace2.newWriter().write(buildEvent(TOPIC3, policies3));

        // Update policy for TOPIC4
        TopicPolicies policies4 = TopicPolicies.builder()
                .maxProducerPerTopic(4)
                .build();
        systemTopicForNamespace2.newWriter().write(buildEvent(TOPIC4, policies4));

        SystemTopic systemTopicForNamespace3 = systemTopicFactory.createSystemTopic(NamespaceName.get(NAMESPACE3), EventType.TOPIC_POLICY);

        // Update policy for TOPIC5
        TopicPolicies policies5 = TopicPolicies.builder()
                .maxProducerPerTopic(5)
                .build();
        systemTopicForNamespace2.newWriter().write(buildEvent(TOPIC5, policies5));

        // Update policy for TOPIC6
        TopicPolicies policies6 = TopicPolicies.builder()
                .maxProducerPerTopic(6)
                .build();
        systemTopicForNamespace3.newWriter().write(buildEvent(TOPIC6, policies6));

        TopicPolicies policiesGet1 = topicPoliciesService.getTopicPoliciesAsync(TOPIC1).get();
        TopicPolicies policiesGet2 = topicPoliciesService.getTopicPoliciesAsync(TOPIC2).get();
        TopicPolicies policiesGet3 = topicPoliciesService.getTopicPoliciesAsync(TOPIC3).get();
        TopicPolicies policiesGet4 = topicPoliciesService.getTopicPoliciesAsync(TOPIC4).get();
        TopicPolicies policiesGet5 = topicPoliciesService.getTopicPoliciesAsync(TOPIC5).get();
        TopicPolicies policiesGet6 = topicPoliciesService.getTopicPoliciesAsync(TOPIC6).get();

        Assert.assertEquals(policies1, policiesGet1);
        Assert.assertEquals(policies2, policiesGet2);
        Assert.assertEquals(policies3, policiesGet3);
        Assert.assertEquals(policies4, policiesGet4);
        Assert.assertEquals(policies5, policiesGet5);
        Assert.assertEquals(policies6, policiesGet6);

        policies1.setMaxProducerPerTopic(101);
        systemTopicForNamespace1.newWriter().write(buildEvent(TOPIC1, policies1));
        policies2.setMaxProducerPerTopic(102);
        systemTopicForNamespace1.newWriter().write(buildEvent(TOPIC2, policies2));
        policies2.setMaxProducerPerTopic(103);
        systemTopicForNamespace1.newWriter().write(buildEvent(TOPIC2, policies2));
        policies1.setMaxProducerPerTopic(104);
        systemTopicForNamespace1.newWriter().write(buildEvent(TOPIC1, policies1));
        policies2.setMaxProducerPerTopic(105);
        systemTopicForNamespace1.newWriter().write(buildEvent(TOPIC2, policies2));
        policies1.setMaxProducerPerTopic(106);
        systemTopicForNamespace1.newWriter().write(buildEvent(TOPIC1, policies1));

        policiesGet1 = topicPoliciesService.getTopicPoliciesAsync(TOPIC1).get();
        policiesGet2 = topicPoliciesService.getTopicPoliciesAsync(TOPIC2).get();
        Assert.assertEquals(policies1, policiesGet1);
        Assert.assertEquals(policies2, policiesGet2);

        // Only cache 2 readers
        Assert.assertEquals(topicPoliciesService.getReaderCacheCount(), 3 - 1);

        // Remove reader cache will remove policies cache
        Assert.assertEquals(topicPoliciesService.getPoliciesCacheSize(), 6 - 2);

        // Check get without cache
        policiesGet1 = topicPoliciesService.getTopicPoliciesWithoutCacheAsync(TOPIC1).get();
        Assert.assertEquals(policies1, policiesGet1);
    }

    private PulsarEvent buildEvent(TopicName topic, TopicPolicies policies) {
        return PulsarEvent.builder()
                .eventType(EventType.TOPIC_POLICY)
                .actionType(ActionType.UPDATE)
                .topicEvent(TopicEvent.builder()
                        .domain(topic.getDomain().toString())
                        .tenant(topic.getTenant())
                        .namespace(topic.getNamespaceObject().getLocalName())
                        .topic(topic.getLocalName())
                        .policies(policies)
                        .build())
                .build();
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
