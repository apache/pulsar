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
package org.apache.pulsar.broker.admin;

import com.google.common.collect.Sets;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

@Slf4j
public class AdminApiDelayedDelivery extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiDelayedDelivery.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("delayed-delivery-messages", tenantInfo);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testDisableDelayedDelivery() throws Exception {
        admin.namespaces().createNamespace("delayed-delivery-messages/default-ns");
        String namespace = "delayed-delivery-messages/default-ns";
        assertNull(admin.namespaces().getDelayedDelivery(namespace));

        DelayedDeliveryPolicies delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                .tickTime(2000)
                .active(false)
                .build();
        admin.namespaces().setDelayedDeliveryMessages(namespace, delayedDeliveryPolicies);
        //zk update takes time
        Awaitility.await().until(() ->
                admin.namespaces().getDelayedDelivery(namespace) != null);
        assertFalse(admin.namespaces().getDelayedDelivery(namespace).isActive());
        assertEquals(2000, admin.namespaces().getDelayedDelivery(namespace).getTickTime());
    }

    @Test
    public void testEnableDelayedDeliveryMessages() throws Exception {
        admin.namespaces().createNamespace("delayed-delivery-messages/default-enable-service-conf");
        String namespace = "delayed-delivery-messages/default-enable-service-conf";
        String topicName = "persistent://delayed-delivery-messages/default-enable-service-conf/test";
        assertNull(admin.namespaces().getDelayedDelivery(namespace));

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("delayed-msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        // Delayed messages might not come in same exact order
        Set<String> delayedMessages = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
            delayedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(delayedMessages.contains("delayed-msg-" + i));
        }
    }

    @Test(timeOut = 30000)
    public void testNamespaceDelayedDeliveryPolicyApi() throws Exception {
        final String namespace = "delayed-delivery-messages/my-ns";
        admin.namespaces().createNamespace(namespace);
        assertNull(admin.namespaces().getDelayedDelivery(namespace));
        DelayedDeliveryPolicies delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                .tickTime(3)
                .active(true)
                .build();
        admin.namespaces().setDelayedDeliveryMessages(namespace, delayedDeliveryPolicies);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getDelayedDelivery(namespace), delayedDeliveryPolicies));

        admin.namespaces().removeDelayedDeliveryMessages(namespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getDelayedDelivery(namespace)));
    }

    @Test(timeOut = 30000)
    public void testDelayedDeliveryApplied() throws Exception {
        cleanup();
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        setup();
        final String namespace = "delayed-delivery-messages/my-ns";
        final String topic = "persistent://" + namespace + "/test" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace);
        pulsarClient.newProducer().topic(topic).create().close();
        //namespace-level default value is null
        assertNull(admin.namespaces().getDelayedDelivery(namespace));
        //topic-level default value is null
        assertNull(admin.topics().getDelayedDeliveryPolicy(topic));
        //use broker-level by default
        DelayedDeliveryPolicies brokerLevelPolicy =
                DelayedDeliveryPolicies.builder()
                        .tickTime(conf.getDelayedDeliveryTickTimeMillis())
                        .active(conf.isDelayedDeliveryEnabled())
                        .build();
        assertEquals(admin.topics().getDelayedDeliveryPolicy(topic, true), brokerLevelPolicy);
        //set namespace-level policy
        DelayedDeliveryPolicies namespaceLevelPolicy = DelayedDeliveryPolicies.builder()
                .tickTime(100)
                .active(true)
                .build();
        admin.namespaces().setDelayedDeliveryMessages(namespace, namespaceLevelPolicy);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getDelayedDelivery(namespace)));
        DelayedDeliveryPolicies policyFromBroker = admin.topics().getDelayedDeliveryPolicy(topic, true);
        assertEquals(policyFromBroker.getTickTime(), 100);
        assertTrue(policyFromBroker.isActive());
        // set topic-level policy
        DelayedDeliveryPolicies topicLevelPolicy = DelayedDeliveryPolicies.builder()
                .tickTime(200)
                .active(true)
                .build();
        admin.topics().setDelayedDeliveryPolicy(topic, topicLevelPolicy);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getDelayedDeliveryPolicy(topic)));
        policyFromBroker = admin.topics().getDelayedDeliveryPolicy(topic, true);
        assertEquals(policyFromBroker.getTickTime(), 200);
        assertTrue(policyFromBroker.isActive());
        //remove topic-level policy
        admin.topics().removeDelayedDeliveryPolicy(topic);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getDelayedDeliveryPolicy(topic, true), namespaceLevelPolicy));
        //remove namespace-level policy
        admin.namespaces().removeDelayedDeliveryMessages(namespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getDelayedDeliveryPolicy(topic, true), brokerLevelPolicy));
    }
}
