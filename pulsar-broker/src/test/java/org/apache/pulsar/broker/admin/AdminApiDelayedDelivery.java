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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

@Slf4j
public class AdminApiDelayedDelivery extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiDelayedDelivery.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("delayed-delivery-messages", tenantInfo);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testDisableDelayedDelivery() throws Exception {
        admin.namespaces().createNamespace("delayed-delivery-messages/default-ns");
        String namespace = "delayed-delivery-messages/default-ns";
        assertTrue(admin.namespaces().getDelayedDelivery(namespace).isActive());

        DelayedDeliveryPolicies delayedDeliveryPolicies = new DelayedDeliveryPolicies(2000, false);
        admin.namespaces().setDelayedDeliveryMessages(namespace, delayedDeliveryPolicies);

        assertFalse(admin.namespaces().getDelayedDelivery(namespace).isActive());
        assertEquals(2000, admin.namespaces().getDelayedDelivery(namespace).getTickTime());
    }

    @Test(timeOut = 20000)
    public void testEnableAndDisableTopicDelayedDelivery() throws Exception {
        String topicName = "persistent://delayed-delivery-messages/default-ns/test" + System.currentTimeMillis();
        String namespace = "delayed-delivery-messages/default-ns";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newLinkedHashSet(Lists.newArrayList("test")));

        admin.topics().createPartitionedTopic(topicName, 3);
        assertNull(admin.topics().getDelayedDeliveryPolicy(topicName));

        DelayedDeliveryPolicies delayedDeliveryPolicies = new DelayedDeliveryPolicies(2000, true);
        admin.topics().setDelayedDeliveryPolicy(topicName, delayedDeliveryPolicies);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName) != null) {
                break;
            }
        }

        assertTrue(admin.topics().getDelayedDeliveryPolicy(topicName).isActive());
        assertEquals(2000, admin.topics().getDelayedDeliveryPolicy(topicName).getTickTime());

        admin.topics().removeDelayedDeliveryPolicy(topicName);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName) == null) {
                break;
            }
        }
        assertNull(admin.topics().getDelayedDeliveryPolicy(topicName));
    }

    @Test(timeOut = 20000)
    public void testEnableTopicDelayedDelivery() throws Exception {
        final String topicName = "persistent://delayed-delivery-messages/default-ns/test" + System.currentTimeMillis();
        final String namespace = "delayed-delivery-messages/default-ns";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newLinkedHashSet(Lists.newArrayList("test")));

        admin.topics().createPartitionedTopic(topicName, 3);
        assertNull(admin.topics().getDelayedDeliveryPolicy(topicName));
        //1 Set topic policy
        DelayedDeliveryPolicies delayedDeliveryPolicies = new DelayedDeliveryPolicies(2000, true);
        admin.topics().setDelayedDeliveryPolicy(topicName, delayedDeliveryPolicies);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName) != null) {
                break;
            }
        }
        //2 Setup consumer and producer
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub" + System.currentTimeMillis())
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName).create();
        //3 Send delay message
        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("delayed-msg-" + i)
                    .deliverAfter(4, TimeUnit.SECONDS)
                    .sendAsync();
        }
        producer.flush();

        //4 There will be no message in the first 3 seconds
        assertNull(consumer.receive(3, TimeUnit.SECONDS));

        Set<String> delayedMessages = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(4, TimeUnit.SECONDS);
            delayedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }
        for (int i = 0; i < 10; i++) {
            assertTrue(delayedMessages.contains("delayed-msg-" + i));
        }
        //5 Disable delayed delivery
        delayedDeliveryPolicies.setActive(false);
        admin.topics().setDelayedDeliveryPolicy(topicName, delayedDeliveryPolicies);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (!admin.topics().getDelayedDeliveryPolicy(topicName).isActive()) {
                break;
            }
        }
        producer.newMessage().value("disabled-msg").deliverAfter(5, TimeUnit.SECONDS).send();
        //6 Delay deliver is disabled, so we can receive message immediately
        Message<String> msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(msg);
        consumer.acknowledge(msg);
        //7 Set a very long tick time, so that trackDelayedDelivery will fail. we can receive msg immediately.
        delayedDeliveryPolicies.setActive(true);
        delayedDeliveryPolicies.setTickTime(Integer.MAX_VALUE);
        admin.topics().setDelayedDeliveryPolicy(topicName, delayedDeliveryPolicies);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName).isActive()) {
                break;
            }
        }
        producer.newMessage().value("long-tick-msg").deliverAfter(5, TimeUnit.SECONDS).send();
        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(msg);
    }

    @Test
    public void testEnableDelayedDeliveryMessages() throws Exception {
        admin.namespaces().createNamespace("delayed-delivery-messages/default-enable-service-conf");
        String namespace = "delayed-delivery-messages/default-enable-service-conf";
        String topicName = "persistent://delayed-delivery-messages/default-enable-service-conf/test";
        assertTrue(admin.namespaces().getDelayedDelivery(namespace).isActive());

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
}
