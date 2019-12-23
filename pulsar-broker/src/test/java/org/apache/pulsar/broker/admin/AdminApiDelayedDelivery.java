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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("delayed-delivery-messages", tenantInfo);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testDelayedDeliveryMessages() throws Exception {
        admin.namespaces().createNamespace("delayed-delivery-messages/default");
        String namespace = "delayed-delivery-messages/default";
        String topicName = "persistent://delayed-delivery-messages/default/test";
        admin.namespaces().setDelayedDeliveryMessages(namespace, true);
        admin.namespaces().setDelayedDeliveryTime(namespace, 1500L);

        Producer producer = pulsarClient.newProducer(Schema.STRING).
                topic(topicName).
                create();
        Consumer consumer = pulsarClient.newConsumer(Schema.STRING).
                topic(topicName).
                subscriptionName("sub-delayed").
                subscriptionType(SubscriptionType.Shared).
                subscribe();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("undelay-msgs-" + i)
                    .sendAsync();

            producer.newMessage()
                    .value("delay-msgs-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }
        producer.flush();

        // consumer will receive the messages immediately while
        // the shared consumer will get them after the delay
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertEquals(msg.getValue(), "undelay-msgs-" + i);
            consumer.acknowledge(msg);
        }

        Set<String> delayedMessages = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
            delayedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }
        assertEquals(delayedMessages.size(), 10);

        for (int i = 0; i < 10; i++) {
            assertTrue(delayedMessages.contains("delay-msgs-" + i));
        }
    }
}
