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
package org.apache.pulsar.tests.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.Test;

public class AdminV5MessagingTest {
    @Test(timeOut = 360_000)
    public void administerAndUseV5Topic() throws Exception {
        try (PulsarContainer broker = new PulsarContainer()) {
            broker.start();
            try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(broker.getPulsarAdminUrl()).build();
                 PulsarClient client = PulsarClient.builder()
                         .serviceUrl(broker.getPlainTextPulsarBrokerUrl()).build()) {
                String tenant = "admin-v5-" + UUID.randomUUID();
                admin.tenants().createTenant(tenant, TenantInfo.builder()
                        .allowedClusters(Set.of("standalone")).build());
                assertEquals(admin.tenants().getTenantInfo(tenant).getAllowedClusters(), Set.of("standalone"));
                String topic = "topic://public/default/admin-v5-" + UUID.randomUUID();
                admin.scalableTopics().createScalableTopic(topic, 1);
                // Exercise Jackson model deserialization as well as the admin HTTP transport.
                assertNotNull(admin.namespaces().getPolicies("public/default"));
                try (Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
                     StreamConsumer<String> consumer = client.newStreamConsumer(Schema.string()).topic(topic)
                             .subscriptionName("admin-v5")
                             .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST).subscribe()) {
                    producer.newMessage().value("hello from v5").send();
                    var message = consumer.receive(Duration.ofSeconds(30));
                    assertNotNull(message);
                    assertEquals(message.value(), "hello from v5");
                    consumer.acknowledgeCumulative(message.id());
                }
            }
        }
    }
}
