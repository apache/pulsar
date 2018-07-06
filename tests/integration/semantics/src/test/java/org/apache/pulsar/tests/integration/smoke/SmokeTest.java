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
package org.apache.pulsar.tests.integration.smoke;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.Container;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class SmokeTest extends PulsarClusterTestBase {

    private final static String clusterName = "test";

    @Test(dataProvider = "ServiceUrls")
    public void testPublishAndConsume(String serviceUrl) throws Exception {

        this.createTenantName("smoke-test", clusterName, "smoke-admin");
        pulsarCluster.createNamespace(clusterName);
        String topic = "persistent://smoke-test/test/ns1/topic1";

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(topic)
            .subscriptionName("test-sub")
            .ackTimeout(10, TimeUnit.SECONDS)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscribe();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
            .topic(topic)
            .enableBatching(false)
            .producerName("effectively-once-producer")
            .initialSequenceId(1L)
            .create();

        for (int i = 0; i < 10; i++) {
            producer.send(("smoke-message" + i));
        }
        for (int i = 0; i < 10; i++) {
            Message m = consumer.receive();
            Assert.assertEquals("smoke-message" + i, new String(m.getData()));
        }
    }

    private Container.ExecResult createTenantName(String tenantName,
                                                  String clusterName,
                                                  String roleName) throws Exception {
        return pulsarCluster.runAdminCommandOnAnyBroker(
            "tenants", "create", tenantName, "--allowed-clusters", clusterName,
            "--admin-roles", roleName);
    }
}
