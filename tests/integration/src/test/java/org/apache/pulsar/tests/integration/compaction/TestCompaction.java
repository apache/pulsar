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
package org.apache.pulsar.tests.integration.compaction;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.testng.annotations.Test;

/**
 * Test cases for compaction.
 */
public class TestCompaction extends PulsarTestSuite {

    @Test(dataProvider = "ServiceUrls")
    public void testPublishCompactAndConsumeCLI(String serviceUrl) throws Exception {

        final String tenant = "compaction-test-cli-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

            try(Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic).create()) {
                producer.newMessage()
                    .key("key0")
                    .value("content0")
                    .send();
                producer.newMessage()
                    .key("key0")
                    .value("content1")
                    .send();
            }

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .readCompacted(true)
                .subscriptionName("sub1")
                .subscribe()) {
                Message<String> m = consumer.receive();
                assertEquals(m.getKey(), "key0");
                assertEquals(m.getValue(), "content0");

                m = consumer.receive();
                assertEquals(m.getKey(), "key0");
                assertEquals(m.getValue(), "content1");
            }

            pulsarCluster.runPulsarBaseCommandOnAnyBroker("compact-topic", "-t", topic);

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .readCompacted(true)
                .subscriptionName("sub1")
                .subscribe()) {
                Message<String> m = consumer.receive();
                assertEquals(m.getKey(), "key0");
                assertEquals(m.getValue(), "content1");
            }
        }
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPublishCompactAndConsumeRest(String serviceUrl) throws Exception {

        final String tenant = "compaction-test-rest-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-clusters", "--clusters", pulsarCluster.getClusterName(), namespace);

        try (PulsarClient client = PulsarClient.create(serviceUrl)) {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

            try(Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).create()) {
                producer.newMessage()
                    .key("key0")
                    .value("content0")
                    .send();
                producer.newMessage()
                    .key("key0")
                    .value("content1")
                    .send();
            }

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
                    .readCompacted(true).subscriptionName("sub1").subscribe()) {
                Message<String> m = consumer.receive();
                assertEquals(m.getKey(), "key0");
                assertEquals(m.getValue(), "content0");

                m = consumer.receive();
                assertEquals(m.getKey(), "key0");
                assertEquals(m.getValue(), "content1");
            }
            pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "compact", topic);

            pulsarCluster.runAdminCommandOnAnyBroker("topics",
                "compaction-status", "-w", topic);

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
                    .readCompacted(true).subscriptionName("sub1").subscribe()) {
                Message<String> m = consumer.receive();
                assertEquals(m.getKey(), "key0");
                assertEquals(m.getValue(), "content1");
            }
        }
    }

    private static void waitAndVerifyCompacted(PulsarClient client, String topic,
                                               String sub, String expectedKey, String expectedValue) throws Exception {
        for (int i = 0; i < 60; i++) {
            try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
                 .readCompacted(true).subscriptionName(sub).subscribe()) {
                Message<String> m = consumer.receive();
                assertEquals(m.getKey(), expectedKey);
                if (m.getValue() == expectedValue) {
                    break;
                }
            }
            Thread.sleep(1000);
        }
        try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
                .readCompacted(true).subscriptionName(sub).subscribe()) {
            Message<String> m = consumer.receive();
            assertEquals(m.getKey(), expectedKey);
            assertEquals(m.getValue(), expectedValue);
        }
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPublishWithAutoCompaction(String serviceUrl) throws Exception {

        final String tenant = "compaction-test-auto-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-compaction-threshold", "--threshold", "1", namespace);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub1").subscribe().close();

            try(Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).create()) {
                producer.newMessage()
                    .key("key0")
                    .value("content0")
                    .send();
                producer.newMessage()
                    .key("key0")
                    .value("content1")
                    .send();
            }

            waitAndVerifyCompacted(client, topic, "sub1", "key0", "content1");

            try(Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).create()) {
                producer.newMessage()
                    .key("key0")
                    .value("content2")
                    .send();
            }
            waitAndVerifyCompacted(client, topic, "sub1", "key0", "content2");
        }
    }

    private ContainerExecResult createTenantName(final String tenantName,
                                                 final String allowedClusterName,
                                                 final String adminRoleName) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
            "tenants", "create", "--allowed-clusters", allowedClusterName,
            "--admin-roles", adminRoleName, tenantName);
        assertEquals(0, result.getExitCode());
        return result;
    }

    private ContainerExecResult createNamespace(final String Ns) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces",
                "create",
                "--clusters",
                pulsarCluster.getClusterName(), Ns);
        assertEquals(0, result.getExitCode());
        return result;
    }


}
