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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.Container;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.util.stream.Collectors.joining;

public class TestCompaction extends PulsarClusterTestBase {

    @Test(dataProvider = "ServiceUrls")
    public void testPublishCompactAndConsumeCLI(String serviceUrl) throws Exception {

        final String tenant = "compaction-test-cli-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

            try(Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
                producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
                producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
            }

            try (Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                    .readCompacted(true).subscriptionName("sub1").subscribe()) {
                Message<byte[]> m = consumer.receive();
                Assert.assertEquals(m.getKey(), "key0");
                Assert.assertEquals(m.getData(), "content0".getBytes());

                m = consumer.receive();
                Assert.assertEquals(m.getKey(), "key0");
                Assert.assertEquals(m.getData(), "content1".getBytes());
            }

            pulsarCluster.runPulsarBaseCommandOnAnyBroker("compact-topic", "-t", topic);

            try (Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                    .readCompacted(true).subscriptionName("sub1").subscribe()) {
                Message<byte[]> m = consumer.receive();
                Assert.assertEquals(m.getKey(), "key0");
                Assert.assertEquals(m.getData(), "content1".getBytes());
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

            try(Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
                producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
                producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
            }

            try (Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                    .readCompacted(true).subscriptionName("sub1").subscribe()) {
                Message<byte[]> m = consumer.receive();
                Assert.assertEquals(m.getKey(), "key0");
                Assert.assertEquals(m.getData(), "content0".getBytes());

                m = consumer.receive();
                Assert.assertEquals(m.getKey(), "key0");
                Assert.assertEquals(m.getData(), "content1".getBytes());
            }
            pulsarCluster.runAdminCommandOnAnyBroker("persistent",
                    "compact", topic);

            pulsarCluster.runAdminCommandOnAnyBroker("persistent",
                "compaction-status", "-w", topic);

            try (Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                    .readCompacted(true).subscriptionName("sub1").subscribe()) {
                Message<byte[]> m = consumer.receive();
                Assert.assertEquals(m.getKey(), "key0");
                Assert.assertEquals(m.getData(), "content1".getBytes());
            }
        }
    }

    private static void waitAndVerifyCompacted(PulsarClient client, String topic,
                                               String sub, String expectedKey, String expectedValue) throws Exception {
        for (int i = 0; i < 60; i++) {
            try (Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                 .readCompacted(true).subscriptionName(sub).subscribe()) {
                Message<byte[]> m = consumer.receive();
                Assert.assertEquals(m.getKey(), expectedKey);
                if (new String(m.getData()).equals(expectedValue)) {
                    break;
                }
            }
            Thread.sleep(1000);
        }
        try (Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                .readCompacted(true).subscriptionName(sub).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), expectedKey);
            Assert.assertEquals(new String(m.getData()), expectedValue);
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

        try (PulsarClient client = PulsarClient.create(serviceUrl)) {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

            try(Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
                producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
                producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
            }

            waitAndVerifyCompacted(client, topic, "sub1", "key0", "content1");

            try(Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
                producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());
            }
            waitAndVerifyCompacted(client, topic, "sub1", "key0", "content2");
        }
    }

    private Container.ExecResult createTenantName(final String tenantName,
                                                  final String allowedClusterName,
                                                  final String adminRoleName) throws Exception {
        return pulsarCluster.runAdminCommandOnAnyBroker(
            "tenants", "create", "--allowed-clusters", allowedClusterName,
            "--admin-roles", adminRoleName, tenantName);
    }

    private Container.ExecResult createNamespace(final String Ns) throws Exception {
        return pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces",
                "create",
                "--clusters",
                pulsarCluster.getClusterName(), Ns);
    }

}
