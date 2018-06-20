/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.tests.integration;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.PulsarClusterUtils;
import org.apache.pulsar.tests.integration.cluster.Cluster3Bookie2Broker;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Ignore
public class TestCompaction {
    private static final String clusterName = "test";
    private Cluster3Bookie2Broker cluster;

    @BeforeClass
    public void waitServicesUp() throws Exception {
        cluster = new Cluster3Bookie2Broker(TestCompaction.class.getSimpleName());
        cluster.start();
        cluster.startAllBrokers();
        cluster.startAllProxies();
    }

    @Test
    public void testPublishCompactAndConsumeCLI() throws Exception {
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "tenants",
            "create", "compaction-test-cli", "--allowed-clusters", clusterName,
            "--admin-roles", "admin");
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "namespaces",
            "create", "compaction-test-cli/ns1");
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "namespaces",
            "set-clusters", "--clusters", clusterName, "compaction-test-cli/ns1");

        String serviceUrl = "pulsar://" + cluster.getPulsarProxyIP() + ":" + cluster.getPulsarProxyPort();

        String topic = "persistent://compaction-test-cli/ns1/topic1";

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

            try (Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
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

            cluster.execInBroker(
                PulsarClusterUtils.PULSAR, "compact-topic",
                "-t", topic);

            try (Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                .readCompacted(true).subscriptionName("sub1").subscribe()) {
                Message<byte[]> m = consumer.receive();
                Assert.assertEquals(m.getKey(), "key0");
                Assert.assertEquals(m.getData(), "content1".getBytes());
            }
        }
    }

    @Test(dependsOnMethods = "testPublishCompactAndConsumeCLI")
    public void testPublishCompactAndConsumeRest() throws Exception {
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "tenants",
            "create", "compaction-test-rest", "--allowed-clusters", clusterName,
            "--admin-roles", "admin");
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "namespaces",
            "create", "compaction-test-rest/ns1");
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "namespaces",
            "set-clusters", "--clusters", "test", "compaction-test-rest/ns1");

        String serviceUrl = "pulsar://" + cluster.getPulsarProxyIP() + ":" + cluster.getPulsarProxyPort();
        String topic = "persistent://compaction-test-rest/ns1/topic1";

        try (PulsarClient client = PulsarClient.create(serviceUrl)) {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

            try (Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
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
            cluster.execInBroker(
                PulsarClusterUtils.PULSAR_ADMIN, "persistent", "compact", topic);

            cluster.execInBroker(
                "/pulsar/bin/pulsar-admin", "persistent", "compaction-status",
                "-w", topic);

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

    @Test(dependsOnMethods = "testPublishCompactAndConsumeRest")
    public void testPublishWithAutoCompaction() throws Exception {
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "tenants",
            "create", "compaction-test-auto",
            "--allowed-clusters", clusterName,
            "--admin-roles", "admin");
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "namespaces",
            "create", "--clusters", "test", "compaction-test-auto/ns1");
        cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "namespaces",
            "set-compaction-threshold", "--threshold", "1", "compaction-test-auto/ns1");

        String serviceUrl = "pulsar://" + cluster.getPulsarProxyIP() + ":" + cluster.getPulsarProxyPort();
        String topic = "persistent://compaction-test-auto/ns1/topic1";

        try (PulsarClient client = PulsarClient.create(serviceUrl)) {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

            try (Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
                producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
                producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
            }

            waitAndVerifyCompacted(client, topic, "sub1", "key0", "content1");

            try (Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
                producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());
            }
            waitAndVerifyCompacted(client, topic, "sub1", "key0", "content2");
        }
    }

    @AfterClass
    public void cleanup() throws Exception {
        cluster.stop();
    }

}
