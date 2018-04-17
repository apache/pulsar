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
package org.apache.pulsar.tests.integration;

import com.github.dockerjava.api.DockerClient;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.DockerUtils;
import org.apache.pulsar.tests.PulsarClusterUtils;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.arquillian.testng.Arquillian;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCompaction extends Arquillian {
    private static String clusterName = "test";

    @ArquillianResource
    DockerClient docker;

    @BeforeMethod
    public void waitServicesUp() throws Exception {
        Assert.assertTrue(PulsarClusterUtils.waitZooKeeperUp(docker, clusterName, 30, TimeUnit.SECONDS));
        Assert.assertTrue(PulsarClusterUtils.waitAllBrokersUp(docker, clusterName));
    }

    @Test
    public void testPublishCompactAndConsumeCLI() throws Exception {
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                                          "/pulsar/bin/pulsar-admin", "tenants",
                                          "create", "compaction-test-cli", "--allowed-clusters", clusterName,
                                          "--admin-roles", "admin");
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                "/pulsar/bin/pulsar-admin", "namespaces",
                "create", "compaction-test-cli/ns1");
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                "/pulsar/bin/pulsar-admin", "namespaces",
                "set-clusters", "--clusters", "test", "compaction-test-cli/ns1");

        String brokerIp = DockerUtils.getContainerIP(
                docker, PulsarClusterUtils.proxySet(docker, clusterName).stream().findAny().get());
        String serviceUrl = "pulsar://" + brokerIp + ":6650";
        String topic = "persistent://compaction-test-cli/ns1/topic1";

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

            PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                                              "/pulsar/bin/pulsar", "compact-topic",
                                              "-t", topic);

            try (Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                    .readCompacted(true).subscriptionName("sub1").subscribe()) {
                Message<byte[]> m = consumer.receive();
                Assert.assertEquals(m.getKey(), "key0");
                Assert.assertEquals(m.getData(), "content1".getBytes());
            }
        }
    }

    @Test
    public void testPublishCompactAndConsumeRest() throws Exception {
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                                          "/pulsar/bin/pulsar-admin", "tenants",
                                          "create", "compaction-test-rest", "--allowed-clusters", clusterName,
                                          "--admin-roles", "admin");
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                "/pulsar/bin/pulsar-admin", "namespaces",
                "create", "compaction-test-rest/ns1");
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                "/pulsar/bin/pulsar-admin", "namespaces",
                "set-clusters", "--clusters", "test", "compaction-test-rest/ns1");

        String brokerIp = DockerUtils.getContainerIP(
                docker, PulsarClusterUtils.proxySet(docker, clusterName).stream().findAny().get());
        String serviceUrl = "pulsar://" + brokerIp + ":6650";
        String topic = "persistent://compaction-test-rest/ns1/topic1";

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
            PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                    "/pulsar/bin/pulsar-admin", "persistent", "compact", topic);

            PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
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

}
