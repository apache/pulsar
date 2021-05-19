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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Test cases for compaction.
 */
@Slf4j
public class TestCompaction extends PulsarTestSuite {

    @Test(dataProvider = "ServiceUrls", timeOut=300_000)
    public void testPublishCompactAndConsumeCLI(Supplier<String> serviceUrl) throws Exception {

        final String tenant = "compaction-test-cli-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl.get()).build()) {
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

    @Test(dataProvider = "ServiceUrls", timeOut=300_000)
    public void testPublishCompactAndConsumeRest(Supplier<String> serviceUrl) throws Exception {

        final String tenant = "compaction-test-rest-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-clusters", "--clusters", pulsarCluster.getClusterName(), namespace);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl.get()).build()) {
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

    @Test(dataProvider = "ServiceUrls", timeOut=300_000)
    public void testPublishCompactAndConsumePartitionedTopics(Supplier<String> serviceUrl) throws Exception {

        final String tenant = "compaction-test-partitioned-topic-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/partitioned-topic";
        final int numKeys = 10;
        final int numValuesPerKey = 10;
        final String subscriptionName = "sub1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-clusters", "--clusters", pulsarCluster.getClusterName(), namespace);

        this.createPartitionedTopic(topic, 2);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl.get()).build()) {
            // force creating individual partitions
            client.newConsumer().topic(topic + "-partition-0").subscriptionName(subscriptionName).subscribe().close();
            client.newConsumer().topic(topic + "-partition-1").subscriptionName(subscriptionName).subscribe().close();

            try(Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return Integer.parseInt(msg.getKey()) % metadata.numPartitions();
                    }
                })
                .create()
            ) {
                for (int i = 0; i < numKeys; i++) {
                    for (int j = 0; j < numValuesPerKey; j++) {
                        producer.newMessage()
                            .key("" + i)
                            .value(("key-" + i + "-value-" + j).getBytes(UTF_8))
                            .send();
                    }
                    log.info("Successfully write {} values for key {}", numValuesPerKey, i);
                }
            }

            // test even partition
            consumePartition(
                client,
                topic + "-partition-0",
                subscriptionName,
                IntStream.range(0, numKeys).filter(i -> i % 2 == 0).boxed().collect(Collectors.toList()),
                numValuesPerKey,
                0);
            // test odd partition
            consumePartition(
                client,
                topic + "-partition-1",
                subscriptionName,
                IntStream.range(0, numKeys).filter(i -> i % 2 != 0).boxed().collect(Collectors.toList()),
                numValuesPerKey,
                0);


            pulsarCluster.runAdminCommandOnAnyBroker("topics",
                "compact", topic + "-partition-0");
            pulsarCluster.runAdminCommandOnAnyBroker("topics",
                "compact", topic + "-partition-1");

            // wait for compaction to be completed. we don't need to sleep here, but sleep will reduce
            // the times of polling compaction-status from brokers
            Thread.sleep(30000);

            pulsarCluster.runAdminCommandOnAnyBroker("topics",
                "compaction-status", "-w", topic + "-partition-0");
            pulsarCluster.runAdminCommandOnAnyBroker("topics",
                "compaction-status", "-w", topic + "-partition-1");

            Map<Integer, String> compactedData = consumeCompactedTopic(client, topic, subscriptionName, numKeys);
            assertEquals(compactedData.size(), numKeys);
            for (int i = 0; i < numKeys; i++) {
                assertEquals("key-" + i + "-value-" + (numValuesPerKey - 1), compactedData.get(i));
            }
        }
    }

    private static void consumePartition(PulsarClient client,
                                         String topic,
                                         String subscription,
                                         List<Integer> keys,
                                         int numValuesPerKey,
                                         int startValue) throws PulsarClientException {
        try (Consumer<byte[]> consumer = client.newConsumer()
             .readCompacted(true)
             .topic(topic)
             .subscriptionName(subscription)
             .subscribe()
        ) {
            for (Integer key : keys) {
                for (int i = 0; i < numValuesPerKey; i++) {
                    Message<byte[]> m = consumer.receive();
                    assertEquals("" + key, m.getKey());
                    assertEquals("key-" + key + "-value-" + (startValue + i), new String(m.getValue(), UTF_8));
                }
                log.info("Read {} values from key {}", numValuesPerKey, key);
            }

        }
    }

    private static Map<Integer, String> consumeCompactedTopic(PulsarClient client,
                                                              String topic,
                                                              String subscription,
                                                              int numKeys) throws PulsarClientException {
        Map<Integer, String> keys = Maps.newHashMap();
        try (Consumer<byte[]> consumer = client.newConsumer()
             .readCompacted(true)
             .topic(topic)
             .subscriptionName(subscription)
             .subscribe()
        ) {
            for (int i = 0; i < numKeys; i++) {
                Message<byte[]> m = consumer.receive();
                keys.put(Integer.parseInt(m.getKey()), new String(m.getValue(), UTF_8));
            }
        }
        return keys;
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

    @Test(dataProvider = "ServiceUrls", timeOut=300_000)
    public void testPublishWithAutoCompaction(Supplier<String> serviceUrl) throws Exception {

        final String tenant = "compaction-test-auto-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        this.createTenantName(tenant, pulsarCluster.getClusterName(), "admin");

        this.createNamespace(namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-compaction-threshold", "--threshold", "1", namespace);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl.get()).build()) {
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

    private ContainerExecResult createPartitionedTopic(final String partitionedTopicName, int numPartitions)
            throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
            "topics",
            "create-partitioned-topic",
            "--partitions", "" + numPartitions,
            partitionedTopicName);
        assertEquals(0, result.getExitCode());
        return result;
    }


}
