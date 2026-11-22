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
package org.apache.pulsar.tests.integration.upgrade;

import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.TOPIC_FIELDS;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.internal;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.outbound;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.dockerjava.api.model.Volume;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.StandaloneContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarStandaloneUpgradeTest {
    private static final String TOPIC = "persistent://public/default/standalone-upgrade";
    private static final String REPLICATION_NAMESPACE = "compatibility/replication";

    @DataProvider
    public Object[][] previousReleases() {
        return new Object[][] {
                {"apachepulsar/pulsar:4.0.13"},
                {"apachepulsar/pulsar:4.2.4"}
        };
    }

    @Test(timeOut = 240_000)
    public void testReplicationStatsAcrossAdminVersions() throws Exception {
        try (Network network = Network.newNetwork();
             StandaloneContainer old = peerBroker("old-broker", "apachepulsar/pulsar:4.0.13", network);
             StandaloneContainer current = peerBroker("new-broker", PulsarContainer.UPGRADE_TEST_IMAGE_NAME, network)) {
            old.start();
            current.start();
            String topic = "persistent://" + REPLICATION_NAMESPACE + "/messages";
            String peer = "other-version";
            try (PulsarAdmin previous = PulsarAdmin.builder().serviceHttpUrl(old.getHttpServiceUrl()).build();
                 PulsarAdmin next = PulsarAdmin.builder().serviceHttpUrl(current.getHttpServiceUrl()).build()) {
                for (PulsarAdmin admin : List.of(previous, next)) {
                    createLocalNamespace(admin, peer, admin == previous ? "new-broker" : "old-broker");
                    admin.topics().createNonPartitionedTopic(topic);
                }
                // Enable the links only after both topics exist, so remote auto-creation cannot race setup.
                for (PulsarAdmin admin : List.of(previous, next)) {
                    String local = admin.brokers().getRuntimeConfigurations().get("clusterName");
                    admin.namespaces().setNamespaceReplicationClusters(REPLICATION_NAMESPACE, Set.of(local, peer),
                            false);
                    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                            assertThat(admin.topics().getStats(topic).getReplication()).containsKey(peer));
                }
            }
            JsonNode newCliOldBroker = cliStats(current, "old-broker", "stats-internal", topic);
            JsonNode oldCliNewBroker = cliStats(old, "new-broker", "stats-internal", topic);
            for (JsonNode crossVersion : new JsonNode[]{newCliOldBroker, oldCliNewBroker}) {
                assertThat(crossVersion.path("ledgers").isArray()).isTrue();
                assertThat(crossVersion.has("replicators")).isFalse();
            }
            for (JsonNode response : new JsonNode[]{cliStats(current, "old-broker", "stats", topic),
                    cliStats(old, "new-broker", "stats", topic)}) {
                JsonNode row = response.path("replication").path(peer);
                assertThat(row.path("connected").isBoolean()).isTrue();
                for (String field : TOPIC_FIELDS) {
                    assertThat(row.has(field)).as(field).isFalse();
                }
            }
            // The 4.0.13 image supplies javac. Compile with each Admin API, then run both binaries on current.
            old.copyFileToContainer(MountableFile.forClasspathResource("upgrade/LegacyReplicationSnapshot.java"),
                    "/tmp/LegacyReplicationSnapshot.java");
            String currentApi = current.execCmd("sh", "-c", "ls /pulsar/lib/*-pulsar-client-admin-api-*.jar")
                    .getStdout().trim();
            byte[] apiJar = current.copyFileFromContainer(currentApi, InputStream::readAllBytes);
            old.copyFileToContainer(Transferable.of(apiJar), "/tmp/current-admin-api.jar");
            JsonNode oldBinary = legacySnapshot(old, current, "/pulsar/lib/*", topic, peer);
            JsonNode recompiled = legacySnapshot(old, current, "/tmp/current-admin-api.jar:/pulsar/lib/*", topic, peer);
            JsonNode newCliNewBroker = cliStats(current, "new-broker", "stats-internal", topic);
            JsonNode stats = cliStats(current, "new-broker", "stats", topic).path("replication").path(peer);
            assertThat(oldBinary.path("replicationBacklog").isIntegralNumber()).isTrue();
            assertThat(recompiled.path("replicationBacklog").isIntegralNumber()).isTrue();
            assertSoftly(softly -> {
                for (String field : TOPIC_FIELDS) {
                    softly.assertThat(oldBinary.path(field).isNull()).as("4.0.13 bytecode, %s", field).isTrue();
                    softly.assertThat(recompiled.path(field).isNull()).as("recompiled source, %s", field).isTrue();
                }
                softly.assertThat(newCliNewBroker.path("replicators").path(peer).path("producerQueueMaxSize").asInt())
                        .as("current CLI and broker queue capacity").isPositive();
                softly.assertThat(stats.path("replicatorState").isTextual())
                        .as("current CLI and broker replicator state").isTrue();
            });
        }
    }

    @Test(timeOut = 180_000)
    public void testDiagnosePartitionCountMismatchAcrossVersions() throws Exception {
        // Separate metadata stores let both clusters have the same topic with different partition counts.
        try (Network network = Network.newNetwork();
             StandaloneContainer old = peerBroker("old-broker", "apachepulsar/pulsar:4.0.13", network);
             StandaloneContainer current = peerBroker("new-broker", PulsarContainer.UPGRADE_TEST_IMAGE_NAME, network)) {
            old.start();
            current.start();
            String peer = "legacy-target";
            String topic = "persistent://" + REPLICATION_NAMESPACE + "/partition-mismatch";
            String partition = topic + "-partition-0";
            try (PulsarAdmin source = PulsarAdmin.builder().serviceHttpUrl(current.getHttpServiceUrl()).build();
                 PulsarAdmin target = PulsarAdmin.builder().serviceHttpUrl(old.getHttpServiceUrl()).build()) {
                String local = createLocalNamespace(source, peer, "old-broker");
                createLocalNamespace(target, "current-source", "new-broker");
                source.topics().createPartitionedTopic(topic, 2);
                target.topics().createPartitionedTopic(topic, 1);
                target.topics().createSubscription(partition, "verify", MessageId.earliest);
                source.namespaces().setNamespaceReplicationClusters(REPLICATION_NAMESPACE, Set.of(local, peer), false);
                await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(
                        source.topics().getInternalStats(partition).cursors).containsKey("pulsar.repl." + peer));
                current.execCmd("bin/pulsar-client", "produce", partition, "-m", "published before repair");
                assertThat(source.topics().getPartitionedTopicMetadata(topic).partitions).isEqualTo(2);
                assertThat(target.topics().getPartitionedTopicMetadata(topic).partitions).isOne();
                await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                    var row = source.topics().getStats(partition).getReplication().get(peer);
                    assertThat(row).isNotNull();
                    assertThat(row.isConnected()).isFalse();
                    assertThat(row.getReplicationBacklog()).isOne();
                });
                JsonNode startup;
                try {
                    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                        JsonNode health = outbound(source, partition, peer).path("replicatorHealth");
                        assertThat(health.path("healthy").isBoolean()).as("replicatorHealth.healthy for %s", partition)
                                .isTrue();
                        assertThat(health.get("healthy").booleanValue()).isFalse();
                        assertThat(health.path("message").asText()).contains("Partition count mismatch",
                                "source=2, target=1", "lastProducerStartFailure");
                    });
                    startup = internal(source, partition, peer).path("lastProducerStartFailure");
                    assertThat(startup.size()).isEqualTo(2);
                    assertThat(startup.path("message").textValue()).isEqualTo(
                            "PulsarServerException: Partition count mismatch (source=2, target=1). "
                                    + "Check partitioned topic metadata on both clusters.");
                    assertThat(startup.path("timestamp").isIntegralNumber()).isTrue();
                } finally {
                    target.topics().updatePartitionedTopic(topic, 2, true);
                    await().atMost(45, TimeUnit.SECONDS).untilAsserted(() -> {
                        assertThat(target.topics().peekMessages(partition, "verify", 1)).singleElement().satisfies(
                                msg -> assertThat(new String(msg.getData(), StandardCharsets.UTF_8))
                                        .isEqualTo("published before repair"));
                        assertThat(source.topics().getStats(partition).getReplication().get(peer)
                                .getReplicationBacklog()).isZero();
                    });
                }
                JsonNode row = outbound(source, partition, peer);
                assertThat(row.path("replicatorHealth").path("healthy").asBoolean()).isTrue();
                assertThat(row.path("lastReplicationSucceededAt").isIntegralNumber()).isTrue();
                JsonNode retained = internal(source, partition, peer).path("lastProducerStartFailure");
                assertThat(retained.path("message")).isEqualTo(startup.path("message"));
                assertThat(retained.path("timestamp").asLong())
                        .isGreaterThanOrEqualTo(startup.path("timestamp").asLong());
            }
        }
    }

    private static StandaloneContainer peerBroker(String host, String image, Network network) {
        return standalone(host + "-" + UUID.randomUUID(), image, network, null, null).withNetworkAliases(host);
    }

    private static String createLocalNamespace(PulsarAdmin admin, String peer, String host) throws Exception {
        String local = admin.brokers().getRuntimeConfigurations().get("clusterName");
        admin.clusters().createCluster(peer, ClusterData.builder().serviceUrl("http://" + host + ":8080")
                .brokerServiceUrl("pulsar://" + host + ":6650").build());
        admin.tenants().createTenant("compatibility",
                TenantInfo.builder().allowedClusters(Set.of(local, peer)).build());
        admin.namespaces().createNamespace(REPLICATION_NAMESPACE, Set.of(local));
        return local;
    }

    private static JsonNode legacySnapshot(StandaloneContainer compiler, StandaloneContainer runtime,
                                           String classpath, String topic, String peer) throws Exception {
        String bytecode = "/tmp/LegacyReplicationSnapshot.class";
        compiler.execCmd("javac", "-proc:none", "--release", "17", "-cp", classpath,
                "/tmp/LegacyReplicationSnapshot.java");
        byte[] application = compiler.copyFileFromContainer(bytecode, InputStream::readAllBytes);
        runtime.copyFileToContainer(Transferable.of(application), bytecode);
        String output = runtime.execCmd("java", "-cp", "/tmp:/pulsar/lib/*", "LegacyReplicationSnapshot", topic, peer)
                .getStdout();
        return ObjectMapperFactory.getMapper().reader().readTree(output.lines()
                .filter(line -> line.startsWith("{")).findFirst().orElseThrow());
    }

    private static JsonNode cliStats(StandaloneContainer cli, String host, String command, String topic)
            throws Exception {
        String json = cli.execCmd("bin/pulsar-admin", "--admin-url", "http://" + host + ":8080",
                "topics", command, topic).getStdout();
        return ObjectMapperFactory.getMapper().reader().readTree(json);
    }

    @Test(dataProvider = "previousReleases", timeOut = 900_000)
    public void testUpgradeWithExistingCookies(String oldImage) throws Exception {
        String clusterName = "standalone-upgrade-" + UUID.randomUUID();
        try (Network network = Network.newNetwork();
             GenericContainer<?> storage = new GenericContainer<>(PulsarContainer.ALPINE_IMAGE_NAME)
                     .withCreateContainerCmdModifier(cmd -> cmd.withVolumes(new Volume("/pulsar/data")))
                     .withCommand("sh", "-c", "chown 10000:0 /pulsar/data && chmod 775 /pulsar/data"
                             + " && echo ready && tail -f /dev/null")
                     .waitingFor(Wait.forLogMessage(".*ready\n", 1))) {
            storage.start();
            String[] cookies = new String[2];
            try (StandaloneContainer old = standalone(clusterName, oldImage, network, storage, null)) {
                old.start();
                old.execCmd("bin/pulsar-admin", "topics", "create", TOPIC);
                // Keep a durable subscription unread so every phase can verify all persisted messages.
                old.execCmd("bin/pulsar-admin", "topics", "create-subscription", "-s", "upgrade", TOPIC);
                produce(old, "before-upgrade");
                for (int i = 0; i < cookies.length; i++) {
                    cookies[i] = cookie(old, i);
                    assertThat(cookies[i]).containsPattern("bookieHost: \"[^\"]+:[0-9]+\"");
                }
            }
            try (StandaloneContainer upgraded = standalone(clusterName, PulsarContainer.UPGRADE_TEST_IMAGE_NAME,
                    network, storage, null)) {
                upgraded.start();
                assertLegacyBookiePorts(upgraded, cookies);
                assertCookiesUnchanged(upgraded, cookies);
                consume(upgraded, "before-upgrade");
                produce(upgraded, "after-upgrade");
            }
            try (StandaloneContainer restarted = standalone(clusterName, PulsarContainer.UPGRADE_TEST_IMAGE_NAME,
                    network, storage, 3191)) {
                restarted.start();
                assertLegacyBookiePorts(restarted, cookies);
                assertCookiesUnchanged(restarted, cookies);
                consume(restarted, "before-upgrade", "after-upgrade");
                produce(restarted, "after-restart");
                consume(restarted, "before-upgrade", "after-upgrade", "after-restart");
            }
        }
    }

    @DataProvider
    public Object[][] standaloneModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "standaloneModes", timeOut = 300_000)
    public void testExplicitBookiePorts(boolean useZookeeper) throws Exception {
        try (Network network = Network.newNetwork();
             StandaloneContainer container = standalone("standalone-ports-" + UUID.randomUUID(),
                     PulsarContainer.UPGRADE_TEST_IMAGE_NAME, network, null, 3181)
                     .withEnv("PULSAR_STANDALONE_USE_ZOOKEEPER", Boolean.toString(useZookeeper))) {
            container.start();
            assertListening(container, 3181);
            assertListening(container, 3182);
            if (!useZookeeper) {
                for (int i = 0; i < 2; i++) {
                    assertThat(cookie(container, i)).contains("bookieHost: \"bk-" + i + "\"");
                }
            }
            container.execCmd("bin/pulsar-admin", "topics", "create", TOPIC);
            container.execCmd("bin/pulsar-admin", "topics", "create-subscription", "-s", "upgrade", TOPIC);
            produce(container, "fixed-ports");
            consume(container, "fixed-ports");
        }
    }

    private static void assertLegacyBookiePorts(StandaloneContainer container, String[] cookies) throws Exception {
        for (String cookie : cookies) {
            Matcher matcher = Pattern.compile("bookieHost: \"[^\"]+:(\\d+)\"").matcher(cookie);
            assertThat(matcher.find()).isTrue();
            assertListening(container, Integer.parseInt(matcher.group(1)));
        }
    }

    private static void assertListening(StandaloneContainer container, int port) throws Exception {
        container.execCmd("bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/" + port);
    }

    private static StandaloneContainer standalone(String clusterName, String image, Network network,
                                                   GenericContainer<?> storage, Integer basePort) {
        StandaloneContainer container = new StandaloneContainer(clusterName, image) {
            @Override
            protected void configure() {
                super.configure();
                if (basePort == null) {
                    setCommand("standalone", "--num-bookies", "2", "--no-functions-worker");
                } else {
                    setCommand("standalone", "--num-bookies", "2", "--no-functions-worker",
                            "--bookkeeper-port", basePort.toString());
                }
            }
        }.withNetwork(network)
                .withEnv("PULSAR_STANDALONE_USE_ZOOKEEPER", "false");
        if (storage != null) {
            container.withVolumesFrom(storage, BindMode.READ_WRITE);
        }
        return container;
    }

    private static String cookie(StandaloneContainer container, int index) throws Exception {
        String directory = "/pulsar/data/standalone/bookkeeper" + (index == 0 ? "" : "/" + index);
        return container.execCmd("cat", directory + "/current/VERSION").getStdout();
    }

    private static void assertCookiesUnchanged(StandaloneContainer container, String[] cookies) throws Exception {
        for (int i = 0; i < cookies.length; i++) {
            assertThat(cookie(container, i)).isEqualTo(cookies[i]);
        }
    }

    private static void produce(StandaloneContainer container, String message) throws Exception {
        container.execCmd("bin/pulsar-client", "produce", TOPIC, "-m", message);
    }

    private static void consume(StandaloneContainer container, String... messages) throws Exception {
        ContainerExecResult result = container.execCmdAsync("bin/pulsar-client", "consume", TOPIC,
                "-s", "read-" + UUID.randomUUID(), "-p", "Earliest", "-n", Integer.toString(messages.length))
                .get(60, TimeUnit.SECONDS);
        for (String message : messages) {
            assertThat(result.getStdout()).contains("content:" + message);
        }
    }
}
