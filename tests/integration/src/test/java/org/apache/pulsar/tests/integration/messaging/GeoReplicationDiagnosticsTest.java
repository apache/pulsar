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
package org.apache.pulsar.tests.integration.messaging;

import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.TOPIC_FIELDS;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.assertInstance;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.assertUnavailable;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.get;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.internal;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.outbound;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Closer;
import io.netty.bootstrap.ChannelInitializerExtension;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.integration.containers.BKContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarGeoClusterTestBase;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Diagnose real geo-replication faults using only information available to an Admin API caller. */
public class GeoReplicationDiagnosticsTest extends PulsarGeoClusterTestBase {

    private static final Duration SETTLE = Duration.ofSeconds(35);
    private static final Duration HEALTH_WINDOW = Duration.ofMinutes(1);
    private static final int QUEUE_CAPACITY = 8;
    private static final int TARGET_MAX_MESSAGE_SIZE = 1024 * 1024;
    private Closer clients;
    private PulsarAdmin source;
    private PulsarAdmin target;
    private PulsarClient producerClient;
    private PulsarClient consumerClient;
    private PulsarCluster sourceCluster;
    private String sourceName;
    private String targetName;

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder[] beforeSetupCluster(
            PulsarClusterSpec.PulsarClusterSpecBuilder... builders) {
        for (var builder : builders) {
            builder.numBrokers(1).brokerEnvs(Map.of(
                    "transactionCoordinatorEnabled", "true",
                    "createTopicToRemoteClusterForReplication", "false",
                    "enableReplicatedSubscriptions", "false",
                    "replicationProducerQueueSize", Integer.toString(QUEUE_CAPACITY),
                    "maxMessageSize", Integer.toString(builder == builders[0]
                            ? TARGET_MAX_MESSAGE_SIZE * 2 : TARGET_MAX_MESSAGE_SIZE),
                    "managedLedgerCacheSizeMB", "0",
                    "managedLedgerReadEntryTimeoutSeconds", "6",
                    "brokerDeleteInactiveTopicsFrequencySeconds", "2",
                    "PULSAR_PREFIX_brokerDeleteInactiveTopicsMaxInactiveDurationSeconds", "3600",
                    "PULSAR_PREFIX_brokerReplicationInactiveThresholdSeconds", "15"));
        }
        return builders;
    }

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        String directory = "/tmp/pip-502-fixtures";
        var broker = getGeoCluster().getClusters()[0].getAnyBroker();
        for (Class<?> fixture : List.of(NullMessageReadFailureProcessor.class, ReplicationChecksumFault.class,
                ReplicationChecksumFault.Initializer.class, ReplicationChecksumFault.OutboundHandler.class)) {
            String file = fixture.getName().replace('.', '/') + ".class";
            broker.withCopyFileToContainer(MountableFile.forClasspathResource(file), directory + "/" + file);
        }
        broker.withEnv("PULSAR_EXTRA_CLASSPATH", directory)
                .withEnv("PULSAR_PREFIX_brokerEntryPayloadProcessors", NullMessageReadFailureProcessor.class.getName())
                // Enable the extension only in the broker. CLI commands in this container also use Netty.
                .withCopyToContainer(Transferable.of("[program:broker]\nenvironment=JAVA_TOOL_OPTIONS=\"-D"
                                + ChannelInitializerExtension.EXTENSIONS_SYSTEM_PROPERTY + "=serviceload\"\n"),
                        "/etc/supervisord/conf.d/replication-checksum.conf")
                .withCopyToContainer(Transferable.of(ReplicationChecksumFault.class.getName() + "\n"),
                        directory + "/META-INF/services/" + ChannelInitializerExtension.class.getName());
    }

    @BeforeClass(alwaysRun = true)
    @Override
    public void setupCluster() throws Exception {
        clients = Closer.create();
        super.setupCluster();
        sourceCluster = getGeoCluster().getClusters()[0];
        PulsarCluster targetCluster = getGeoCluster().getClusters()[1];
        sourceName = sourceCluster.getClusterName();
        targetName = targetCluster.getClusterName();
        source = clients.register(PulsarAdmin.builder().serviceHttpUrl(sourceCluster.getAnyBroker().getHttpServiceUrl())
                .build());
        target = clients.register(PulsarAdmin.builder().serviceHttpUrl(targetCluster.getAnyBroker().getHttpServiceUrl())
                .build());
        producerClient = clients.register(PulsarClient.builder().serviceUrl(sourceCluster.getPlainTextServiceUrl())
                .enableTransaction(true).build());
        consumerClient = clients.register(PulsarClient.builder().serviceUrl(targetCluster.getPlainTextServiceUrl())
                .build());
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void tearDownCluster() throws Exception {
        try {
            clients.close();
        } finally {
            super.tearDownCluster();
        }
    }

    private String newNamespace() throws Exception {
        String ns = "public/replication-stats-" + UUID.randomUUID();
        source.namespaces().createNamespace(ns, Set.of(sourceName, targetName));
        return ns;
    }

    private String createTopic(String ns) throws Exception {
        String topic = "persistent://" + ns + "/messages-" + UUID.randomUUID();
        await().atMost(SETTLE).untilAsserted(() -> assertTrue(
                target.namespaces().getNamespaceReplicationClusters(ns).contains(targetName)));
        target.topics().createNonPartitionedTopic(topic);
        source.topics().createNonPartitionedTopic(topic);
        return topic;
    }

    private Consumer<String> subscribe(String topic) throws Exception {
        return consumerClient.newConsumer(Schema.STRING).topic(topic).subscriptionName("verify")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
    }

    private static void receive(Consumer<String> consumer, String expected) throws Exception {
        Message<String> message = consumer.receive(30, TimeUnit.SECONDS);
        assertNotNull(message, expected);
        assertEquals(message.getValue(), expected);
        consumer.acknowledge(message);
    }

    private void waitForConnection(String topic) {
        await().atMost(SETTLE).untilAsserted(() ->
                assertTrue(outbound(source, topic, targetName).path("connected").asBoolean()));
    }

    private static void assertHealthy(JsonNode stats) {
        assertTrue(stats.path("replicatorHealth").path("healthy").isBoolean(), stats.toString());
        assertTrue(stats.path("replicatorHealth").get("healthy").booleanValue(), stats.toString());
        assertTrue(stats.path("replicatorHealth").path("message").isNull(), stats.toString());
    }

    private static void assertUnhealthy(JsonNode stats, String diagnostic) {
        JsonNode health = stats.path("replicatorHealth");
        assertTrue(health.path("healthy").isBoolean(), stats.toString());
        assertFalse(health.get("healthy").booleanValue(), stats.toString());
        assertTrue(health.path("message").asText().contains(diagnostic), health.toString());
    }

    private static long timestamp(JsonNode object, String field) {
        JsonNode value = object.path(field);
        // All brokers are local containers. Reject seconds, monotonic durations and zero sentinels.
        long now = System.currentTimeMillis();
        assertTrue(value.isIntegralNumber() && value.longValue() >= now - TimeUnit.DAYS.toMillis(1)
                && value.longValue() <= now + TimeUnit.MINUTES.toMillis(1),
                field + " in " + object);
        return value.longValue();
    }

    @Test(timeOut = 150_000)
    public void testIdleFilteredTrafficAndPolicyLifecycle() throws Exception {
        String topic = createTopic(newNamespace());
        try (Consumer<String> consumer = subscribe(topic);
             Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create()) {
            waitForConnection(topic);
            producer.send("first");
            receive(consumer, "first");
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                assertHealthy(row);
                timestamp(row, "lastSendStartedAt");
                timestamp(row, "lastReplicationSucceededAt");
            });
            JsonNode before = outbound(source, topic, targetName);
            MessageIdAdv filtered = (MessageIdAdv) producer.newMessage()
                    .disableReplication().value("filtered").send();
            String cursor = "pulsar.repl." + targetName;
            String filteredPosition = filtered.getLedgerId() + ":" + filtered.getEntryId();
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode details = get(source, topic, "internalStats");
                assertEquals(details.path("cursors").path(cursor).path("markDeletePosition").asText(),
                        filteredPosition);
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.get("lastSendStartedAt"), before.get("lastSendStartedAt"));
                assertEquals(row.get("lastReplicationSucceededAt"), before.get("lastReplicationSucceededAt"));
                assertHealthy(row);
            });
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.path("replicatorState").asText(), "DISCONNECTED");
                assertHealthy(row);
                assertUnavailable(row.path("lastCursorReadStartedAt"));
                assertUnavailable(row.path("producerState"));
                JsonNode instance = internal(source, topic, targetName);
                assertInstance(instance);
                assertTrue(instance.get("producerQueueSize").isNull());
                assertEquals(instance.get("producerQueueMaxSize").intValue(), QUEUE_CAPACITY);
                for (JsonNode task : instance.get("inFlightTasks")) {
                    assertTrue(task.get("entries").isNull(), task.toString());
                }
            });
            producer.send("wake up");
            receive(consumer, "wake up");
            JsonNode printed = ObjectMapperFactory.getMapper().reader().readTree(sourceCluster
                    .runAdminCommandOnAnyBroker("topics", "stats-internal", topic).getStdout());
            assertEquals(printed.path("replicators").path(targetName).path("producerQueueMaxSize").asInt(),
                    QUEUE_CAPACITY);

            producer.send("before removing target");
            receive(consumer, "before removing target");
            var remoteBroker = getGeoCluster().getClusters()[1].getAnyBroker();
            remoteBroker.getDockerClient().pauseContainerCmd(remoteBroker.getContainerId()).exec();
            try {
                producer.send("pending at removal");
                await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertTrue(
                        internal(source, topic, targetName).path("producerQueueSize").asInt() > 0));
                source.topicPolicies().setReplicationClusters(topic, List.of(sourceName)).get(10, TimeUnit.SECONDS);
                // The paused broker holds the close-producer response until we resume it.
                await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertEquals(row.path("replicatorState").asText(), "TERMINATING");
                    assertUnavailable(row.path("replicatorHealth"));
                    JsonNode instance = internal(source, topic, targetName);
                    assertInstance(instance);
                    boolean discarding = false;
                    for (JsonNode task : instance.get("inFlightTasks")) {
                        discarding |= task.get("skipReadResultDueToCursorRewind").booleanValue();
                    }
                    assertTrue(discarding, instance.toString());
                });
            } finally {
                remoteBroker.getDockerClient().unpauseContainerCmd(remoteBroker.getContainerId()).exec();
            }
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode targets = get(source, topic, "internalStats").path("replicators");
                assertTrue(targets.isObject() && targets.isEmpty(), targets.toString());
            });
            // A target-side publish wakes the reverse direction even if its producer was closed while idle.
            try (Producer<String> reverse = consumerClient.newProducer(Schema.STRING).topic(topic).create()) {
                reverse.send("inbound only");
                await().atMost(SETTLE).untilAsserted(() -> {
                    JsonNode inbound = outbound(source, topic, targetName);
                    assertTrue(inbound.path("inboundConnection").isTextual(), inbound.toString());
                    TOPIC_FIELDS.forEach(field -> assertUnavailable(inbound.path(field)));
                    assertFalse(get(source, topic, "internalStats").path("replicators").has(targetName));
                });
            }
            source.topicPolicies().removeReplicationClusters(topic);
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode instance = internal(source, topic, targetName);
                assertInstance(instance);
                assertTrue(instance.get("lastPublishFailure").isNull());
                assertUnavailable(outbound(source, topic, targetName).path("lastReplicationSucceededAt"));
            });
        }
    }

    @Test(timeOut = 150_000)
    public void testUnavailableTargetAndPerInstanceQueueCapacity() throws Exception {
        String unavailable = "unconfigured-" + UUID.randomUUID();
        ClusterData remote = source.clusters().getCluster(targetName);
        // A binary URL is valid cluster metadata, but the broker needs an Admin URL to construct a replicator.
        source.clusters().createCluster(unavailable,
                ClusterData.builder().brokerServiceUrl(remote.getBrokerServiceUrl()).build());
        String tenant = "replication-" + UUID.randomUUID();
        Set<String> allowed = Set.of(sourceName, targetName, unavailable);
        source.tenants().createTenant(tenant, TenantInfo.builder().allowedClusters(allowed).build());
        String ns = tenant + "/stats";
        source.namespaces().createNamespace(ns, Set.of(sourceName, targetName));
        String original = createTopic(ns);
        String setting = "replicationProducerQueueSize";
        try (Consumer<String> consumer = subscribe(original);
             Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(original).create()) {
            waitForConnection(original);
            source.namespaces().setNamespaceReplicationClusters(ns, allowed, false);
            producer.send("healthy target");
            receive(consumer, "healthy target");
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode targets = get(source, original, "internalStats").path("replicators");
                assertTrue(targets.path(unavailable).isNull(), targets.toString());
                JsonNode unknown = outbound(source, original, unavailable);
                TOPIC_FIELDS.forEach(field -> assertUnavailable(unknown.path(field)));
                assertInstance(targets.path(targetName));
                assertHealthy(outbound(source, original, targetName));
            });
            source.namespaces().setNamespaceReplicationClusters(ns, Set.of(sourceName, targetName), false);
            source.brokers().updateDynamicConfiguration(setting, "4");
            try {
                await().atMost(SETTLE).untilAsserted(() ->
                        assertEquals(source.brokers().getRuntimeConfigurations().get(setting), "4"));
                String another = createTopic(ns);
                await().atMost(SETTLE).untilAsserted(() -> {
                    assertEquals(internal(source, another, targetName).path("producerQueueMaxSize").asInt(-1), 4);
                    JsonNode targets = get(source, original, "internalStats").path("replicators");
                    assertFalse(targets.has(unavailable));
                    assertEquals(targets.path(targetName).path("producerQueueMaxSize").asInt(-1), QUEUE_CAPACITY);
                });
            } finally {
                source.brokers().deleteDynamicConfiguration(setting);
                await().atMost(SETTLE).untilAsserted(() -> assertEquals(
                        source.brokers().getRuntimeConfigurations().get(setting), Integer.toString(QUEUE_CAPACITY)));
            }
        }
    }

    @DataProvider
    public Object[][] replicationStartPositions() {
        return new Object[][] {{"latest", 1}, {"earliest", 2}};
    }

    @Test(timeOut = 150_000, dataProvider = "replicationStartPositions")
    public void testDiagnoseBacklogWhenReplicationIsEnabled(String startAt, int backlog) throws Exception {
        String ns = newNamespace();
        source.namespaces().setRetention(ns, new RetentionPolicies(10, 10));
        String topic = "persistent://" + ns + "/history";
        String cursor = "pulsar.repl." + targetName;
        source.topics().createNonPartitionedTopic(topic);
        source.topicPolicies().setReplicationClusters(topic, List.of(sourceName)).get(30, TimeUnit.SECONDS);
        try (Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(topic)
                .enableBatching(false).create()) {
            await().atMost(SETTLE).untilAsserted(() -> {
                assertEquals(source.topicPolicies().getReplicationClusters(topic, true), Set.of(sourceName));
                assertFalse(get(source, topic, "internalStats").path("cursors").has(cursor));
            });
            producer.send("before replication");
            String setting = "replicationStartAt";
            source.brokers().updateDynamicConfiguration(setting, startAt);
            try {
                await().atMost(SETTLE).untilAsserted(() ->
                        assertEquals(source.brokers().getRuntimeConfigurations().get(setting), startAt));
                source.topicPolicies().removeReplicationClusters(topic);
                await().atMost(SETTLE).untilAsserted(() -> {
                    JsonNode info = ObjectMapperFactory.getMapper().reader().readTree(source.topics()
                            .getInternalInfo(topic));
                    assertTrue(info.path("cursors").path(cursor).path("creationDate").isTextual(), info.toString());
                });
                producer.send("after replication");
                // Keep the target absent so both the cursor creation time and initial backlog can be sampled.
                await().atMost(SETTLE).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertEquals(row.path("replicationBacklog").asInt(-1), backlog);
                    assertUnhealthy(row, "lastProducerStartFailure");
                    assertUnavailable(row.path("lastReplicationSucceededAt"));
                });
                target.topics().createNonPartitionedTopic(topic);
                try (Consumer<String> consumer = subscribe(topic)) {
                    if ("earliest".equals(startAt)) {
                        receive(consumer, "before replication");
                    }
                    receive(consumer, "after replication");
                    await().atMost(SETTLE).untilAsserted(() -> {
                        JsonNode row = outbound(source, topic, targetName);
                        assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                        timestamp(row, "lastReplicationSucceededAt");
                        assertHealthy(row);
                    });
                }
            } finally {
                source.brokers().deleteDynamicConfiguration(setting);
            }
        }
    }

    @Test(timeOut = 150_000)
    public void testDiagnoseMissingTargetThenTerminatedTarget() throws Exception {
        String ns = newNamespace();
        source.namespaces().setRetention(ns, new RetentionPolicies(10, 10));
        String topic = "persistent://" + ns + "/missing";
        source.topics().createNonPartitionedTopic(topic);
        try (Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(topic).create()) {
            // Wait for the replication cursor before sending: its initial position is latest.
            await().atMost(SETTLE).untilAsserted(() ->
                    assertTrue(get(source, topic, "internalStats").path("cursors").has("pulsar.repl." + targetName)));
            JsonNode info = ObjectMapperFactory.getMapper().reader().readTree(source.topics().getInternalInfo(topic));
            assertTrue(info.path("cursors").path("pulsar.repl." + targetName).path("creationDate").isTextual());
            producer.send("during startup failure");
            assertFalse(target.topics().getList(ns).contains(topic));
            String missingTopic = "PulsarServerException: Target topic does not exist. Create a non-partitioned topic"
                    + " on the target or enable createTopicToRemoteClusterForReplication on the source.";
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertTrue(row.path("replicationBacklog").asLong() > 0, row.toString());
                assertUnhealthy(row, "lastProducerStartFailure");
                assertUnhealthy(row, "Target topic does not exist");
                assertUnhealthy(row, "createTopicToRemoteClusterForReplication");
                assertUnavailable(row.path("producerState"));
                assertUnavailable(row.path("lastCursorReadStartedAt"));
                assertUnavailable(row.path("lastSendStartedAt"));
                assertUnavailable(row.path("lastReplicationSucceededAt"));
                JsonNode instance = internal(source, topic, targetName);
                assertInstance(instance);
                assertTrue(instance.get("producerQueueSize").isNull());
                assertTrue(instance.get("sendingMessageSchemaCompatible").isNull());
                assertTrue(instance.get("inFlightTasks").isEmpty());
                assertTrue(instance.get("lastReadFailure").isNull());
                assertTrue(instance.get("lastPublishFailure").isNull());
                JsonNode failure = instance.get("lastProducerStartFailure");
                assertEquals(failure.size(), 2, failure.toString());
                assertEquals(failure.path("message").textValue(), missingTopic);
                timestamp(failure, "timestamp");
            });
            target.topics().createNonPartitionedTopic(topic);
            try (Consumer<String> consumer = subscribe(topic)) {
                receive(consumer, "during startup failure");
                await().atMost(SETTLE).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertHealthy(row);
                    timestamp(row, "lastReplicationSucceededAt");
                });
                JsonNode startup = internal(source, topic, targetName).get("lastProducerStartFailure");
                assertEquals(startup.path("message").textValue(), missingTopic);
                JsonNode lastSuccess = outbound(source, topic, targetName).get("lastReplicationSucceededAt");
                target.topics().terminateTopic(topic);
                producer.send("after termination");
                await().atMost(SETTLE).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertTrue(row.path("replicationBacklog").asLong() > 0, row.toString());
                    assertEquals(row.get("lastReplicationSucceededAt"), lastSuccess);
                    JsonNode instance = internal(source, topic, targetName);
                    assertEquals(instance.get("lastProducerStartFailure"), startup);
                    JsonNode failure = instance.path("lastPublishFailure");
                    assertEquals(failure.path("message").asText(), "TopicTerminatedException");
                    timestamp(failure, "timestamp");
                });
                await().atMost(HEALTH_WINDOW.plus(SETTLE)).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertEquals(row.path("producerState").textValue(), "TERMINATED");
                    assertEquals(row.path("replicationBacklog").asLong(-1), 1L);
                    assertUnhealthy(row, "The target topic is terminated and cannot accept replicated messages.");
                    assertUnhealthy(row, "lastPublishFailure");
                    JsonNode instance = internal(source, topic, targetName);
                    assertTrue(instance.path("lastReadFailure").isNull());
                    assertEquals(instance.path("lastPublishFailure").path("message").textValue(),
                            "TopicTerminatedException");
                });
            }
        }
    }

    private static GenericSchema<GenericRecord> recordSchema(String field) {
        var record = SchemaBuilder.record("ReplicationRecord");
        record.field(field).type(SchemaType.INT64).required();
        return Schema.generic(record.build(SchemaType.AVRO));
    }

    @Test(timeOut = 390_000)
    public void testDiagnoseSchemaConflictAndCountBatchedEntries() throws Exception {
        String ns = newNamespace();
        String topic = createTopic(ns);
        var oldSchema = recordSchema("oldId");
        var newSchema = recordSchema("newId");
        target.topicPolicies().setSchemaCompatibilityStrategy(topic, SchemaCompatibilityStrategy.FULL);
        await().atMost(SETTLE).untilAsserted(() -> assertEquals(
                target.topicPolicies().getSchemaCompatibilityStrategy(topic, true), SchemaCompatibilityStrategy.FULL));
        target.schemas().createSchema(topic, oldSchema.getSchemaInfo());
        final int batchSize = 20;
        try (Consumer<byte[]> consumer = consumerClient.newConsumer().topic(topic).subscriptionName("verify")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
             Producer<GenericRecord> producer = producerClient.newProducer(newSchema).topic(topic)
                     .batchingMaxMessages(batchSize).batchingMaxPublishDelay(1, TimeUnit.MINUTES).create()) {
            waitForConnection(topic);
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                sends.add(producer.sendAsync(newSchema.newRecordBuilder().set("newId", (long) i).build()));
            }
            producer.flush();
            MessageIdAdv first = (MessageIdAdv) sends.get(0).join();
            for (int i = 0; i < batchSize; i++) {
                MessageIdAdv id = (MessageIdAdv) sends.get(i).join();
                assertEquals(id.getLedgerId(), first.getLedgerId());
                assertEquals(id.getEntryId(), first.getEntryId());
                assertEquals(id.getBatchIndex(), i);
            }
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.path("replicationBacklog").asLong(-1), 1L);
                assertEquals(row.path("producerState").asText(), "REGISTERING_SCHEMA");
                assertUnavailable(row.path("lastReplicationSucceededAt"));
                JsonNode instance = internal(source, topic, targetName);
                assertInstance(instance);
                assertEquals(instance.get("producerQueueSize").intValue(), 1);
                assertTrue(instance.get("sendingMessageSchemaCompatible").isBoolean());
                assertFalse(instance.get("sendingMessageSchemaCompatible").booleanValue());
                assertTrue(instance.get("lastPublishFailure").isNull());
            });
            // No acknowledgement or mark-delete advance has occurred since this new cursor acquired a backlog.
            assertUnavailable(outbound(source, topic, targetName).path("replicatorHealth"));
            await().atMost(HEALTH_WINDOW.plus(SETTLE)).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertUnhealthy(row, "The target rejected the schema of the next message to replicate.");
                assertUnhealthy(row, "Compare the source and target topic schemas");
            });
            String cursor = "pulsar.repl." + targetName;
            JsonNode mark = get(source, topic, "internalStats").path("cursors").path(cursor).get("markDeletePosition");
            try (Producer<GenericRecord> local = producerClient.newProducer(newSchema).topic(topic)
                    .enableBatching(false).create()) {
                local.newMessage().disableReplication().value(newSchema.newRecordBuilder().set("newId", -1L).build())
                        .send();
                await().atMost(SETTLE).untilAsserted(() -> {
                    JsonNode stats = get(source, topic, "internalStats").path("cursors").path(cursor);
                    assertTrue(stats.path("totalNonContiguousDeletedMessagesRange").asInt() > 0, stats.toString());
                    assertEquals(stats.get("markDeletePosition"), mark);
                    assertUnhealthy(outbound(source, topic, targetName), "target rejected the schema");
                });
            }
            for (int i = 1; i < QUEUE_CAPACITY; i++) {
                var entry = newSchema.newRecordBuilder().set("newId", (long) batchSize + i - 1).build();
                CompletableFuture<MessageId> sent = producer.sendAsync(entry);
                producer.flush();
                sent.get(30, TimeUnit.SECONDS);
            }
            await().atMost(HEALTH_WINDOW.plus(SETTLE)).untilAsserted(() -> {
                JsonNode instance = internal(source, topic, targetName);
                assertEquals(instance.path("producerQueueSize").asInt(-1), QUEUE_CAPACITY);
                assertEquals(instance.path("producerQueueMaxSize").asInt(-1), QUEUE_CAPACITY);
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.path("replicationBacklog").asLong(-1), (long) QUEUE_CAPACITY);
                assertUnhealthy(row, "target rejected the schema");
                assertTrue(row.path("replicationDelayInSeconds").asLong() > 0);
            });
            assertEquals(target.schemas().getSchemaInfo(topic).getSchema(), oldSchema.getSchemaInfo().getSchema());
            target.topicPolicies().setSchemaCompatibilityStrategy(topic, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            await().atMost(SETTLE).untilAsserted(() -> assertEquals(
                    target.topicPolicies().getSchemaCompatibilityStrategy(topic, true),
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE));
            for (int i = 0; i < batchSize + QUEUE_CAPACITY - 1; i++) {
                Message<byte[]> msg = consumer.receive(i == 0 ? 75 : 30, TimeUnit.SECONDS);
                assertNotNull(msg, "Expected message " + i + " after allowing the new schema");
                assertEquals(newSchema.decode(msg.getData()).getField("newId"), (long) i);
                consumer.acknowledge(msg);
            }
            assertEquals(target.schemas().getSchemaInfo(topic).getSchema(), newSchema.getSchemaInfo().getSchema());
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertHealthy(row);
                timestamp(row, "lastReplicationSucceededAt");
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                assertTrue(internal(source, topic, targetName).path("sendingMessageSchemaCompatible").isNull());
            });
        }

        // Age the backlog behind the transaction read limit while the target remains available.
        String pending = createTopic(ns);
        try (Consumer<byte[]> consumer = consumerClient.newConsumer().topic(pending).subscriptionName("verify")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
             Producer<GenericRecord> producer = producerClient.newProducer(newSchema).topic(pending)
                     .enableBatching(false).create()) {
            waitForConnection(pending);
            Transaction txn = producerClient.newTransaction().withTransactionTimeout(3, TimeUnit.MINUTES)
                    .build().get(30, TimeUnit.SECONDS);
            try (Closer paused = Closer.create()) {
                producer.newMessage(txn).value(newSchema.newRecordBuilder().set("newId", 42L).build()).send();
                await().atMost(HEALTH_WINDOW.plus(SETTLE)).untilAsserted(() ->
                        assertUnhealthy(outbound(source, pending, targetName), "transaction-buffer-stats"));
                // Hold the first schema write just long enough to observe registration before the target replies.
                pauseBookies(getGeoCluster().getClusters()[1], paused);
                txn.commit().get(10, TimeUnit.SECONDS);
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                    JsonNode row = outbound(source, pending, targetName);
                    assertEquals(row.path("producerState").textValue(), "REGISTERING_SCHEMA");
                    assertTrue(row.path("replicationBacklog").asLong() > 0, row.toString());
                    assertUnhealthy(row, "Schema registration on the target is pending.");
                    JsonNode instance = internal(source, pending, targetName);
                    assertTrue(instance.path("sendingMessageSchemaCompatible").isNull(), instance.toString());
                    assertEquals(instance.path("producerQueueSize").asInt(-1), 1);
                });
            } finally {
                if (txn.getState() == Transaction.State.OPEN) {
                    txn.abort().get(10, TimeUnit.SECONDS);
                }
            }
            Message<byte[]> delivered = consumer.receive(90, TimeUnit.SECONDS);
            assertNotNull(delivered);
            assertEquals(newSchema.decode(delivered.getData()).getField("newId"), 42L);
            consumer.acknowledge(delivered);
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, pending, targetName);
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                assertHealthy(row);
            });
        }
    }

    @DataProvider
    public Object[][] replicationQuotas() {
        return new Object[][] {{1, -1L}, {-1, 1L}};
    }

    @Test(timeOut = 180_000, dataProvider = "replicationQuotas")
    public void testDiagnoseDispatchLimit(int messages, long bytes) throws Exception {
        String ns = newNamespace();
        DispatchRate rate = DispatchRate.builder().dispatchThrottlingRateInMsg(messages)
                .dispatchThrottlingRateInByte(bytes).ratePeriodInSecond(300).build();
        source.namespaces().setReplicatorDispatchRate(ns, rate);
        String topic = createTopic(ns);
        try (Consumer<String> consumer = subscribe(topic);
             Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create()) {
            waitForConnection(topic);
            for (int i = 0; i < 12; i++) {
                producer.send("limited-" + i);
            }
            // No stats requests on this topic during the entire observation window.
            await().pollDelay(HEALTH_WINDOW.plusSeconds(1)).atMost(HEALTH_WINDOW.plus(SETTLE)).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertTrue(row.path("connected").asBoolean());
                assertTrue(row.path("replicationBacklog").asLong() > 0);
                DispatchRate applied = source.topicPolicies().getReplicatorDispatchRate(topic, true);
                assertEquals(applied.getDispatchThrottlingRateInMsg(), messages);
                assertEquals(applied.getDispatchThrottlingRateInByte(), bytes);
                assertEquals(applied.getRatePeriodInSecond(), 300);
                assertUnhealthy(row, "replicatorDispatchRate");
                JsonNode instance = internal(source, topic, targetName);
                assertEquals(instance.path("producerQueueSize").asInt(-1), 0);
                assertTrue(instance.path("lastPublishFailure").isNull());
            });
            source.namespaces().removeReplicatorDispatchRate(ns);
            for (int i = 0; i < 12; i++) {
                receive(consumer, "limited-" + i);
            }
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertHealthy(row);
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                timestamp(row, "lastReplicationSucceededAt");
            });
        }
    }

    @Test(timeOut = 210_000)
    public void testDiagnoseTransactionVisibility() throws Exception {
        String topic = createTopic(newNamespace());
        try (Consumer<String> consumer = subscribe(topic);
             Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create()) {
            waitForConnection(topic);
            MessageIdAdv committed = (MessageIdAdv) producer.send("visible");
            receive(consumer, "visible");
            Transaction txn = producerClient.newTransaction().withTransactionTimeout(3, TimeUnit.MINUTES)
                    .build().get(30, TimeUnit.SECONDS);
            try {
                MessageIdAdv hidden = (MessageIdAdv) producer.newMessage(txn).value("aborted").send();
                MessageIdAdv last = (MessageIdAdv) producer.send("after transaction");
                await().atMost(SETTLE).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertTrue(row.path("replicationBacklog").asLong() > 0);
                    assertUnavailable(row.path("lastCursorReadStartedAt"));
                    JsonNode details = get(source, topic, "internalStats");
                    JsonNode cursor = details.path("cursors").path("pulsar.repl." + targetName);
                    assertEquals(cursor.path("readPosition").asText(),
                            hidden.getLedgerId() + ":" + hidden.getEntryId());
                    assertEquals(source.transactions().getTransactionBufferStats(topic).maxReadPosition,
                            committed.getLedgerId() + ":" + committed.getEntryId());
                    assertEquals(details.path("lastConfirmedEntry").asText(),
                            last.getLedgerId() + ":" + last.getEntryId());
                    assertTrue(last.compareTo(hidden) >= 0);
                    JsonNode instance = details.path("replicators").path(targetName);
                    assertInstance(instance);
                    assertTrue(instance.get("lastReadFailure").isNull());
                    assertTrue(instance.get("lastPublishFailure").isNull());
                });
                await().atMost(HEALTH_WINDOW.plus(SETTLE)).untilAsserted(() ->
                        assertUnhealthy(outbound(source, topic, targetName), "transaction-buffer-stats"));
            } finally {
                txn.abort().get(30, TimeUnit.SECONDS);
            }
            receive(consumer, "after transaction");
            await().atMost(SETTLE).untilAsserted(() ->
                    assertEquals(outbound(source, topic, targetName).path("replicationBacklog").asLong(-1), 0L));
        }
    }

    private static void pauseBookies(PulsarCluster cluster, Closer paused) {
        for (BKContainer bookie : cluster.getBookies()) {
            bookie.getDockerClient().pauseContainerCmd(bookie.getContainerId()).exec();
            paused.register(() -> bookie.getDockerClient().unpauseContainerCmd(bookie.getContainerId()).exec());
        }
    }

    @Test(timeOut = 120_000)
    public void testDiagnoseChecksumRetry() throws Exception {
        String topic = createTopic(newNamespace());
        try (Consumer<String> consumer = subscribe(topic);
             Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create()) {
            waitForConnection(topic);
            producer.send("control");
            receive(consumer, "control");
            JsonNode baseline = internal(source, topic, targetName);
            assertTrue(baseline.path("lastProducerRetry").isNull(), baseline.toString());
            assertTrue(baseline.path("lastPublishFailure").isNull(), baseline.toString());
            long targetEntries = get(target, topic, "internalStats").required("entriesAddedCounter").longValue();
            await().atMost(SETTLE).untilAsserted(() ->
                    assertEquals(outbound(source, topic, targetName).path("replicationBacklog").asLong(-1), 0L));
            long lastSuccess = timestamp(outbound(source, topic, targetName), "lastReplicationSucceededAt");

            String wire = ReplicationChecksumFault.WIRE + UUID.randomUUID();
            JsonNode retry;
            try (Closer paused = Closer.create()) {
                // Hold the good retransmission at target storage so its callback is still pending at observation.
                pauseBookies(getGeoCluster().getClusters()[1], paused);
                producer.send(wire);
                await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertTrue(row.path("connected").asBoolean(), row.toString());
                    assertEquals(row.path("replicationBacklog").asLong(-1), 1L);
                    assertEquals(timestamp(row, "lastReplicationSucceededAt"), lastSuccess);
                    JsonNode instance = internal(source, topic, targetName);
                    JsonNode event = instance.path("lastProducerRetry");
                    assertEquals(event.path("reason").textValue(), "ChecksumError", instance.toString());
                    assertTrue(event.path("sourcePosition").isNull(), event.toString());
                    assertTrue(timestamp(event, "timestamp") >= lastSuccess);
                    assertEquals(instance.path("producerQueueSize").asInt(-1), 1);
                    assertTrue(instance.path("lastPublishFailure").isNull());
                    assertTrue(instance.path("lastReadFailure").isNull());
                    assertEquals(get(target, topic, "internalStats").path("pendingAddEntriesCount").asInt(-1), 1);
                });
                retry = internal(source, topic, targetName).required("lastProducerRetry");
            }
            receive(consumer, wire);
            JsonNode afterResend = internal(source, topic, targetName);
            assertEquals(afterResend.path("lastProducerRetry"), retry);
            assertTrue(afterResend.path("lastPublishFailure").isNull(), afterResend.toString());

            // A bad retained frame fails the send callback. Replication must reread the intact source entry.
            String retained = ReplicationChecksumFault.RETAINED + UUID.randomUUID();
            producer.send(retained);
            receive(consumer, retained);
            JsonNode failure = internal(source, topic, targetName).required("lastPublishFailure");
            assertEquals(failure.path("message").textValue(), "ChecksumException", failure.toString());
            assertTrue(timestamp(failure, "timestamp") >= retry.get("timestamp").longValue());

            MessageIdAdv barrier = (MessageIdAdv) producer.send("after checksums");
            receive(consumer, "after checksums");
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode details = get(source, topic, "internalStats");
                assertEquals(details.path("cursors").path("pulsar.repl." + targetName).path("markDeletePosition")
                        .textValue(), barrier.getLedgerId() + ":" + barrier.getEntryId());
                JsonNode instance = details.path("replicators").path(targetName);
                assertEquals(instance.path("lastProducerRetry"), retry);
                assertEquals(instance.path("lastPublishFailure"), failure);
                assertTrue(instance.path("lastReadFailure").isNull());
                assertEquals(instance.path("producerQueueSize").asInt(-1), 0);
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                assertTrue(timestamp(row, "lastReplicationSucceededAt") >= failure.get("timestamp").longValue());
                assertHealthy(row);
                // Two recovered messages and the barrier, each appended exactly once.
                assertEquals(get(target, topic, "internalStats").path("entriesAddedCounter").asLong(-1),
                        targetEntries + 3);
            });
        }
    }

    @Test(timeOut = 120_000)
    public void testDiagnoseSlowTargetAcknowledgementsAndSkippedSend() throws Exception {
        String topic = createTopic(newNamespace());
        try (Consumer<byte[]> consumer = consumerClient.newConsumer().topic(topic).subscriptionName("verify")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
             Producer<byte[]> producer = producerClient.newProducer().topic(topic).enableBatching(false).create()) {
            waitForConnection(topic);
            producer.send(new byte[] {0});
            Message<byte[]> control = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(control);
            consumer.acknowledge(control);
            await().atMost(SETTLE).untilAsserted(() ->
                    assertEquals(outbound(source, topic, targetName).path("replicationBacklog").asLong(-1), 0L));
            JsonNode beforePause = outbound(source, topic, targetName);
            try (Closer paused = Closer.create()) {
                pauseBookies(getGeoCluster().getClusters()[1], paused);
                for (int i = 0; i < QUEUE_CAPACITY; i++) {
                    producer.send(("pending-" + i).getBytes(StandardCharsets.UTF_8));
                }
                // The target broker stays reachable; its bookies cannot acknowledge these writes.
                await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertTrue(row.path("connected").asBoolean(), row.toString());
                    assertEquals(row.path("replicationBacklog").asLong(-1), (long) QUEUE_CAPACITY);
                    assertTrue(row.path("replicationDelayInSeconds").asLong() > 0);
                    long acknowledged = timestamp(beforePause, "lastReplicationSucceededAt");
                    assertEquals(timestamp(row, "lastReplicationSucceededAt"), acknowledged);
                    assertTrue(timestamp(row, "lastSendStartedAt") >= acknowledged);
                    JsonNode instance = internal(source, topic, targetName);
                    assertInstance(instance);
                    assertEquals(instance.get("producerQueueSize").asInt(-1), QUEUE_CAPACITY);
                    assertEquals(instance.get("producerQueueMaxSize").asInt(-1), QUEUE_CAPACITY);
                    assertTrue(instance.get("sendingMessageSchemaCompatible").isBoolean()
                            && instance.get("sendingMessageSchemaCompatible").booleanValue());
                    assertTrue(instance.get("lastPublishFailure").isNull());
                    assertTrue(instance.get("lastReadFailure").isNull());
                    assertTrue(instance.get("lastProducerRetry").isNull());
                });
            }
            for (int i = 0; i < QUEUE_CAPACITY; i++) {
                Message<byte[]> msg = consumer.receive(30, TimeUnit.SECONDS);
                assertNotNull(msg);
                assertEquals(new String(msg.getValue(), StandardCharsets.UTF_8), "pending-" + i);
                consumer.acknowledge(msg);
            }
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertHealthy(row);
                assertTrue(timestamp(row, "lastReplicationSucceededAt") > timestamp(beforePause,
                        "lastReplicationSucceededAt"));
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                JsonNode instance = internal(source, topic, targetName);
                assertEquals(instance.path("producerQueueSize").asInt(-1), 0);
                assertTrue(instance.path("sendingMessageSchemaCompatible").isNull());
            });

            // The source accepts this entry; the replication producer's negotiated target limit rejects it.
            JsonNode beforeSkip = outbound(source, topic, targetName);
            MessageIdAdv oversized = (MessageIdAdv) producer.send(new byte[TARGET_MAX_MESSAGE_SIZE + 1]);
            String position = oversized.getLedgerId() + ":" + oversized.getEntryId();
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode details = get(source, topic, "internalStats");
                assertEquals(details.path("cursors").path("pulsar.repl." + targetName)
                        .path("markDeletePosition").asText(), position);
                assertTrue(details.path("replicators").path(targetName).path("lastPublishFailure").isNull());
                JsonNode row = outbound(source, topic, targetName);
                assertTrue(timestamp(row, "lastReplicationSucceededAt") > timestamp(beforeSkip,
                        "lastReplicationSucceededAt"));
            });
            producer.send(new byte[] {1});
            Message<byte[]> afterSkip = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(afterSkip);
            assertEquals(afterSkip.getData(), new byte[] {1});
            consumer.acknowledge(afterSkip);
        }
    }

    @Test(timeOut = 230_000)
    public void testDiagnoseReadFailureWithoutMessage() throws Exception {
        String topic = createTopic(newNamespace());
        var broker = sourceCluster.getAnyBroker();
        try (Consumer<String> consumer = subscribe(topic);
             Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create()) {
            producer.send("before failure");
            receive(consumer, "before failure");
            await().atMost(SETTLE).untilAsserted(() -> {
                assertEquals(outbound(source, topic, targetName).path("replicationBacklog").asLong(-1), 0L);
                assertTrue(internal(source, topic, targetName).path("lastReadFailure").isNull());
            });
            JsonNode before = outbound(source, topic, targetName);
            long lastSuccess = timestamp(before, "lastReplicationSucceededAt");
            long lastSend = timestamp(before, "lastSendStartedAt");
            String cursor = "pulsar.repl." + targetName;
            JsonNode mark = get(source, topic, "internalStats").path("cursors").required(cursor)
                    .required("markDeletePosition");
            long failureTime;
            String expectedMessage;
            broker.execCmd("touch", NullMessageReadFailureProcessor.SWITCH);
            try {
                MessageIdAdv marked = (MessageIdAdv) producer.newMessage()
                        .property(NullMessageReadFailureProcessor.MARKER, "true")
                        .value("retry this entry").send();
                String markedPosition = marked.getLedgerId() + ":" + marked.getEntryId();
                expectedMessage = "readPos=" + markedPosition + " ManagedLedgerException (no exception message)";
                await().atMost(SETTLE).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertEquals(row.path("replicationBacklog").asLong(-1), 1L);
                    assertEquals(timestamp(row, "lastReplicationSucceededAt"), lastSuccess);
                    assertEquals(timestamp(row, "lastSendStartedAt"), lastSend);
                    JsonNode details = get(source, topic, "internalStats");
                    assertEquals(details.path("cursors").path(cursor).path("markDeletePosition"), mark);
                    JsonNode instance = details.path("replicators").path(targetName);
                    assertInstance(instance);
                    JsonNode failure = instance.path("lastReadFailure");
                    timestamp(failure, "timestamp");
                    assertEquals(failure.path("message").textValue(), expectedMessage, failure.toString());
                    assertEquals(failure.size(), 2, failure.toString());
                    assertEquals(instance.path("producerQueueSize").asInt(-1), 0);
                    assertTrue(instance.path("lastPublishFailure").isNull());
                    for (JsonNode task : instance.get("inFlightTasks")) {
                        // A retry can replace the failed batch before the stats request arrives.
                        if (markedPosition.equals(task.path("readPos").textValue())
                                && task.path("entries").asInt(-1) == 0) {
                            assertEquals(task.path("readException").textValue(), "ManagedLedgerException",
                                    task.toString());
                        } else if (task.path("entries").isNull()) {
                            assertTrue(task.path("readException").isNull(), task.toString());
                        }
                    }
                });
                await().atMost(HEALTH_WINDOW.plus(SETTLE)).untilAsserted(() -> {
                    JsonNode row = outbound(source, topic, targetName);
                    assertUnhealthy(row, "Retrying the source cursor read for the batch starting at " + markedPosition);
                    assertUnhealthy(row, "ManagedLedgerException (no exception message)");
                    long failedAt = timestamp(internal(source, topic, targetName).path("lastReadFailure"), "timestamp");
                    assertUnhealthy(row, "source broker log at " + failedAt);
                });
                failureTime = timestamp(internal(source, topic, targetName).path("lastReadFailure"), "timestamp");
            } finally {
                broker.execCmd("rm", "-f", NullMessageReadFailureProcessor.SWITCH);
            }
            // The failure backoff can reach one minute before the read is retried.
            await().atMost(HEALTH_WINDOW.plus(SETTLE)).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertHealthy(row);
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                assertTrue(timestamp(row, "lastReplicationSucceededAt") > lastSuccess);
                JsonNode failure = internal(source, topic, targetName).path("lastReadFailure");
                assertTrue(timestamp(failure, "timestamp") >= failureTime);
                assertEquals(failure.path("message").textValue(), expectedMessage, failure.toString());
                assertEquals(failure.size(), 2, failure.toString());
            });
            receive(consumer, "retry this entry");
        }
    }

    @Test(timeOut = 180_000)
    public void testDiagnoseSourceReadTimeoutAndRecovery() throws Exception {
        String ns = newNamespace();
        source.namespaces().setReplicatorDispatchRate(ns, DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1).dispatchThrottlingRateInByte(-1).ratePeriodInSecond(300).build());
        String topic = createTopic(ns);
        try (Consumer<String> consumer = subscribe(topic);
             Producer<String> producer = producerClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create()) {
            waitForConnection(topic);
            for (int i = 0; i < 5; i++) {
                producer.send("read-" + i);
            }
            // A newly activated quota can have zero or one permit. Wait until the unread tail is stable.
            await().during(Duration.ofSeconds(1)).atMost(SETTLE).untilAsserted(() -> {
                long backlog = outbound(source, topic, targetName).path("replicationBacklog").asLong(-1);
                assertTrue(backlog == 4 || backlog == 5, "backlog=" + backlog);
            });
            // The unread tail must now come from the source bookies because the ledger cache is disabled.
            JsonNode observedFailure;
            String expectedReadPos = source.topics().getInternalStats(topic).cursors
                    .get("pulsar.repl." + targetName).readPosition;
            try (Closer paused = Closer.create()) {
                pauseBookies(sourceCluster, paused);
                source.namespaces().removeReplicatorDispatchRate(ns);
                AtomicLong previousRead = new AtomicLong();
                await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                    JsonNode details = get(source, topic, "internalStats");
                    JsonNode instance = details.path("replicators").path(targetName);
                    JsonNode cursor = details.path("cursors").path("pulsar.repl." + targetName);
                    JsonNode row = outbound(source, topic, targetName);
                    long started = timestamp(row, "lastCursorReadStartedAt");
                    assertTrue(instance.path("inFlightTasks").isArray());
                    boolean pending = false;
                    for (JsonNode task : instance.get("inFlightTasks")) {
                        pending |= task.path("entries").isNull()
                                && task.path("readPos").asText().equals(cursor.path("readPosition").asText());
                    }
                    assertTrue(pending, details.toString());
                    assertEquals(instance.path("producerQueueSize").asInt(-1), 0);
                    assertEquals(started, previousRead.getAndSet(started), "waiting on the same cursor read");
                });
                await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
                    JsonNode instance = internal(source, topic, targetName);
                    JsonNode failure = instance.path("lastReadFailure");
                    timestamp(failure, "timestamp");
                    // This is the ManagedLedgerException text for the BookKeeper read timeout we induced.
                    assertEquals(failure.size(), 2, failure.toString());
                    assertEquals(failure.path("message").asText(), "readPos=" + expectedReadPos
                            + " ManagedLedgerException: Bookie operation timeout error code: -23");
                    assertTrue(instance.path("lastPublishFailure").isNull());
                });
                observedFailure = internal(source, topic, targetName).get("lastReadFailure");
            }
            for (int i = 0; i < 5; i++) {
                receive(consumer, "read-" + i);
            }
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                assertHealthy(row);
                timestamp(row, "lastSendStartedAt");
                timestamp(row, "lastReplicationSucceededAt");
                JsonNode failure = internal(source, topic, targetName).path("lastReadFailure");
                long latest = timestamp(failure, "timestamp");
                long observed = timestamp(observedFailure, "timestamp");
                assertTrue(latest >= observed, failure.toString());
                if (latest == observed) {
                    assertEquals(failure, observedFailure);
                }
            });

            // Unloading keeps the durable cursor but constructs a new replicator when the topic is loaded again.
            source.topics().unload(topic);
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                assertHealthy(row);
                assertUnavailable(row.path("lastSendStartedAt"));
                assertUnavailable(row.path("lastReplicationSucceededAt"));
                assertTrue(internal(source, topic, targetName).path("lastReadFailure").isNull());
            });
            producer.send("after reload");
            receive(consumer, "after reload");
            await().atMost(SETTLE).untilAsserted(() -> {
                JsonNode row = outbound(source, topic, targetName);
                assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
                timestamp(row, "lastSendStartedAt");
                timestamp(row, "lastReplicationSucceededAt");
                assertTrue(internal(source, topic, targetName).path("lastReadFailure").isNull());
            });
        }
    }
}
