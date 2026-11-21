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
package org.apache.pulsar.tests.integration.auth.token;

import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.internal;
import static org.apache.pulsar.tests.integration.messaging.GeoReplicationAdminStats.outbound;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Closer;
import io.restassured.RestAssured;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.integration.containers.BKContainer;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.ProxyContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.StandaloneContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class TokenAuthWithSymmetricKeys extends PulsarTokenAuthenticationBaseSuite {

    private String secretKey;

    @Test
    public void testReplicationStatsUseTopicStatsPermission() throws Exception {
        String tenant = "stats-" + randomName(8);
        String ns = tenant + "/namespace";
        String topic = "persistent://" + ns + "/messages";
        String url = pulsarCluster.getHttpServiceUrl();
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(url)
                .authentication(AuthenticationFactory.token(superUserAuthToken)).build()) {
            admin.tenants().createTenant(tenant, new TenantInfoImpl(Collections.singleton(SUPER_USER_ROLE),
                    Collections.singleton(pulsarCluster.getClusterName())));
            admin.namespaces().createNamespace(ns, Collections.singleton(pulsarCluster.getClusterName()));
            admin.topics().createNonPartitionedTopic(topic);
            String path = "/admin/v2/" + TopicName.get(topic).getRestPath() + "/";
            for (String endpoint : List.of("stats", "internalStats")) {
                RestAssured.given().baseUri(url).auth().oauth2(clientAuthToken).get(path + endpoint)
                        .then().statusCode(401);
            }
            admin.namespaces().grantPermissionOnNamespace(ns, REGULAR_USER_ROLE, EnumSet.of(AuthAction.consume));
            await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                    RestAssured.given().baseUri(url).auth().oauth2(clientAuthToken).get(path + "stats")
                            .then().statusCode(200));
            var internal = RestAssured.given().baseUri(url).auth().oauth2(clientAuthToken)
                    .get(path + "internalStats").then().statusCode(200).extract().jsonPath();
            admin.namespaces().revokePermissionsOnNamespace(ns, REGULAR_USER_ROLE);
            await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
                for (String endpoint : List.of("stats", "internalStats")) {
                    RestAssured.given().baseUri(url).auth().oauth2(clientAuthToken).get(path + endpoint)
                            .then().statusCode(401);
                }
            });
            assertEquals(internal.getMap("replicators"), Collections.emptyMap());
        }
    }

    private enum TargetFault {
        AUTHORIZATION_DENIED,
        DEDUPLICATION_UNDECIDED
    }

    @DataProvider
    public Object[][] targetReplicationFaults() {
        return new Object[][] {{TargetFault.AUTHORIZATION_DENIED}, {TargetFault.DEDUPLICATION_UNDECIDED}};
    }

    @Test(dataProvider = "targetReplicationFaults", timeOut = 480_000)
    public void testDiagnoseTargetReplicationFailure(TargetFault fault) throws Exception {
        String tenant = "replication-" + randomName(8);
        String ns = tenant + "/namespace";
        String topic = "persistent://" + ns + "/messages";
        String peer = pulsarCluster.getClusterName();
        boolean denyAccess = fault == TargetFault.AUTHORIZATION_DENIED;
        String targetBroker = pulsarCluster.getAnyBroker().getHostName();
        String binaryHost = denyAccess ? targetBroker : pulsarCluster.getProxy().getHostname();
        try (StandaloneContainer broker = new StandaloneContainer(tenant, PulsarContainer.UPGRADE_TEST_IMAGE_NAME)
                     .withNetwork(pulsarCluster.getNetwork())
                     .withEnv("PULSAR_LOG_APPENDER", "Console");
             PulsarAdmin target = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                     .authentication(AuthenticationFactory.token(superUserAuthToken)).build()) {
            broker.start();
            try (PulsarAdmin source = PulsarAdmin.builder().serviceHttpUrl(broker.getHttpServiceUrl()).build()) {
                String local = source.brokers().getRuntimeConfigurations().get("clusterName");
                source.clusters().createCluster(peer, ClusterData.builder()
                        .serviceUrl("http://" + targetBroker + ":8080")
                        .brokerServiceUrl("pulsar://" + binaryHost + ":6650")
                        .authenticationPlugin(AuthenticationToken.class.getName())
                        .authenticationParameters("token:" + clientAuthToken).build());
                source.tenants().createTenant(tenant, new TenantInfoImpl(Set.of(), Set.of(local, peer)));
                target.tenants().createTenant(tenant, new TenantInfoImpl(Set.of(SUPER_USER_ROLE), Set.of(peer)));
                source.namespaces().createNamespace(ns, Set.of(local));
                target.namespaces().createNamespace(ns, Set.of(peer));
                if (!denyAccess) {
                    target.namespaces().grantPermissionOnNamespace(ns, REGULAR_USER_ROLE,
                            EnumSet.of(AuthAction.consume, AuthAction.produce));
                    target.namespaces().setDeduplicationStatus(ns, true);
                }
                target.topics().createNonPartitionedTopic(topic);
                target.topics().createSubscription(topic, "verify", MessageId.earliest);
                source.topics().createNonPartitionedTopic(topic);
                source.namespaces().setNamespaceReplicationClusters(ns, Set.of(local, peer), false);
                await().atMost(60, TimeUnit.SECONDS).untilAsserted(() ->
                        assertTrue(source.topics().getInternalStats(topic).cursors.containsKey("pulsar.repl." + peer)));
                if (denyAccess) {
                    assertReplicationIdentityDenied(broker, source, target, ns, topic, peer);
                } else {
                    assertUncertainDeduplication(broker, source, target, topic, local, peer);
                }
            }
        }
    }

    private void assertReplicationIdentityDenied(StandaloneContainer broker, PulsarAdmin source, PulsarAdmin target,
                                                 String ns, String topic, String peer) throws Exception {
        broker.execCmd("bin/pulsar-client", "produce", topic, "-m", "held by authorization");
        long deniedLookup = awaitProducerStartFailure(source, topic, peer, "NotAuthorizedException",
                "Topic preparation was denied. Verify the replication credentials and topic metadata permissions.");
        try (PulsarAdmin identity = PulsarAdmin.builder().serviceHttpUrl(target.getServiceUrl())
                .authentication(AuthenticationFactory.token(clientAuthToken)).build()) {
            expectThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> identity.topics().getPartitionedTopicMetadata(topic));
            target.namespaces().grantPermissionOnNamespace(ns, REGULAR_USER_ROLE, EnumSet.of(AuthAction.consume));
            await().atMost(60, TimeUnit.SECONDS).ignoreException(PulsarAdminException.NotAuthorizedException.class)
                    .untilAsserted(() -> assertEquals(identity.topics().getPartitionedTopicMetadata(topic).partitions,
                            0));
        }
        String deniedPublish = "Producer creation was denied. Check the replication identity's publish permission "
                + "on the target.";
        long failedAt = awaitProducerStartFailure(source, topic, peer, "AuthorizationException", deniedPublish);
        assertTrue(failedAt > deniedLookup);
        target.namespaces().grantPermissionOnNamespace(ns, REGULAR_USER_ROLE,
                EnumSet.of(AuthAction.consume, AuthAction.produce));
        await().atMost(90, TimeUnit.SECONDS).untilAsserted(() -> {
            JsonNode row = outbound(source, topic, peer);
            assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
            assertEquals(row.path("producerState").asText(), "READY");
            assertTrue(row.path("connected").asBoolean());
            assertTrue(row.path("replicatorHealth").path("healthy").asBoolean());
            assertTrue(row.path("replicatorHealth").path("message").isNull());
            assertTrue(row.path("lastReplicationSucceededAt").asLong() >= failedAt);
            JsonNode failure = internal(source, topic, peer).path("lastProducerStartFailure");
            assertEquals(failure.path("message").textValue(), "AuthorizationException: " + deniedPublish);
            assertTrue(failure.path("timestamp").asLong() >= failedAt);
            assertEquals(target.topics().peekMessages(topic, "verify", 1).get(0).getData(),
                    "held by authorization".getBytes(StandardCharsets.UTF_8));
        });
    }

    private static long awaitProducerStartFailure(PulsarAdmin source, String topic, String peer,
                                                  String exception, String action) throws Exception {
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            JsonNode row = outbound(source, topic, peer);
            assertEquals(row.path("replicationBacklog").asLong(-1), 1L);
            assertTrue(row.path("connected").isBoolean(), row.toString());
            assertFalse(row.get("connected").booleanValue(), row.toString());
            JsonNode health = row.path("replicatorHealth");
            assertTrue(health.path("healthy").isBoolean(), health.toString());
            assertFalse(health.get("healthy").booleanValue(), health.toString());
            String diagnostic = health.path("message").asText();
            assertTrue(diagnostic.contains("lastProducerStartFailure"), diagnostic);
            assertTrue(diagnostic.contains(action), diagnostic);
            for (String field : List.of("producerState", "lastSendStartedAt", "lastReplicationSucceededAt")) {
                assertFalse(row.hasNonNull(field), row.toString());
            }
            JsonNode instance = internal(source, topic, peer);
            JsonNode failure = instance.path("lastProducerStartFailure");
            assertEquals(failure.size(), 2, failure.toString());
            assertEquals(failure.path("message").textValue(), exception + ": " + action);
            assertTrue(failure.path("timestamp").isIntegralNumber() && failure.get("timestamp").asLong() > 0);
            assertTrue(instance.path("producerQueueSize").isNull());
            assertTrue(instance.path("lastReadFailure").isNull());
            assertTrue(instance.path("lastPublishFailure").isNull());
        });
        return internal(source, topic, peer).required("lastProducerStartFailure").required("timestamp").longValue();
    }

    private void assertUncertainDeduplication(StandaloneContainer broker, PulsarAdmin source, PulsarAdmin target,
                                               String topic, String local, String peer) throws Exception {
        String cursor = "pulsar.repl." + peer;
        // The upgrade run uses a 4.0.13 target and the same current source and fault as the current/current run.
        boolean legacyTarget = "4.0.13".equals(target.brokers().getVersion());
        String expectedReason = legacyTarget ? "PersistenceError" : "DEDUPLICATION_UNDECIDED";
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(target.topics().getStats(topic).getDeduplicationStatus(), "Enabled"));
        broker.execCmd("bin/pulsar-client", "produce", topic, "-m", "control");
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            JsonNode row = outbound(source, topic, peer);
            assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
            assertTrue(row.path("lastReplicationSucceededAt").asLong() > 0);
            assertEquals(target.topics().peekMessages(topic, "verify", 1).get(0).getData(),
                    "control".getBytes(StandardCharsets.UTF_8));
        });
        long lastSuccess = outbound(source, topic, peer).get("lastReplicationSucceededAt").longValue();
        assertTrue(internal(source, topic, peer).required("lastProducerRetry").isNull());
        JsonNode previousStartFailure = internal(source, topic, peer).required("lastProducerStartFailure");
        String mark = source.topics().getInternalStats(topic).cursors.get(cursor).markDeletePosition;
        String confirmed = target.topics().getInternalStats(topic).lastConfirmedEntry;
        String retriedPosition;
        try (Closer paused = Closer.create()) {
            Closer pausedBookies = paused.register(Closer.create());
            for (BKContainer bookie : pulsarCluster.getBookies()) {
                bookie.getDockerClient().pauseContainerCmd(bookie.getContainerId()).exec();
                pausedBookies.register(() -> bookie.getDockerClient().unpauseContainerCmd(bookie.getContainerId())
                        .exec());
            }
            broker.execCmd("bin/pulsar-client", "produce", topic, "-m", "retry once persisted");
            retriedPosition = source.topics().getInternalStats(topic).lastConfirmedEntry;
            assertNotEquals(retriedPosition, mark);
            await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(target.topics().getInternalStats(topic).pendingAddEntriesCount, 1));

            // Supervisord restarts the proxy. The reconnecting producer resends the same source position
            // while the target broker still has the original ledger write outstanding.
            pulsarCluster.getProxy().execCmd("supervisorctl", "signal", "KILL", "proxy");
            String diagnostic = "[" + topic + "][pulsar.repl." + local + "-->" + peer
                    + "]Cannot determine whether the message is a duplicate at this time";
            await().pollInterval(1, TimeUnit.SECONDS).atMost(45, TimeUnit.SECONDS).untilAsserted(() ->
                    assertTrue(broker.getLogs().contains(diagnostic), "Expected the target deduplication error"));
            await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
                var ledger = target.topics().getInternalStats(topic);
                assertEquals(ledger.pendingAddEntriesCount, 1);
                assertEquals(ledger.lastConfirmedEntry, confirmed);
                assertEquals(source.topics().getInternalStats(topic).cursors.get(cursor).markDeletePosition, mark);
                JsonNode row = outbound(source, topic, peer);
                assertEquals(row.path("replicationBacklog").asLong(-1), 1L);
                assertEquals(row.path("lastReplicationSucceededAt").asLong(-1), lastSuccess);
                assertTrue(row.path("lastSendStartedAt").asLong() > lastSuccess);
                assertTrue(row.path("replicationDelayInSeconds").asLong() > 0);
                JsonNode instance = internal(source, topic, peer);
                assertEquals(instance.path("producerQueueSize").asInt(-1), 1);
                assertEquals(instance.path("lastProducerStartFailure"), previousStartFailure);
                JsonNode retry = instance.required("lastProducerRetry");
                assertEquals(retry.path("reason").asText(), expectedReason);
                assertEquals(retry.required("sourcePosition").textValue(), legacyTarget ? null : retriedPosition);
                assertTrue(retry.path("timestamp").asLong() >= row.path("lastSendStartedAt").asLong());
                assertTrue(instance.path("lastReadFailure").isNull());
                assertTrue(instance.path("lastPublishFailure").isNull());
            });
            // Hold the ACK for the health window while allowing target storage to recover.
            var proxy = pulsarCluster.getProxy();
            proxy.getDockerClient().pauseContainerCmd(proxy.getContainerId()).exec();
            paused.register(() -> proxy.getDockerClient().unpauseContainerCmd(proxy.getContainerId()).exec());
            pausedBookies.close();
            await().atMost(85, TimeUnit.SECONDS).untilAsserted(() -> {
                JsonNode health = outbound(source, topic, peer).path("replicatorHealth");
                assertEquals(health.path("healthy").asText(), "false");
                String message = health.path("message").asText();
                if (legacyTarget) {
                    assertTrue(message.contains("target acknowledgements"), message);
                    JsonNode retry = internal(source, topic, peer).required("lastProducerRetry");
                    assertTrue(message.contains("latest producer retry was PersistenceError at "
                            + retry.required("timestamp").longValue()), message);
                    assertTrue(message.contains("lastProducerRetry"), message);
                    assertFalse(message.contains(retriedPosition), message);
                } else {
                    assertTrue(message.contains("lastProducerRetry") && message.contains(retriedPosition), message);
                }
            });
        }
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            JsonNode row = outbound(source, topic, peer);
            assertEquals(row.path("replicationBacklog").asLong(-1), 0L);
            assertTrue(row.path("lastReplicationSucceededAt").asLong() > lastSuccess);
            assertEquals(source.topics().getInternalStats(topic).cursors.get(cursor).markDeletePosition,
                    retriedPosition);
            JsonNode instance = internal(source, topic, peer);
            assertEquals(instance.path("producerQueueSize").asInt(-1), 0);
            assertTrue(instance.path("lastPublishFailure").isNull());
            JsonNode retry = instance.required("lastProducerRetry");
            assertEquals(retry.required("sourcePosition").textValue(), legacyTarget ? null : retriedPosition);
            assertEquals(retry.path("reason").asText(), expectedReason);
            assertTrue(row.path("replicatorHealth").path("healthy").asBoolean());
        });
        broker.execCmd("bin/pulsar-client", "produce", topic, "-m", "after recovery");
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            var messages = target.topics().peekMessages(topic, "verify", 4);
            assertEquals(messages.stream().map(m -> new String(m.getData(), StandardCharsets.UTF_8)).toList(),
                    List.of("control", "retry once persisted", "after recovery"));
        });
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void createKeysAndTokens(PulsarContainer container) throws Exception {
        secretKey = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create-secret-key", "--base64")
                .getStdout();
        log.info().attr("key", secretKey).log("Created secret key");

        clientAuthToken = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:;base64," + secretKey,
                        "--subject", REGULAR_USER_ROLE)
                .getStdout().trim();
        log.info().attr("token", clientAuthToken).log("Created client token");

        superUserAuthToken = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:;base64," + secretKey,
                        "--subject", SUPER_USER_ROLE)
                .getStdout().trim();
        log.info().attr("token", superUserAuthToken).log("Created super-user token");

        proxyAuthToken = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:;base64," + secretKey,
                        "--subject", PROXY_ROLE)
                .getStdout().trim();
        log.info().attr("token", proxyAuthToken).log("Created proxy token");
    }

    @Override
    protected void configureBroker(BrokerContainer brokerContainer) throws Exception {
        brokerContainer.withEnv("tokenSecretKey", "data:;base64," + secretKey);
        // The replication connection through the proxy forwards the original token identity.
        brokerContainer.withEnv("proxyRoles", PROXY_ROLE);
        // Keep the first write outstanding while the replication connection is re-established.
        brokerContainer.withEnv("bookkeeperClientTimeoutInSeconds", "120");
    }

    @Override
    protected void configureProxy(ProxyContainer proxyContainer) throws Exception {
        proxyContainer.withEnv("tokenSecretKey", "data:;base64," + secretKey);
    }

    @Override
    protected String createClientTokenWithExpiry(long expiryTime, TimeUnit unit) throws Exception {
        return cmdContainer
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:;base64," + secretKey,
                        "--subject", REGULAR_USER_ROLE,
                        "--expiry-time", unit.toSeconds(expiryTime) + "s")
                .getStdout().trim();
    }
}
