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
package org.apache.pulsar.broker.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class SubscriptionCursorInternalStatsTest extends SharedPulsarBaseTest {
    private static final String SUBSCRIPTION = "subscription";
    private static final String DELETED_RANGES = "individuallyDeletedMessages";

    @Test
    public void testSingleTopicIncludeOptionAndEncoding() throws Exception {
        String topic = newTopicName();
        String subscription = "cursor stats+/literal%2F";
        String cursorName = Codec.encode(subscription);
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionProperties(Map.of("name", subscription))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();
                Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create()) {
            producer.send(new byte[] {1});
            MessageId secondMessageId = producer.send(new byte[] {2});
            assertThat(consumer.receive(10, TimeUnit.SECONDS)).isNotNull();
            Message<byte[]> second = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(second).isNotNull();
            assertThat(second.getMessageId()).isEqualTo(secondMessageId);
            consumer.acknowledge(second);
            Awaitility.await().untilAsserted(() -> assertThat(admin.topics().getInternalStats(topic)
                    .cursors.get(cursorName).totalNonContiguousDeletedMessagesRange).isEqualTo(1));
        }

        CursorStats topicCursor = admin.topics().getInternalStats(topic).cursors.get(cursorName);
        assertThat(topicCursor.individuallyDeletedMessages).isNotBlank();
        CursorStats included = admin.topics().getSubscriptionCursorInternalStatsAsync(topic, subscription, true)
                .get(10, TimeUnit.SECONDS);
        assertThat(included).usingRecursiveComparison().isEqualTo(topicCursor);
        CursorStats defaultStats = admin.topics().getSubscriptionCursorInternalStats(topic, subscription);
        CursorStats excluded = admin.topics().getSubscriptionCursorInternalStats(topic, subscription, false);
        assertThat(defaultStats).usingRecursiveComparison().isEqualTo(excluded);
        assertThat(excluded.individuallyDeletedMessages).isNull();
        assertThat(excluded).usingRecursiveComparison().ignoringFields(DELETED_RANGES).isEqualTo(topicCursor);
        assertThatThrownBy(() -> admin.topics().getSubscriptionCursorInternalStats(topic, cursorName))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);

        try (HttpClient client = HttpClient.newHttpClient()) {
            assertThat(getSubscriptionJson(client, topic, subscription, "properties").path("name").asText())
                    .isEqualTo(subscription);
            JsonNode withRanges = getSubscriptionJson(client, topic, subscription,
                    "internalStats?includeIndividualDeletedMessages=true");
            assertThat(withRanges.path(DELETED_RANGES).asText()).isEqualTo(topicCursor.individuallyDeletedMessages);
            JsonNode defaultJson = getSubscriptionJson(client, topic, subscription, "internalStats");
            JsonNode withoutRanges = getSubscriptionJson(client, topic, subscription,
                    "internalStats?includeIndividualDeletedMessages=false");
            assertThat(defaultJson).isEqualTo(withoutRanges);
            assertThat(withoutRanges.has(DELETED_RANGES)).isFalse();
            assertThat(withoutRanges.path("totalNonContiguousDeletedMessagesRange").asInt()).isEqualTo(1);

            TopicName name = TopicName.get(topic);
            URI matrixUri = URI.create(getWebServiceUrl() + "/admin/v2/persistent;v=1/" + name.getNamespace()
                    + "/" + name.getEncodedLocalName() + "/subscription/" + Codec.encode(cursorName)
                    + "/internalStats");
            var request = HttpRequest.newBuilder(matrixUri).timeout(Duration.ofSeconds(10)).GET().build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(ObjectMapperFactory.getMapper().reader().readTree(response.body())).isEqualTo(defaultJson);
        }
    }

    @Test
    public void testLegacyWhitespaceSubscriptionName() throws Exception {
        String topic = newTopicName();
        String subscription = " ";
        // The default Admin subscription creation policy allows names rejected by ConsumerBuilder.
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, subscription, MessageId.earliest);
        assertThat(admin.topics().getSubscriptions(topic)).containsExactly(subscription);

        CursorStats stats = admin.topics().getSubscriptionCursorInternalStats(topic, subscription, true);
        assertThat(stats).usingRecursiveComparison()
                .isEqualTo(admin.topics().getInternalStats(topic).cursors.get(Codec.encode(subscription)));
    }

    @Test
    public void testPendingReadAndNonDurableSubscriptionLifetime() throws Exception {
        String topic = newTopicName();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscribe()) {
            assertThat(consumer.isConnected()).isTrue();
            Awaitility.await().untilAsserted(() -> {
                CursorStats topicCursor = admin.topics().getInternalStats(topic).cursors.get(SUBSCRIPTION);
                assertThat(topicCursor).isNotNull();
                assertThat(topicCursor.subscriptionHavePendingRead).isTrue();
                CursorStats selected = admin.topics().getSubscriptionCursorInternalStats(topic, SUBSCRIPTION);
                assertThat(selected.subscriptionHavePendingRead).isTrue();
                assertThat(selected.subscriptionHavePendingReplayRead)
                        .isEqualTo(topicCursor.subscriptionHavePendingReplayRead);
            });
        }
        Awaitility.await().untilAsserted(() -> assertThatThrownBy(
                () -> admin.topics().getSubscriptionCursorInternalStats(topic, SUBSCRIPTION))
                .isInstanceOf(PulsarAdminException.NotFoundException.class));
    }

    @Test
    public void testNonPersistentRoutesAreAbsent() throws Exception {
        String topic = "non-persistent://" + getNamespace() + "/topic";
        try (HttpClient client = HttpClient.newHttpClient()) {
            for (String resource : Set.of("internalStats", "partitioned-internalStats")) {
                assertThat(getSubscriptionResponse(client, topic, SUBSCRIPTION, resource).statusCode()).isEqualTo(404);
            }
        }
    }

    @Test
    public void testTopicKindAndMissingResources() throws Exception {
        String nonPartitioned = newTopicName();
        String partitioned = newTopicName();
        String missing = newTopicName();
        admin.topics().createNonPartitionedTopic(nonPartitioned);
        admin.topics().createPartitionedTopic(partitioned, 2);
        String physical = TopicName.get(partitioned).getPartition(0).toString();
        assertThatThrownBy(() -> admin.topics().getSubscriptionCursorInternalStats(partitioned, SUBSCRIPTION))
                .isInstanceOf(PulsarAdminException.PreconditionFailedException.class);
        assertThatThrownBy(
                () -> admin.topics().getPartitionedSubscriptionCursorInternalStats(nonPartitioned, SUBSCRIPTION))
                .isInstanceOf(PulsarAdminException.PreconditionFailedException.class);
        assertThatThrownBy(() -> admin.topics().getPartitionedSubscriptionCursorInternalStats(physical, SUBSCRIPTION))
                .isInstanceOf(PulsarAdminException.PreconditionFailedException.class);
        assertThatThrownBy(() -> admin.topics().getSubscriptionCursorInternalStats(nonPartitioned, SUBSCRIPTION))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
        assertThatThrownBy(() -> admin.topics().getSubscriptionCursorInternalStats(missing, SUBSCRIPTION))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
        assertThatThrownBy(() -> admin.topics().getPartitionedSubscriptionCursorInternalStats(missing, SUBSCRIPTION))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
        assertThat(admin.topics().getSubscriptions(nonPartitioned)).isEmpty();
        assertThat(admin.topics().getList(getNamespace())).doesNotContain(missing);
        assertThat(admin.topics().getPartitionedTopicList(getNamespace())).doesNotContain(missing);

        admin.topics().createSubscription(nonPartitioned, SUBSCRIPTION, MessageId.earliest);
        admin.topics().unload(nonPartitioned);
        Awaitility.await().untilAsserted(() -> assertThat(getTopicReference(nonPartitioned)).isEmpty());
        assertThat(admin.topics().getSubscriptionCursorInternalStats(nonPartitioned, SUBSCRIPTION)
                .markDeletePosition).isNotNull();
    }

    @Test
    public void testMissingTopicDoesNotAcquireBundleOwnership() throws Exception {
        TopicName missing = TopicName.get(newTopicName());
        var namespaceService = getPulsar().getNamespaceService();
        var bundle = namespaceService.getBundleAsync(missing).get(10, TimeUnit.SECONDS);
        assertThat(namespaceService.checkOwnershipPresentAsync(bundle).get(10, TimeUnit.SECONDS)).isFalse();

        assertThatThrownBy(() -> admin.topics().getSubscriptionCursorInternalStats(missing.toString(), SUBSCRIPTION))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);

        assertThat(namespaceService.checkOwnershipPresentAsync(bundle).get(10, TimeUnit.SECONDS)).isFalse();
    }

    @Test
    public void testPartitionedResults() throws Exception {
        String topic = newTopicName();
        String subscription = "cursor stats+/literal%2F";
        String firstPartition = TopicName.get(topic).getPartition(0).toString();
        String secondPartition = TopicName.get(topic).getPartition(1).toString();
        admin.topics().createPartitionedTopic(topic, 2);
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = getSubscriptionResponse(
                    client, topic, subscription, "partitioned-internalStats");
            assertThat(response.statusCode()).isEqualTo(404);
            JsonNode error = ObjectMapperFactory.getMapper().reader().readTree(response.body());
            assertThat(error.path("reason").asText()).isNotBlank();
            assertThat(error.has("partitions")).isFalse();
            assertThat(error.has("errors")).isFalse();
            assertThatThrownBy(() -> admin.topics().getPartitionedSubscriptionCursorInternalStats(topic, subscription))
                    .isInstanceOf(PulsarAdminException.NotFoundException.class);

            admin.topics().createSubscription(firstPartition, subscription, MessageId.earliest);
            JsonNode partial = getSubscriptionJson(client, topic, subscription, "partitioned-internalStats");
            assertThat(partial.path("partitions").has(firstPartition)).isTrue();
        }
        assertThat(admin.topics().getSubscriptionCursorInternalStats(firstPartition, subscription)
                .markDeletePosition).isNotNull();
        PartitionedSubscriptionCursorInternalStats partial =
                admin.topics().getPartitionedSubscriptionCursorInternalStatsAsync(topic, subscription)
                        .get(10, TimeUnit.SECONDS);
        assertThat(partial.metadata.partitions).isEqualTo(2);
        assertThat(partial.complete).isFalse();
        assertThat(partial.partitions).containsOnlyKeys(firstPartition);
        assertThat(partial.partitions.get(firstPartition).individuallyDeletedMessages).isNull();
        assertThat(partial.errors).containsOnlyKeys(secondPartition);
        assertThat(partial.errors.get(secondPartition).statusCode).isEqualTo(404);
        assertThat(partial.errors.get(secondPartition).errorCode).isEqualTo("NOT_FOUND");
        assertThat(partial.errors.get(secondPartition).reason).isNotBlank();

        admin.topics().createSubscription(secondPartition, subscription, MessageId.earliest);
        PartitionedSubscriptionCursorInternalStats full =
                admin.topics().getPartitionedSubscriptionCursorInternalStats(topic, subscription, true);
        assertThat(full.metadata.partitions).isEqualTo(2);
        assertThat(full.complete).isTrue();
        assertThat(full.partitions).containsOnlyKeys(firstPartition, secondPartition);
        assertThat(full.errors).isEmpty();
        for (var partition : full.partitions.entrySet()) {
            CursorStats physical = admin.topics().getSubscriptionCursorInternalStats(
                    partition.getKey(), subscription, true);
            assertThat(partition.getValue().individuallyDeletedMessages).isNotNull()
                    .isEqualTo(physical.individuallyDeletedMessages);
        }
    }

    @Test
    public void testLocalFanOutRequestDoesNotRedirectToPeerCluster() throws Exception {
        TopicName partition = TopicName.get(newTopicName()).getPartition(0);
        var namespace = partition.getNamespaceObject();
        String localCluster = getConfig().getClusterName();
        String peerCluster = "cursor-stats-peer-" + partition.getLocalName();
        URI peerWebService = URI.create("http://127.0.0.1:1");
        var previousPeers = admin.clusters().getCluster(localCluster).getPeerClusterNames();
        var peers = previousPeers == null ? new LinkedHashSet<String>() : new LinkedHashSet<>(previousPeers);
        peers.add(peerCluster);
        var namespaceResources = getPulsar().getPulsarResources().getNamespaceResources();

        admin.clusters().createCluster(peerCluster, ClusterData.builder().serviceUrl(peerWebService.toString())
                .brokerServiceUrl("pulsar://127.0.0.1:1").build());
        try {
            admin.clusters().updatePeerClusterNames(localCluster, peers);
            try (HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()) {
                // Model a namespace that now routes to a peer after the coordinator began its request.
                namespaceResources.setPolicies(namespace, policies -> {
                    policies.replication_clusters = Set.of(peerCluster);
                    return policies;
                });
                URI endpoint = URI.create(getWebServiceUrl() + "/admin/v2/" + partition.getRestPath()
                        + "/subscription/" + SUBSCRIPTION + "/internalStats");
                HttpRequest direct = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(10)).GET().build();
                Awaitility.await().untilAsserted(() -> {
                    var redirect = client.send(direct, HttpResponse.BodyHandlers.discarding());
                    assertThat(redirect.statusCode()).isEqualTo(307);
                    assertThat(URI.create(redirect.headers().firstValue("Location").orElseThrow()).getAuthority())
                            .isEqualTo(peerWebService.getAuthority());
                });

                HttpRequest fanOut = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(10))
                        .header("X-Pulsar-Cursor-Stats-Local-Only", "true").GET().build();
                var response = client.send(fanOut, HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(503);
                assertThat(response.headers().firstValue("Location")).isEmpty();
                assertThat(ObjectMapperFactory.getMapper().reader().readTree(response.body()).path("reason").asText())
                        .isNotBlank();
            } finally {
                namespaceResources.setPolicies(namespace, policies -> {
                    policies.replication_clusters = Set.of(localCluster);
                    return policies;
                });
            }
        } finally {
            admin.clusters().updatePeerClusterNames(localCluster, previousPeers);
            admin.clusters().deleteCluster(peerCluster);
        }
    }

    private JsonNode getSubscriptionJson(HttpClient client, String topic, String subscription, String resource)
            throws Exception {
        HttpResponse<String> response = getSubscriptionResponse(client, topic, subscription, resource);
        assertThat(response.statusCode()).as("response body: %s", response.body()).isEqualTo(200);
        return ObjectMapperFactory.getMapper().reader().readTree(response.body());
    }

    private HttpResponse<String> getSubscriptionResponse(HttpClient client, String topic, String subscription,
                                                          String resource) throws Exception {
        // Subscription resources accept a form-encoded name inside one URI path segment.
        String encodedSubscription = Codec.encode(Codec.encode(subscription));
        URI uri = URI.create(getWebServiceUrl() + "/admin/v2/" + TopicName.get(topic).getRestPath()
                + "/subscription/" + encodedSubscription + "/" + resource);
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
