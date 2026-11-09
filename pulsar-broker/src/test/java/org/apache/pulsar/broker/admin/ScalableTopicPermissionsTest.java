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
import static org.awaitility.Awaitility.await;
import io.jsonwebtoken.Jwts;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.auth.AuthenticationFactory;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.apache.pulsar.common.policies.data.ScalableTopicStats.SegmentSubscriptionStats;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Topic-level permissions on scalable topics, with the default authorization provider. The V5 client produces and
 * consumes through per-segment producers and consumers attached to the {@code segment://} topics of a scalable
 * topic: they must be authorized by the permissions granted on the scalable topic's {@code topic://} name.
 */
@Test(groups = "broker-admin")
public class ScalableTopicPermissionsTest extends AuthZTest {

    private static final String NAMESPACE = "public/default";
    private static final String CLIENT_ROLE = "scalable-topic-client";
    private static final String CLIENT_TOKEN = Jwts.builder()
            .claim("sub", CLIENT_ROLE).signWith(SECRET_KEY).compact();
    private static final int NUM_MESSAGES = 20;

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    public void setup() {
        configureTokenAuthentication();
        configureDefaultAuthorization();
        start();
        this.superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        close();
    }

    /** A scalable topic with two segments, so the messages of a test spread over more than one segment. */
    private String newScalableTopic() throws Exception {
        String topic = "topic://" + NAMESPACE + "/" + UUID.randomUUID();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 2);
        return topic;
    }

    private PulsarClient newClient(String token) throws Exception {
        return PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(token))
                .build();
    }

    @FunctionalInterface
    private interface Receiver {
        Message<String> receive(Duration timeout) throws PulsarClientException;
    }

    private static Set<String> receiveValues(Receiver receiver, int count) throws Exception {
        Set<String> values = new HashSet<>();
        for (int i = 0; i < count; i++) {
            Message<String> message = receiver.receive(Duration.ofSeconds(10));
            assertThat(message).as("message %s of %s", i, count).isNotNull();
            values.add(message.value());
        }
        return values;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static void assertNotAuthorized(ThrowingRunnable call) {
        assertThatThrownBy(call::run)
                .isInstanceOf(PulsarClientException.class)
                .hasStackTraceContaining("is not authorized");
    }

    @Test
    public void testTopicPermissionsAuthorizeProduceAndConsume() throws Exception {
        String topic = newScalableTopic();
        superUserAdmin.scalableTopics().grantPermission(topic, CLIENT_ROLE,
                Set.of(AuthAction.produce, AuthAction.consume));

        @Cleanup
        PulsarClient client = newClient(CLIENT_TOKEN);
        @Cleanup
        StreamConsumer<String> streamConsumer = client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("stream-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        QueueConsumer<String> queueConsumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("queue-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();

        Set<String> sent = new HashSet<>();
        for (int i = 0; i < NUM_MESSAGES; i++) {
            String value = "msg-" + i;
            // Keyed, so the messages are routed to both segments.
            producer.newMessage().key("key-" + i).value(value).send();
            sent.add(value);
        }

        assertThat(receiveValues(streamConsumer::receive, NUM_MESSAGES)).isEqualTo(sent);
        assertThat(receiveValues(queueConsumer::receive, NUM_MESSAGES)).isEqualTo(sent);
    }

    @Test
    public void testNoTopicPermissionsCannotProduceOrConsume() throws Exception {
        String topic = newScalableTopic();
        // The permissions granted on another scalable topic don't apply to this one.
        superUserAdmin.scalableTopics().grantPermission(newScalableTopic(), CLIENT_ROLE,
                Set.of(AuthAction.produce, AuthAction.consume));

        @Cleanup
        PulsarClient client = newClient(CLIENT_TOKEN);
        assertNotAuthorized(() -> client.newProducer(Schema.string()).topic(topic).create());
        assertNotAuthorized(() -> client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("stream-sub")
                .subscribe());
        assertNotAuthorized(() -> client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("queue-sub")
                .subscribe());
    }

    /**
     * Either action is enough to watch the topic's layout, so these are refused by the segments themselves: they
     * only allow the actions granted on the parent topic.
     */
    @Test
    public void testSegmentsAllowOnlyTheActionsGrantedOnTheTopic() throws Exception {
        String consumeOnlyTopic = newScalableTopic();
        superUserAdmin.scalableTopics().grantPermission(consumeOnlyTopic, CLIENT_ROLE, Set.of(AuthAction.consume));
        String produceOnlyTopic = newScalableTopic();
        superUserAdmin.scalableTopics().grantPermission(produceOnlyTopic, CLIENT_ROLE, Set.of(AuthAction.produce));

        @Cleanup
        PulsarClient client = newClient(CLIENT_TOKEN);

        @Cleanup
        Producer<String> refusedProducer = client.newProducer(Schema.string())
                .topic(consumeOnlyTopic)
                .create();
        assertNotAuthorized(() -> refusedProducer.newMessage().value("refused").send());

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(produceOnlyTopic)
                .create();
        producer.newMessage().value("allowed").send();
        assertNotAuthorized(() -> client.newQueueConsumer(Schema.string())
                .topic(produceOnlyTopic)
                .subscriptionName("queue-sub")
                .subscribe());
    }

    /** The segments that a split creates are authorized with the permissions of the topic, like the first ones. */
    @Test
    public void testSegmentsCreatedBySplitAreAuthorizedWithTheTopicPermissions() throws Exception {
        String topic = "topic://" + NAMESPACE + "/" + UUID.randomUUID();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        superUserAdmin.scalableTopics().grantPermission(topic, CLIENT_ROLE,
                Set.of(AuthAction.produce, AuthAction.consume));

        @Cleanup
        PulsarClient client = newClient(CLIENT_TOKEN);
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("queue-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();

        Set<String> sent = new HashSet<>();
        for (int i = 0; i < NUM_MESSAGES; i++) {
            producer.newMessage().key("key-" + i).value("before-" + i).send();
            sent.add("before-" + i);
        }
        superUserAdmin.scalableTopics().splitSegment(topic, 0);
        await().untilAsserted(() -> assertThat(superUserAdmin.scalableTopics().getMetadata(topic).getSegments()
                .values().stream().filter(ScalableTopicMetadata.SegmentInfo::isActive).count()).isEqualTo(2));
        // Routed to the two segments the split created.
        for (int i = 0; i < NUM_MESSAGES; i++) {
            producer.newMessage().key("key-" + i).value("after-" + i).send();
            sent.add("after-" + i);
        }

        assertThat(receiveValues(consumer::receive, sent.size())).isEqualTo(sent);
    }

    @Test
    public void testGrantGetAndRevokePermissions() throws Exception {
        String topic = newScalableTopic();
        assertThat(superUserAdmin.scalableTopics().getPermissions(topic)).doesNotContainKey(CLIENT_ROLE);

        superUserAdmin.scalableTopics().grantPermission(topic, CLIENT_ROLE,
                Set.of(AuthAction.produce, AuthAction.consume));
        assertThat(superUserAdmin.scalableTopics().getPermissions(topic))
                .containsEntry(CLIENT_ROLE, Set.of(AuthAction.produce, AuthAction.consume));

        superUserAdmin.scalableTopics().revokePermissions(topic, CLIENT_ROLE);
        assertThat(superUserAdmin.scalableTopics().getPermissions(topic)).doesNotContainKey(CLIENT_ROLE);
        @Cleanup
        PulsarClient client = newClient(CLIENT_TOKEN);
        assertNotAuthorized(() -> client.newProducer(Schema.string()).topic(topic).create());
    }

    /**
     * A role reaches the permission endpoints unchanged, as a single path component: a '/' doesn't split it, a '%'
     * doesn't escape anything, and a space and a '+' are different roles.
     */
    @Test
    public void testGrantGetAndRevokeRolesWithReservedCharacters() throws Exception {
        String topic = newScalableTopic();
        Map<String, Set<AuthAction>> grants = Map.of(
                "group/app", Set.of(AuthAction.consume),
                "100%app", Set.of(AuthAction.produce),
                "team app", Set.of(AuthAction.consume),
                "team+app", Set.of(AuthAction.produce, AuthAction.consume));
        for (Map.Entry<String, Set<AuthAction>> grant : grants.entrySet()) {
            superUserAdmin.scalableTopics().grantPermission(topic, grant.getKey(), grant.getValue());
        }
        assertThat(superUserAdmin.scalableTopics().getPermissions(topic)).containsAllEntriesOf(grants);

        superUserAdmin.scalableTopics().revokePermissions(topic, "team app");
        assertThat(superUserAdmin.scalableTopics().getPermissions(topic))
                .doesNotContainKey("team app")
                .containsEntry("team+app", Set.of(AuthAction.produce, AuthAction.consume));

        for (String role : List.of("group/app", "100%app", "team+app")) {
            superUserAdmin.scalableTopics().revokePermissions(topic, role);
        }
        assertThat(superUserAdmin.scalableTopics().getPermissions(topic)).doesNotContainKeys("group/app", "100%app",
                "team app", "team+app");
    }

    /** Revoking the permission granted on the topic disconnects the producers already attached to its segments. */
    @Test
    public void testRevokeDisconnectsSegmentProducers() throws Exception {
        String topic = newScalableTopic();
        superUserAdmin.scalableTopics().grantPermission(topic, CLIENT_ROLE, Set.of(AuthAction.produce));

        @Cleanup
        PulsarClient client = newClient(CLIENT_TOKEN);
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < NUM_MESSAGES; i++) {
            producer.newMessage().key("key-" + i).value("msg-" + i).send();
        }
        assertThat(superUserAdmin.scalableTopics().getStats(topic).getProducers()).isNotEmpty();

        superUserAdmin.scalableTopics().revokePermissions(topic, CLIENT_ROLE);

        await().untilAsserted(() ->
                assertThat(superUserAdmin.scalableTopics().getStats(topic).getProducers()).isEmpty());
    }

    /** Revoking the permission granted on the topic disconnects the consumers already attached to its segments. */
    @Test
    public void testRevokeDisconnectsSegmentConsumers() throws Exception {
        String topic = newScalableTopic();
        superUserAdmin.scalableTopics().createSubscription(topic, "queue-sub", ScalableSubscriptionType.QUEUE);
        superUserAdmin.scalableTopics().grantPermission(topic, CLIENT_ROLE, Set.of(AuthAction.consume));

        @Cleanup
        PulsarClient client = newClient(CLIENT_TOKEN);
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("queue-sub")
                .subscribe();
        // One per-segment consumer on each of the two segments.
        await().untilAsserted(() -> assertThat(segmentConsumerCount(topic, "queue-sub")).isEqualTo(2));

        superUserAdmin.scalableTopics().revokePermissions(topic, CLIENT_ROLE);

        await().untilAsserted(() -> assertThat(segmentConsumerCount(topic, "queue-sub")).isZero());
    }

    private int segmentConsumerCount(String topic, String subscription) throws Exception {
        return superUserAdmin.scalableTopics().getStats(topic).getSubscriptions().get(subscription).getSegments()
                .values().stream().mapToInt(SegmentSubscriptionStats::getConsumerCount).sum();
    }

    @Test
    public void testPermissionsOfMissingTopic() {
        String topic = "topic://" + NAMESPACE + "/" + UUID.randomUUID();
        assertThatThrownBy(() -> superUserAdmin.scalableTopics().getPermissions(topic))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
        assertThatThrownBy(() -> superUserAdmin.scalableTopics().grantPermission(topic, CLIENT_ROLE,
                Set.of(AuthAction.produce)))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
        assertThatThrownBy(() -> superUserAdmin.scalableTopics().revokePermissions(topic, CLIENT_ROLE))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
    }

    /** As for a regular topic, a topic created again with the name of a deleted one doesn't inherit its grants. */
    @Test
    public void testDeleteRemovesTopicPermissions() throws Exception {
        String topic = newScalableTopic();
        superUserAdmin.scalableTopics().grantPermission(topic, CLIENT_ROLE, Set.of(AuthAction.produce));

        superUserAdmin.scalableTopics().deleteScalableTopic(topic, true);
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);

        assertThat(superUserAdmin.scalableTopics().getPermissions(topic)).doesNotContainKey(CLIENT_ROLE);
    }
}
