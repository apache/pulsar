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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import io.jsonwebtoken.Jwts;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.auth.AuthenticationFactory;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Topic-level permissions across the PIP-475 migration of a regular topic to a scalable topic, with the default
 * authorization provider. After the migration, the V5 clients produce to and consume from new {@code segment://}
 * segments, which are authorized with the permissions of the scalable topic's {@code topic://} name: the migration
 * must grant there the permissions of the regular topic, for the roles allowed on it to keep their access.
 */
@Test(groups = "broker-admin")
public class ScalableTopicMigrationPermissionsTest extends AuthZTest {

    private static final String NAMESPACE = "public/default";
    private static final String CLIENT_ROLE = "migrated-topic-client";
    private static final String CLIENT_TOKEN = Jwts.builder()
            .claim("sub", CLIENT_ROLE).signWith(SECRET_KEY).compact();
    private static final String PARTITION_ROLE = "migrated-partition-client";
    private static final String NAMESPACE_ROLE = "migrated-namespace-client";
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

    private PulsarClient newClient() throws Exception {
        return PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(CLIENT_TOKEN))
                .build();
    }

    /** Send keyed messages, so they're routed to every segment, and add their values to {@code sent}. */
    private static void send(Producer<String> producer, String prefix, Set<String> sent) throws Exception {
        for (int i = 0; i < NUM_MESSAGES; i++) {
            String value = prefix + i;
            producer.newMessage().key("key-" + i).value(value).send();
            sent.add(value);
        }
    }

    private static Set<String> receiveValues(QueueConsumer<String> consumer, int count) throws Exception {
        Set<String> values = new HashSet<>();
        for (int i = 0; i < count; i++) {
            Message<String> message = consumer.receive(Duration.ofSeconds(10));
            assertThat(message).as("message %s of %s", i, count).isNotNull();
            values.add(message.value());
            consumer.acknowledge(message.id());
        }
        return values;
    }

    /**
     * An application allowed on the regular topic keeps producing through the migration: once the migration
     * commits, its producer moves to the new segments of the scalable topic.
     */
    @Test
    public void testProducerKeepsProducingThroughMigration() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/" + UUID.randomUUID();
        superUserAdmin.topics().createPartitionedTopic(topic, 2);
        superUserAdmin.topics().grantPermission(topic, CLIENT_ROLE, Set.of(AuthAction.produce, AuthAction.consume));

        @Cleanup
        PulsarClient client = newClient();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        Set<String> sent = new HashSet<>();
        send(producer, "pre-", sent);

        superUserAdmin.scalableTopics().migrateToScalable(topic, false);

        send(producer, "post-", sent);

        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        // The messages sent before the migration come from the legacy segments, the others from the new ones.
        assertThat(receiveValues(consumer, sent.size())).isEqualTo(sent);
    }

    /** After the migration, an application allowed on the regular topic can use its scalable topic:// name. */
    @Test
    public void testMigratedTopicIsAccessibleThroughItsScalableName() throws Exception {
        String name = NAMESPACE + "/" + UUID.randomUUID();
        superUserAdmin.topics().createNonPartitionedTopic("persistent://" + name);
        superUserAdmin.topics().grantPermission("persistent://" + name, CLIENT_ROLE,
                Set.of(AuthAction.produce, AuthAction.consume));

        @Cleanup
        PulsarClient client = newClient();
        Set<String> sent = new HashSet<>();
        try (Producer<String> producer = client.newProducer(Schema.string()).topic("persistent://" + name).create()) {
            send(producer, "pre-", sent);
        }

        superUserAdmin.scalableTopics().migrateToScalable(name, false);

        String scalableTopic = "topic://" + name;
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(scalableTopic).create();
        send(producer, "post-", sent);
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(scalableTopic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        assertThat(receiveValues(consumer, sent.size())).isEqualTo(sent);
    }

    /**
     * Only the permissions granted on the regular topic itself are copied: not those of its namespace, which apply to
     * the scalable topic already, nor those granted on a single partition.
     */
    @Test
    public void testMigrationCopiesOnlyThePermissionsOfTheTopic() throws Exception {
        String name = NAMESPACE + "/" + UUID.randomUUID();
        String topic = "persistent://" + name;
        superUserAdmin.topics().createPartitionedTopic(topic, 2);
        superUserAdmin.topics().grantPermission(topic, CLIENT_ROLE, Set.of(AuthAction.consume));
        superUserAdmin.topics().grantPermission(topic + "-partition-0", PARTITION_ROLE, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().grantPermissionOnNamespace(NAMESPACE, NAMESPACE_ROLE, Set.of(AuthAction.consume));
        try {
            superUserAdmin.scalableTopics().migrateToScalable(topic, false);
        } finally {
            superUserAdmin.namespaces().revokePermissionsOnNamespace(NAMESPACE, NAMESPACE_ROLE);
        }

        assertThat(superUserAdmin.scalableTopics().getPermissions("topic://" + name))
                .containsEntry(CLIENT_ROLE, Set.of(AuthAction.consume))
                .doesNotContainKeys(PARTITION_ROLE, NAMESPACE_ROLE);
    }

    /**
     * A migration holds the creation lock of the scalable topic from its check that the topic doesn't exist to its
     * creation, so a creation of the same topic can't get in between and receive the permissions the migration
     * grants: it's refused while the migration is about to copy them.
     */
    @Test
    public void testCreationIsRefusedWhileAMigrationCopiesPermissions() throws Exception {
        String name = NAMESPACE + "/" + UUID.randomUUID();
        superUserAdmin.topics().createNonPartitionedTopic("persistent://" + name);
        superUserAdmin.topics().grantPermission("persistent://" + name, CLIENT_ROLE, Set.of(AuthAction.produce));

        CompletableFuture<Void> copyStarted = new CompletableFuture<>();
        CompletableFuture<Void> resumeCopy = new CompletableFuture<>();
        doAnswer(invocation -> {
            copyStarted.complete(null);
            return resumeCopy.thenCompose(__ -> callRealMethod(invocation));
        }).when(authorizationService).copyPermissionsAsync(any(), any());
        try {
            CompletableFuture<Void> migration = superUserAdmin.scalableTopics().migrateToScalableAsync(name, false);
            copyStarted.get(30, TimeUnit.SECONDS);

            assertThatThrownBy(() -> superUserAdmin.scalableTopics().createScalableTopic("topic://" + name, 1))
                    .isInstanceOf(PulsarAdminException.ConflictException.class);

            resumeCopy.complete(null);
            migration.get(30, TimeUnit.SECONDS);
        } finally {
            resumeCopy.complete(null);
            doCallRealMethod().when(authorizationService).copyPermissionsAsync(any(), any());
        }

        // The scalable topic is the migrated one, with the permissions of the regular topic.
        assertThat(superUserAdmin.scalableTopics().getMetadata("topic://" + name).getSegments().values())
                .anyMatch(segment -> segment.getLegacyTopicName() != null);
        assertThat(superUserAdmin.scalableTopics().getPermissions("topic://" + name))
                .containsEntry(CLIENT_ROLE, Set.of(AuthAction.produce));
    }

    /** A migration that finds the scalable topic already created fails, and grants nothing on it. */
    @Test
    public void testMigrationToAnExistingScalableTopicGrantsNothingOnIt() throws Exception {
        String name = NAMESPACE + "/" + UUID.randomUUID();
        superUserAdmin.topics().createNonPartitionedTopic("persistent://" + name);
        superUserAdmin.topics().grantPermission("persistent://" + name, CLIENT_ROLE, Set.of(AuthAction.produce));
        superUserAdmin.scalableTopics().createScalableTopic("topic://" + name, 1);

        assertThatThrownBy(() -> superUserAdmin.scalableTopics().migrateToScalable(name, false))
                .isInstanceOf(PulsarAdminException.ConflictException.class);
        assertThat(superUserAdmin.scalableTopics().getPermissions("topic://" + name)).doesNotContainKey(CLIENT_ROLE);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> callRealMethod(InvocationOnMock invocation) {
        try {
            return (CompletableFuture<Void>) invocation.callRealMethod();
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }
}
