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
package org.apache.pulsar.client.api.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.jsonwebtoken.Jwts;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.v5.auth.AuthenticationFactory;
import org.apache.pulsar.client.api.v5.config.ProducerAccessMode;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * A V5 client whose role is authenticated but not permitted gets a typed
 * {@link PulsarClientException.AuthorizationException}, whichever request the broker rejects: the topic
 * lookup (DAG watch), the namespace watch, the subscribe to the topic's controller, or a per-segment producer,
 * consumer or reader. A client whose credentials are rejected gets a typed
 * {@link PulsarClientException.AuthenticationException}. See error-model.md §2 and §4.
 */
public class V5AuthErrorTest extends MockedPulsarStandalone {

    private static final String NAMESPACE = "public/default";
    private static final String PRODUCER_ROLE = "v5-producer";
    private static final String CONSUMER_ROLE = "v5-consumer";

    private PulsarAdmin superUserAdmin;
    /** No permission at all. */
    private PulsarClient nobodyClient;
    /** Produce permission on the namespace only. */
    private PulsarClient producerClient;
    /** Consume permission on the namespace only. */
    private PulsarClient consumerClient;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        configureTokenAuthentication();
        configureDefaultAuthorization();
        start();
        superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        superUserAdmin.namespaces().grantPermissionOnNamespace(NAMESPACE, PRODUCER_ROLE, Set.of(AuthAction.produce));
        superUserAdmin.namespaces().grantPermissionOnNamespace(NAMESPACE, CONSUMER_ROLE, Set.of(AuthAction.consume));
        nobodyClient = newClient(NOBODY_TOKEN);
        producerClient = newClient(tokenOf(PRODUCER_ROLE));
        consumerClient = newClient(tokenOf(CONSUMER_ROLE));
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        for (AutoCloseable c : new AutoCloseable[] {nobodyClient, producerClient, consumerClient, superUserAdmin}) {
            if (c != null) {
                c.close();
            }
        }
        close();
    }

    @Test
    public void testLookupNotAuthorized() throws Exception {
        String topic = newScalableTopic();

        assertNotAuthorized(() -> nobodyClient.newProducer(Schema.string()).topic(topic).create());
        assertNotAuthorized(nobodyClient.newProducer(Schema.string()).topic(topic).createAsync());
        assertNotAuthorized(() -> nobodyClient.newQueueConsumer(Schema.string()).topic(topic)
                .subscriptionName("queue").subscribe());
        assertNotAuthorized(() -> nobodyClient.newStreamConsumer(Schema.string()).topic(topic)
                .subscriptionName("stream").subscribe());
        assertNotAuthorized(nobodyClient.newStreamConsumer(Schema.string()).topic(topic)
                .subscriptionName("stream").subscribeAsync());
        assertNotAuthorized(() -> nobodyClient.newCheckpointConsumer(Schema.string()).topic(topic).create());
        assertNotAuthorized(() -> nobodyClient.newCheckpointConsumer(Schema.string()).topic(topic)
                .consumerGroup("group").create());
    }

    @Test
    public void testNamespaceWatchNotAuthorized() {
        assertNotAuthorized(() -> nobodyClient.newStreamConsumer(Schema.string()).namespace(NAMESPACE)
                .subscriptionName("stream").subscribe());
        assertNotAuthorized(() -> nobodyClient.newQueueConsumer(Schema.string()).namespace(NAMESPACE)
                .subscriptionName("queue").subscribe());
    }

    @Test
    public void testSubscribeNotAuthorized() throws Exception {
        String topic = newScalableTopic();
        // The role can look the topic up and produce to it...
        @Cleanup
        Producer<String> producer = producerClient.newProducer(Schema.string()).topic(topic).create();
        producer.newMessage().value("allowed").send();

        // ...but neither the controller (stream, consumer group) nor a segment (queue, checkpoint) lets it consume.
        assertNotAuthorized(() -> producerClient.newStreamConsumer(Schema.string()).topic(topic)
                .subscriptionName("stream").subscribe());
        assertNotAuthorized(() -> producerClient.newCheckpointConsumer(Schema.string()).topic(topic)
                .consumerGroup("group").create());
        assertNotAuthorized(() -> producerClient.newQueueConsumer(Schema.string()).topic(topic)
                .subscriptionName("queue").subscribe());
        assertNotAuthorized(producerClient.newQueueConsumer(Schema.string()).topic(topic)
                .subscriptionName("queue").subscribeAsync());
        assertNotAuthorized(() -> producerClient.newCheckpointConsumer(Schema.string()).topic(topic).create());
    }

    @Test
    public void testProduceNotAuthorized() throws Exception {
        String topic = newScalableTopic();
        // The role can look the topic up and subscribe to it...
        @Cleanup
        StreamConsumer<String> consumer = consumerClient.newStreamConsumer(Schema.string()).topic(topic)
                .subscriptionName("stream").subscribe();
        // ...and create a producer, which attaches to the segments on the first send...
        @Cleanup
        Producer<String> producer = consumerClient.newProducer(Schema.string()).topic(topic).create();

        // ...where the segment rejects it.
        assertNotAuthorized(() -> producer.newMessage().value("rejected").send());
        assertNotAuthorized(producer.async().newMessage().value("rejected").send());
        // An exclusive producer attaches to the segments on create.
        assertNotAuthorized(() -> consumerClient.newProducer(Schema.string()).topic(topic)
                .accessMode(ProducerAccessMode.EXCLUSIVE).create());
    }

    @Test
    public void testCredentialsRejected() throws Exception {
        String topic = newScalableTopic();
        @Cleanup
        PulsarClient client = newClient("not-a-valid-token");

        assertThatThrownBy(() -> client.newProducer(Schema.string()).topic(topic).create())
                .isInstanceOf(PulsarClientException.AuthenticationException.class);
        assertThatThrownBy(() -> client.newStreamConsumer(Schema.string()).topic(topic)
                .subscriptionName("stream").subscribe())
                .isInstanceOf(PulsarClientException.AuthenticationException.class);
    }

    private static void assertNotAuthorized(ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOf(PulsarClientException.AuthorizationException.class);
    }

    private static void assertNotAuthorized(CompletableFuture<?> future) {
        assertThat(future).failsWithin(30, TimeUnit.SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(PulsarClientException.AuthorizationException.class);
    }

    private String newScalableTopic() throws Exception {
        String topic = "topic://" + NAMESPACE + "/" + UUID.randomUUID();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        return topic;
    }

    private PulsarClient newClient(String token) throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(token))
                .build();
    }

    private static String tokenOf(String role) {
        return Jwts.builder().claim("sub", role).signWith(SECRET_KEY).compact();
    }
}
