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
package org.apache.pulsar.client.impl.v5;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A namespace (multi-topic) stream consumer attaches only to the topics the namespace watch reports, so it must
 * never auto-create a topic, even though the broker allows auto topic creation: a topic deleted while the consumer
 * looks it up stays deleted. The consumer looks a topic up for the first attach of each topic of the initial
 * snapshot, for the attach of each topic added later, and whenever the session of an attached topic reconnects.
 *
 * <p>Each test holds back the consumer's lookups of a topic, deletes the topic, and then lets the lookups go.
 */
public class V5NamespaceStreamConsumerTopicDeletionTest extends V5ClientBaseTest {

    /**
     * A v4 client that holds back the lookups of the scalable topics the test picks until the test releases them,
     * and counts the lookups of each scalable topic.
     */
    private static final class LookupHoldingClient extends PulsarClientImpl {

        private final Set<String> heldTopics = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<Void> lookupsReleased = new CompletableFuture<>();
        private final Map<String, AtomicInteger> lookups = new ConcurrentHashMap<>();

        LookupHoldingClient(ClientConfigurationData conf) throws PulsarClientException {
            super(conf);
        }

        void holdLookups(String topic) {
            heldTopics.add(topic);
        }

        void releaseLookups() {
            lookupsReleased.complete(null);
        }

        /** How many times the client looked up the scalable topic, held or not. */
        int lookups(String topic) {
            AtomicInteger count = lookups.get(topic);
            return count == null ? 0 : count.get();
        }

        /** Close every connection of the client, as a network failure would. */
        void closeConnections() {
            getCnxPool().getConnections().forEach(cnx -> cnx.thenAccept(c -> c.ctx().channel().close()));
        }

        // The V5 client looks up a scalable topic only through this method.
        @Override
        public CompletableFuture<ClientCnx> getConnection(String topic) {
            lookups.computeIfAbsent(topic, __ -> new AtomicInteger()).incrementAndGet();
            if (heldTopics.contains(topic)) {
                return lookupsReleased.thenCompose(__ -> super.getConnection(topic));
            }
            return super.getConnection(topic);
        }
    }

    @Test
    public void testInitialTopicDeletedBeforeFirstAttachIsNotRecreated() throws Exception {
        String topic = newScalableTopic(1);
        PulsarClientV5 client = newLookupHoldingClient();
        LookupHoldingClient lookupClient = (LookupHoldingClient) client.v4Client();
        lookupClient.holdLookups(topic);

        CompletableFuture<StreamConsumer<String>> subscribe = client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscribeAsync();
        Awaitility.await().until(() -> lookupClient.lookups(topic) > 0);
        admin.scalableTopics().deleteScalableTopic(topic, true);
        lookupClient.releaseLookups();
        @Cleanup
        StreamConsumer<String> consumer = subscribe.get(30, SECONDS);

        assertStaysDeleted(topic);
    }

    @Test
    public void testTopicDeletedWhileAttachingIsNotRecreated() throws Exception {
        PulsarClientV5 client = newLookupHoldingClient();
        LookupHoldingClient lookupClient = (LookupHoldingClient) client.v4Client();
        @Cleanup
        StreamConsumer<String> consumer = client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscribe();

        String topic = newScalableTopicName();
        lookupClient.holdLookups(topic);
        admin.scalableTopics().createScalableTopic(topic, 1);
        Awaitility.await().until(() -> lookupClient.lookups(topic) > 0);
        admin.scalableTopics().deleteScalableTopic(topic, true);
        lookupClient.releaseLookups();

        assertStaysDeleted(topic);
    }

    @Test
    public void testTopicDeletedWhileReconnectingIsNotRecreated() throws Exception {
        String topic = newScalableTopic(1);
        PulsarClientV5 client = newLookupHoldingClient();
        LookupHoldingClient lookupClient = (LookupHoldingClient) client.v4Client();
        @Cleanup
        StreamConsumer<String> consumer = client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscribe();

        int lookupsBeforeReconnect = lookupClient.lookups(topic);
        lookupClient.holdLookups(topic);
        lookupClient.closeConnections();
        Awaitility.await().until(() -> lookupClient.lookups(topic) > lookupsBeforeReconnect);
        admin.scalableTopics().deleteScalableTopic(topic, true);
        lookupClient.releaseLookups();

        assertStaysDeleted(topic);
    }

    // --- Helpers ---

    private PulsarClientV5 newLookupHoldingClient() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(getBrokerServiceUrl());
        return track(new PulsarClientV5(new LookupHoldingClient(conf), "lookup-holding-client", null));
    }

    private String newScalableTopicName() {
        return "topic://" + getNamespace() + "/scalable-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Held for a few seconds: the released lookups reach the broker well within that. */
    private void assertStaysDeleted(String topic) {
        Awaitility.await().atMost(15, SECONDS).during(3, SECONDS).untilAsserted(() ->
                assertThatThrownBy(() -> admin.scalableTopics().getMetadata(topic))
                        .as("the deleted topic stays deleted")
                        .isInstanceOf(PulsarAdminException.NotFoundException.class));
    }
}
