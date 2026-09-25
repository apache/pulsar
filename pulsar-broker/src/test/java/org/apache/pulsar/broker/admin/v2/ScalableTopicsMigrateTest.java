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
package org.apache.pulsar.broker.admin.v2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import jakarta.ws.rs.container.AsyncResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

/**
 * Coverage for the topic-name handling of
 * {@link ScalableTopics#migrateToScalable}: the {@code @Encoded} path segment
 * must be URL-decoded before deriving the scalable and persistent identities,
 * matching {@code createScalableTopic}. Without the decode, migrating a topic
 * whose local name contains URL-encodable characters authorizes, looks up, and
 * checks existence of a differently-named topic.
 */
public class ScalableTopicsMigrateTest {

    /**
     * Exposes {@link ScalableTopics} to direct invocation without a servlet
     * container: routes {@code pulsar()} to a mock and short-circuits
     * authorization, recording the topic name it was asked to authorize.
     */
    private static class TestableScalableTopics extends ScalableTopics {
        private final PulsarService pulsarMock;
        private TopicName authorizedTopic;

        TestableScalableTopics(PulsarService pulsarMock) {
            this.pulsarMock = pulsarMock;
        }

        @Override
        protected PulsarService pulsar() {
            return pulsarMock;
        }

        @Override
        public String clientAppId() {
            return "test-client";
        }

        @Override
        public CompletableFuture<Void> validateTopicOperationAsync(TopicName topicName,
                                                                   TopicOperation operation) {
            this.authorizedTopic = topicName;
            return CompletableFuture.completedFuture(null);
        }
    }

    @Test
    public void testMigrateDecodesEncodedTopicName() {
        // "a%20b" is how the local name "a b" is spelled in the REST path;
        // @Encoded hands it to the resource still URL-encoded.
        assertMigrateResolvesLocalName("a%20b", "a b");
    }

    @Test
    public void testMigrateDecodesNonAsciiTopicName() {
        assertMigrateResolvesLocalName("caf%C3%A9", "café");
    }

    private static void assertMigrateResolvesLocalName(String encodedTopic, String localName) {
        PulsarService pulsar = mock(PulsarService.class);
        PulsarResources pulsarResources = mock(PulsarResources.class);
        ScalableTopicResources scalableResources = mock(ScalableTopicResources.class);
        NamespaceService namespaceService = mock(NamespaceService.class);
        when(pulsar.getPulsarResources()).thenReturn(pulsarResources);
        when(pulsarResources.getScalableTopicResources()).thenReturn(scalableResources);
        when(pulsar.getNamespaceService()).thenReturn(namespaceService);
        when(scalableResources.getScalableTopicMetadataAsync(any(TopicName.class)))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        // Stop the pipeline at the existence check; every identity the endpoint
        // derives has been observed by then.
        when(namespaceService.checkTopicExistsAsync(any(TopicName.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("stop")));

        TestableScalableTopics resource = new TestableScalableTopics(pulsar);
        AsyncResponse response = mock(AsyncResponse.class);

        resource.migrateToScalable(response, "tenant", "ns", encodedTopic, false);

        // Every future in the chain is already complete, so the pipeline ran
        // synchronously and the response has been resumed by now.
        verify(response).resume(any(Throwable.class));

        TopicName expectedScalable = TopicName.get("topic://tenant/ns/" + localName);
        TopicName expectedPersistent = TopicName.get("persistent://tenant/ns/" + localName);

        assertEquals(resource.authorizedTopic, expectedPersistent);

        ArgumentCaptor<TopicName> metadataLookup = ArgumentCaptor.forClass(TopicName.class);
        verify(scalableResources).getScalableTopicMetadataAsync(metadataLookup.capture());
        assertEquals(metadataLookup.getValue(), expectedScalable);

        ArgumentCaptor<TopicName> existenceCheck = ArgumentCaptor.forClass(TopicName.class);
        verify(namespaceService).checkTopicExistsAsync(existenceCheck.capture());
        assertEquals(existenceCheck.getValue(), expectedPersistent);
    }
}
