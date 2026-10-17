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
package org.apache.pulsar.websocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.eclipse.jetty.ee10.websocket.server.JettyServerUpgradeResponse;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ConsumerHandlerTest {
    private static final String SOURCE = "persistent://tenant/ns/source";
    private static final String DESTINATION = "persistent://other/ns/destination";
    private static final String SUBSCRIPTION = "subscription";
    private static final String ROLE = "client-role";

    @DataProvider
    public Object[][] destinations() {
        return new Object[][] {
                {false, false, true}, {false, false, false},
                {false, true, true}, {false, true, false},
                {true, false, true}, {true, false, false},
                {true, true, true}, {true, true, false}
        };
    }

    @Test(dataProvider = "destinations")
    public void testDestinationAuthorization(boolean multiTopic, boolean useDefault, boolean allowed) throws Exception {
        Fixture fixture = new Fixture(true);
        Map<String, String[]> params = dlqParams(useDefault ? null : DESTINATION);
        String destination = useDefault ? SOURCE + "-" + SUBSCRIPTION + "-DLQ" : DESTINATION;
        fixture.allowConsume(true);
        fixture.allowProduce(destination, CompletableFuture.completedFuture(allowed));

        ConsumerHandler handler = fixture.create(params, multiTopic);

        assertThat(handler.isAllowConnect()).isEqualTo(allowed);
        verify(fixture.authorization).allowTopicOperationAsync(eq(TopicName.get(destination)),
                eq(TopicOperation.PRODUCE), eq(ROLE), any(AuthenticationDataSource.class));
        if (allowed) {
            verify(fixture.builder).subscribe();
            verifyNoInteractions(fixture.response);
            assertThat(fixture.builder.getConf().getDeadLetterPolicy().getDeadLetterTopic()).isEqualTo(destination);
        } else {
            verify(fixture.builder, never()).subscribe();
            verify(fixture.response).sendError(eq(HttpServletResponse.SC_FORBIDDEN), anyString());
        }
    }

    @DataProvider
    public Object[][] paddedDestinations() {
        return new Object[][] {
                {" destination", false}, {"destination ", false}, {" destination ", false},
                {" destination", true}, {"destination ", true}, {" destination ", true}
        };
    }

    @Test(dataProvider = "paddedDestinations")
    public void testDestinationMatchesProducerNormalization(String destination, boolean allowed) throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.allowConsume(true);
        ProducerBuilderImpl<byte[]> producer = new ProducerBuilderImpl<>(null, Schema.BYTES);
        producer.topic(destination);
        String actualDestination = producer.getConf().getTopicName();
        assertThat(TopicName.get(destination)).isNotEqualTo(TopicName.get(actualDestination));
        // Only the trimmed topic that the producer uses should be checked.
        fixture.allowProduce(destination, CompletableFuture.completedFuture(true));
        fixture.allowProduce(actualDestination, CompletableFuture.completedFuture(allowed));

        ConsumerHandler handler = fixture.create(dlqParams(destination), false);

        assertThat(handler.isAllowConnect()).isEqualTo(allowed);
        verify(fixture.authorization).allowTopicOperationAsync(eq(TopicName.get(actualDestination)),
                eq(TopicOperation.PRODUCE), eq(ROLE), any(AuthenticationDataSource.class));
        verify(fixture.authorization, never()).allowTopicOperationAsync(eq(TopicName.get(destination)),
                eq(TopicOperation.PRODUCE), eq(ROLE), any(AuthenticationDataSource.class));
        assertThat(fixture.builder.getConf().getDeadLetterPolicy().getDeadLetterTopic()).isEqualTo(actualDestination);
        if (allowed) {
            verify(fixture.builder).subscribe();
        } else {
            verify(fixture.builder, never()).subscribe();
            verify(fixture.response).sendError(eq(HttpServletResponse.SC_FORBIDDEN), anyString());
        }
    }

    @Test(dataProvider = "failures")
    public void testPatternDestinationAuthorization(boolean allowed) throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.allowConsume(true);
        fixture.allowProduce(DESTINATION, CompletableFuture.completedFuture(allowed));
        Map<String, String[]> params = dlqParams(DESTINATION);
        params.put("topicsPattern", new String[] {"persistent://tenant/ns/source.*"});
        ConsumerHandler handler = fixture.create(params, true);
        assertThat(handler.isAllowConnect()).isEqualTo(allowed);
        verify(fixture.authorization).allowTopicOperationAsync(eq(TopicName.get(DESTINATION)),
                eq(TopicOperation.PRODUCE), eq(ROLE), any(AuthenticationDataSource.class));
        if (allowed) {
            verify(fixture.builder).subscribe();
        } else {
            verify(fixture.builder, never()).subscribe();
        }
    }

    @Test
    public void testPartitionDefaultDestination() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.source = SOURCE + "-partition-0";
        String destination = fixture.source + "-" + SUBSCRIPTION + "-DLQ";
        fixture.allowConsume(true);
        fixture.allowProduce(destination, CompletableFuture.completedFuture(true));
        ConsumerHandler handler = fixture.create(dlqParams(null), false);
        assertThat(handler.isAllowConnect()).isTrue();
        assertThat(fixture.builder.getConf().getDeadLetterPolicy().getDeadLetterTopic()).isEqualTo(destination);
        verify(fixture.authorization).allowTopicOperationAsync(eq(TopicName.get(destination)),
                eq(TopicOperation.PRODUCE), eq(ROLE), any(AuthenticationDataSource.class));
    }

    @Test
    public void testConsumeDeniedBeforeDestinationCheck() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.allowConsume(false);
        ConsumerHandler handler = fixture.create(dlqParams(DESTINATION), false);
        assertThat(handler.isAllowConnect()).isFalse();
        verify(fixture.authorization, never()).allowTopicOperationAsync(any(), eq(TopicOperation.PRODUCE),
                anyString(), any(AuthenticationDataSource.class));
        verify(fixture.builder, never()).subscribe();
        verify(fixture.response).sendError(eq(HttpServletResponse.SC_FORBIDDEN), anyString());
    }

    @Test
    public void testNoDeadLetterPolicy() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.allowConsume(true);
        ConsumerHandler handler = fixture.create(new HashMap<>(), false);
        assertThat(handler.isAllowConnect()).isTrue();
        verify(fixture.authorization, never()).allowTopicOperationAsync(any(), eq(TopicOperation.PRODUCE),
                anyString(), any(AuthenticationDataSource.class));
        verify(fixture.builder).subscribe();
    }

    @DataProvider
    public Object[][] failures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "failures")
    public void testDestinationAuthorizationFailure(boolean timeout) throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.allowConsume(true);
        fixture.config.setMetadataStoreOperationTimeoutSeconds(timeout ? 0 : 30);
        fixture.allowProduce(DESTINATION, timeout ? new CompletableFuture<>()
                : CompletableFuture.failedFuture(new IllegalStateException("authorization unavailable")));
        ConsumerHandler handler = fixture.create(dlqParams(DESTINATION), false);
        assertThat(handler.isAllowConnect()).isFalse();
        verify(fixture.builder, never()).subscribe();
        verify(fixture.response).sendError(eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR), anyString());
    }

    @DataProvider
    public Object[][] invalidDestinations() {
        return new Object[][] {{""}, {" "}, {"invalid://tenant/ns/topic"}};
    }

    @Test(dataProvider = "invalidDestinations")
    public void testInvalidDestination(String destination) throws Exception {
        Fixture fixture = new Fixture(true);
        ConsumerHandler handler = fixture.create(dlqParams(destination), false);
        assertThat(handler.isAllowConnect()).isFalse();
        verify(fixture.builder, never()).subscribe();
        verify(fixture.response).sendError(eq(HttpServletResponse.SC_BAD_REQUEST), anyString());
    }

    @Test
    public void testAuthorizationDisabledPreservesBlankDestination() throws Exception {
        Fixture fixture = new Fixture(false);
        ConsumerHandler handler = fixture.create(dlqParams(""), false);
        assertThat(handler.isAllowConnect()).isTrue();
        verifyNoInteractions(fixture.authorization);
        verify(fixture.builder).subscribe();
        assertThat(fixture.builder.getConf().getDeadLetterPolicy().getDeadLetterTopic()).isEmpty();
    }

    private static Map<String, String[]> dlqParams(String destination) {
        Map<String, String[]> params = new HashMap<>();
        params.put("maxRedeliverCount", new String[] {"1"});
        if (destination != null) {
            params.put("deadLetterTopic", new String[] {destination});
        }
        return params;
    }

    private static class Fixture {
        private String source = SOURCE;
        private final WebSocketService service = mock(WebSocketService.class);
        private final AuthorizationService authorization = mock(AuthorizationService.class);
        private final ServiceConfiguration config = new ServiceConfiguration();
        private final ConsumerBuilderImpl<byte[]> builder = spy(new ConsumerBuilderImpl<>(null, Schema.BYTES));
        private final JettyServerUpgradeResponse response = mock(JettyServerUpgradeResponse.class);

        @SuppressWarnings({"unchecked", "deprecation"})
        Fixture(boolean authorizationEnabled) throws Exception {
            PulsarClient client = mock(PulsarClient.class);
            AuthenticationService authentication = mock(AuthenticationService.class);
            when(service.getPulsarClient()).thenReturn(client);
            when(client.newConsumer()).thenReturn(builder);
            doReturn(mock(Consumer.class)).when(builder).subscribe();
            when(service.getCryptoKeyReader()).thenReturn(Optional.empty());
            when(service.addConsumer(any())).thenReturn(true);
            when(service.isAuthenticationEnabled()).thenReturn(true);
            when(service.isAuthorizationEnabled()).thenReturn(authorizationEnabled);
            when(service.getAuthenticationService()).thenReturn(authentication);
            when(authentication.authenticateHttpRequest(any(HttpServletRequest.class))).thenReturn(ROLE);
            when(service.getAuthorizationService()).thenReturn(authorization);
            when(service.getConfig()).thenReturn(config);
        }

        void allowConsume(boolean allowed) {
            when(authorization.allowTopicOperationAsync(any(), eq(TopicOperation.CONSUME), eq(ROLE),
                    any(AuthenticationDataSource.class))).thenReturn(CompletableFuture.completedFuture(allowed));
        }

        void allowProduce(String destination, CompletableFuture<Boolean> result) {
            when(authorization.allowTopicOperationAsync(eq(TopicName.get(destination)), eq(TopicOperation.PRODUCE),
                    eq(ROLE), any(AuthenticationDataSource.class))).thenReturn(result);
        }

        ConsumerHandler create(Map<String, String[]> params, boolean multiTopic) {
            HttpServletRequest request = mock(HttpServletRequest.class);
            if (multiTopic && !params.containsKey("topicsPattern")) {
                params.put("topics", new String[] {source + ",persistent://tenant/ns/second"});
            }
            when(request.getParameterMap()).thenReturn(params);
            when(request.getRemoteAddr()).thenReturn("127.0.0.1");
            when(request.getRequestURI()).thenReturn(multiTopic ? "/ws/v3/consumer/" + SUBSCRIPTION
                    : "/ws/v2/consumer/persistent/tenant/ns/" + TopicName.get(source).getLocalName() + "/"
                            + SUBSCRIPTION);
            return multiTopic ? new MultiTopicConsumerHandler(service, request, response)
                    : new ConsumerHandler(service, request, response);
        }
    }
}
