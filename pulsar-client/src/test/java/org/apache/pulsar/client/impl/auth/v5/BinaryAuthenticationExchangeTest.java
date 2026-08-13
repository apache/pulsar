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
package org.apache.pulsar.client.impl.auth.v5;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.ChallengeResponse;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * PIP-478: the shared {@link BinaryAuthenticationExchange} — the single routing
 * implementation both v5→v4 adapters use — must treat <em>only</em> the exact
 * {@link AuthData#REFRESH_AUTH_DATA_BYTES} payload as the refresh sentinel that re-produces the
 * credential (normative rule 2). Every other {@code CommandAuthChallenge} payload — an empty one, the
 * {@code INIT} marker, or an arbitrary challenge — is a challenge answered by the challenge handler
 * (rule 3); routing any of those to the credential provider would reset a live SASL/custom conversation
 * and send a fresh initial frame instead of the expected response.
 */
public class BinaryAuthenticationExchangeTest {

    private static AuthenticationExchange exchange(CountingBody body) {
        return new BinaryAuthenticationExchange(body, V5AuthContexts.binaryCallContext("broker-1"));
    }

    @Test
    public void refreshSentinelRoutesToCredentialProviderNotChallengeHandler() throws Exception {
        CountingBody body = new CountingBody();
        AuthData result = exchange(body).authenticateAsync(AuthData.REFRESH_AUTH_DATA).get();

        assertThat(body.dataCalls).isEqualTo(1);
        assertThat(body.challengeCalls).isEqualTo(0);
        assertThat(new String(result.getBytes(), UTF_8)).isEqualTo("credential");
    }

    @Test
    public void emptyChallengeRoutesToChallengeHandlerNotCredentialProvider() throws Exception {
        CountingBody body = new CountingBody();
        AuthData result = exchange(body).authenticateAsync(AuthData.of(new byte[0])).get();

        // An empty payload is a legitimate SASL/custom challenge round, not a refresh: it must reach the
        // challenge handler, never re-produce the credential.
        assertThat(body.challengeCalls).isEqualTo(1);
        assertThat(body.dataCalls).isEqualTo(0);
        assertThat(new String(result.getBytes(), UTF_8)).isEqualTo("response-to:");
    }

    @Test
    public void initPayloadRoutesToChallengeHandlerNotCredentialProvider() throws Exception {
        CountingBody body = new CountingBody();
        AuthData result = exchange(body).authenticateAsync(AuthData.INIT_AUTH_DATA).get();

        // The INIT marker is not the refresh sentinel; the previous over-match wrongly re-produced the
        // credential. It must now route to the challenge handler like any other non-refresh payload.
        assertThat(body.challengeCalls).isEqualTo(1);
        assertThat(body.dataCalls).isEqualTo(0);
        assertThat(new String(result.getBytes(), UTF_8)).isEqualTo("response-to:PulsarAuthInit");
    }

    @Test
    public void isRefreshSentinelMatchesOnlyTheRefreshPayload() {
        assertThat(BinaryAuthenticationExchange.isRefreshSentinel(AuthData.REFRESH_AUTH_DATA)).isTrue();
        assertThat(BinaryAuthenticationExchange.isRefreshSentinel(AuthData.INIT_AUTH_DATA)).isFalse();
        assertThat(BinaryAuthenticationExchange.isRefreshSentinel(AuthData.of(new byte[0]))).isFalse();
        assertThat(BinaryAuthenticationExchange.isRefreshSentinel(AuthData.of("challenge".getBytes(UTF_8))))
                .isFalse();
        assertThat(BinaryAuthenticationExchange.isRefreshSentinel(null)).isFalse();
    }

    /**
     * A v5 body exposing both binary capabilities, counting how often each is invoked so a test can prove
     * which cell a payload routed to.
     */
    private static final class CountingBody
            implements Authentication, BinaryAuthDataProvider, BinaryAuthChallengeHandler {

        private int dataCalls;
        private int challengeCalls;

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String authMethodName() {
            return "counting";
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
            dataCalls++;
            return CompletableFuture.completedFuture(new BinaryAuthData("credential".getBytes(UTF_8)));
        }

        @Override
        public CompletableFuture<ChallengeResponse> respondToChallengeAsync(AuthenticationCallContext ctx,
                AuthChallenge authChallenge) {
            challengeCalls++;
            String answer = "response-to:" + new String(authChallenge.bytes(), UTF_8);
            return CompletableFuture.completedFuture(new ChallengeResponse(answer.getBytes(UTF_8)));
        }
    }
}
