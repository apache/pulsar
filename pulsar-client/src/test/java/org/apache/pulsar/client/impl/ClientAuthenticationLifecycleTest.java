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
package org.apache.pulsar.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.SinglePassAuthentication;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

/**
 * PIP-478: the client drives a v5 {@code Authentication}, so the v5 slot — not only the v4 one — has to
 * follow the client's lifecycle.
 *
 * <p>Both invariants below are invisible from the v4 slot, which is why they need their own coverage: the
 * v4 slot is updated and closed correctly in each case while the authentication the client actually drives
 * is not.
 */
public class ClientAuthenticationLifecycleTest {

    /**
     * A cluster failover swaps the client's authentication at runtime ({@code AutoClusterFailover} /
     * {@code ControlledClusterFailover}). The credential the client presents must follow the swap.
     */
    @Test
    public void swappingTheAuthenticationSwapsWhatTheClientDrives() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .authentication(new AuthenticationToken("first-cluster-token"))
                .build();
        try {
            ClientConfigurationData conf = ((PulsarClientImpl) client).conf;
            assertThat(credentialOf(conf)).isEqualTo("first-cluster-token");

            ((PulsarClientImpl) client).updateAuthentication(new AuthenticationToken("second-cluster-token"));

            assertThat(credentialOf(conf))
                    .as("a stale v5 body would keep presenting the old cluster's credential — from a plugin "
                            + "updateAuthentication has already closed")
                    .isEqualTo("second-cluster-token");
            assertThat(conf.getV5AuthenticationDriver())
                    .as("and the driver must be re-resolved with it, since ClientCnx memoizes it")
                    .isNotNull();
        } finally {
            client.close();
        }
    }

    /**
     * A v5-native plugin configured through the v5 builder lives only in the v5 slot — the v4 slot still
     * holds {@code AuthenticationDisabled} — so closing the client must reach it there.
     */
    @Test
    public void closingTheClientClosesAV5NativePlugin() throws Exception {
        CountingV5Authentication plugin = new CountingV5Authentication();
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        ((PulsarClientImpl) client).conf.setV5Authentication(plugin);

        client.close();

        assertThat(plugin.closed)
                .as("a plugin the client drove but never closed leaks whatever it holds open")
                .hasValue(1);
    }

    /**
     * {@code build()} hands the client the builder's own configuration object, so anything the client
     * derives and stores there outlives the build. A second client from the same — or a cloned — builder
     * must still present the credential that builder is now configured with.
     */
    @Test
    public void aSecondClientFromTheSameBuilderUsesTheSecondCredential() throws Exception {
        org.apache.pulsar.client.api.ClientBuilder builder = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .authentication(new AuthenticationToken("first-token"));
        PulsarClient first = builder.build();
        PulsarClient second = null;
        PulsarClient cloned = null;
        try {
            assertThat(credentialOf(((PulsarClientImpl) first).conf)).isEqualTo("first-token");

            second = builder.authentication(new AuthenticationToken("second-token")).build();
            assertThat(credentialOf(((PulsarClientImpl) second).conf))
                    .as("the second client must not inherit the first client's resolved credential")
                    .isEqualTo("second-token");

            cloned = builder.clone().authentication(new AuthenticationToken("third-token")).build();
            assertThat(credentialOf(((PulsarClientImpl) cloned).conf))
                    .as("a shallow clone carries the configuration's slots, so a stale derived body would "
                            + "travel with it")
                    .isEqualTo("third-token");
        } finally {
            first.close();
            if (second != null) {
                second.close();
            }
            if (cloned != null) {
                cloned.close();
            }
        }
    }

    private static String credentialOf(ClientConfigurationData conf) throws Exception {
        BinaryAuthData data = conf.getV5AuthenticationDriver()
                .newAuthenticationExchange("broker.example.com")
                .getAuthDataAsync()
                .thenApply(authData -> new BinaryAuthData(authData.getBytes()))
                .get();
        return new String(data.bytes(), UTF_8);
    }

    /** Counts its own closing. */
    private static final class CountingV5Authentication implements SinglePassAuthentication {

        private final AtomicInteger closed = new AtomicInteger();

        @Override
        public String authMethodName() {
            return "counting";
        }

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
            return CompletableFuture.completedFuture(new BinaryAuthData(new byte[0]));
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
            return CompletableFuture.completedFuture(HttpAuthHeaders.of(java.util.Map.of()));
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }
    }
}
