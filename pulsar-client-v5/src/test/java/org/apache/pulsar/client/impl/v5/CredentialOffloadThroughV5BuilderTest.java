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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.impl.auth.v5.DefaultClientAuthenticationServices;
import org.apache.pulsar.client.impl.v5.auth.LegacyV4AuthenticationAdapter;
import org.apache.pulsar.client.impl.v5.auth.V5ToV4AuthenticationAdapter;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * Verifies PIP-478: a bridged v4 credential plugin deployed on the v5 client keeps its async offload
 * adapter (rather than being unwrapped onto the raw v4 engine), so its blocking {@code getAuthData} runs on
 * the client's blocking executor — never inline on the Netty event loop. A TLS-only plugin, which performs
 * no credential I/O, is driven raw.
 */
public class CredentialOffloadThroughV5BuilderTest {

    private static final String BLOCKING_THREAD = "v5-builder-offload-test-thread";

    @Test
    public void credentialV4PluginStaysWrappedAndOffloadsGetAuthData() throws Exception {
        BlockingCredentialV4 v4 = new BlockingCredentialV4();
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(v4));

        // Decision: the credential plugin is NOT unwrapped to the raw v4 engine; it stays wrapped so that
        // its (blocking) getAuthData off-loads instead of running on the Netty event loop.
        org.apache.pulsar.client.api.Authentication resolved = builder.resolveAuthenticationForTest();
        assertThat(resolved).isInstanceOf(V5ToV4AuthenticationAdapter.class);

        ExecutorService blocking = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, BLOCKING_THREAD);
            t.setDaemon(true);
            return t;
        });
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            V5ToV4AuthenticationAdapter adapter = (V5ToV4AuthenticationAdapter) resolved;
            adapter.bindClientAuthenticationServices(services(blocking, scheduler));
            adapter.start();

            String callerThread = Thread.currentThread().getName();
            AuthenticationExchange exchange = adapter.newAuthenticationExchange("broker.example.com");
            CompletableFuture<AuthData> future = exchange.getAuthDataAsync();

            // The fetch is off-loaded: it started on the blocking executor while the caller proceeds.
            assertThat(v4.entered.await(5, SECONDS)).isTrue();
            AuthData data = future.get(5, SECONDS);
            assertThat(new String(data.getBytes(), UTF_8)).isEqualTo("the-credential");
            assertThat(v4.fetchThread.get())
                    .isEqualTo(BLOCKING_THREAD)
                    .isNotEqualTo(callerThread);
        } finally {
            blocking.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    public void tlsOnlyV4PluginRunsRawWithoutOffloadWrapper() {
        // A plugin that reports TLS material but no command/HTTP credential performs no I/O to off-load, so
        // it is driven raw on the v4 client (its TLS material is presented at the transport layer).
        TlsOnlyV4 v4 = new TlsOnlyV4();
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(v4));
        assertThat(builder.resolveAuthenticationForTest()).isSameAs(v4);
    }

    @Test
    public void combinedTlsAndCredentialV4WithoutTlsPolicyRunsRawToPreserveTls() {
        // A v4 plugin that presents BOTH a client certificate (mTLS) and a fetched credential must run raw
        // when no tlsPolicy is configured: the legacy TLS path reads its certificate from getAuthData(), so
        // wrapping it for credential off-load would drop the certificate. Off-load is regained by
        // configuring tlsPolicy(...) (which moves TLS material to the factory path).
        CombinedTlsCredentialV4 v4 = new CombinedTlsCredentialV4();
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(v4));
        assertThat(builder.resolveAuthenticationForTest()).isSameAs(v4);
    }

    private static ClientAuthenticationServices services(ExecutorService blocking,
            ScheduledExecutorService scheduler) {
        // The offload path does not use HTTP, so a throwing factory suffices.
        return new DefaultClientAuthenticationServices(config -> {
            throw new UnsupportedOperationException("HTTP client not used in this test");
        }, scheduler, blocking, Clock.systemUTC(), OpenTelemetry.noop(), "test-client");
    }

    /** A v4 credential plugin whose {@code getCommandData} blocks (models blocking credential I/O). */
    @SuppressWarnings("deprecation")
    private static final class BlockingCredentialV4 implements org.apache.pulsar.client.api.Authentication {

        private final CountDownLatch entered = new CountDownLatch(1);
        private final AtomicReference<String> fetchThread = new AtomicReference<>();

        @Override
        public String getAuthMethodName() {
            return "blocking-cred";
        }

        @Override
        public AuthenticationDataProvider getAuthData() {
            return new AuthenticationDataProvider() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    fetchThread.set(Thread.currentThread().getName());
                    entered.countDown();
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "the-credential";
                }
            };
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    /** A v4 plugin that reports only TLS material (no command/HTTP credential) under a non-"tls" method name. */
    @SuppressWarnings("deprecation")
    private static final class TlsOnlyV4 implements org.apache.pulsar.client.api.Authentication {

        @Override
        public String getAuthMethodName() {
            return "custom-tls";
        }

        @Override
        public AuthenticationDataProvider getAuthData() {
            return new AuthenticationDataProvider() {
                @Override
                public boolean hasDataForTls() {
                    return true;
                }
            };
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    /** A v4 plugin that reports BOTH a client certificate (mTLS) and a command credential. */
    @SuppressWarnings("deprecation")
    private static final class CombinedTlsCredentialV4 implements org.apache.pulsar.client.api.Authentication {

        @Override
        public String getAuthMethodName() {
            return "combined";
        }

        @Override
        public AuthenticationDataProvider getAuthData() {
            return new AuthenticationDataProvider() {
                @Override
                public boolean hasDataForTls() {
                    return true;
                }

                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    return "the-credential";
                }
            };
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }
}
