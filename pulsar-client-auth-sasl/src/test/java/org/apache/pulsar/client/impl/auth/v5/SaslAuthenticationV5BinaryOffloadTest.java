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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * PIP-478 FIX D: the v5-native SASL binary body must run its per-exchange SASL provider creation and
 * {@code evaluateChallenge} (GSSAPI/Kerberos) on the client's bounded blocking executor, never inline on the
 * caller thread (the Netty event loop in production). Driven through the real {@link V5BinaryAuthenticationDriver}
 * with a fake, deliberately-blocking SASL provider.
 */
public class SaslAuthenticationV5BinaryOffloadTest {

    private static final String BLOCKING_THREAD_NAME = "sasl-blocking-test-thread";

    private static ClientAuthenticationServices services(ExecutorService blocking,
            ScheduledExecutorService scheduler) {
        // This test exercises the blocking-executor offload, not HTTP, so a throwing factory suffices.
        return new DefaultClientAuthenticationServices(config -> {
            throw new UnsupportedOperationException("HTTP client not used in this test");
        }, scheduler, blocking, Clock.systemUTC(), OpenTelemetry.noop(), "test-client");
    }

    @Test
    public void gssapiEvaluateChallengeRunsOnBlockingExecutorNotCaller() throws Exception {
        ExecutorService blocking = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, BLOCKING_THREAD_NAME);
            t.setDaemon(true);
            return t;
        });
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            AtomicReference<String> authThread = new AtomicReference<>();
            CountDownLatch entered = new CountDownLatch(1);
            SaslAuthenticationV5 body = new SaslAuthenticationV5(
                    brokerHost -> new BlockingSaslProvider(authThread, entered));

            AuthenticationExchange exchange = new V5BinaryAuthenticationDriver(body, services(blocking, scheduler))
                    .newAuthenticationExchange("broker.example.com");

            String callerThread = Thread.currentThread().getName();
            CompletableFuture<AuthData> future = exchange.getAuthDataAsync();

            // The GSSAPI work is off-loaded: the future must NOT already be complete on the caller thread.
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(future).isNotCompleted();

            AuthData data = future.get(5, SECONDS);
            assertThat(new String(data.getBytes(), UTF_8)).isEqualTo("sasl-init");
            // evaluateChallenge ran on the client's blocking executor, not on the caller thread.
            assertThat(authThread.get()).isEqualTo(BLOCKING_THREAD_NAME);
            assertThat(authThread.get()).isNotEqualTo(callerThread);
        } finally {
            blocking.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    /** A fake SASL data provider whose authenticate() records its thread and blocks, standing in for GSSAPI. */
    private static final class BlockingSaslProvider implements AuthenticationDataProvider {
        private static final long serialVersionUID = 1L;
        private final transient AtomicReference<String> authThread;
        private final transient CountDownLatch entered;

        BlockingSaslProvider(AtomicReference<String> authThread, CountDownLatch entered) {
            this.authThread = authThread;
            this.entered = entered;
        }

        @Override
        public AuthData authenticate(AuthData commandData) {
            authThread.set(Thread.currentThread().getName());
            entered.countDown();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return AuthData.of("sasl-init".getBytes(UTF_8));
        }
    }
}
