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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.impl.auth.oauth2.OAuth2AuthenticationV5;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * Verifies the PIP-478 off-event-loop offload: a credential-fetching v5 body (OAuth2 here)
 * whose supplier blocks on network I/O must run that fetch on the client's bounded blocking executor,
 * never on the caller thread (which, for a real connection, is the Netty event loop).
 */
public class CredentialOffloadTest {

    private static final String BLOCKING_THREAD_NAME = "auth-blocking-test-thread";

    private static ClientAuthenticationServices services(ExecutorService blocking,
            ScheduledExecutorService scheduler) {
        // This test exercises the blocking-executor offload, not HTTP, so a throwing factory suffices.
        return new DefaultClientAuthenticationServices(config -> {
            throw new UnsupportedOperationException("HTTP client not used in this test");
        }, scheduler, blocking, Clock.systemUTC(), OpenTelemetry.noop(), "test-client");
    }

    @Test
    public void slowCredentialSupplierRunsOnBlockingExecutorNotCaller() throws Exception {
        ExecutorService blocking = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, BLOCKING_THREAD_NAME);
            t.setDaemon(true);
            return t;
        });
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            AtomicReference<String> supplierThread = new AtomicReference<>();
            CountDownLatch entered = new CountDownLatch(1);
            OAuth2AuthenticationV5 body = new OAuth2AuthenticationV5(() -> {
                supplierThread.set(Thread.currentThread().getName());
                entered.countDown();
                sleep(300);
                return "the-access-token";
            });

            V5BinaryAuthenticationDriver driver =
                    new V5BinaryAuthenticationDriver(body, services(blocking, scheduler));
            AuthenticationExchange exchange = driver.newAuthenticationExchange("broker.example.com");

            String callerThread = Thread.currentThread().getName();
            CompletableFuture<AuthData> future = exchange.getAuthDataAsync();

            // The fetch is off-loaded: the future must NOT already be complete on the caller thread — the
            // caller (the event loop, in production) is free to proceed while the supplier blocks.
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(future).isNotCompleted();

            AuthData data = future.get(5, SECONDS);
            assertThat(new String(data.getBytes(), UTF_8)).isEqualTo("the-access-token");
            // The blocking supplier ran on the client's blocking executor, not on the caller thread.
            assertThat(supplierThread.get()).isEqualTo(BLOCKING_THREAD_NAME);
            assertThat(supplierThread.get()).isNotEqualTo(callerThread);
        } finally {
            blocking.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    public void slowFileTokenSupplierRunsOnBlockingExecutorNotCaller() throws Exception {
        // PIP-478 FIX B: the built-in token body's supplier can be a file:/... reader (Files.readAllBytes),
        // so its read must be off-loaded onto the blocking executor, never run inline on the caller/event loop.
        ExecutorService blocking = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, BLOCKING_THREAD_NAME);
            t.setDaemon(true);
            return t;
        });
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            AtomicReference<String> supplierThread = new AtomicReference<>();
            CountDownLatch entered = new CountDownLatch(1);
            TokenAuthenticationV5 body = new TokenAuthenticationV5(() -> {
                supplierThread.set(Thread.currentThread().getName());
                entered.countDown();
                sleep(300);
                return "the-file-token";
            });

            V5BinaryAuthenticationDriver driver =
                    new V5BinaryAuthenticationDriver(body, services(blocking, scheduler));
            AuthenticationExchange exchange = driver.newAuthenticationExchange("broker.example.com");

            String callerThread = Thread.currentThread().getName();
            CompletableFuture<AuthData> future = exchange.getAuthDataAsync();

            // The read is off-loaded: the future must NOT already be complete on the caller thread.
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(future).isNotCompleted();

            AuthData data = future.get(5, SECONDS);
            assertThat(new String(data.getBytes(), UTF_8)).isEqualTo("the-file-token");
            // The blocking supplier ran on the client's blocking executor, not on the caller thread.
            assertThat(supplierThread.get()).isEqualTo(BLOCKING_THREAD_NAME);
            assertThat(supplierThread.get()).isNotEqualTo(callerThread);
        } finally {
            blocking.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    public void supplierFailureCompletesFutureExceptionallyNotSynchronously() throws Exception {
        ExecutorService blocking = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, BLOCKING_THREAD_NAME);
            t.setDaemon(true);
            return t;
        });
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            OAuth2AuthenticationV5 body = new OAuth2AuthenticationV5(() -> {
                throw new RuntimeException("token endpoint down");
            });
            AuthenticationExchange exchange = new V5BinaryAuthenticationDriver(body, services(blocking, scheduler))
                    .newAuthenticationExchange("broker.example.com");

            // Never throws synchronously: the failure surfaces through the returned future.
            CompletableFuture<AuthData> future = exchange.getAuthDataAsync();
            assertThatThrownBy(() -> future.get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseMessage("token endpoint down");
        } finally {
            blocking.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    public void withoutBoundExecutorRunsInlineOnCaller() throws Exception {
        AtomicReference<String> supplierThread = new AtomicReference<>();
        OAuth2AuthenticationV5 body = new OAuth2AuthenticationV5(() -> {
            supplierThread.set(Thread.currentThread().getName());
            return "the-access-token";
        });
        // No services bound (plugin used outside a client) -> degraded inline fetch on the caller thread.
        AuthenticationExchange exchange = new V5BinaryAuthenticationDriver(body)
                .newAuthenticationExchange("broker.example.com");

        AuthData data = exchange.getAuthDataAsync().get(5, SECONDS);
        assertThat(new String(data.getBytes(), UTF_8)).isEqualTo("the-access-token");
        assertThat(supplierThread.get()).isEqualTo(Thread.currentThread().getName());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
