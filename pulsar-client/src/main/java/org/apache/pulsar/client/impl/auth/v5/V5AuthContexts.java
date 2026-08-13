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

import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.http.PulsarHttpClientFactory;

/**
 * Minimal {@link AuthenticationInitContext} / {@link AuthenticationCallContext} implementations shared
 * by the built-in v4 auth plugins, which drive their v5-native bodies over the binary transport
 * (PIP-478). These are lightweight value holders; the per-call context carries a
 * {@link ConcurrentHashMap}-backed state slot keyed by class.
 *
 * <p>The client's real framework services (scheduler, bounded blocking executor,
 * HTTP client factory, client instance id) are late-bound into the init context via
 * {@link ClientAuthenticationServices}
 * — so a credential-fetching body (OAuth2, Athenz) can off-load its blocking fetch onto the blocking
 * executor instead of running it on the Netty event loop. When no services are bound (a plugin used
 * outside a client), the context degrades to {@code null} services and the body falls back to inline
 * computation.
 */
public final class V5AuthContexts {

    private V5AuthContexts() {
    }

    /**
     * Build an init context from the client's late-bound framework services. When
     * {@code services} is {@code null} the context exposes no services (scheduler / blocking executor /
     * HTTP client factory are all {@code null}), matching the pre-binding behaviour.
     *
     * @param services the late-bound client services, or {@code null} if none were bound
     * @param clientInstanceId a stable id for logging correlation when no services are bound
     * @return a new init context
     */
    public static AuthenticationInitContext initContext(ClientAuthenticationServices services,
            String clientInstanceId) {
        return services == null
                ? new InitContext(clientInstanceId)
                : new BoundInitContext(services);
    }

    /**
     * @param brokerHost the broker host
     * @return a new binary-protocol call context with a fresh state slot
     */
    public static AuthenticationCallContext binaryCallContext(String brokerHost) {
        return new BinaryCallContext(brokerHost);
    }

    /**
     * Run a potentially-blocking credential fetch off the caller thread (PIP-478): a
     * credential-fetching body (OAuth2, Athenz) whose supplier performs network / disk I/O must not run
     * that fetch on the Netty event loop. When a bounded blocking executor is bound, the task runs there;
     * otherwise it runs inline (the degraded pre-binding behaviour), still reporting failures through the
     * returned future rather than throwing synchronously.
     *
     * @param blockingExecutor the bound blocking executor, or {@code null} if none was bound
     * @param task             the blocking credential computation
     * @param <T>              the result type
     * @return a future of the result; never throws synchronously
     */
    public static <T> CompletableFuture<T> supplyBlocking(Executor blockingExecutor, Supplier<T> task) {
        if (blockingExecutor != null) {
            try {
                return CompletableFuture.supplyAsync(task, blockingExecutor);
            } catch (Throwable t) {
                // A saturated bounded pool with an AbortPolicy rejects synchronously (RejectedExecutionException);
                // never throw from a future-returning method — surface it through the future instead.
                return CompletableFuture.failedFuture(t);
            }
        }
        try {
            return CompletableFuture.completedFuture(task.get());
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private static final class InitContext implements AuthenticationInitContext {
        private final String clientInstanceId;

        InitContext(String clientInstanceId) {
            this.clientInstanceId = clientInstanceId;
        }

        @Override
        public PulsarHttpClientFactory httpClientFactory() {
            return null;
        }

        @Override
        public ScheduledExecutorService scheduler() {
            return null;
        }

        @Override
        public Executor blockingExecutor() {
            return null;
        }

        @Override
        public Clock clock() {
            return Clock.systemUTC();
        }

        @Override
        public OpenTelemetry openTelemetry() {
            return OpenTelemetry.noop();
        }

        @Override
        public String clientInstanceId() {
            return clientInstanceId;
        }
    }

    private static final class BoundInitContext implements AuthenticationInitContext {
        private final ClientAuthenticationServices services;

        BoundInitContext(ClientAuthenticationServices services) {
            this.services = services;
        }

        @Override
        public PulsarHttpClientFactory httpClientFactory() {
            return services.httpClientFactory();
        }

        @Override
        public ScheduledExecutorService scheduler() {
            return services.scheduler();
        }

        @Override
        public Executor blockingExecutor() {
            return services.blockingExecutor();
        }

        @Override
        public Clock clock() {
            Clock clock = services.clock();
            return clock == null ? Clock.systemUTC() : clock;
        }

        @Override
        public OpenTelemetry openTelemetry() {
            OpenTelemetry otel = services.openTelemetry();
            return otel == null ? OpenTelemetry.noop() : otel;
        }

        @Override
        public String clientInstanceId() {
            return services.clientInstanceId();
        }
    }

    private static final class BinaryCallContext implements AuthenticationCallContext {
        private final String brokerHost;
        private final ConcurrentHashMap<Class<?>, Object> stateSlot = new ConcurrentHashMap<>();

        BinaryCallContext(String brokerHost) {
            this.brokerHost = brokerHost;
        }

        @Override
        public String brokerHost() {
            return brokerHost;
        }

        @Override
        public <T> Optional<T> getStateObject(Class<T> clazz) {
            return Optional.ofNullable(clazz.cast(stateSlot.get(clazz)));
        }

        @Override
        public <T> void setStateObject(Class<T> clazz, T value) {
            if (value == null) {
                stateSlot.remove(clazz);
            } else {
                stateSlot.put(clazz, value);
            }
        }
    }
}
