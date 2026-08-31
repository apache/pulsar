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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
 * executor instead of running it on the Netty event loop. When no services are bound (a connection opened
 * outside a {@code PulsarClient} — the proxy's broker connections, embedders, tests), the context reports
 * no services at all, and a body that holds its credential in memory simply resolves inline. The one
 * component that cannot do that is the legacy v4 bridge, which borrows {@link #sharedBlockingExecutor()}.
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
     * The blocking executor of last resort, for work that must not run on the calling thread when no
     * client-owned executor is available. See {@link SharedBlockingExecutor}.
     *
     * @return the shared blocking executor
     */
    static Executor sharedBlockingExecutor() {
        return SharedBlockingExecutor.INSTANCE;
    }

    /**
     * Resolve the executor that potentially-blocking authentication work must run on: the one the owning
     * component lent, or the shared fallback pool when it lent none. Never the caller thread.
     *
     * <p>Exposed for a caller that has to run more than one step on the same executor — the deprecated v4
     * HTTP composition runs the credential resolution and the {@code newRequestHeader} continuation there —
     * and so needs the resolved instance rather than only {@link #supplyBlocking}'s internal choice.
     *
     * @param blockingExecutor the bound blocking executor, or {@code null} if none was bound
     * @return the executor to run the blocking work on; never {@code null}
     */
    public static Executor blockingExecutorOrShared(Executor blockingExecutor) {
        return blockingExecutor != null ? blockingExecutor : sharedBlockingExecutor();
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
     * otherwise it runs on the shared fallback pool. Failures are always reported through the returned
     * future rather than thrown synchronously.
     *
     * <p>When no executor is bound — a connection opened outside a {@code PulsarClient}, such as the
     * proxy's broker connections — the work goes to {@link #sharedBlockingExecutor()} rather than running
     * inline. Running inline there would put the credential fetch on the caller thread, which on that path
     * is a Netty event loop: a body whose credential costs a token endpoint round trip, a ZTS role-token
     * fetch, a GSSAPI exchange with the KDC, or simply reading a token file would stall every connection
     * multiplexed on that loop.
     *
     * @param blockingExecutor the bound blocking executor, or {@code null} if none was bound
     * @param task             the blocking credential computation
     * @param <T>              the result type
     * @return a future of the result; never throws synchronously
     */
    public static <T> CompletableFuture<T> supplyBlocking(Executor blockingExecutor, Supplier<T> task) {
        Executor executor = blockingExecutorOrShared(blockingExecutor);
        try {
            return CompletableFuture.supplyAsync(task, executor);
        } catch (Throwable t) {
            // A rejecting executor throws synchronously; never throw from a future-returning method —
            // surface it through the future instead.
            return CompletableFuture.failedFuture(t);
        }
    }

    /**
     * The blocking executor used when no client services are bound.
     *
     * <p>Not every connection is opened by a {@code PulsarClient}: the proxy builds broker connections
     * straight from a configuration, and so do embedders and tests. Those callers have no client-owned
     * executor to lend, but a bridged v4 plugin still must not run its credential call on the caller
     * thread — which on that path is a Netty event loop. Falling back to running inline would reintroduce
     * exactly the stall PIP-478 exists to remove, and refusing to run at all would break connections v4
     * made fine, so the library keeps one shared pool for them.
     *
     * <p>Only the v4 bridge borrows it. A v5-native body is handed no executor at all when none is bound,
     * so a credential it already holds in memory still resolves inline, without a thread hop per connect.
     *
     * <p>It costs nothing when unused: no core threads, daemon threads that retire after a minute of idle.
     * Held in a holder class so it is created on first use rather than on class load.
     */
    static final class SharedBlockingExecutor {
        private static final int MAX_THREADS = 8;
        static final Executor INSTANCE = create();

        private static Executor create() {
            // Queue rather than reject. A SynchronousQueue with the default abort policy would fail the
            // ninth concurrent credential call outright, and the caller of a rejected call is a connection
            // attempt: a proxy reconnect storm with a legacy plugin would turn into failed connections
            // instead of slower ones. Work beyond the thread ceiling therefore waits.
            ThreadPoolExecutor executor = new ThreadPoolExecutor(MAX_THREADS, MAX_THREADS, 60L,
                    TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
                        Thread thread = new Thread(runnable, "pulsar-auth-blocking-shared");
                        thread.setDaemon(true);
                        return thread;
                    });
            // With a queue in play the pool would otherwise hold its core threads forever; letting them
            // time out is what keeps an unused pool free.
            executor.allowCoreThreadTimeOut(true);
            return executor;
        }
    }

    /**
     * The scheduler handed to a plugin when no client bound services. Like the blocking pool it is a
     * process-lifetime daemon pool, so a plugin that schedules a credential refresh outside a client gets a
     * working scheduler rather than an NPE. One thread is enough: the SPI's contract is that scheduled work
     * hands the actual blocking off to {@code blockingExecutor()}.
     */
    static final class SharedScheduler {
        static final ScheduledExecutorService INSTANCE = create();

        private static ScheduledExecutorService create() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
                Thread thread = new Thread(runnable, "pulsar-auth-scheduler-shared");
                thread.setDaemon(true);
                return thread;
            });
            executor.setRemoveOnCancelPolicy(true);
            // Nothing owns this pool, so an idle one must not hold a thread for the life of the process.
            executor.setKeepAliveTime(60L, TimeUnit.SECONDS);
            executor.allowCoreThreadTimeOut(true);
            return executor;
        }
    }

    private static final class InitContext implements AuthenticationInitContext {
        private final String clientInstanceId;

        InitContext(String clientInstanceId) {
            this.clientInstanceId = clientInstanceId;
        }

        @Override
        public PulsarHttpClientFactory httpClientFactory() {
            // The one accessor that stays null when no client bound services, and the SPI says so: an HTTP
            // client factory cannot be conjured without the client's TLS configuration and lifecycle, and
            // handing back a bare one would give a plugin an HTTP client that ignores the deployment's trust
            // settings. A plugin needing HTTP outside a client supplies its own.
            return null;
        }

        @Override
        public ScheduledExecutorService scheduler() {
            // Never null, for the same reason as blockingExecutor(): the SPI invites a plugin to schedule
            // credential refresh here, so null makes every third-party plugin NPE or roll its own pool.
            return SharedScheduler.INSTANCE;
        }

        @Override
        public Executor blockingExecutor() {
            // Never null: the SPI tells a plugin to off-load its blocking work here, so handing it null
            // would make every third-party plugin either NPE or defensively do the one thing the contract
            // forbids — run credential I/O on the calling thread. The built-ins are only safe from that
            // because they funnel through supplyBlocking, which substitutes this same pool.
            return sharedBlockingExecutor();
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
            // Same fallback as the unbound context, and for the same reason: a component may bind services
            // while leaving an accessor it has no use for null — the admin binds no scheduler, because
            // nothing on its own path schedules periodic authentication work. Without this, binding
            // *partial* services would be worse for a plugin than binding none at all, since the unbound
            // context guarantees non-null. The SPI's "never null" contract now holds on every path.
            ScheduledExecutorService scheduler = services.scheduler();
            return scheduler == null ? SharedScheduler.INSTANCE : scheduler;
        }

        @Override
        public Executor blockingExecutor() {
            Executor executor = services.blockingExecutor();
            return executor == null ? sharedBlockingExecutor() : executor;
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
