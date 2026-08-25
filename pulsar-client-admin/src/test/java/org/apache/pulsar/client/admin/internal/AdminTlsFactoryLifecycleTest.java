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
package org.apache.pulsar.client.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: a {@code PulsarAdmin} must construct, initialize and close its {@link PulsarTlsFactory} exactly
 * once.
 *
 * <p>This is not automatic, because an admin ends up with <em>two</em> {@code AsyncHttpConnector} instances:
 * the one {@code PulsarAdminImpl} builds directly, and the one Jersey creates lazily on the first request.
 * Each used to resolve its own factory, which meant a by-name custom factory was constructed and
 * {@code initialize()}d twice — against the SPI's "called once by the framework" — and a factory adopted from
 * the broker's admin attach had {@code close()} called on it twice. The v4 (PIP-337) code duplicated the
 * factory the same way, but never closed it, so only the duplication existed there and not the double close.
 *
 * <p>{@code AsyncHttpConnectorProvider} now resolves one factory per admin and owns it; the connectors take
 * their own subscriptions but never close it. Both tests below issue a request precisely to force the
 * Jersey-side connector into existence — without it the duplication is invisible.
 */
public class AdminTlsFactoryLifecycleTest {

    @BeforeMethod
    public void setUp() {
        CountingFactory.reset();
    }

    @Test
    public void aByNameFactoryIsConstructedInitializedAndClosedExactlyOncePerAdmin() throws Exception {
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("https://localhost:8443")
                .tlsFactoryClassName(CountingFactory.class.getName())
                .build();
        forceJerseyConnector(admin);

        assertThat(CountingFactory.INSTANTIATED).as("one factory per admin, not one per connector").hasValue(1);
        assertThat(CountingFactory.INITIALIZED).as("the SPI says initialize is called once").hasValue(1);

        admin.close();
        assertThat(CountingFactory.CLOSED).as("and it is closed exactly once").hasValue(1);
    }

    @Test
    public void anAdoptedFactoryIsInitializedAndClosedExactlyOnce() throws Exception {
        // How the broker's admin attach hands in a factory it built (PulsarService.applyBrokerClientTlsFactoryToAdmin).
        CountingFactory adopted = new CountingFactory();
        PulsarAdminBuilder builder = PulsarAdmin.builder().serviceHttpUrl("https://localhost:8443");
        ((PulsarAdminBuilderImpl) builder).getConf().setTlsFactory(adopted);
        PulsarAdmin admin = builder.build();
        forceJerseyConnector(admin);

        assertThat(CountingFactory.INSTANTIATED).as("the adopted instance is used as-is").hasValue(1);
        assertThat(CountingFactory.INITIALIZED).hasValue(1);

        admin.close();
        assertThat(CountingFactory.CLOSED)
                .as("closing the admin must not close the adopted factory twice").hasValue(1);
    }

    /**
     * PIP-478: and exactly once means the adopted instance belongs to the admin that took it.
     *
     * <p>Nothing stopped a second admin being built from the same builder — the configuration copy
     * {@code build()} makes is shallow, so it carries the same instance — and that admin's provider would
     * {@code initialize()} it a second time and queue a second {@code close()}, which is the very invariant
     * the tests above pin. There is no public {@code tlsFactory(...)} on the admin builder; the seam is the
     * one the broker and the functions worker use to route their admin clients onto a custom factory, and
     * both of them build exactly one admin from a fresh builder. This keeps that the only way to use it.
     */
    @Test
    public void anAdoptedFactoryIsHandedToOneAdminOnly() throws Exception {
        CountingFactory adopted = new CountingFactory();
        PulsarAdminBuilder builder = PulsarAdmin.builder().serviceHttpUrl("https://localhost:8443");
        ((PulsarAdminBuilderImpl) builder).getConf().setTlsFactory(adopted);

        PulsarAdmin admin = builder.build();
        try {
            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already been adopted");
            assertThatThrownBy(() -> builder.clone().build())
                    .as("and cloning the builder must not be a way around it — the copy carries the instance")
                    .isInstanceOf(IllegalStateException.class);
            assertThat(CountingFactory.INITIALIZED).as("neither rejected build may re-initialize it").hasValue(1);
        } finally {
            admin.close();
        }
        assertThat(CountingFactory.CLOSED).as("the admin that adopted it closes it, once").hasValue(1);
    }

    /**
     * PIP-478: the two sides of what "handed over" means.
     *
     * <p>A build that fails <em>before</em> the constructor — the missing service URL below — has not
     * touched the factory, so the builder must still accept it. A build that fails <em>after</em> the
     * provider resolved it has initialized it, and the failure path closed it (that is what
     * {@link #aFailedAdminBuildDoesNotLeakTheResolvedFactory} pins), so the builder must refuse it: the
     * alternative is handing the next admin a closed factory instead of telling the caller.
     */
    @Test
    public void aBuildThatReachedTheFactorySpendsItAndOneThatDidNotDoesNot() throws Exception {
        CountingFactory adopted = new CountingFactory();
        PulsarAdminBuilder builder = PulsarAdmin.builder();
        ((PulsarAdminBuilderImpl) builder).getConf().setTlsFactory(adopted);

        assertThatThrownBy(builder::build)
                .as("no service URL: rejected before the constructor that adopts")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Service URL needs to be specified");
        assertThat(CountingFactory.INITIALIZED).as("so nothing consumed the factory").hasValue(0);

        // Now let the build reach the factory and fail there: it refuses to serve CLIENT_DEFAULT.
        CountingFactory.FAIL_CLIENT_DEFAULT.set(true);
        builder.serviceHttpUrl("https://localhost:8443");
        assertThatThrownBy(builder::build).as("the build must fail at the factory for this to be the case")
                .isNotNull();
        assertThat(CountingFactory.INITIALIZED).as("but this time it was initialized").hasValue(1);
        assertThat(CountingFactory.CLOSED).as("and closed on the way out").hasValue(1);

        CountingFactory.FAIL_CLIENT_DEFAULT.set(false);
        assertThatThrownBy(builder::build)
                .as("so the instance is spent, even though no admin exists")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already been adopted");
    }

    /**
     * PIP-478: a build that failed before reaching the factory hands over nothing, so the same factory must
     * still be usable.
     *
     * <p>The guard used to record the instance before the construction, so any failure on the way to the TLS
     * factory — a blank service URL is rejected by {@code PulsarAdminImpl}'s first statement, well before the
     * provider that adopts — poisoned the builder against a factory that was never initialized and never
     * closed.
     */
    @Test
    public void aFailedBuildLeavesTheAdoptedFactoryUsable() throws Exception {
        CountingFactory adopted = new CountingFactory();
        PulsarAdminBuilder builder = PulsarAdmin.builder();
        ((PulsarAdminBuilderImpl) builder).getConf().setTlsFactory(adopted);

        assertThatThrownBy(builder::build)
                .as("no service URL: rejected before the TLS factory is looked at")
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(CountingFactory.INITIALIZED).as("nothing consumed the factory").hasValue(0);
        assertThat(CountingFactory.CLOSED).hasValue(0);

        builder.serviceHttpUrl("https://localhost:8443");
        PulsarAdmin admin = builder.build();
        try {
            assertThat(CountingFactory.INITIALIZED).as("the retry adopts the same instance").hasValue(1);
        } finally {
            admin.close();
        }
        assertThat(CountingFactory.CLOSED).hasValue(1);
    }

    /**
     * PIP-478: and cloning is not a way around the rule in either order.
     *
     * <p>The record of what has been handed over is shared with every clone rather than copied into it.
     * Copied, a builder cloned <em>before</em> its first build would start with an empty record, and both
     * copies would then adopt the same instance — the configuration copy does not separate them, because
     * {@code clone()} is shallow.
     */
    @Test
    public void cloningBeforeABuildDoesNotDuplicateTheAdoption() throws Exception {
        CountingFactory adopted = new CountingFactory();
        PulsarAdminBuilder builder = PulsarAdmin.builder().serviceHttpUrl("https://localhost:8443");
        ((PulsarAdminBuilderImpl) builder).getConf().setTlsFactory(adopted);

        PulsarAdminBuilder copy = builder.clone();
        PulsarAdmin admin = builder.build();
        try {
            assertThatThrownBy(copy::build)
                    .as("the clone carries the same instance, so it must see that it is spent")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already been adopted");
            assertThat(CountingFactory.INITIALIZED).hasValue(1);
        } finally {
            admin.close();
        }
        assertThat(CountingFactory.CLOSED).hasValue(1);
    }

    @Test
    public void aFailedAdminBuildDoesNotLeakTheResolvedFactory() throws Exception {
        // close() is unreachable when the constructor throws, so whatever the provider resolved before the
        // failure has to be released on the way out. The factory below fails the build by refusing to serve
        // CLIENT_DEFAULT for an https admin URL, which is where the eager context acquisition throws.
        CountingFactory.FAIL_CLIENT_DEFAULT.set(true);
        assertThatThrownBy(() -> PulsarAdmin.builder()
                .serviceHttpUrl("https://localhost:8443")
                .tlsFactoryClassName(CountingFactory.class.getName())
                .build())
                .as("the build must fail for this test to be about the failure path").isNotNull();

        assertThat(CountingFactory.INSTANTIATED).as("the factory was resolved before the failure").hasValue(1);
        assertThat(CountingFactory.CLOSED)
                .as("and must be closed on the way out: a failed build leaves no handle to close it later, so "
                        + "it would otherwise leak the factory and its non-daemon rotation thread")
                .hasValue(1);
    }

    /** Jersey creates its connector lazily on the first request; the request itself is expected to fail. */
    private static void forceJerseyConnector(PulsarAdmin admin) {
        try {
            admin.clusters().getClusters();
        } catch (Exception expected) {
            // No broker is listening — reaching the transport is all this needs.
        }
    }

    /** Counts its own construction, initialization and closing. */
    public static class CountingFactory implements PulsarTlsFactory {

        static final AtomicInteger INSTANTIATED = new AtomicInteger();
        static final AtomicInteger INITIALIZED = new AtomicInteger();
        static final AtomicInteger CLOSED = new AtomicInteger();
        static final java.util.concurrent.atomic.AtomicBoolean FAIL_CLIENT_DEFAULT =
                new java.util.concurrent.atomic.AtomicBoolean();

        static void reset() {
            INSTANTIATED.set(0);
            INITIALIZED.set(0);
            CLOSED.set(0);
            FAIL_CLIENT_DEFAULT.set(false);
        }

        public CountingFactory() {
            INSTANTIATED.incrementAndGet();
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            INITIALIZED.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            if (FAIL_CLIENT_DEFAULT.get() && TlsPurpose.CLIENT_DEFAULT.equals(purpose)) {
                return CompletableFuture.failedFuture(new IllegalStateException("cannot serve CLIENT_DEFAULT"));
            }
            if (instanceClass != SslContext.class) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            try {
                SslContext context = SslContextBuilder.forClient().build();
                TlsHandle<T> handle = new TlsHandle<>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public T get() {
                        return (T) context;
                    }

                    @Override
                    public void dispose() {
                    }
                };
                return CompletableFuture.completedFuture(Optional.of(handle));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return createInstance(purpose, instanceClass).thenApply(opt -> {
                opt.ifPresent(handle -> onLoadOrReload.accept(handle.get()));
                return opt;
            });
        }

        @Override
        public void close() {
            CLOSED.incrementAndGet();
        }
    }
}
