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

        static void reset() {
            INSTANTIATED.set(0);
            INITIALIZED.set(0);
            CLOSED.set(0);
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
