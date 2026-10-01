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
package org.apache.pulsar.client.impl.tls;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsEndpoint;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.Test;

/**
 * PIP-478 (finding C12): a custom broker-client {@link PulsarTlsFactory} is driven by the client transport,
 * which hardcodes {@link TlsPurpose#CLIENT_DEFAULT}, but the stable-API contract resolves a server component's
 * outbound Pulsar-client traffic under the fixed {@link TlsPurpose#BROKER_CLIENT} purpose. The wrapper
 * {@link ClientTlsFactorySupport#wrapBrokerClientPurpose} installed on the broker-client instantiation sites
 * must translate {@code CLIENT_DEFAULT} to {@code BROKER_CLIENT} across all {@code createInstance} overloads so
 * a factory serving only {@code BROKER_CLIENT} is reached, while other purposes pass through untouched.
 */
public class BrokerClientPurposeFactoryTest {

    @Test
    public void oneShotTranslatesClientDefaultToBrokerClient() throws Exception {
        BrokerClientOnlyFactory delegate = new BrokerClientOnlyFactory();

        // Control: the unwrapped factory serves only BROKER_CLIENT, so a raw CLIENT_DEFAULT request is empty.
        assertThat(delegate.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class).get())
                .as("unwrapped factory has no CLIENT_DEFAULT material")
                .isEmpty();

        PulsarTlsFactory wrapped = ClientTlsFactorySupport.wrapBrokerClientPurpose(delegate);
        Optional<TlsHandle<SslContext>> handle =
                wrapped.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class).get();

        assertThat(handle).as("wrapped CLIENT_DEFAULT resolves the BROKER_CLIENT material").isPresent();
        assertThat(delegate.lastPurpose)
                .as("the wrapper delegates CLIENT_DEFAULT as BROKER_CLIENT")
                .isEqualTo(TlsPurpose.BROKER_CLIENT);
    }

    @Test
    public void subscribingOverloadTranslatesClientDefaultToBrokerClient() throws Exception {
        BrokerClientOnlyFactory delegate = new BrokerClientOnlyFactory();
        PulsarTlsFactory wrapped = ClientTlsFactorySupport.wrapBrokerClientPurpose(delegate);

        AtomicBoolean delivered = new AtomicBoolean();
        Optional<TlsHandle<SslContext>> handle = wrapped
                .createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class, ctx -> delivered.set(ctx != null))
                .get();

        assertThat(handle).as("subscribing CLIENT_DEFAULT resolves the BROKER_CLIENT material").isPresent();
        assertThat(delegate.lastPurpose)
                .as("the subscribing overload delegates CLIENT_DEFAULT as BROKER_CLIENT")
                .isEqualTo(TlsPurpose.BROKER_CLIENT);
        assertThat(delivered).as("the reload callback received the instance").isTrue();
    }

    @Test
    public void endpointOverloadTranslatesClientDefaultToBrokerClient() throws Exception {
        BrokerClientOnlyFactory delegate = new BrokerClientOnlyFactory();
        PulsarTlsFactory wrapped = ClientTlsFactorySupport.wrapBrokerClientPurpose(delegate);

        Optional<TlsHandle<SslContext>> handle = wrapped
                .createInstance(TlsPurpose.CLIENT_DEFAULT, new TlsEndpoint("broker.example", 6651), SslContext.class)
                .get();

        assertThat(handle).as("endpoint-form CLIENT_DEFAULT resolves the BROKER_CLIENT material").isPresent();
        assertThat(delegate.lastPurpose)
                .as("the endpoint overload delegates CLIENT_DEFAULT as BROKER_CLIENT")
                .isEqualTo(TlsPurpose.BROKER_CLIENT);
    }

    @Test
    public void nonClientDefaultPurposePassesThroughUntouched() throws Exception {
        BrokerClientOnlyFactory delegate = new BrokerClientOnlyFactory();
        PulsarTlsFactory wrapped = ClientTlsFactorySupport.wrapBrokerClientPurpose(delegate);

        wrapped.createInstance(TlsPurpose.CLIENT_OAUTH2, SslContext.class).get();

        assertThat(delegate.lastPurpose)
                .as("a non-CLIENT_DEFAULT purpose is not translated")
                .isEqualTo(TlsPurpose.CLIENT_OAUTH2);
    }

    @Test
    public void initializeAndClosePassThrough() {
        BrokerClientOnlyFactory delegate = new BrokerClientOnlyFactory();
        PulsarTlsFactory wrapped = ClientTlsFactorySupport.wrapBrokerClientPurpose(delegate);

        wrapped.initialize(null);
        wrapped.close();

        assertThat(delegate.initialized).as("initialize is delegated").isTrue();
        assertThat(delegate.closed).as("close is delegated").isTrue();
    }

    /** A {@link PulsarTlsFactory} that serves an {@link SslContext} only for {@link TlsPurpose#BROKER_CLIENT}. */
    private static final class BrokerClientOnlyFactory implements PulsarTlsFactory {
        volatile TlsPurpose lastPurpose;
        volatile boolean initialized;
        volatile boolean closed;

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            initialized = true;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass) {
            lastPurpose = purpose;
            if (!TlsPurpose.BROKER_CLIENT.equals(purpose) || instanceClass != SslContext.class) {
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
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return createInstance(purpose, instanceClass).thenApply(opt -> {
                opt.ifPresent(handle -> onLoadOrReload.accept(handle.get()));
                return opt;
            });
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
