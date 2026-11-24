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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OAuth2OnlyTlsFactoryTest {

    @Test
    public void plaintextClientAcceptsOAuth2OnlyFactory() throws Exception {
        OAuth2OnlyFactory factory = new OAuth2OnlyFactory();
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .tlsFactory(factory)
                .build()) {
            ClientConfigurationData conf = ((PulsarClientV5) client).v4Client().getConfiguration();
            assertThat(conf.isUseTls()).as("the broker transport stays plaintext").isFalse();
            assertThat(conf.getTlsFactory()).as("the client retains the adopted factory").isSameAs(factory);
            assertThat(factory.initialized).hasValue(1);
            assertThat(factory.closed).hasValue(0);
        }
        assertThat(factory.closed).as("the client closes the adopted factory exactly once").hasValue(1);
    }

    @DataProvider
    public Object[][] tlsTransports() {
        return new Object[][] {
                {"pulsar+ssl://localhost:6651", false},
                {"pulsar://localhost:6650", true}
        };
    }

    @Test(dataProvider = "tlsTransports")
    public void tlsTransportStillRequiresClientDefault(String serviceUrl, boolean configureTransportPolicy)
            throws Exception {
        OAuth2OnlyFactory factory = new OAuth2OnlyFactory();
        PulsarClientBuilder builder = PulsarClient.builder().serviceUrl(serviceUrl).tlsFactory(factory);
        if (configureTransportPolicy) {
            builder.tlsPolicy(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder().build());
        }

        assertThatThrownBy(() -> {
            try (PulsarClient ignored = builder.build()) {
                // A missing transport context must fail during build(), before any connection is attempted.
            }
        }).isInstanceOf(PulsarClientException.InvalidConfigurationException.class)
                .rootCause().isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("supplied no Netty SslContext")
                .hasMessageContaining(TlsPurpose.CLIENT_DEFAULT.toString());
        assertThat(factory.initialized).hasValue(1);
        assertThat(factory.closed).as("a failed build releases the factory exactly once").hasValue(1);
    }

    private static final class OAuth2OnlyFactory implements PulsarTlsFactory {
        private final SSLContext oauthContext = SSLContext.getDefault();
        private final AtomicInteger initialized = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();

        private OAuth2OnlyFactory() throws Exception {
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            initialized.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            if (!TlsPurpose.CLIENT_OAUTH2.equals(purpose) || instanceClass != SSLContext.class) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return CompletableFuture.completedFuture(Optional.of(new TlsHandle<T>() {
                @Override
                public T get() {
                    return instanceClass.cast(oauthContext);
                }

                @Override
                public void dispose() {
                }
            }));
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return createInstance(purpose, instanceClass).thenApply(handle -> {
                handle.ifPresent(value -> onLoadOrReload.accept(value.get()));
                return handle;
            });
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }
    }
}
