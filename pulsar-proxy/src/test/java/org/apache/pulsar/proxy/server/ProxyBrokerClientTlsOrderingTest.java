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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: {@code ProxyService.start()} must build the broker-client TLS state
 * ({@code brokerClientTlsFactory} / {@code brokerClientSslContext} / {@code lookupClientTlsFactory})
 * <em>before</em> it binds its listeners. It used to bind first, so a connection accepted in that window
 * reached {@code DirectProxyHandler} with a null {@code brokerClientSslContext}, or took a null factory from
 * {@code ProxyConnection.createClientConfiguration}. The window is specific to this series — under PIP-337 the
 * proxy held no broker-client TLS state at all, and {@code DirectProxyHandler} built its factory lazily per
 * remote host, so nothing could observe a half-built state.
 *
 * <p>Racing an accept against the bind would be flaky, so the ordering is observed directly instead: a custom
 * broker-client factory records, at {@code initialize(...)} time, whether the proxy has bound a listener yet.
 * Building before the binds means it must see none. Reverting the ordering makes both ports visible and fails
 * this test.
 */
public class ProxyBrokerClientTlsOrderingTest extends MockedPulsarServiceBaseTest {

    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        OrderingProbeTlsFactory.reset();
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setAdvertisedAddress("localhost");
        proxyConfig.setTlsCertificateFilePath(PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(PROXY_KEY_FILE_PATH);
        // The broker-client leg is what start() must build before binding.
        proxyConfig.setTlsEnabledWithBroker(true);
        proxyConfig.setBrokerClientTlsFactoryClassName(OrderingProbeTlsFactory.class.getName());
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper)))
                .when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();
        // The probe reads the ports through this reference, from inside start().
        OrderingProbeTlsFactory.proxyService = proxyService;
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        if (proxyService != null) {
            proxyService.close();
        }
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
        OrderingProbeTlsFactory.reset();
    }

    @Test
    public void brokerClientTlsIsBuiltBeforeAnyListenerIsBound() throws Exception {
        proxyService.start();

        assertThat(OrderingProbeTlsFactory.initialized)
                .as("the broker-client factory must be built and initialized during start()").isTrue();
        assertThat(OrderingProbeTlsFactory.plaintextPortAtInitialize)
                .as("the plaintext listener must not be bound yet when the broker-client TLS is built — a "
                        + "connection accepted in that window would read a null brokerClientSslContext")
                .isEmpty();
        assertThat(OrderingProbeTlsFactory.tlsPortAtInitialize)
                .as("nor the TLS listener").isEmpty();

        // Sanity: the binds do happen, so the assertions above are about ordering and not about a proxy that
        // never started listening.
        assertThat(proxyService.getListenPort()).isPresent();
        assertThat(proxyService.getListenPortTls()).isPresent();
    }

    /**
     * Records which listeners the proxy had bound at the moment its broker-client TLS factory was
     * initialized. Serves {@link TlsPurpose#BROKER_CLIENT} and the transport's translated request so that
     * {@code start()} completes normally.
     */
    public static class OrderingProbeTlsFactory implements PulsarTlsFactory {

        static volatile ProxyService proxyService;
        static volatile Optional<Integer> plaintextPortAtInitialize = Optional.empty();
        static volatile Optional<Integer> tlsPortAtInitialize = Optional.empty();
        static volatile boolean initialized;

        static void reset() {
            proxyService = null;
            plaintextPortAtInitialize = Optional.empty();
            tlsPortAtInitialize = Optional.empty();
            initialized = false;
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            ProxyService service = proxyService;
            if (service != null) {
                plaintextPortAtInitialize = service.getListenPort();
                tlsPortAtInitialize = service.getListenPortTls();
            }
            initialized = true;
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
        }
    }
}
