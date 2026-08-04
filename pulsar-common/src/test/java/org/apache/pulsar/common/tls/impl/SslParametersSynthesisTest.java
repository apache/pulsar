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
package org.apache.pulsar.common.tls.impl;

import static org.apache.pulsar.common.tls.impl.TlsTestSupport.handshake;
import static org.apache.pulsar.common.tls.impl.TlsTestSupport.resource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import org.apache.pulsar.common.util.tls.JdkSslContexts;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.Test;

/**
 * PIP-478: a factory that supplies only a JDK {@code SSLContext} may accompany it with a
 * {@code javax.net.ssl.SSLParameters} companion carrying the engine-level baseline a bare {@code SSLContext}
 * cannot express (protocols, cipher suites, endpoint identification, server client-auth mode). This exercises
 * the framework's composition of that companion into the synthesized Netty context — both directly through
 * {@link TlsContexts} and end-to-end through {@link TlsContextAcquisition} — per the documented merge order.
 */
public class SslParametersSynthesisTest {

    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String SERVER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String SERVER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    // A client identity with the clientAuth EKU, trusted by the shared CA above.
    private static final String CLIENT_CERT = resource("certificate-authority/client-keys/admin.cert.pem");
    private static final String CLIENT_KEY = resource("certificate-authority/client-keys/admin.key-pk8.pem");

    private static SSLContext serverJdkContext() throws Exception {
        return JdkSslContexts.createSslContext(false, CA, SERVER_CERT, SERVER_KEY, null);
    }

    private static SSLContext clientJdkContextWithCert() throws Exception {
        return JdkSslContexts.createSslContext(false, CA, CLIENT_CERT, CLIENT_KEY, null);
    }

    private static SSLContext clientJdkContextTrustOnly() throws Exception {
        return JdkSslContexts.createSslContext(false, PemReader.loadCertificatesFromPemFile(CA), null);
    }

    // ---- (a) protocol restriction from the factory companion ----

    @Test
    public void factoryProtocolsRestrictSynthesizedClientEngine() throws Exception {
        SSLParameters baseline = new SSLParameters();
        baseline.setProtocols(new String[] {"TLSv1.2"});

        SslContext context = TlsContexts.synthesizeNettyClientFromJdk(clientJdkContextTrustOnly(), false, baseline);
        SSLEngine engine = context.newEngine(ByteBufAllocator.DEFAULT);

        assertThat(engine.getEnabledProtocols()).containsExactly("TLSv1.2");
    }

    @Test
    public void factoryProtocolRestrictionHonoredAtHandshake() throws Exception {
        // Matching TLSv1.2-only baselines on both ends complete the handshake...
        SslContext client = TlsContexts.synthesizeNettyClientFromJdk(
                clientJdkContextTrustOnly(), false, protocols("TLSv1.2"));
        SslContext server = TlsContexts.synthesizeNettyServerFromJdk(serverJdkContext(), false, protocols("TLSv1.2"));
        handshake(client.newEngine(ByteBufAllocator.DEFAULT), server.newEngine(ByteBufAllocator.DEFAULT));

        // ...but a TLSv1.3-only client against a TLSv1.2-only server has no shared protocol and fails.
        SslContext client13 = TlsContexts.synthesizeNettyClientFromJdk(
                clientJdkContextTrustOnly(), false, protocols("TLSv1.3"));
        SslContext server12 = TlsContexts.synthesizeNettyServerFromJdk(serverJdkContext(), false, protocols("TLSv1.2"));
        assertThatThrownBy(() -> handshake(client13.newEngine(ByteBufAllocator.DEFAULT),
                server12.newEngine(ByteBufAllocator.DEFAULT)))
                .isInstanceOf(SSLException.class);
    }

    // ---- default protocol floor: unset -> {TLSv1.3, TLSv1.2}, not the provider default ----

    @Test
    public void synthesizedClientWithoutCompanionPinsDefaultProtocols() throws Exception {
        SslContext context = TlsContexts.synthesizeNettyClientFromJdk(clientJdkContextTrustOnly(), false, null);
        assertThat(context.newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
                .containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
    }

    @Test
    public void synthesizedClientWithCompanionMissingProtocolsPinsDefault() throws Exception {
        // A companion that sets other members but no protocols still gets the default floor.
        SSLParameters companion = new SSLParameters();
        companion.setEndpointIdentificationAlgorithm("HTTPS");
        SslContext context = TlsContexts.synthesizeNettyClientFromJdk(clientJdkContextTrustOnly(), false, companion);
        assertThat(context.newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
                .containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
    }

    @Test
    public void synthesizedServerWithoutCompanionPinsDefaultProtocols() throws Exception {
        SslContext context = TlsContexts.synthesizeNettyServerFromJdk(serverJdkContext(), false, null);
        assertThat(context.newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
                .containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
    }

    // ---- (b) endpoint identification: factory value wins, else the consumer flag applies "HTTPS" ----

    @Test
    public void factoryEndpointIdentificationAlgorithmWinsOverConsumerFlag() throws Exception {
        SSLParameters baseline = new SSLParameters();
        baseline.setEndpointIdentificationAlgorithm("HTTPS");

        // Consumer hostname verification disabled, but the factory pins HTTPS -> the factory wins.
        SslContext context = TlsContexts.synthesizeNettyClientFromJdk(clientJdkContextTrustOnly(), false, baseline);
        SSLEngine engine = context.newEngine(ByteBufAllocator.DEFAULT);
        assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
    }

    @Test
    public void consumerFlagAppliesHttpsWhenFactoryLeavesAlgorithmNull() throws Exception {
        // Companion present but with no endpoint-identification algorithm -> the consumer flag applies "HTTPS".
        SslContext verifying = TlsContexts.synthesizeNettyClientFromJdk(
                clientJdkContextTrustOnly(), true, new SSLParameters());
        assertThat(verifying.newEngine(ByteBufAllocator.DEFAULT).getSSLParameters()
                .getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");

        // Companion present, algorithm null, consumer flag off -> no endpoint identification.
        SslContext plain = TlsContexts.synthesizeNettyClientFromJdk(
                clientJdkContextTrustOnly(), false, new SSLParameters());
        assertThat(plain.newEngine(ByteBufAllocator.DEFAULT).getSSLParameters()
                .getEndpointIdentificationAlgorithm()).isNull();

        // No companion at all, consumer flag on -> "HTTPS" (unchanged pre-companion behavior).
        SslContext noCompanion = TlsContexts.synthesizeNettyClientFromJdk(clientJdkContextTrustOnly(), true, null);
        assertThat(noCompanion.newEngine(ByteBufAllocator.DEFAULT).getSSLParameters()
                .getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
    }

    // ---- (c) server client-auth mode: factory companion is authoritative (merge rule 4) ----

    @Test
    public void serverNeedClientAuthFromFactoryEnforcedAtHandshake() throws Exception {
        // Consumer requests only OPTIONAL client auth (requireTrustedClientCert=false), but the factory
        // companion sets needClientAuth -> the factory wins and the server requires a client certificate.
        SSLParameters serverBaseline = protocols("TLSv1.2");
        serverBaseline.setNeedClientAuth(true);
        SslContext server = TlsContexts.synthesizeNettyServerFromJdk(serverJdkContext(), false, serverBaseline);
        assertThat(server.newEngine(ByteBufAllocator.DEFAULT).getNeedClientAuth()).isTrue();

        // A client that presents a trusted certificate completes the handshake.
        SslContext clientWithCert = TlsContexts.synthesizeNettyClientFromJdk(
                clientJdkContextWithCert(), false, protocols("TLSv1.2"));
        handshake(clientWithCert.newEngine(ByteBufAllocator.DEFAULT),
                TlsContexts.synthesizeNettyServerFromJdk(serverJdkContext(), false, needClientAuthTls12())
                        .newEngine(ByteBufAllocator.DEFAULT));

        // A client that presents no certificate is rejected by the client-auth-requiring server.
        SslContext clientNoCert = TlsContexts.synthesizeNettyClientFromJdk(
                clientJdkContextTrustOnly(), false, protocols("TLSv1.2"));
        assertThatThrownBy(() -> handshake(clientNoCert.newEngine(ByteBufAllocator.DEFAULT),
                TlsContexts.synthesizeNettyServerFromJdk(serverJdkContext(), false, needClientAuthTls12())
                        .newEngine(ByteBufAllocator.DEFAULT)))
                .isInstanceOf(SSLException.class);
    }

    @Test
    public void factoryClientAuthOverridesConsumerRequireBothWays() throws Exception {
        // Consumer would REQUIRE a trusted client cert, but the factory companion states NONE (need=want=false)
        // -> the factory is authoritative and the server requests no client certificate.
        SslContext server = TlsContexts.synthesizeNettyServerFromJdk(serverJdkContext(), true, new SSLParameters());
        SSLEngine engine = server.newEngine(ByteBufAllocator.DEFAULT);
        assertThat(engine.getNeedClientAuth()).isFalse();
        assertThat(engine.getWantClientAuth()).isFalse();
    }

    // ---- (f) the factory companion is mutable: a later mutation must not affect an acquired context ----

    @Test
    public void mutationAfterSupplyDoesNotAffectAcquiredContext() throws Exception {
        SSLParameters baseline = new SSLParameters();
        baseline.setProtocols(new String[] {"TLSv1.2"});

        SslContext context = TlsContexts.synthesizeNettyClientFromJdk(clientJdkContextTrustOnly(), false, baseline);

        // Mutate the factory's original object after the context was synthesized.
        baseline.setProtocols(new String[] {"TLSv1.3"});
        baseline.setEndpointIdentificationAlgorithm("HTTPS");

        // Engines produced after the mutation still reflect the snapshot taken at synthesis time.
        SSLEngine engine = context.newEngine(ByteBufAllocator.DEFAULT);
        assertThat(engine.getEnabledProtocols()).containsExactly("TLSv1.2");
        assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isNull();
    }

    // ---- end-to-end: the acquisition helper requests and threads the companion ----

    @Test
    public void acquisitionThreadsFactorySuppliedCompanion() throws Exception {
        SSLParameters companion = new SSLParameters();
        companion.setProtocols(new String[] {"TLSv1.2"});
        companion.setEndpointIdentificationAlgorithm("HTTPS");
        CompanionFactory factory = new CompanionFactory(clientJdkContextTrustOnly(), companion);

        TlsHandle<SslContext> handle = TlsContextAcquisition.acquireNettyContext(
                factory, TlsPurpose.CLIENT_DEFAULT, TlsSynthesisSpec.client(false)).join().orElseThrow();

        SSLEngine engine = handle.get().newEngine(ByteBufAllocator.DEFAULT);
        assertThat(engine.getEnabledProtocols()).containsExactly("TLSv1.2");
        assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        assertThat(factory.companionRequests).as("the framework requested the SSLParameters companion").isPositive();
        handle.dispose();
    }

    @Test
    public void subscribingAcquisitionReRequestsCompanionOnEachRotation() throws Exception {
        SSLParameters companion = new SSLParameters();
        companion.setProtocols(new String[] {"TLSv1.2"});
        CompanionFactory factory = new CompanionFactory(serverJdkContext(), companion);

        TlsHandle<SslContext> handle = TlsContextAcquisition.acquireNettyContext(
                factory, TlsPurpose.BROKER, TlsSynthesisSpec.server(false), ctx -> { }).join().orElseThrow();

        // The first delivery reflects the initial companion (TLSv1.2), fetched once by the outer pre-fetch that
        // serves the synchronous first delivery.
        assertThat(handle.get().newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols()).containsExactly("TLSv1.2");
        assertThat(factory.companionRequests).as("companion fetched once for the first delivery").isEqualTo(1);

        // Rotate material AND engine policy (TLSv1.3). The companion must be re-requested on the rotation
        // delivery (PIP-478: engine policy rotates WITH the material), so the new floor takes effect.
        factory.rotate(serverJdkContext(), protocols("TLSv1.3"));
        assertThat(factory.companionRequests).as("companion re-requested on the rotation delivery").isEqualTo(2);
        assertThat(handle.get().newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols()).containsExactly("TLSv1.3");
        handle.dispose();
    }

    @Test
    public void subscribingRotationKeepsLastGoodCompanionWhenReRequestFails() throws Exception {
        CompanionFactory factory = new CompanionFactory(serverJdkContext(), protocols("TLSv1.2"));

        TlsHandle<SslContext> handle = TlsContextAcquisition.acquireNettyContext(
                factory, TlsPurpose.BROKER, TlsSynthesisSpec.server(false), ctx -> { }).join().orElseThrow();
        assertThat(handle.get().newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols()).containsExactly("TLSv1.2");

        // On the rotation delivery the companion re-request fails; engine policy must not be downgraded — the
        // last-good TLSv1.2 floor is retained even though the material rotated.
        factory.failCompanionRequests();
        factory.rotate(serverJdkContext(), protocols("TLSv1.3"));
        assertThat(handle.get().newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
                .as("a failed companion re-request retains the last-good engine policy")
                .containsExactly("TLSv1.2");
        handle.dispose();
    }

    @Test
    public void staleOutOfOrderCompanionCannotRegressTheLastGoodBaseline() throws Exception {
        CompanionFactory factory = new CompanionFactory(serverJdkContext(), protocols("TLSv1.2"));
        TlsHandle<SslContext> handle = TlsContextAcquisition.acquireNettyContext(
                factory, TlsPurpose.BROKER, TlsSynthesisSpec.server(false), ctx -> { }).join().orElseThrow();

        // Two rotations whose companion requests stay pending, then complete OUT OF ORDER: the newer
        // (generation-3) companion first, the stale generation-2 one late.
        factory.deferCompanionRequests();
        factory.rotate(serverJdkContext(), null);
        factory.rotate(serverJdkContext(), null);
        factory.completePendingCompanion(1, protocols("TLSv1.3"));
        assertThat(handle.get().newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
                .containsExactly("TLSv1.3");
        factory.completePendingCompanion(0, protocols("TLSv1.2"));

        // A further rotation whose companion request FAILS must republish the last-good generation-3 policy;
        // the stale generation-2 completion above must not have regressed it.
        factory.failCompanionRequests();
        factory.rotate(serverJdkContext(), null);
        assertThat(handle.get().newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
                .as("the stale out-of-order companion must not regress the retained last-good policy")
                .containsExactly("TLSv1.3");
        handle.dispose();
    }

    @Test
    public void companionFailureReleasesTheAcquiredJdkHandle() throws Exception {
        CompanionFactory factory = new CompanionFactory(clientJdkContextTrustOnly(), protocols("TLSv1.2"));
        factory.failCompanionRequests();

        assertThatThrownBy(() -> TlsContextAcquisition.acquireNettyContext(
                factory, TlsPurpose.CLIENT_DEFAULT, TlsSynthesisSpec.client(false)).join())
                .hasMessageContaining("companion unavailable");
        assertThat(factory.jdkHandleDisposals.get())
                .as("the already-acquired JDK handle is released when the companion request fails")
                .isEqualTo(1);
    }

    // ---- T4: LIVE handshakes driven through the full factory -> acquisition path (not the low-level
    // synthesize helper, and not just getters): the factory-supplied companion actually decides a real
    // handshake outcome after flowing through TlsContextAcquisition.acquireNettyContext. ----

    @Test
    public void acquiredCompanionProtocolRestrictionEnforcedAtHandshake() throws Exception {
        // A companion pinning TLSv1.2 on contexts obtained through the acquisition path completes a matching
        // TLSv1.2 handshake and rejects a TLSv1.3-only peer.
        TlsHandle<SslContext> server = acquireServer(serverJdkContext(), protocols("TLSv1.2"));
        TlsHandle<SslContext> okClient = acquireClient(clientJdkContextTrustOnly(), protocols("TLSv1.2"));
        handshake(okClient.get().newEngine(ByteBufAllocator.DEFAULT),
                server.get().newEngine(ByteBufAllocator.DEFAULT));

        TlsHandle<SslContext> badClient = acquireClient(clientJdkContextTrustOnly(), protocols("TLSv1.3"));
        assertThatThrownBy(() -> handshake(badClient.get().newEngine(ByteBufAllocator.DEFAULT),
                server.get().newEngine(ByteBufAllocator.DEFAULT)))
                .as("a TLSv1.3-only client cannot handshake with the companion-pinned TLSv1.2 server")
                .isInstanceOf(SSLException.class);

        server.dispose();
        okClient.dispose();
        badClient.dispose();
    }

    @Test
    public void acquiredCompanionNeedClientAuthEnforcedAtHandshake() throws Exception {
        // The consumer requests only OPTIONAL client auth (server(false)); the companion's needClientAuth is
        // authoritative through the acquisition path — a client presenting a trusted certificate completes the
        // handshake, a client presenting none is rejected.
        TlsHandle<SslContext> server = acquireServer(serverJdkContext(), needClientAuthTls12());
        TlsHandle<SslContext> clientWithCert = acquireClient(clientJdkContextWithCert(), protocols("TLSv1.2"));
        handshake(clientWithCert.get().newEngine(ByteBufAllocator.DEFAULT),
                server.get().newEngine(ByteBufAllocator.DEFAULT));

        TlsHandle<SslContext> clientNoCert = acquireClient(clientJdkContextTrustOnly(), protocols("TLSv1.2"));
        assertThatThrownBy(() -> handshake(clientNoCert.get().newEngine(ByteBufAllocator.DEFAULT),
                server.get().newEngine(ByteBufAllocator.DEFAULT)))
                .as("the companion's needClientAuth rejects a client presenting no certificate")
                .isInstanceOf(SSLException.class);

        server.dispose();
        clientWithCert.dispose();
        clientNoCert.dispose();
    }

    private static TlsHandle<SslContext> acquireServer(SSLContext jdkContext, SSLParameters companion)
            throws Exception {
        return TlsContextAcquisition.acquireNettyContext(new CompanionFactory(jdkContext, companion),
                TlsPurpose.BROKER, TlsSynthesisSpec.server(false)).join().orElseThrow();
    }

    private static TlsHandle<SslContext> acquireClient(SSLContext jdkContext, SSLParameters companion)
            throws Exception {
        return TlsContextAcquisition.acquireNettyContext(new CompanionFactory(jdkContext, companion),
                TlsPurpose.CLIENT_DEFAULT, TlsSynthesisSpec.client(false)).join().orElseThrow();
    }

    private static SSLParameters protocols(String... protocols) {
        SSLParameters params = new SSLParameters();
        params.setProtocols(protocols);
        return params;
    }

    private static SSLParameters needClientAuthTls12() {
        SSLParameters params = protocols("TLSv1.2");
        params.setNeedClientAuth(true);
        return params;
    }

    /**
     * A minimal factory that supplies only the JDK {@code SSLContext} (returning {@code empty()} for the Netty
     * class, forcing synthesis) plus a fixed {@code SSLParameters} companion, counting companion requests.
     */
    private static final class CompanionFactory implements PulsarTlsFactory {
        private volatile SSLContext jdkContext;
        private volatile SSLParameters companion;
        private volatile Consumer<SSLContext> sslContextSubscriber;
        private volatile boolean failCompanion;
        private volatile boolean deferCompanion;
        private final List<CompletableFuture<Optional<TlsHandle<SSLParameters>>>> pendingCompanions =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger jdkHandleDisposals = new AtomicInteger();
        private int companionRequests;

        CompanionFactory(SSLContext jdkContext, SSLParameters companion) {
            this.jdkContext = jdkContext;
            this.companion = companion;
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass) {
            if (instanceClass == SSLParameters.class && failCompanion) {
                companionRequests++;
                return CompletableFuture.failedFuture(new IllegalStateException("companion unavailable"));
            }
            if (instanceClass == SSLParameters.class && deferCompanion) {
                companionRequests++;
                CompletableFuture<Optional<TlsHandle<SSLParameters>>> pending = new CompletableFuture<>();
                pendingCompanions.add(pending);
                return (CompletableFuture) pending;
            }
            return CompletableFuture.completedFuture(instanceFor(instanceClass));
        }

        /** Make subsequent one-shot {@code SSLParameters} companion requests complete exceptionally. */
        void failCompanionRequests() {
            this.failCompanion = true;
        }

        /** Leave subsequent companion requests uncompleted; complete them manually (possibly out of order). */
        void deferCompanionRequests() {
            this.deferCompanion = true;
        }

        void completePendingCompanion(int index, SSLParameters params) {
            pendingCompanions.get(index).complete(Optional.of(handleOf(params)));
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            if (instanceClass == SSLContext.class) {
                this.sslContextSubscriber = (Consumer<SSLContext>) onLoadOrReload;
            }
            Optional<TlsHandle<T>> instance = instanceFor(instanceClass);
            instance.ifPresent(handle -> onLoadOrReload.accept(handle.get()));
            return CompletableFuture.completedFuture(instance);
        }

        /** Push a new material + engine-policy delivery to the SSLContext subscriber, simulating a rotation. */
        void rotate(SSLContext newContext, SSLParameters newCompanion) {
            this.jdkContext = newContext;
            this.companion = newCompanion;
            sslContextSubscriber.accept(newContext);
        }

        @SuppressWarnings("unchecked")
        private <T> Optional<TlsHandle<T>> instanceFor(Class<T> instanceClass) {
            if (instanceClass == SSLContext.class) {
                SSLContext context = jdkContext;
                return Optional.of((TlsHandle<T>) new TlsHandle<SSLContext>() {
                    @Override
                    public SSLContext get() {
                        return context;
                    }

                    @Override
                    public void dispose() {
                        jdkHandleDisposals.incrementAndGet();
                    }
                });
            }
            if (instanceClass == SSLParameters.class) {
                companionRequests++;
                return Optional.of((TlsHandle<T>) handleOf(companion));
            }
            // Netty SslContext and anything else: unsupported -> the framework synthesizes from the SSLContext.
            return Optional.empty();
        }

        private static <V> TlsHandle<V> handleOf(V value) {
            return new TlsHandle<>() {
                @Override
                public V get() {
                    return value;
                }

                @Override
                public void dispose() {
                }
            };
        }

        @Override
        public void close() {
        }
    }
}
