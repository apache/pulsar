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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.impl.auth.v5.LegacyV4AuthenticationAdapter;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.Test;

/**
 * Service-URL validation on the V5 client builder. The v5 client only speaks the
 * broker binary protocol, so {@code pulsar://} / {@code pulsar+ssl://} are the
 * only valid schemes — anything else (especially the admin/web service URL) gets
 * rejected at configure-time with a message that points to the right URL.
 */
public class PulsarClientBuilderV5Test {

    @Test
    public void testAcceptsPulsarScheme() {
        // Must not throw — these are the valid forms.
        PulsarClient.builder().serviceUrl("pulsar://localhost:6650");
        PulsarClient.builder().serviceUrl("pulsar+ssl://localhost:6651");
        PulsarClient.builder().serviceUrl("pulsar://h1:6650,h2:6650,h3:6650");
    }

    @Test
    public void testRejectsHttpWithGuidance() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("http://localhost:8080"));
        assertTrue(e.getMessage().contains("pulsar://"),
                "error must point at the correct scheme: " + e.getMessage());
        assertTrue(e.getMessage().toLowerCase().contains("admin")
                        || e.getMessage().toLowerCase().contains("web"),
                "error must call out the http→admin-URL confusion: " + e.getMessage());
        assertTrue(e.getMessage().contains("6650"),
                "error must hint at the broker port: " + e.getMessage());
    }

    @Test
    public void testRejectsHttpsWithGuidance() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("https://localhost:8443"));
        assertTrue(e.getMessage().contains("pulsar+ssl://"),
                "error must mention the TLS broker scheme: " + e.getMessage());
    }

    @Test
    public void testRejectsUnknownScheme() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("ws://localhost:6650"));
        assertTrue(e.getMessage().contains("pulsar://"),
                "error must point at the correct scheme: " + e.getMessage());
    }

    @Test
    public void testRejectsNullAndBlank() {
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl(null));
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl(""));
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl("   "));
    }

    @Test
    public void testProxyServiceUrlIsValidatedToo() {
        PulsarClientBuilder builder = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650");

        ConnectionPolicy badProxy = ConnectionPolicy.builder()
                .proxy("http://proxy:8080", null)
                .build();

        IllegalArgumentException e = assertThrowsIAE(() -> builder.connectionPolicy(badProxy));
        assertTrue(e.getMessage().contains("proxyServiceUrl"),
                "error must name the offending field: " + e.getMessage());
    }

    /**
     * PIP-478 stage 3b: on the v5-builder TLS path a bad {@link TlsPolicy} (here a non-existent trust
     * cert file) fails the client build fast — at {@code build()} time, before any connection is
     * attempted — with an actionable error, rather than surfacing later as an opaque handshake failure.
     */
    @Test
    public void testTlsPolicyBadPathFailsClientBuild() {
        String badPath = "/nonexistent/pip478/ca-does-not-exist.pem";
        PulsarClientException e = null;
        try {
            PulsarClient.builder()
                    .serviceUrl("pulsar+ssl://localhost:6651")
                    .tlsPolicy(TlsPolicy.pem(badPath, null, null))
                    .build();
            fail("expected the client build to fail fast on the bad TLS policy");
        } catch (PulsarClientException ex) {
            e = ex;
        }
        assertNotNull(e, "a bad TLS policy must fail the client build");
        assertTrue(allMessages(e).toLowerCase().contains("tls"),
                "the failure must be actionable and mention TLS: " + allMessages(e));
    }

    /**
     * PIP-478 stage 3c: a bridged third-party v4 plugin (not a built-in TLS class) that reports
     * {@code hasDataForTls()} with file-based cert/key must have that material folded into
     * {@link TlsPurpose#CLIENT_DEFAULT} at build time. Proven here by pointing the plugin at a non-existent
     * cert file: the fold makes the client build fail fast on the missing file; without the fold the
     * system-default CLIENT_DEFAULT policy would build cleanly.
     */
    @Test
    public void genericV4TlsFilePluginMaterialIsFoldedIntoClientDefault() {
        AuthenticationDataProvider data = new AuthenticationDataProvider() {
            @Override
            public boolean hasDataForTls() {
                return true;
            }

            @Override
            public String getTlsCertificateFilePath() {
                return "/nonexistent/pip478/generic-cert.pem";
            }

            @Override
            public String getTlsPrivateKeyFilePath() {
                return "/nonexistent/pip478/generic-key.pem";
            }
        };
        Authentication v5 = LegacyV4AuthenticationAdapter.wrap(new GenericTlsV4Auth("custom-tls", data));

        PulsarClientException e = null;
        try {
            PulsarClient.builder()
                    .serviceUrl("pulsar+ssl://localhost:6651")
                    .authentication(v5)
                    .tlsPolicy(TlsPolicy.pem(null, null, null))
                    .build();
            fail("expected the client build to fail fast on the folded generic TLS material");
        } catch (PulsarClientException ex) {
            e = ex;
        }
        assertNotNull(e, "the folded generic cert path must fail the client build");
        assertTrue(allMessages(e).toLowerCase().contains("tls"),
                "the failure must be actionable and mention TLS: " + allMessages(e));
    }

    /**
     * PIP-478 stage 3c: a generic v4 plugin exposing only in-memory TLS material cannot be represented in a
     * file-path {@link TlsPolicy}; it is logged rather than folded, and the client build still succeeds
     * (the material simply is not applied on this path).
     */
    @Test
    public void genericV4InMemoryOnlyTlsMaterialDoesNotFailBuild() throws Exception {
        AuthenticationDataProvider inMemoryOnly = new AuthenticationDataProvider() {
            @Override
            public boolean hasDataForTls() {
                return true;
            }
            // No file paths and no keystore params — only (notional) in-memory material.
        };
        Authentication v5 = LegacyV4AuthenticationAdapter.wrap(new GenericTlsV4Auth("custom-tls", inMemoryOnly));

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://localhost:6651")
                .authentication(v5)
                .tlsPolicy(TlsPolicy.pem(null, null, null))
                .build()) {
            assertNotNull(client, "an in-memory-only generic TLS plugin must not fail the client build");
        }
    }

    @Test
    public void testTlsPolicyFieldsPropagate() {
        // Build a fully-populated TlsPolicy and confirm the policy — every field intact — lands on the
        // underlying v4 ClientConfigurationData. Used to be a stub that only set useTls=true; under
        // PIP-478 the policy rides conf.tlsPolicyMap under TlsPurpose.CLIENT_DEFAULT (consumed by the
        // client TLS factory) instead of the legacy per-field TLS settings.
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        TlsPolicy policy = TlsPolicy.builder()
                .trustCertsFilePath("/path/to/ca.pem")
                .keyFilePath("/path/to/client.key")
                .certificateFilePath("/path/to/client.cert")
                .allowInsecureConnection(true)
                .enableHostnameVerification(false)
                .build();

        builder.tlsPolicy(policy);

        ClientConfigurationData conf = builder.getConfForTesting();
        assertTrue(conf.isUseTls());
        TlsPolicy applied = conf.getTlsPolicyMap().get(TlsPurpose.CLIENT_DEFAULT);
        assertNotNull(applied, "the policy must land on the conf under CLIENT_DEFAULT");
        assertEquals(applied.trustCertsFilePath(), "/path/to/ca.pem");
        assertEquals(applied.keyFilePath(), "/path/to/client.key");
        assertEquals(applied.certificateFilePath(), "/path/to/client.cert");
        assertTrue(applied.allowInsecureConnection());
        assertFalse(applied.enableHostnameVerification());
    }

    /**
     * PIP-478: "{@code tlsFactory(PulsarTlsFactory)} likewise supplies material for all purposes without
     * enabling transport TLS." Adopting a factory used to set {@code useTls}, so a client that adopted one
     * to serve {@code CLIENT_OAUTH2} — an HTTPS identity provider behind a private CA — attempted a TLS
     * handshake against its plaintext {@code pulsar://} broker port and could not connect. The factory must
     * still be recorded: {@code PulsarClientImpl.needsClientTlsFactory()} composes it on its own arm.
     */
    @Test
    public void testTlsFactoryDoesNotEnableTransportTls() {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        PulsarTlsFactory factory = new NoOpTlsFactory();

        builder.serviceUrl("pulsar://localhost:6650").tlsFactory(factory);

        ClientConfigurationData conf = builder.getConfForTesting();
        assertFalse(conf.isUseTls(), "adopting a factory must not switch the broker transport to TLS");
        assertEquals(conf.getTlsFactory(), factory, "the adopted factory must still reach the conf");
    }

    /** The transport is still enabled the normal way — by the service URL scheme. */
    @Test
    public void testTlsFactoryLeavesAnSslUrlEnabled() {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();

        builder.serviceUrl("pulsar+ssl://localhost:6651").tlsFactory(new NoOpTlsFactory());

        assertTrue(builder.getConfForTesting().isUseTls(), "pulsar+ssl:// still selects TLS");
    }

    /**
     * PIP-478: two clients built from one v5 builder must not share a TLS factory.
     *
     * <p>{@code PulsarClientImpl} stores the factory it composes back onto the {@code
     * ClientConfigurationData} it was given, so while {@code build()} handed over the builder's own
     * instance a second build adopted and re-initialized the first client's live factory, and closing
     * either client closed TLS for the other — the survivor still reporting itself open while every
     * connect and reconnect failed with "closed". This is the builder where it matters most: {@code
     * tlsFactory(...)} and {@code tlsPolicy(...)} exist only here, so it is the only path that reaches
     * the adopted-factory arm deliberately, and the one that turns on the fail-fast probe whose failure
     * handler closes the factory. Nothing in {@link PulsarClientBuilder} makes {@code build()}
     * single-use.
     */
    @Test
    public void twoClientsFromOneBuilderDoNotShareATlsFactory() throws Exception {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.serviceUrl("pulsar+ssl://localhost:6651")
                .tlsPolicy(TlsPolicy.builder().allowInsecureConnection(true).build());

        PulsarClient first = builder.build();
        PulsarClient second = builder.build();
        try {
            PulsarTlsFactory firstFactory =
                    ((PulsarClientV5) first).v4Client().getConfiguration().getTlsFactory();
            PulsarTlsFactory secondFactory =
                    ((PulsarClientV5) second).v4Client().getConfiguration().getTlsFactory();
            assertNotNull(firstFactory);
            assertNotSame(secondFactory, firstFactory, "each client must compose its own factory");

            first.close();

            assertTrue(secondFactory.createInstance(TlsPurpose.CLIENT_DEFAULT, SSLContext.class)
                            .get().isPresent(),
                    "closing the first client must leave the second client's TLS working");
        } finally {
            second.close();
        }
    }

    /**
     * PIP-478: an adopted {@link PulsarTlsFactory} instance belongs to one client.
     *
     * <p>Copying the configuration gives each client its own <em>composed</em> factory, but an adopted one is
     * the caller's instance and the copy carries the same reference. A second client would have the framework
     * {@code initialize()} it twice and {@code close()} it twice — the SPI says exactly once and at most once
     * — and would leave whichever client is closed second serving TLS from a closed factory while still
     * reporting itself open. So {@code build()} says so at the point where the mistake is cheap to fix, and a
     * fresh instance re-arms the builder.
     */
    @Test
    public void anAdoptedTlsFactoryIsHandedToOneClientOnly() throws Exception {
        AdoptableTlsFactory adopted = new AdoptableTlsFactory();
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.serviceUrl("pulsar+ssl://localhost:6651").tlsFactory(adopted);

        PulsarClient first = builder.build();
        try {
            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already been adopted")
                    .hasMessageContaining("fresh instance");
            assertThat(adopted.initialized).as("and the rejected build must not re-initialize it").hasValue(1);
        } finally {
            first.close();
        }
        assertThat(adopted.closed).as("the client that adopted it closes it").hasValue(1);

        AdoptableTlsFactory rearmed = new AdoptableTlsFactory();
        builder.tlsFactory(rearmed);
        builder.build().close();
        assertThat(rearmed.closed).as("a fresh instance re-arms the builder").hasValue(1);
    }

    /**
     * PIP-478: the two sides of what "handed over" means.
     *
     * <p>A build that fails <em>before</em> the construction — the missing {@code serviceUrl} below — has not
     * touched the factory, so the builder must still accept it. A build that fails <em>after</em>
     * {@code setupClientTlsFactory()} has initialized it, and the failure path closed it (the probe's own
     * handler, or {@code shutdown()} for anything later), so the builder must refuse it: the alternative is
     * handing the next client a closed factory instead of telling the caller.
     */
    @Test
    public void aBuildThatReachedTheFactorySpendsItAndOneThatDidNotDoesNot() throws Exception {
        AdoptableTlsFactory adopted = new AdoptableTlsFactory();
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsFactory(adopted);

        assertThatThrownBy(builder::build)
                .as("no serviceUrl: rejected before the construction that adopts")
                .isInstanceOf(PulsarClientException.class);
        assertThat(adopted.initialized).as("so nothing consumed the factory").hasValue(0);

        // Now let the build reach the factory and fail there: it refuses to serve CLIENT_DEFAULT, which is
        // what the v5-builder path probes for.
        adopted.failClientDefault = true;
        builder.serviceUrl("pulsar+ssl://localhost:6651");
        assertThatThrownBy(builder::build).as("the build must fail at the factory for this to be the case")
                .isNotNull();
        assertThat(adopted.initialized).as("but this time it was initialized").hasValue(1);
        assertThat(adopted.closed).as("and closed on the way out").hasValue(1);

        adopted.failClientDefault = false;
        assertThatThrownBy(builder::build)
                .as("so the instance is spent, even though no client exists")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already been adopted");
    }

    /**
     * PIP-478: a build that failed before reaching the factory hands over nothing, so it must still be usable.
     *
     * <p>The guard used to record the instance before the construction, which made any failure on the way to
     * the TLS factory poison the builder against a factory that was never initialized and never closed — a
     * blank {@code serviceUrl} is rejected by {@code PulsarClientImpl}'s constructor well before
     * {@code setupClientTlsFactory()}. With no way to clear the slot ({@code tlsFactory(null)} throws), a
     * typo in a URL cost the caller a whole new factory: for the HSM- and KMS-backed factories this seam
     * exists to serve, that is not free.
     */
    @Test
    public void aFailedBuildLeavesTheAdoptedFactoryUsable() throws Exception {
        AdoptableTlsFactory adopted = new AdoptableTlsFactory();
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsFactory(adopted);

        // No serviceUrl: rejected during construction, before the TLS factory is looked at.
        assertThatThrownBy(builder::build).isInstanceOf(PulsarClientException.class);
        assertThat(adopted.initialized).as("nothing consumed the factory").hasValue(0);
        assertThat(adopted.closed).hasValue(0);

        builder.serviceUrl("pulsar+ssl://localhost:6651");
        PulsarClient client = builder.build();
        try {
            assertThat(adopted.initialized).as("the retry adopts the same instance").hasValue(1);
        } finally {
            client.close();
        }
        assertThat(adopted.closed).hasValue(1);
        assertThatThrownBy(builder::build)
                .as("and it is spent once a client has actually taken it")
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * PIP-478: {@code build()} must resolve the authentication into the client's own configuration.
     *
     * <p>The copy handed to the client used to be taken <em>after</em> {@code applyAuthentication()}, which
     * writes: it puts the resolved plugin in one of the two authentication slots and folds a bridged v4
     * plugin's certificate and key into {@code CLIENT_DEFAULT}. Both writes landed on the builder, so they
     * outlived the client — a second client built after swapping to a plugin with no file-based material of
     * its own kept presenting the first plugin's client certificate, and silently authenticated as it.
     *
     * <p>A factory is adopted here only so the two builds succeed without real certificate files: it serves
     * {@code CLIENT_DEFAULT} itself, so the fail-fast probe never reads the folded paths. The fold is
     * unaffected — it happens before the factory is resolved, and the policy is what the assertions read.
     */
    @Test
    public void aSecondClientDoesNotInheritTheFirstPluginsCertificate() throws Exception {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.serviceUrl("pulsar+ssl://localhost:6651")
                .tlsPolicy(TlsPolicy.builder().trustCertsFilePath("/path/to/ca.pem").build())
                .tlsFactory(new AdoptableTlsFactory());
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(
                new org.apache.pulsar.client.impl.auth.AuthenticationTls(
                        "/path/to/first.cert", "/path/to/first.key")));

        PulsarClient first = builder.build();
        try {
            assertThat(clientDefaultOf(first).certificateFilePath())
                    .as("the first client's own policy does carry its plugin's identity")
                    .isEqualTo("/path/to/first.cert");
            assertThat(builder.getConfForTesting().getTlsPolicyMap().get(TlsPurpose.CLIENT_DEFAULT)
                    .certificateFilePath())
                    .as("but the builder's policy must not have been folded into")
                    .isNull();

            builder.tlsFactory(new AdoptableTlsFactory());
            builder.authentication(LegacyV4AuthenticationAdapter.wrap(
                    new org.apache.pulsar.client.impl.auth.AuthenticationDisabled()));
            PulsarClient second = builder.build();
            try {
                assertThat(clientDefaultOf(second).certificateFilePath())
                        .as("so the second client must not authenticate as the first client's plugin")
                        .isNull();
                assertThat(clientDefaultOf(second).trustCertsFilePath())
                        .as("while the trust material the caller configured is still carried")
                        .isEqualTo("/path/to/ca.pem");
            } finally {
                second.close();
            }
        } finally {
            first.close();
        }
    }

    private static TlsPolicy clientDefaultOf(PulsarClient client) {
        return ((PulsarClientV5) client).v4Client().getConfiguration().getTlsPolicyMap()
                .get(TlsPurpose.CLIENT_DEFAULT);
    }

    /** Serves {@link TlsPurpose#CLIENT_DEFAULT}, so a client can actually adopt it, and counts its lifecycle. */
    private static final class AdoptableTlsFactory implements PulsarTlsFactory {

        private final AtomicInteger initialized = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        /** Makes the fail-fast probe fail, so a build gets past initialize() and then throws. */
        private volatile boolean failClientDefault;

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            initialized.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            if (failClientDefault && TlsPurpose.CLIENT_DEFAULT.equals(purpose)) {
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
            closed.incrementAndGet();
        }
    }

    /** A factory that is never initialized or asked for an instance — the builder wiring is what is tested. */
    private static final class NoOpTlsFactory implements PulsarTlsFactory {
        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testTlsPolicyInsecureShortcut() {
        // TlsPolicy.insecure() is the dev convenience that disables verification.
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsPolicy(TlsPolicy.insecure());

        ClientConfigurationData conf = builder.getConfForTesting();
        assertTrue(conf.isUseTls());
        TlsPolicy applied = conf.getTlsPolicyMap().get(TlsPurpose.CLIENT_DEFAULT);
        assertNotNull(applied, "the insecure policy must land on the conf under CLIENT_DEFAULT");
        assertTrue(applied.allowInsecureConnection());
        assertFalse(applied.enableHostnameVerification());
    }

    @Test
    public void testAuthenticationPluginAndParamsInstantiatesAuthentication() throws Exception {
        // Regression for a bug where authentication(plugin, params) only set the strings and
        // never instantiated the plugin. PulsarClientImpl reads the actual Authentication instance
        // via conf.getAuthentication() at connect time — without it, the client connects with no
        // credentials and the broker rejects the handshake. Under PIP-478 the plugin is built eagerly
        // at authentication(...) and resolved into conf.authentication at build();
        // resolveAuthenticationForTest() runs that resolution. Use the v4 AuthenticationDisabled stub
        // which always exists on the classpath; we only care that *some* instance lands on the conf.
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.authentication(
                "org.apache.pulsar.client.impl.auth.AuthenticationDisabled", "");

        ClientConfigurationData conf = builder.getConfForTesting();
        assertEquals(conf.getAuthPluginClassName(),
                "org.apache.pulsar.client.impl.auth.AuthenticationDisabled");
        assertNotNull(builder.resolveAuthenticationForTest(),
                "Authentication instance must be created and attached to the conf");
        assertNotNull(conf.getAuthentication(),
                "Authentication instance must be created and attached to the conf");
    }

    @Test
    public void testAuthenticationPluginNotFoundIsWrapped() {
        // A bad plugin class name should surface as V5 PulsarClientException (not a v4 exception
        // type leaking through the surface).
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        assertThrows(PulsarClientException.class, () ->
                builder.authentication("com.example.NoSuchAuth", ""));
    }

    /**
     * PIP-478: a cross-format TLS-material fold must fail loud, not silently drop trust anchors. A PEM
     * {@code tlsPolicy(...)} carrying a private-CA {@code trustCertsFilePath} combined with a keystore-format
     * auth plugin ({@link org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls}) would, if folded into a
     * keystore policy, drop the PEM truststore and fall back to the system trust store. Reject it at build time
     * (matching {@code TlsPolicy.build()}'s fail-loud format validation) and point at the remedy.
     */
    @Test
    public void testCrossFormatFoldPemTrustWithKeyStoreIdentityFailsLoud() {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsPolicy(TlsPolicy.pem("/path/to/private-ca.pem", null, null));
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(
                new org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls(
                        "PKCS12", "/path/to/client-keystore.p12", "changeit")));

        IllegalArgumentException e = assertThrowsIAE(builder::resolveAuthenticationForTest);
        assertTrue(e.getMessage().contains("trustCertsFilePath"),
                "the failure must name the dropped PEM truststore: " + e.getMessage());
        assertTrue(e.getMessage().toLowerCase().contains("same format"),
                "the failure must point at the same-format remedy: " + e.getMessage());
    }

    /**
     * PIP-478: the reverse cross-format fold — a keystore {@code tlsPolicy(...)} carrying a private-CA
     * {@code trustStorePath} combined with a PEM-format auth plugin
     * ({@link org.apache.pulsar.client.impl.auth.AuthenticationTls}) — would drop the keystore truststore if
     * folded into a PEM policy. It must fail loud for the same reason.
     */
    @Test
    public void testCrossFormatFoldKeyStoreTrustWithPemIdentityFailsLoud() {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsPolicy(TlsPolicy.keyStore("/path/to/truststore.jks", "changeit", null, null, "JKS"));
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(
                new org.apache.pulsar.client.impl.auth.AuthenticationTls(
                        "/path/to/client.cert", "/path/to/client.key")));

        IllegalArgumentException e = assertThrowsIAE(builder::resolveAuthenticationForTest);
        assertTrue(e.getMessage().contains("trustStorePath"),
                "the failure must name the dropped keystore truststore: " + e.getMessage());
        assertTrue(e.getMessage().toLowerCase().contains("same format"),
                "the failure must point at the same-format remedy: " + e.getMessage());
    }

    /**
     * PIP-478: a same-format fold is the supported case and must succeed with trust preserved. A PEM
     * {@code tlsPolicy(...)} truststore combined with a PEM auth plugin folds the plugin's client identity into
     * {@link TlsPurpose#CLIENT_DEFAULT} while keeping the configured {@code trustCertsFilePath}.
     */
    @Test
    public void testSameFormatFoldPreservesPemTrust() {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsPolicy(TlsPolicy.pem("/path/to/private-ca.pem", null, null));
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(
                new org.apache.pulsar.client.impl.auth.AuthenticationTls(
                        "/path/to/client.cert", "/path/to/client.key")));

        builder.resolveAuthenticationForTest();

        TlsPolicy applied = builder.getConfForTesting().getTlsPolicyMap().get(TlsPurpose.CLIENT_DEFAULT);
        assertNotNull(applied, "the folded policy must land under CLIENT_DEFAULT");
        assertEquals(applied.format(), TlsPolicy.Format.PEM);
        assertEquals(applied.trustCertsFilePath(), "/path/to/private-ca.pem",
                "the configured PEM trust must be preserved across the fold");
        assertEquals(applied.certificateFilePath(), "/path/to/client.cert");
        assertEquals(applied.keyFilePath(), "/path/to/client.key");
    }

    /**
     * PIP-478: the same-format keystore fold likewise preserves the configured keystore truststore while
     * folding the plugin's keystore client identity.
     */
    @Test
    public void testSameFormatFoldPreservesKeyStoreTrust() {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsPolicy(TlsPolicy.builder()
                .format(TlsPolicy.Format.KEYSTORE)
                .trustStorePath("/path/to/truststore.jks")
                .trustStorePassword("changeit")
                .trustStoreType("JKS")
                .build());
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(
                new org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls(
                        "PKCS12", "/path/to/client-keystore.p12", "keypw")));

        builder.resolveAuthenticationForTest();

        TlsPolicy applied = builder.getConfForTesting().getTlsPolicyMap().get(TlsPurpose.CLIENT_DEFAULT);
        assertNotNull(applied, "the folded policy must land under CLIENT_DEFAULT");
        assertEquals(applied.format(), TlsPolicy.Format.KEYSTORE);
        assertEquals(applied.trustStorePath(), "/path/to/truststore.jks",
                "the configured keystore trust must be preserved across the fold");
        assertEquals(applied.trustStoreType(), "JKS",
                "the configured truststore type must be preserved across the fold");
        assertEquals(applied.keyStorePath(), "/path/to/client-keystore.p12");
        assertEquals(applied.keyStoreType(), "PKCS12");
    }

    /**
     * PIP-478: the pinned JSSE (SSLContext) provider on a {@code tlsPolicy(...)} — as a FIPS deployment
     * configures — must survive the bridged-v4 auth fold. The fold rebuilds the CLIENT_DEFAULT policy from the
     * base flags; a PEM auth plugin folded on top must keep {@code jsseProvider} so the transport still pins the
     * engine instead of falling back to the default JDK provider.
     */
    @Test
    public void testFoldPreservesJsseProvider() {
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsPolicy(TlsPolicy.builder()
                .trustCertsFilePath("/path/to/private-ca.pem")
                .jsseProvider("BCJSSE")
                .build());
        builder.authentication(LegacyV4AuthenticationAdapter.wrap(
                new org.apache.pulsar.client.impl.auth.AuthenticationTls(
                        "/path/to/client.cert", "/path/to/client.key")));

        builder.resolveAuthenticationForTest();

        TlsPolicy applied = builder.getConfForTesting().getTlsPolicyMap().get(TlsPurpose.CLIENT_DEFAULT);
        assertNotNull(applied, "the folded policy must land under CLIENT_DEFAULT");
        assertEquals(applied.jsseProvider(), "BCJSSE",
                "the pinned JSSE provider must be preserved across the fold");
        assertEquals(applied.certificateFilePath(), "/path/to/client.cert");
        assertEquals(applied.keyFilePath(), "/path/to/client.key");
    }

    /** A minimal generic (non-built-in) v4 TLS plugin for the fold tests. */
    @SuppressWarnings("deprecation")
    private static final class GenericTlsV4Auth implements org.apache.pulsar.client.api.Authentication {
        private final String methodName;
        private final AuthenticationDataProvider data;

        GenericTlsV4Auth(String methodName, AuthenticationDataProvider data) {
            this.methodName = methodName;
            this.data = data;
        }

        @Override
        public String getAuthMethodName() {
            return methodName;
        }

        @Override
        public AuthenticationDataProvider getAuthData() {
            return data;
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    private static String allMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
            if (c.getMessage() != null) {
                sb.append(c.getMessage()).append(" | ");
            }
        }
        return sb.toString();
    }

    private static IllegalArgumentException assertThrowsIAE(Runnable r) {
        try {
            r.run();
            fail("expected IllegalArgumentException");
            return null; // unreachable
        } catch (IllegalArgumentException e) {
            return e;
        }
    }

    @Test
    public void anIdpOnlyPolicyDoesNotEnableBrokerTls() {
        // CLIENT_OAUTH2 is a separate trust domain (the identity provider); useTls governs the binary broker
        // transport. Setting only an IdP policy against a plaintext pulsar:// broker used to turn broker TLS
        // on, so the connection pool attempted TLS against a plaintext port.
        PulsarClientBuilderV5 builder = (PulsarClientBuilderV5) PulsarClient.builder()
                .serviceUrl("pulsar://my-pulsar:6650")
                .tlsPolicy(TlsPurpose.CLIENT_OAUTH2, TlsPolicy.builder()
                        .trustCertsFilePath("/tls/idp-ca.pem").build());

        ClientConfigurationData conf = builder.getConfForTesting();
        assertFalse(conf.isUseTls(), "an IdP-only policy must leave the broker transport plaintext");
        assertNotNull(conf.getTlsPolicyMap().get(TlsPurpose.CLIENT_OAUTH2),
                "the IdP policy must still be registered so its factory is built");
    }

    @Test
    public void anIdpOnlyPolicySurvivesTheClientBuild() throws Exception {
        // Registering the policy on the config is only half of it: PulsarClientImpl separately decides whether
        // to compose the client TLS factory at all, and used to gate that on broker TLS (or an auth plugin
        // carrying its own IdP material). An explicit IdP policy on a plaintext broker therefore satisfied
        // neither arm, so the factory stayed unset and the framework HTTP client fell back to platform-default
        // trust — silently dropping the trust domain the caller configured. Assert through build().
        PulsarClientBuilderV5 builder = (PulsarClientBuilderV5) PulsarClient.builder()
                .serviceUrl("pulsar://my-pulsar:6650")
                .tlsPolicy(TlsPurpose.CLIENT_OAUTH2, TlsPolicy.builder()
                        .trustCertsFilePath("/tls/idp-ca.pem").build());

        // build() hands PulsarClientImpl a copy of the config, so the composed factory lands on the
        // client's own instance rather than back on the builder's (see
        // twoClientsFromOneBuilderDoNotShareATlsFactory for why it must not be shared). Assert there.
        try (PulsarClient client = builder.build()) {
            assertNotNull(client);
            ClientConfigurationData conf = ((PulsarClientV5) client).v4Client().getConfiguration();
            assertFalse(conf.isUseTls(), "an IdP-only policy must leave the broker transport plaintext");
            assertNotNull(conf.getTlsFactory(),
                    "an explicitly configured CLIENT_OAUTH2 policy must compose the client TLS factory, so the "
                            + "framework HTTP client uses the configured IdP trust and not the platform default");
        }
    }

    @Test
    public void aTransportPolicyStillEnablesBrokerTls() {
        PulsarClientBuilderV5 builder = (PulsarClientBuilderV5) PulsarClient.builder()
                .serviceUrl("pulsar+ssl://my-pulsar:6651")
                .tlsPolicy(TlsPolicy.builder().trustCertsFilePath("/tls/ca.pem").build());

        assertTrue(builder.getConfForTesting().isUseTls(),
                "a CLIENT_DEFAULT policy configures the broker transport and must enable TLS");
    }
}
