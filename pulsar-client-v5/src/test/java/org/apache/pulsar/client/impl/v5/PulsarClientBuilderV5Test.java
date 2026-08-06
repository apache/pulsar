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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Map;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.v5.auth.LegacyV4AuthenticationAdapter;
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
}
