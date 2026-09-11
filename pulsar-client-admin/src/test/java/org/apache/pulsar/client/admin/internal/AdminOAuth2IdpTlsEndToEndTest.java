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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.moreThanOrExactly;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.common.io.Resources;
import java.io.File;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.auth.v5.FrameworkHttpClientFactory;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.http.HttpRequest;
import org.apache.pulsar.http.HttpResponse;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.http.PulsarHttpClientConfig;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478 / issue #24944, end to end: the admin counterpart of {@code OAuth2IdpTlsFrameworkClientTest}.
 * {@link AdminOAuth2IdpTlsBindingTest} asserts the <em>binding decision</em> — that the IdP policy is folded
 * into the admin's configuration under {@link TlsPurpose#CLIENT_OAUTH2}. That is necessary but not
 * sufficient: the fold is only meaningful if the admin also has a TLS factory to resolve that purpose
 * against, and if the framework HTTP client reads it. This test closes that gap by making a real HTTPS
 * request to a WireMock IdP whose certificate is signed by the shared test CA — trusted only via the
 * plugin's {@code trustCertsFilePath}.
 *
 * <p>The admin's own service URL is plaintext {@code http://} on purpose: that is where the gap lived. The
 * connector composed a TLS factory only for an {@code https://} admin URL, so a plaintext admin folded the
 * IdP policy into a configuration that no factory ever read, and the IdP connection silently fell back to
 * platform-default trust.
 */
public class AdminOAuth2IdpTlsEndToEndTest {

    // broker.cert.pem carries SAN:localhost and chains to ca.cert.pem, so both trust and hostname verification
    // pass against the WireMock server (the shared broker.keystore.jks carries the same SAN but is self-signed,
    // so it fails trust verification rather than hostname verification).
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String CA_CERT = resource("certificate-authority/certs/ca.cert.pem");
    private static final String STORE_PW = "111111";

    private WireMockServer idp;
    private Path serverKeystore;

    @BeforeMethod
    public void setUp() throws Exception {
        serverKeystore = buildServerKeystore();
        idp = new WireMockServer(wireMockConfig().dynamicHttpsPort()
                .keystorePath(serverKeystore.toString()).keystoreType("JKS")
                .keyManagerPassword(STORE_PW).keystorePassword(STORE_PW));
        idp.start();
        idp.stubFor(get(urlEqualTo("/probe")).willReturn(aResponse().withStatus(200).withBody("ok")));
        // Enough of an OIDC discovery document for ClientCredentialsFlow.initialize() to complete, so a test
        // can exercise the real AuthenticationOAuth2.start() rather than stubbing it out.
        idp.stubFor(get(urlEqualTo("/.well-known/openid-configuration")).willReturn(aResponse().withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"issuer\":\"" + idp.baseUrl() + "\",\"token_endpoint\":\""
                        + idp.baseUrl() + "/oauth/token\"}")));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (idp != null) {
            idp.stop();
        }
        if (serverKeystore != null) {
            Files.deleteIfExists(serverKeystore);
        }
    }

    @Test
    public void theFoldedIdpTrustReachesTheIdpThroughTheAdminsFrameworkHttpClient() throws Exception {
        try (PulsarAdminImpl admin = adminWith(oauth2WithTrust(CA_CERT))) {
            HttpResponse response = probe(admin);
            assertThat(response.statusCode())
                    .as("the admin's framework HTTP client must reach the IdP using the folded CLIENT_OAUTH2 "
                            + "trust; a null factory would leave it on platform-default trust")
                    .isEqualTo(200);
        }
    }

    @Test
    public void withoutTheIdpTrustTheAdminRejectsTheIdpCertificate() throws Exception {
        // The contrast that proves the folded policy — and not some ambient trust — is what carries the CA.
        try (PulsarAdminImpl admin = adminWith(oauth2WithTrust(null))) {
            assertThatThrownBy(() -> probe(admin))
                    .as("platform-default trust must reject the test-CA-signed IdP certificate")
                    .satisfies(t -> assertThat(causeChain(t)).containsAnyOf("SSL", "certificat", "PKIX",
                            "unable to find valid certification path"));
        }
    }

    @Test
    public void theRealOauth2StartFetchesIdpMetadataOverTheFoldedTrust() throws Exception {
        // The other two tests stub start(), so they exercise only the per-request path. start() is the
        // earlier — and stricter — user of the factory: ClientCredentialsFlow.initialize() fetches the IdP
        // discovery document through the framework HTTP client while the PulsarAdmin is still being
        // constructed. That is before any AsyncHttpConnector exists, so the factory has to come from the
        // provider; sourcing it from a connector left it null here and fell back to platform trust.
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{\"type\":\"client_credentials\",\"issuerUrl\":\"" + idp.baseUrl()
                + "\",\"privateKey\":\"data:application/json;base64,e30=\",\"trustCertsFilePath\":\""
                + CA_CERT + "\"}");

        try (PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(auth)
                .build()) {
            assertThat(admin).isNotNull();
        }
        // The count is not the point (the resolver may probe more than once) — reaching the IdP at all over
        // HTTPS is, since the handshake only succeeds on the folded trust.
        idp.verify(moreThanOrExactly(1), getRequestedFor(urlEqualTo("/.well-known/openid-configuration")));
    }

    @Test
    public void withoutTheIdpTrustTheRealOauth2StartFailsToReachTheIdp() throws Exception {
        // The contrast for the start() path: same flow, no trustCertsFilePath, so the metadata fetch must be
        // rejected by platform-default trust and fail the build rather than silently succeeding.
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{\"type\":\"client_credentials\",\"issuerUrl\":\"" + idp.baseUrl()
                + "\",\"privateKey\":\"data:application/json;base64,e30=\"}");

        assertThatThrownBy(() -> PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(auth)
                .build())
                .as("platform-default trust must reject the test-CA-signed IdP certificate, failing the build")
                // The OAuth2 metadata resolver reports the failure without chaining the TLS cause, so this
                // asserts the observable outcome: with the folded trust the same flow reaches the IdP (test
                // above), without it the handshake fails and the build does not complete.
                .satisfies(t -> assertThat(causeChain(t)).contains("OAuth 2.0 server metadata"));
    }

    /** Issue the IdP request through exactly the client the OAuth2 plugin would use for its token fetch. */
    private HttpResponse probe(PulsarAdminImpl admin) throws Exception {
        FrameworkHttpClientFactory factory = admin.authHttpClientFactoryForTest();
        assertThat(factory).as("the framework HTTP client factory must be bound").isNotNull();
        PulsarHttpClient client = factory.newHttpClient(PulsarHttpClientConfig.builder(TlsPurpose.CLIENT_OAUTH2)
                .connectTimeout(Duration.ofSeconds(10))
                .readTimeout(Duration.ofSeconds(10))
                .requestTimeout(Duration.ofSeconds(10))
                .userAgent("admin-oauth2-idp-tls-test")
                .build());
        return client.execute(HttpRequest.builder(HttpRequest.Method.GET,
                URI.create(idp.baseUrl() + "/probe")).build()).get(30, TimeUnit.SECONDS);
    }

    private static PulsarAdminImpl adminWith(AuthenticationOAuth2 auth) throws Exception {
        // Plaintext admin URL on purpose — see the class javadoc.
        return (PulsarAdminImpl) PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(auth)
                .build();
    }

    /** start() is stubbed: it would fetch OAuth2 server metadata; the fold runs before it. */
    private AuthenticationOAuth2 oauth2WithTrust(String trustCertsFilePath) {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2() {
            private static final long serialVersionUID = 1L;

            @Override
            public void start() {
            }
        };
        auth.configure("{\"type\":\"client_credentials\",\"issuerUrl\":\"" + idp.baseUrl()
                + "\",\"privateKey\":\"data:application/json;base64,e30=\""
                + (trustCertsFilePath == null ? "" : ",\"trustCertsFilePath\":\"" + trustCertsFilePath + "\"")
                + "}");
        return auth;
    }

    /** Build a JKS keystore holding broker.cert.pem (SAN:localhost) + its key, chained to the test CA. */
    private static Path buildServerKeystore() throws Exception {
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);
        X509Certificate[] leaf = PemReader.loadCertificatesFromPemFile(BROKER_CERT);
        X509Certificate[] ca = PemReader.loadCertificatesFromPemFile(CA_CERT);
        Certificate[] chain = new Certificate[leaf.length + ca.length];
        System.arraycopy(leaf, 0, chain, 0, leaf.length);
        System.arraycopy(ca, 0, chain, leaf.length, ca.length);
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry("broker", key, STORE_PW.toCharArray(), chain);
        Path path = Files.createTempFile("pip478-admin-oauth2-idp-keystore", ".jks");
        try (OutputStream out = Files.newOutputStream(path)) {
            keyStore.store(out, STORE_PW.toCharArray());
        }
        return path;
    }

    private static String causeChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            sb.append(cur.getClass().getSimpleName()).append(':').append(String.valueOf(cur.getMessage())).append('\n');
        }
        return sb.toString();
    }

    private static String resource(String name) {
        return new File(Resources.getResource(name).getPath()).getAbsolutePath();
    }
}
