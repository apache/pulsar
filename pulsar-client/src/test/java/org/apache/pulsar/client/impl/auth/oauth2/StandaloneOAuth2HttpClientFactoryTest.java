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
package org.apache.pulsar.client.impl.auth.oauth2;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.http.HttpRequest;
import org.apache.pulsar.http.HttpResponse;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.http.PulsarHttpClientConfig;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: an OAuth2 {@link FlowBase} run standalone — outside any {@code PulsarClient}/{@code PulsarAdmin}
 * that would bind a framework HTTP client factory — self-provisions one via
 * {@link StandaloneOAuth2HttpClientFactory}. This is the path the proxy / broker broker-client / CLI use
 * (via {@code AuthenticationFactory.create(...).start()}); stage 4c had made it throw, breaking those
 * server-side components. These tests exercise the factory directly against a WireMock IdP:
 * <ul>
 *   <li>with no IdP TLS material it talks to a plain-HTTP IdP (self-provisioned event loop, platform trust);</li>
 *   <li>with the flow's own {@code trustCertsFilePath} it trusts a test-CA-signed HTTPS IdP — restoring the
 *       pre-PIP-478 standalone IdP-trust support (issue #24944) on the new SPI;</li>
 *   <li>without that trust, the same HTTPS IdP handshake is rejected (system default trust), proving the
 *       flow's material — not ambient trust — carries the CA.</li>
 * </ul>
 */
public class StandaloneOAuth2HttpClientFactoryTest {

    // broker.cert.pem carries SAN:localhost and chains to ca.cert.pem, so both trust and hostname verification
    // pass when the IdP presents it.
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String CA_CERT = resource("certificate-authority/certs/ca.cert.pem");
    private static final String STORE_PW = "111111";

    private WireMockServer idp;
    private Path serverKeystore;

    @BeforeMethod
    public void setUp() throws Exception {
        serverKeystore = buildServerKeystore();
        idp = new WireMockServer(wireMockConfig().dynamicPort().dynamicHttpsPort()
                .keystorePath(serverKeystore.toString()).keystoreType("JKS")
                .keyManagerPassword(STORE_PW).keystorePassword(STORE_PW));
        idp.start();
        idp.stubFor(get(urlEqualTo("/probe")).willReturn(aResponse().withStatus(200).withBody("ok")));
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
    public void talksToPlainHttpIdpWithoutIdpTlsMaterial() throws Exception {
        try (StandaloneOAuth2HttpClientFactory factory = new StandaloneOAuth2HttpClientFactory(
                Optional.empty(), 300, "standalone-oauth2-test")) {
            PulsarHttpClient client = factory.newHttpClient(oauth2Config());
            HttpResponse response = probe(client, "http://localhost:" + idp.port() + "/probe");
            assertThat(response.statusCode()).isEqualTo(200);
        }
    }

    @Test
    public void trustsCaSignedHttpsIdpWithFlowTrustCerts() throws Exception {
        TlsPolicy idpPolicy = TlsPolicy.builder().format(TlsPolicy.Format.PEM).trustCertsFilePath(CA_CERT).build();
        try (StandaloneOAuth2HttpClientFactory factory = new StandaloneOAuth2HttpClientFactory(
                Optional.of(idpPolicy), 300, "standalone-oauth2-test")) {
            PulsarHttpClient client = factory.newHttpClient(oauth2Config());
            HttpResponse response = probe(client, "https://localhost:" + idp.httpsPort() + "/probe");
            assertThat(response.statusCode()).isEqualTo(200);
        }
    }

    @Test
    public void rejectsCaSignedHttpsIdpWithoutFlowTrustCerts() throws Exception {
        try (StandaloneOAuth2HttpClientFactory factory = new StandaloneOAuth2HttpClientFactory(
                Optional.empty(), 300, "standalone-oauth2-test")) {
            PulsarHttpClient client = factory.newHttpClient(oauth2Config());
            assertThatThrownBy(() -> probe(client, "https://localhost:" + idp.httpsPort() + "/probe"))
                    .as("system default trust must reject the test-CA-signed IdP certificate")
                    .satisfies(t -> assertThat(causeChain(t)).containsAnyOf("SSL", "certificat", "PKIX",
                            "unable to find valid certification path"));
        }
    }

    private static HttpResponse probe(PulsarHttpClient client, String url) throws Exception {
        return client.execute(HttpRequest.builder(HttpRequest.Method.GET, URI.create(url)).build())
                .get(30, TimeUnit.SECONDS);
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
        Path path = Files.createTempFile("pip478-standalone-oauth2-keystore", ".jks");
        try (OutputStream out = Files.newOutputStream(path)) {
            keyStore.store(out, STORE_PW.toCharArray());
        }
        return path;
    }

    private static PulsarHttpClientConfig oauth2Config() {
        return PulsarHttpClientConfig.builder(TlsPurpose.CLIENT_OAUTH2)
                .connectTimeout(Duration.ofSeconds(10))
                .readTimeout(Duration.ofSeconds(10))
                .requestTimeout(Duration.ofSeconds(10))
                .userAgent("standalone-oauth2-test")
                .build();
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
