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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.opentelemetry.api.OpenTelemetry;
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
import java.util.LinkedHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.auth.v5.FrameworkHttpClientFactory;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.tls.ClientTlsFactorySupport;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.http.HttpRequest;
import org.apache.pulsar.http.HttpResponse;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.http.PulsarHttpClientConfig;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 4a: an OAuth2 plugin's own IdP TLS trust reaches the IdP connection through the framework
 * {@link PulsarHttpClient} on the new TLS path (issue #24944). A WireMock HTTPS server presents a
 * certificate signed by the shared test CA; the framework client trusts it only because the OAuth2
 * plugin's {@code trustCertsFilePath} was folded into the {@code CLIENT_OAUTH2} policy the client's TLS
 * factory serves. Without the fold, the same request is rejected (system default trust store), proving the
 * fold — not some ambient trust — is what carries the IdP CA.
 */
public class OAuth2IdpTlsFrameworkClientTest {

    // The shared self-signed broker.keystore.jks carries no SAN, so it fails modern hostname verification (which
    // the folded CLIENT_OAUTH2 policy keeps on, matching v4). Build the WireMock server keystore from
    // broker.cert.pem instead — it has SAN:localhost and chains to ca.cert.pem — so both trust and hostname pass.
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String CA_CERT = resource("certificate-authority/certs/ca.cert.pem");
    private static final String STORE_PW = "111111";

    private WireMockServer idp;
    private Path serverKeystore;
    private EventLoopGroup eventLoopGroup;
    private Timer timer;
    private ScheduledExecutorService scheduler;

    @BeforeMethod
    public void setUp() throws Exception {
        serverKeystore = buildServerKeystore();
        idp = new WireMockServer(wireMockConfig().dynamicHttpsPort()
                .keystorePath(serverKeystore.toString()).keystoreType("JKS")
                .keyManagerPassword(STORE_PW).keystorePassword(STORE_PW));
        idp.start();
        idp.stubFor(get(urlEqualTo("/probe")).willReturn(aResponse().withStatus(200).withBody("ok")));
        eventLoopGroup = new NioEventLoopGroup(1);
        timer = new HashedWheelTimer();
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (idp != null) {
            idp.stop();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
        if (timer != null) {
            timer.stop();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (serverKeystore != null) {
            Files.deleteIfExists(serverKeystore);
        }
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
        Path path = Files.createTempFile("pip478-oauth2-idp-keystore", ".jks");
        try (OutputStream out = Files.newOutputStream(path)) {
            keyStore.store(out, STORE_PW.toCharArray());
        }
        return path;
    }

    @Test
    public void pluginTrustCertsReachIdpViaFrameworkClient() throws Exception {
        ClientConfigurationData conf = newTlsClientConf();
        // The OAuth2 plugin carries its own IdP trust CA (the WireMock server's signer).
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{\"type\":\"client_credentials\",\"issuerUrl\":\"" + idp.baseUrl()
                + "\",\"privateKey\":\"data:application/json;base64,e30=\",\"trustCertsFilePath\":\"" + CA_CERT
                + "\"}");
        conf.setAuthentication(auth);

        PulsarTlsFactory factory = ClientTlsFactorySupport.resolveClientTlsFactory(
                conf, scheduler, scheduler, OpenTelemetry.noop());
        conf.setTlsFactory(factory);
        try (FrameworkHttpClientFactory httpFactory = frameworkHttpFactory(conf)) {
            PulsarHttpClient client = httpFactory.newHttpClient(oauth2Config());
            HttpResponse response = client.execute(HttpRequest.builder(HttpRequest.Method.GET,
                    URI.create(idp.baseUrl() + "/probe")).build()).get(30, TimeUnit.SECONDS);
            assertThat(response.statusCode()).isEqualTo(200);
        } finally {
            factory.close();
        }
    }

    @Test
    public void withoutTheFoldTheFrameworkClientRejectsTheIdpCert() throws Exception {
        // No OAuth2 IdP trust: CLIENT_OAUTH2 resolves to the system default trust store, which does not trust
        // the test CA, so the IdP handshake fails — the contrast that proves the fold is what carries the CA.
        ClientConfigurationData conf = newTlsClientConf();

        PulsarTlsFactory factory = ClientTlsFactorySupport.resolveClientTlsFactory(
                conf, scheduler, scheduler, OpenTelemetry.noop());
        conf.setTlsFactory(factory);
        try (FrameworkHttpClientFactory httpFactory = frameworkHttpFactory(conf)) {
            PulsarHttpClient client = httpFactory.newHttpClient(oauth2Config());
            assertThatThrownBy(() -> client.execute(HttpRequest.builder(HttpRequest.Method.GET,
                    URI.create(idp.baseUrl() + "/probe")).build()).get(30, TimeUnit.SECONDS))
                    .as("system default trust must reject the test-CA-signed IdP certificate")
                    .satisfies(t -> assertThat(causeChain(t)).containsAnyOf("SSL", "certificat", "PKIX",
                            "unable to find valid certification path"));
        } finally {
            factory.close();
        }
    }

    /** A minimal TLS client config on the new PIP-478 path (CLIENT_DEFAULT trust = test CA for the probe). */
    private ClientConfigurationData newTlsClientConf() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar+ssl://localhost:6651");
        conf.setTlsPolicyMap(new LinkedHashMap<>());
        conf.setTlsTrustCertsFilePath(CA_CERT);
        return conf;
    }

    private FrameworkHttpClientFactory frameworkHttpFactory(ClientConfigurationData conf) {
        return new FrameworkHttpClientFactory(() -> eventLoopGroup, () -> timer, () -> null,
                conf::getTlsFactory, conf, "oauth2-idp-tls-test");
    }

    private static PulsarHttpClientConfig oauth2Config() {
        return PulsarHttpClientConfig.builder(TlsPurpose.CLIENT_OAUTH2)
                .connectTimeout(Duration.ofSeconds(10))
                .readTimeout(Duration.ofSeconds(10))
                .requestTimeout(Duration.ofSeconds(10))
                .userAgent("oauth2-idp-tls-test")
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
