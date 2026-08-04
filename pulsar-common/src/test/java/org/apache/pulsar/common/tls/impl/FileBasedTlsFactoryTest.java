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
import static org.apache.pulsar.common.tls.impl.TlsTestSupport.initContext;
import static org.apache.pulsar.common.tls.impl.TlsTestSupport.resource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.awaitility.Awaitility;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FileBasedTlsFactoryTest {

    private static final String RSA_CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");
    // EC identity is signed by the EC CA — untrusted by the RSA CA the server above trusts.
    private static final String EC_CLIENT_CERT = resource("certificate-authority/ec/client.cert.pem");
    private static final String EC_CLIENT_KEY = resource("certificate-authority/ec/client.key-pk8.pem");

    private static final String KEYSTORE = resource("certificate-authority/jks/broker.keystore.jks");
    private static final String TRUSTSTORE = resource("certificate-authority/jks/broker.truststore.jks");
    private static final String STORE_PW = "111111";

    private ScheduledExecutorService scheduler;
    private final Executor directExecutor = Runnable::run;
    private Path tempDir;

    @BeforeMethod
    public void setUp() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        tempDir = Files.createTempDirectory("pip478-tls-");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        scheduler.shutdownNow();
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    private FileBasedTlsFactory factory(Map<TlsPurpose, TlsPolicy> policies, FileBasedTlsFactorySettings settings) {
        FileBasedTlsFactory factory = new FileBasedTlsFactory(policies, settings);
        factory.initialize(initContext(scheduler, directExecutor)).join();
        return factory;
    }

    @Test
    public void buildsNettyAndJdkContextsFromPem() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        Optional<TlsHandle<SslContext>> netty = factory.createInstance(TlsPurpose.BROKER, SslContext.class).join();
        Optional<TlsHandle<SSLContext>> jdk = factory.createInstance(TlsPurpose.BROKER, SSLContext.class).join();

        assertThat(netty).isPresent();
        assertThat(netty.get().get()).isNotNull();
        assertThat(jdk).isPresent();
        assertThat(jdk.get().get()).isNotNull();
        netty.get().dispose();
        jdk.get().dispose();
        factory.close();
    }

    // PIP-478 Part D: a TlsPolicy.jsseProvider pins the named java.security.Provider — the JDK SSLContext is
    // built with that provider, and the Netty context builds on the JDK engine backed by it. SunJSSE is always
    // installed, so this round-trips the wiring without a FIPS/BouncyCastle dependency.
    @Test
    public void jsseProviderPinsTheNamedProviderOnTheBuiltContexts() throws Exception {
        TlsPolicy policy = TlsPolicy.builder()
                .format(TlsPolicy.Format.PEM)
                .trustCertsFilePath(RSA_CA)
                .certificateFilePath(BROKER_CERT)
                .keyFilePath(BROKER_KEY)
                .jsseProvider("SunJSSE")
                .build();
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.defaults());

        Optional<TlsHandle<SSLContext>> jdk = factory.createInstance(TlsPurpose.BROKER, SSLContext.class).join();
        assertThat(jdk).isPresent();
        assertThat(jdk.get().get().getProvider().getName())
                .as("the JDK SSLContext is built with the pinned jsseProvider").isEqualTo("SunJSSE");

        Optional<TlsHandle<SslContext>> netty = factory.createInstance(TlsPurpose.BROKER, SslContext.class).join();
        assertThat(netty).as("the Netty context builds on the JDK engine backed by the pinned provider").isPresent();
        assertThat(netty.get().get()).isNotNull();

        jdk.get().dispose();
        netty.get().dispose();
        factory.close();
    }

    @Test
    public void buildsContextsFromKeystore() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.keyStore(TRUSTSTORE, STORE_PW, KEYSTORE, STORE_PW, "JKS")),
                FileBasedTlsFactorySettings.defaults());

        Optional<TlsHandle<SslContext>> netty = factory.createInstance(TlsPurpose.BROKER, SslContext.class).join();
        assertThat(netty).isPresent();
        assertThat(netty.get().get()).isNotNull();
        factory.close();
    }

    @Test
    public void unconfiguredMintedClientPurposeResolvesToSystemDefault() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.pem(RSA_CA, null, null)),
                FileBasedTlsFactorySettings.defaults());

        // An arbitrary minted client purpose with no configured policy resolves terminally to the system
        // default (present, not empty, not an error) — there is no fallback chain to another purpose.
        TlsPurpose minted = TlsPurpose.client("oauth2.myPlugin");
        Optional<TlsHandle<SslContext>> resolved = factory.createInstance(minted, SslContext.class).join();
        assertThat(resolved).as("unconfigured minted client purpose resolves to the system default").isPresent();
        assertThat(resolved.get().get()).isNotNull();
        factory.close();
    }

    @Test
    public void oauth2ResolvesToSystemDefaultNotEmptyNotError() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.pem(RSA_CA, null, null)),
                FileBasedTlsFactorySettings.defaults());

        Optional<TlsHandle<SslContext>> handle =
                factory.createInstance(TlsPurpose.CLIENT_OAUTH2, SslContext.class).join();
        assertThat(handle).as("CLIENT_OAUTH2 empty fallback resolves to the system default").isPresent();

        // The system default verifies hostnames (secure defaults) — proven by the baked HTTPS algorithm.
        SSLEngine engine = ((SslContext) handle.get().get()).newEngine(ByteBufAllocator.DEFAULT);
        assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        factory.close();
    }

    @Test
    public void unconfiguredServerPurposeFailsExceptionally() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        // PROXY (server role, no fallback) is not configured: this is an error, not empty().
        assertThatThrownBy(() -> factory.createInstance(TlsPurpose.PROXY, SslContext.class).join())
                .hasCauseInstanceOf(FileBasedTlsFactory.TlsMaterialUnavailableException.class);
        factory.close();
    }

    @Test
    public void unsupportedClassReturnsEmpty() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        // A class the factory cannot build natively (stands in for Jetty's SslContextFactory.Server,
        // which pulsar-common cannot reference) yields empty(), which the framework synthesizes.
        Optional<TlsHandle<String>> handle = factory.createInstance(TlsPurpose.BROKER, String.class).join();
        assertThat(handle).isEmpty();
        factory.close();
    }

    @Test
    public void resolvedButUnbuildableFailsExceptionallyNeverEmpty() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA,
                        tempDir.resolve("missing-cert.pem").toString(),
                        tempDir.resolve("missing-key.pem").toString())),
                FileBasedTlsFactorySettings.defaults());

        assertThatThrownBy(() -> factory.createInstance(TlsPurpose.BROKER, SslContext.class).join())
                .isInstanceOf(Exception.class);
        factory.close();
    }

    // SPI contract: argument-validation failures complete the returned future exceptionally; the
    // subscribing overload must not throw NPE on the calling thread for a null callback.
    @Test
    public void subscribingCreateInstanceRejectsNullCallbackAsynchronously() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());
        java.util.concurrent.CompletableFuture<?> future =
                factory.createInstance(TlsPurpose.BROKER, SslContext.class, null);
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).hasCauseInstanceOf(NullPointerException.class);
        factory.close();
    }

    @Test
    public void clientHostnameVerificationIsBakedIntoTheContext() throws Exception {
        FileBasedTlsFactory verifying = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).enableHostnameVerification(true).build()),
                FileBasedTlsFactorySettings.defaults());
        FileBasedTlsFactory notVerifying = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).enableHostnameVerification(false).build()),
                FileBasedTlsFactorySettings.defaults());

        SslContext verifyingContext = (SslContext) verifying.createInstance(TlsPurpose.CLIENT_DEFAULT,
                SslContext.class).join().get().get();
        SslContext plainContext = (SslContext) notVerifying.createInstance(TlsPurpose.CLIENT_DEFAULT,
                SslContext.class).join().get().get();
        SSLEngine verifyingEngine = verifyingContext.newEngine(ByteBufAllocator.DEFAULT);
        SSLEngine plainEngine = plainContext.newEngine(ByteBufAllocator.DEFAULT);
        // The peer-info variant matters independently: Netty 4.2 defaults client engines created with peer
        // info to "HTTPS" endpoint identification, which the policy-off context build must clear.
        SSLEngine verifyingPeerEngine = verifyingContext.newEngine(ByteBufAllocator.DEFAULT, "broker.example", 6651);
        SSLEngine plainPeerEngine = plainContext.newEngine(ByteBufAllocator.DEFAULT, "broker.example", 6651);

        assertThat(verifyingEngine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        assertThat(plainEngine.getSSLParameters().getEndpointIdentificationAlgorithm()).isNullOrEmpty();
        assertThat(verifyingPeerEngine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        assertThat(plainPeerEngine.getSSLParameters().getEndpointIdentificationAlgorithm()).isNullOrEmpty();
        verifying.close();
        notVerifying.close();
    }

    @Test
    public void insecureServerAcceptsUntrustedClientAndCapturesItsCert() throws Exception {
        // Insecure server installs InsecureTrustManagerFactory but keeps ClientAuth OPTIONAL,
        // so an untrusted (cross-CA) client certificate still completes the handshake and is captured.
        FileBasedTlsFactory server = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                        .allowInsecureConnection(true).build()),
                FileBasedTlsFactorySettings.builder().requireTrustedClientCert(false).build());
        FileBasedTlsFactory client = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(EC_CLIENT_CERT).keyFilePath(EC_CLIENT_KEY)
                        .enableHostnameVerification(false).build()),
                FileBasedTlsFactorySettings.defaults());

        SSLEngine serverEngine = serverEngine(server);
        SSLEngine clientEngine = clientEngine(client);
        handshake(clientEngine, serverEngine);

        assertThat(serverEngine.getSession().getPeerCertificates())
                .as("insecure server captured the untrusted client certificate").isNotEmpty();
        server.close();
        client.close();
    }

    @Test
    public void secureServerDoesNotCaptureUntrustedClientCert() throws Exception {
        // The contrast to the insecure case: a SECURE server (real trust manager) advertises only its trusted
        // CA (RSA) in the CertificateRequest, so the client withholds its cross-CA (EC) certificate and
        // the handshake completes anonymously — the server captures no client certificate.
        FileBasedTlsFactory server = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                        .allowInsecureConnection(false).build()),
                FileBasedTlsFactorySettings.defaults());
        FileBasedTlsFactory client = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(EC_CLIENT_CERT).keyFilePath(EC_CLIENT_KEY)
                        .enableHostnameVerification(false).build()),
                FileBasedTlsFactorySettings.defaults());

        SSLEngine serverEngine = serverEngine(server);
        SSLEngine clientEngine = clientEngine(client);
        handshake(clientEngine, serverEngine);
        assertThatThrownBy(() -> serverEngine.getSession().getPeerCertificates())
                .as("secure server did not capture the untrusted client certificate")
                .isInstanceOf(SSLPeerUnverifiedException.class);
        server.close();
        client.close();
    }

    // ---- T1: forced-untrusted-cert Netty rejection (a real negative, not mere non-capture). ----
    // The two tests above depend on the client's default JSSE key manager silently WITHHOLDING its cross-CA
    // certificate when the server advertises only its own trusted CA — so they never prove the server would
    // reject a cert that is actually presented. These force the client to present the untrusted (EC, cross-CA)
    // certificate via a ForcingKeyManager and assert the server-side decision directly: a secure server (real
    // trust manager) REJECTS the handshake, an insecure server (InsecureTrustManagerFactory) accepts it and
    // captures the cert (the behavior TLS authentication relies on). Mirrors JettyTlsFactoryTest's
    // optionalClientAuthScopesTrustAllToInsecureFlag on the Netty engine.

    @Test
    public void forcedUntrustedClientCertRejectedBySecureServer() throws Exception {
        FileBasedTlsFactory server = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                        .allowInsecureConnection(false).build()),
                FileBasedTlsFactorySettings.builder().requireTrustedClientCert(true).build());

        SSLEngine serverEngine = tls12(serverEngine(server));
        SSLEngine clientEngine = tls12(forcingUntrustedClientEngine());
        // The forced EC certificate is actually sent; the secure server's real (RSA) trust manager rejects it
        // and aborts the handshake — the security-critical negative.
        assertThatThrownBy(() -> handshake(clientEngine, serverEngine))
                .as("secure server rejects the forced untrusted client certificate")
                .isInstanceOf(SSLException.class);
        server.close();
    }

    @Test
    public void forcedUntrustedClientCertAcceptedAndCapturedByInsecureServer() throws Exception {
        FileBasedTlsFactory server = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                        .allowInsecureConnection(true).build()),
                FileBasedTlsFactorySettings.builder().requireTrustedClientCert(false).build());

        SSLEngine serverEngine = tls12(serverEngine(server));
        SSLEngine clientEngine = tls12(forcingUntrustedClientEngine());
        handshake(clientEngine, serverEngine);
        assertThat(serverEngine.getSession().getPeerCertificates())
                .as("insecure server captured the forced untrusted client certificate").isNotEmpty();
        server.close();
    }

    @Test
    public void rotationDeliversRebuiltInstanceToSubscriber() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        List<SslContext> deliveries = new CopyOnWriteArrayList<>();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, deliveries::add).join();
        assertThat(deliveries).as("initial delivery").hasSize(1);

        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.size() == 2);
        assertThat(deliveries.get(1)).as("rebuilt on rotation").isNotSameAs(deliveries.get(0));
        factory.close();
    }

    @Test
    public void touchWithoutContentChangeSuppressesReload() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        AtomicInteger deliveries = new AtomicInteger();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, ctx -> deliveries.incrementAndGet()).join();
        assertThat(deliveries.get()).isEqualTo(1);

        // Touch the cert file (advance mtime) without changing its content.
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(System.currentTimeMillis() + 5000));
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(4))
                .until(() -> deliveries.get() == 1);
        factory.close();
    }

    @Test
    public void failedRotationKeepsLastGoodThenRecovers() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        List<SslContext> deliveries = new CopyOnWriteArrayList<>();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, deliveries::add).join();
        assertThat(deliveries).hasSize(1);

        // Corrupt the cert file: the rebuild fails, the subscriber keeps the last-good instance.
        Files.writeString(tempDir.resolve("cert.pem"), "-----BEGIN CERTIFICATE-----\nnot a cert\n");
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(System.currentTimeMillis() + 5000));
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(4))
                .until(() -> deliveries.size() == 1);

        // A subsequent good change is picked up (retry-on-next-change).
        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.size() == 2);
        factory.close();
    }

    @Test
    public void oneShotServesLastGoodWhenReloadFails() throws Exception {
        // PIP-478: a one-shot acquisition during a non-atomic rotation window (cert briefly unreadable)
        // must serve the last-good cached context rather than failing, mirroring the subscription poll's
        // keep-last-good semantics.
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.defaults());

        TlsHandle<SslContext> firstHandle =
                factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get();
        SslContext first = firstHandle.get();
        assertThat(first).isNotNull();

        // Corrupt the cert file so a fresh load fails, and advance mtime so the source attempts a reload.
        Files.writeString(tempDir.resolve("cert.pem"), "-----BEGIN CERTIFICATE-----\nnot a cert\n");
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"),
                FileTime.fromMillis(System.currentTimeMillis() + 5000));

        Optional<TlsHandle<SslContext>> retry =
                factory.createInstance(TlsPurpose.BROKER, SslContext.class).join();
        assertThat(retry).as("keep-last-good instead of failing the acquisition").isPresent();
        assertThat(retry.get().get()).as("served the last-good context").isSameAs(first);

        retry.get().dispose();
        firstHandle.dispose();
        factory.close();
    }

    @Test
    public void oneShotFailsWhenNoLastGoodExists() throws Exception {
        // With no prior successful load there is nothing to fall back on: the acquisition fails (fail-fast).
        Files.copy(Paths.get(RSA_CA), tempDir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.writeString(tempDir.resolve("cert.pem"), "-----BEGIN CERTIFICATE-----\nnot a cert\n");
        Files.copy(Paths.get(BROKER_KEY), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        TlsPolicy policy = TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString());
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.defaults());

        assertThatThrownBy(() -> factory.createInstance(TlsPurpose.BROKER, SslContext.class).join())
                .hasCauseInstanceOf(Exception.class);
        factory.close();
    }

    @Test
    public void subscriberCallbackExceptionDoesNotKillSubscription() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        AtomicInteger deliveries = new AtomicInteger();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, ctx -> {
            deliveries.incrementAndGet();
            throw new RuntimeException("boom");
        }).join();
        assertThat(deliveries.get()).as("initial delivery still happened despite throwing callback").isEqualTo(1);

        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.get() == 2);
        factory.close();
    }

    /**
     * A one-shot acquisition must not swallow a rotation. {@code currentMaterial()} calls {@code refresh()},
     * which commits the new mtime baseline, so whichever caller re-stats first is the ONLY one that sees
     * {@code changed=true}; every later poll reports no change. Before the fix, a one-shot acquirer that
     * observed the rotation first left the subscribers on that same purpose wedged on the pre-rotation context
     * indefinitely (the https:// / HTTP-lookup client case: {@code FrameworkHttpClientFactory} subscribes to
     * {@code CLIENT_DEFAULT} while another path acquires one-shot on it).
     *
     * <p>Polling is disabled here ({@code refreshIntervalSeconds(0)}) so the ONLY way the subscriber can learn
     * of the rotation is the one-shot's fan-out — no timing race, no background poll to mask the wedge.
     */
    @Test
    public void oneShotObservingRotationFirstStillNotifiesSubscribers() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(0).build());

        List<SslContext> deliveries = new CopyOnWriteArrayList<>();
        TlsHandle<SslContext> subscription =
                factory.createInstance(TlsPurpose.BROKER, SslContext.class, deliveries::add).join().orElseThrow();
        assertThat(deliveries).as("initial delivery").hasSize(1);

        overwriteServerCerts(PROXY_CERT, PROXY_KEY);

        // The one-shot acquirer re-stats first and consumes the source's change signal.
        TlsHandle<SslContext> oneShot =
                factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().orElseThrow();

        assertThat(deliveries).as("the rotation observed by the one-shot is still pushed to the subscriber")
                .hasSize(2);
        assertThat(deliveries.get(1)).as("subscriber got the rebuilt context").isNotSameAs(deliveries.get(0));
        assertThat(subscription.get()).as("the subscription handle serves the rotated context")
                .isSameAs(deliveries.get(1)).isSameAs(oneShot.get());

        // A further acquisition without any file change must not re-deliver.
        factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().orElseThrow().dispose();
        assertThat(deliveries).as("no change -> no further delivery").hasSize(2);

        oneShot.dispose();
        subscription.dispose();
        factory.close();
    }

    // ---- PIP-478: OpenSSL rotation use-after-free guard (deferred release + per-use pinning). ----
    // These exercise the OpenSSL engine specifically: on the JDK engine SslContext ref-counting is a no-op, so
    // CI's default JDK provider can never surface the use-after-free the review flagged. On OpenSSL the native
    // SSL_CTX is freed when a context's refcount reaches zero.

    @Test
    public void openSslRotationKeepsSupersededContextUsableWhileBorrowed() throws Exception {
        assumeOpenSslAvailable();
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().engineProvider(SslProvider.OPENSSL)
                        .refreshIntervalSeconds(1).build());

        List<SslContext> deliveries = new CopyOnWriteArrayList<>();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, deliveries::add).join();
        assertThat(deliveries).hasSize(1);
        SslContext borrowed = deliveries.get(0);
        assertThat(borrowed).as("OpenSSL contexts are reference-counted").isInstanceOf(ReferenceCounted.class);

        // A consumer read the borrow off its volatile and is about to build a handler/engine from it; interleave
        // a rotation that supersedes it on the poll thread.
        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.size() == 2);

        // Deferred release: the just-superseded borrow was NOT released to refcount 0 on the poll thread,
        // so the in-flight consumer can still build an engine from it. Pre-fix, its native SSL_CTX was freed the
        // instant the new context was published, and this newEngine would use freed memory.
        assertThat(((ReferenceCounted) borrowed).refCnt()).as("superseded borrow kept alive").isPositive();
        SSLEngine engine = borrowed.newEngine(ByteBufAllocator.DEFAULT);
        assertThat(engine).isNotNull();
        ReferenceCountUtil.release(engine);
        factory.close();
    }

    @Test
    public void openSslRotationReleasesSupersededContextOneGenerationLater() throws Exception {
        assumeOpenSslAvailable();
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().engineProvider(SslProvider.OPENSSL)
                        .refreshIntervalSeconds(1).build());

        List<SslContext> deliveries = new CopyOnWriteArrayList<>();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, deliveries::add).join();
        SslContext gen0 = deliveries.get(0);

        // One rotation: gen0 is superseded but kept alive one extra generation.
        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.size() == 2);
        assertThat(((ReferenceCounted) gen0).refCnt()).as("survives one generation").isPositive();

        // A second rotation makes gen0 the N-1 instance, released on this (N+1th) delivery — the deferral is
        // bounded, so nothing leaks.
        overwriteServerCerts(BROKER_CERT, BROKER_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.size() == 3);
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(((ReferenceCounted) gen0).refCnt()).as("released one generation later").isZero());
        factory.close();
    }

    @Test
    public void withPinnedContextReReadsWhenBorrowWasFreed() throws Exception {
        assumeOpenSslAvailable();
        // Two independent OpenSSL contexts: the first is freed, standing in for a borrow that a rotation
        // released between the volatile read and the pin; the second is the live current context.
        SslContext dead = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).build();
        SslContext live = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).build();
        ReferenceCountUtil.release(dead);
        assertThat(((ReferenceCounted) dead).refCnt()).as("freed borrow").isZero();

        AtomicInteger reads = new AtomicInteger();
        Supplier<SslContext> source = () -> reads.getAndIncrement() == 0 ? dead : live;
        int liveBefore = ((ReferenceCounted) live).refCnt();

        // retain() on the freed borrow throws IllegalReferenceCountException; the helper must re-read the source
        // and pin the live context instead.
        SslContext used = TlsContextAcquisition.withPinnedContext(source, ctx -> ctx);

        assertThat(used).as("re-read past the freed borrow").isSameAs(live);
        assertThat(reads.get()).as("read at least twice").isGreaterThanOrEqualTo(2);
        assertThat(((ReferenceCounted) live).refCnt()).as("pin is balanced (nets to zero)").isEqualTo(liveBefore);
        ReferenceCountUtil.release(live);
    }

    @Test
    public void createInstanceAfterCloseFailsWithoutRebuilding() {
        // PIP-478: a closed factory must not rebuild/cache a context (which would leak, since close()
        // already released the memos). createInstance completes exceptionally instead.
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());
        factory.close();

        assertThatThrownBy(() -> factory.createInstance(TlsPurpose.BROKER, SslContext.class).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() ->
                factory.createInstance(TlsPurpose.BROKER, SslContext.class, ctx -> { }).join())
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    public void createInstanceRacingConcurrentCloseFailsAndDoesNotRebuild() {
        // PIP-478 FIX F: close() runs releaseAll() under the source lock. A createInstance task that passed
        // the outer closed-check but is only scheduled to run AFTER close() completed must NOT build+cache a
        // fresh context (whose factory-owned reference close() would never release -> shutdown leak). A gated
        // executor reproduces exactly that interleaving deterministically; the task must fail instead.
        GatedExecutor gated = new GatedExecutor();
        FileBasedTlsFactory factory = new FileBasedTlsFactory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());
        factory.initialize(initContext(scheduler, gated)).join();

        gated.pause();
        // Passes the outer closed-check; the build task is now held on the executor, not yet run.
        CompletableFuture<Optional<TlsHandle<SslContext>>> pending =
                factory.createInstance(TlsPurpose.BROKER, SslContext.class);
        assertThat(pending).isNotDone();

        // close() runs first (releaseAll under the source lock).
        factory.close();

        // Now the held task runs and re-checks closed under the source lock: it fails instead of rebuilding.
        gated.releaseHeld();
        assertThat(pending).isDone();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);

        // The subscribing variant behaves identically.
        GatedExecutor gated2 = new GatedExecutor();
        FileBasedTlsFactory factory2 = new FileBasedTlsFactory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());
        factory2.initialize(initContext(scheduler, gated2)).join();
        gated2.pause();
        CompletableFuture<Optional<TlsHandle<SslContext>>> pending2 =
                factory2.createInstance(TlsPurpose.BROKER, SslContext.class, ctx -> { });
        factory2.close();
        gated2.releaseHeld();
        assertThatThrownBy(pending2::join).hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    public void oneShotHandleDisposeIsIdempotentUnderConcurrency() throws Exception {
        // PIP-478 FIX E: OneShotHandle.dispose() must release exactly once. A plain check-then-set on a
        // volatile flag lets two concurrent disposers both pass the guard and over-release, freeing the shared
        // cached context's native SSL_CTX (refCnt -> 0). OpenSSL is required because JDK contexts are not
        // reference-counted (release is a no-op there, so the bug cannot surface). With the AtomicBoolean
        // compareAndSet guard, exactly one release happens and the cache keeps its reference across iterations.
        assumeOpenSslAvailable();
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.builder().engineProvider(SslProvider.OPENSSL).build());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 500; i++) {
                TlsHandle<SslContext> handle =
                        factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get();
                SslContext ctx = handle.get();
                assertThat(ctx).isInstanceOf(ReferenceCounted.class);
                CyclicBarrier barrier = new CyclicBarrier(2);
                Callable<Void> disposer = () -> {
                    barrier.await();
                    handle.dispose();
                    return null;
                };
                Future<Void> f1 = pool.submit(disposer);
                Future<Void> f2 = pool.submit(disposer);
                f1.get();
                f2.get();
                // Exactly one release despite two concurrent dispose() calls: the cache still owns the context.
                // Pre-fix, both callers over-released and freed the shared cached context (refCnt 0), which the
                // next iteration's retain() would reject with IllegalReferenceCountException.
                assertThat(((ReferenceCounted) ctx).refCnt())
                        .as("cache reference survives concurrent dispose at iteration %d", i).isPositive();
            }
        } finally {
            pool.shutdownNow();
            factory.close();
        }
    }

    /** An executor that can hold submitted tasks so a test can interleave a close() before they run. */
    private static final class GatedExecutor implements Executor {
        private volatile boolean paused;
        private final CopyOnWriteArrayList<Runnable> held = new CopyOnWriteArrayList<>();

        void pause() {
            paused = true;
        }

        void releaseHeld() {
            paused = false;
            for (Runnable r : held) {
                r.run();
            }
            held.clear();
        }

        @Override
        public void execute(Runnable command) {
            if (paused) {
                held.add(command);
            } else {
                command.run();
            }
        }
    }

    private static void assumeOpenSslAvailable() {
        if (!OpenSsl.isAvailable()) {
            throw new SkipException("Native OpenSSL (netty-tcnative) not available in this environment");
        }
    }

    @Test
    public void initialDeliveryHappensBeforeFutureCompletes() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        AtomicInteger deliveries = new AtomicInteger();
        // No await: by the time join() returns, the initial delivery must already have run.
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, ctx -> deliveries.incrementAndGet()).join();
        assertThat(deliveries.get()).isEqualTo(1);
        factory.close();
    }

    @Test
    public void synthesizeNettyFromJdkWrapsTheJdkContext() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());
        SSLContext jdk = (SSLContext) factory.createInstance(TlsPurpose.BROKER, SSLContext.class)
                .join().get().get();

        SslContext synthesized = TlsContexts.synthesizeNettyFromJdk(jdk, false, true);
        assertThat(synthesized).isNotNull();
        assertThat(synthesized.isServer()).isTrue();
        assertThat(synthesized.newEngine(ByteBufAllocator.DEFAULT)).isNotNull();
        factory.close();
    }

    @Test
    public void returnsEmptyForSslParametersCompanion() {
        // PIP-478: the default file-based factory bakes engine policy natively into its Netty/JDK contexts,
        // so it exposes no SSLParameters companion for the framework to overlay -> empty() for every form.
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        assertThat(factory.createInstance(TlsPurpose.BROKER, SSLParameters.class).join()).isEmpty();
        assertThat(factory.createInstance(TlsPurpose.BROKER, SSLParameters.class, p -> { }).join()).isEmpty();
        factory.close();
    }

    @Test
    public void probeRetainsInitialInstanceAndFailsFastOnBootError() {
        FileBasedTlsFactory ok = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());
        TlsHandle<SslContext> handle = TlsFactoryProbe.probe(ok, TlsPurpose.BROKER, SslContext.class);
        assertThat(handle.get()).isNotNull();
        ok.close();

        FileBasedTlsFactory broken = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA,
                        tempDir.resolve("nope-cert.pem").toString(), tempDir.resolve("nope-key.pem").toString())),
                FileBasedTlsFactorySettings.defaults());
        assertThatThrownBy(() -> TlsFactoryProbe.probe(broken, TlsPurpose.BROKER, SslContext.class))
                .isInstanceOf(IllegalStateException.class);
        broken.close();
    }

    // ---- PIP-478 stage 4c: ports the removed SslContextTest matrix (SslProvider x ciphers x keystore/PEM) ----
    // onto the new FileBasedTlsFactory. OpenSSL rejects the JDK-named TLS 1.2 ciphers used here (matching the
    // removed test's assertion); the JDK engine accepts them, and keystore-format material builds regardless.

    private static final List<String> MATRIX_CIPHERS = List.of(
            "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

    @DataProvider(name = "engineAndCiphers")
    public static Object[][] engineAndCiphers() {
        return new Object[][] {
                {SslProvider.JDK, MATRIX_CIPHERS},
                {SslProvider.JDK, null},
                {SslProvider.OPENSSL, MATRIX_CIPHERS},
                {SslProvider.OPENSSL, null},
        };
    }

    @Test(dataProvider = "engineAndCiphers")
    public void serverPemContextAcrossEngineAndCiphers(SslProvider provider, List<String> ciphers) {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                        .ciphers(ciphers).build()),
                FileBasedTlsFactorySettings.builder().engineProvider(provider)
                        .requireTrustedClientCert(true).build());
        assertNettyContextBuildsUnlessOpenSslWithCiphers(factory, TlsPurpose.BROKER, provider, ciphers);
        factory.close();
    }

    @Test(dataProvider = "engineAndCiphers")
    public void clientPemContextAcrossEngineAndCiphers(SslProvider provider, List<String> ciphers) {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).allowInsecureConnection(true)
                        .ciphers(ciphers).build()),
                FileBasedTlsFactorySettings.builder().engineProvider(provider).build());
        assertNettyContextBuildsUnlessOpenSslWithCiphers(factory, TlsPurpose.CLIENT_DEFAULT, provider, ciphers);
        factory.close();
    }

    @Test
    public void serverKeystoreContextBuildsWithCiphers() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .format(TlsPolicy.Format.KEYSTORE).keyStoreType("JKS").trustStoreType("JKS")
                        .trustStorePath(TRUSTSTORE).trustStorePassword(STORE_PW)
                        .keyStorePath(KEYSTORE).keyStorePassword(STORE_PW)
                        .ciphers(MATRIX_CIPHERS).build()),
                FileBasedTlsFactorySettings.builder().requireTrustedClientCert(true).build());
        Optional<TlsHandle<SslContext>> handle = factory.createInstance(TlsPurpose.BROKER, SslContext.class).join();
        assertThat(handle).isPresent();
        assertThat(handle.get().get()).isNotNull();
        handle.get().dispose();
        factory.close();
    }

    @Test
    public void clientKeystoreContextBuildsWithCiphers() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .format(TlsPolicy.Format.KEYSTORE).keyStoreType("JKS").trustStoreType("JKS")
                        .trustStorePath(TRUSTSTORE).trustStorePassword(STORE_PW)
                        .ciphers(MATRIX_CIPHERS).build()),
                FileBasedTlsFactorySettings.defaults());
        Optional<TlsHandle<SslContext>> handle =
                factory.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class).join();
        assertThat(handle).isPresent();
        assertThat(handle.get().get()).isNotNull();
        handle.get().dispose();
        factory.close();
    }

    @Test
    public void defaultProtocolsAppliedWhenPolicyLeavesThemUnset() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY),
                        TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder().trustCertsFilePath(RSA_CA).build()),
                FileBasedTlsFactorySettings.defaults());
        // With no protocols configured the {TLSv1.3, TLSv1.2} floor is enabled on both the server and the
        // client context (the set the removed DefaultPulsarSslFactory forced), not the provider default — so an
        // upgrade to the PIP-478 TLS path does not silently change the enabled protocol set.
        assertThat(serverEngine(factory).getEnabledProtocols()).containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
        assertThat(clientEngine(factory).getEnabledProtocols()).containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
        factory.close();
    }

    private static void assertNettyContextBuildsUnlessOpenSslWithCiphers(FileBasedTlsFactory factory,
            TlsPurpose purpose, SslProvider provider, List<String> ciphers) {
        if (ciphers != null && provider == SslProvider.OPENSSL) {
            // OpenSSL does not support these JDK-named TLS 1.2 ciphers (as the removed SslContextTest asserted).
            assertThatThrownBy(() -> factory.createInstance(purpose, SslContext.class).join())
                    .hasCauseInstanceOf(SSLException.class);
            return;
        }
        Optional<TlsHandle<SslContext>> handle = factory.createInstance(purpose, SslContext.class).join();
        assertThat(handle).isPresent();
        assertThat(handle.get().get()).isNotNull();
        handle.get().dispose();
    }

    private SSLEngine serverEngine(FileBasedTlsFactory factory) {
        SslContext ctx = (SslContext) factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get().get();
        SSLEngine engine = ctx.newEngine(ByteBufAllocator.DEFAULT);
        engine.setUseClientMode(false);
        return engine;
    }

    private SSLEngine clientEngine(FileBasedTlsFactory factory) {
        SslContext ctx = (SslContext) factory.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class)
                .join().get().get();
        SSLEngine engine = ctx.newEngine(ByteBufAllocator.DEFAULT);
        engine.setUseClientMode(true);
        return engine;
    }

    // Pin TLSv1.2 so an untrusted-cert rejection surfaces synchronously within the in-memory handshake pump
    // rather than as a TLS 1.3 post-handshake alert (client auth completes symmetrically within the handshake).
    private static SSLEngine tls12(SSLEngine engine) {
        engine.setEnabledProtocols(new String[] {"TLSv1.2"});
        return engine;
    }

    // A JDK client engine whose key manager is FORCED to present the untrusted (EC, cross-CA) certificate
    // regardless of the server's advertised acceptable-CA list, so the certificate is actually sent (and can
    // therefore be rejected) rather than silently withheld by the default JSSE key manager. Trusts the RSA CA
    // so the client accepts the server certificate.
    private static SSLEngine forcingUntrustedClientEngine() throws Exception {
        X509Certificate[] chain = PemReader.loadCertificatesFromPemFile(EC_CLIENT_CERT);
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(EC_CLIENT_KEY);
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        X509Certificate[] ca = PemReader.loadCertificatesFromPemFile(RSA_CA);
        for (int i = 0; i < ca.length; i++) {
            trustStore.setCertificateEntry("ca-" + i, ca[i]);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(trustStore);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[] {new ForcingKeyManager(chain, key)}, tmf.getTrustManagers(), null);
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(true);
        return engine;
    }

    /** A key manager that always presents a fixed certificate chain, ignoring the server's CA hints. */
    private static final class ForcingKeyManager extends X509ExtendedKeyManager {
        private static final String ALIAS = "client";
        private final X509Certificate[] chain;
        private final PrivateKey key;

        ForcingKeyManager(X509Certificate[] chain, PrivateKey key) {
            this.chain = chain;
            this.key = key;
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return new String[] {ALIAS};
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return ALIAS;
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            return ALIAS;
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return null;
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return null;
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return chain;
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return key;
        }
    }

    private TlsPolicy copyServerCertsToTemp(String certSrc, String keySrc) throws Exception {
        Files.copy(Paths.get(RSA_CA), tempDir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(certSrc), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(keySrc), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        return TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString());
    }

    private void overwriteServerCerts(String certSrc, String keySrc) throws Exception {
        Files.copy(Paths.get(certSrc), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(keySrc), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        long later = System.currentTimeMillis() + 5000;
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(later));
        Files.setLastModifiedTime(tempDir.resolve("key.pem"), FileTime.fromMillis(later));
    }
}
