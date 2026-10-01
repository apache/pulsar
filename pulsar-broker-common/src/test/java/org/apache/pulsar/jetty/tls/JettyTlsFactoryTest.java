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
package org.apache.pulsar.jetty.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.io.Resources;
import io.opentelemetry.api.OpenTelemetry;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
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
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.common.util.tls.JdkSslContexts;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.awaitility.Awaitility;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JettyTlsFactoryTest {

    /**
     * Reload executor for tests: same-thread, so a delivery's reload has completed by the time the call
     * that triggered it returns and the assertions below stay deterministic. That publish genuinely runs
     * OFF the delivery thread when a real executor is supplied has its own test
     * ({@link #rotationReloadRunsOnTheSuppliedExecutorNotTheDeliveryThread()}).
     */
    private static final Executor SAME_THREAD = Runnable::run;

    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");
    // Client certificates (clientAuth EKU) trusted by the shared CA above.
    private static final String TRUSTED_CLIENT_CERT = resource("certificate-authority/client-keys/admin.cert.pem");
    private static final String TRUSTED_CLIENT_KEY = resource("certificate-authority/client-keys/admin.key-pk8.pem");
    private static final String USER1_CLIENT_CERT = resource("certificate-authority/client-keys/user1.cert.pem");
    private static final String USER1_CLIENT_KEY = resource("certificate-authority/client-keys/user1.key-pk8.pem");
    // A self-signed client certificate NOT signed by the shared CA (from a separate my-ca root).
    private static final String UNTRUSTED_CLIENT_CERT = resource("ssl/my-ca/client-ca.pem");
    private static final String UNTRUSTED_CLIENT_KEY = resource("ssl/my-ca/client-key.pem");

    private ScheduledExecutorService scheduler;
    private Path tempDir;

    @BeforeMethod
    public void setUp() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        tempDir = Files.createTempDirectory("pip478-jetty-");
        Files.copy(Paths.get(CA), tempDir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(BROKER_CERT), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(BROKER_KEY), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        scheduler.shutdownNow();
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void reloadingServerFactoryServesRotatedCertificateToNewConnections() throws Exception {
        FileBasedTlsFactory factory = new FileBasedTlsFactory(
                Map.of(TlsPurpose.WEB, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString())),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);

        Server server = new Server();
        ServerConnector connector = new ServerConnector(server, reloadable.sslContextFactory());
        connector.setPort(0);
        server.setConnectors(new ServerConnector[] {connector});
        server.start();
        try {
            SSLContext clientContext = JdkSslContexts.createSslContext(false,
                    PemReader.loadCertificatesFromPemFile(CA), null, null);
            int port = connector.getLocalPort();

            BigInteger initialSerial = serverCertSerial(clientContext, port);
            assertThat(initialSerial).isEqualTo(certSerial(BROKER_CERT));

            // Rotate the server material to a different (proxy) certificate signed by the same CA.
            Files.copy(Paths.get(PROXY_CERT), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(Paths.get(PROXY_KEY), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
            long later = System.currentTimeMillis() + 5000;
            Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(later));
            Files.setLastModifiedTime(tempDir.resolve("key.pem"), FileTime.fromMillis(later));

            BigInteger proxySerial = certSerial(PROXY_CERT);
            Awaitility.await().atMost(Duration.ofSeconds(15))
                    .until(() -> serverCertSerial(clientContext, port).equals(proxySerial));
            assertThat(serverCertSerial(clientContext, port))
                    .as("new connections use the rotated certificate")
                    .isEqualTo(proxySerial).isNotEqualTo(initialSerial);
        } finally {
            reloadable.subscription().dispose();
            server.stop();
            factory.close();
        }
    }

    /**
     * PIP-478: a self-reloading {@link SslContextFactory.Client} presents rotated client-certificate
     * material to new connections while the factory stays alive (mirrors the server rotation test).
     */
    @Test
    public void reloadingClientFactoryPresentsRotatedClientCertOnNewConnections() throws Exception {
        // Distinct filenames from the server rotation test's cert.pem/key.pem; these hold a client-auth
        // certificate (the broker/proxy server certs carry a serverAuth-only EKU and cannot be presented as
        // a client identity).
        Path clientCert = tempDir.resolve("clientcert.pem");
        Path clientKey = tempDir.resolve("clientkey.pem");
        Files.copy(Paths.get(TRUSTED_CLIENT_CERT), clientCert, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(TRUSTED_CLIENT_KEY), clientKey, StandardCopyOption.REPLACE_EXISTING);

        FileBasedTlsFactory factory = new FileBasedTlsFactory(
                Map.of(TlsPurpose.BROKER_CLIENT, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        clientCert.toString(), clientKey.toString())),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableClientTls reloadable = JettyTlsFactory.createReloadingClientFactory(
                factory, TlsPurpose.BROKER_CLIENT, SAME_THREAD, null, true);
        SslContextFactory.Client clientFactory = reloadable.sslContextFactory();
        clientFactory.start();
        try {
            assertThat(presentedClientCertSerial(clientFactory)).isEqualTo(certSerial(TRUSTED_CLIENT_CERT));

            // Rotate the client identity to a different client certificate signed by the same CA.
            Files.copy(Paths.get(USER1_CLIENT_CERT), clientCert, StandardCopyOption.REPLACE_EXISTING);
            Files.copy(Paths.get(USER1_CLIENT_KEY), clientKey, StandardCopyOption.REPLACE_EXISTING);
            long later = System.currentTimeMillis() + 5000;
            Files.setLastModifiedTime(clientCert, FileTime.fromMillis(later));
            Files.setLastModifiedTime(clientKey, FileTime.fromMillis(later));

            BigInteger rotatedSerial = certSerial(USER1_CLIENT_CERT);
            Awaitility.await().atMost(Duration.ofSeconds(15))
                    .until(() -> presentedClientCertSerial(clientFactory).equals(rotatedSerial));
            assertThat(presentedClientCertSerial(clientFactory))
                    .as("new connections present the rotated client certificate")
                    .isEqualTo(rotatedSerial).isNotEqualTo(certSerial(TRUSTED_CLIENT_CERT));
        } finally {
            clientFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    // Handshake the reloading client factory's current context against a client-auth-requiring server and
    // return the serial of the client certificate the server observed.
    private BigInteger presentedClientCertSerial(SslContextFactory.Client clientFactory) throws Exception {
        SSLContext serverContext = JdkSslContexts.createSslContext(false, CA, BROKER_CERT, BROKER_KEY, null);
        try (SSLServerSocket serverSocket =
                     (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket(0)) {
            serverSocket.setNeedClientAuth(true);
            // TLSv1.2 so client auth completes symmetrically within the handshake (avoids the TLS 1.3
            // post-handshake close race where the server closes before the client flushes its Finished).
            serverSocket.setEnabledProtocols(new String[] {"TLSv1.2"});
            serverSocket.setSoTimeout(15000);
            int port = serverSocket.getLocalPort();
            CompletableFuture<BigInteger> serverSaw = new CompletableFuture<>();
            Thread serverThread = new Thread(() -> {
                try (SSLSocket accepted = (SSLSocket) serverSocket.accept()) {
                    accepted.setSoTimeout(15000);
                    accepted.startHandshake();
                    X509Certificate peer = (X509Certificate) accepted.getSession().getPeerCertificates()[0];
                    serverSaw.complete(peer.getSerialNumber());
                } catch (Throwable t) {
                    serverSaw.completeExceptionally(t);
                }
            });
            serverThread.setDaemon(true);
            serverThread.start();

            SSLContext clientContext = clientFactory.getSslContext();
            try (SSLSocket clientSocket =
                         (SSLSocket) clientContext.getSocketFactory().createSocket("localhost", port)) {
                clientSocket.setEnabledProtocols(new String[] {"TLSv1.2"});
                clientSocket.setSoTimeout(15000);
                clientSocket.startHandshake();
            }
            return serverSaw.get(15, TimeUnit.SECONDS);
        }
    }

    /**
     * PIP-478: under optional client auth ({@code requireTrustedClientCert=false}), an <em>untrusted</em>
     * client certificate is rejected at the web listener's handshake when {@code allowInsecureConnection=false}
     * and accepted when it is true; a trusted client certificate is accepted in both cases.
     */
    @Test
    public void optionalClientAuthScopesTrustAllToInsecureFlag() throws Exception {
        // Secure (insecure=false): untrusted client cert is rejected, trusted client cert is accepted. The
        // server aborts the handshake, surfaced to the client either as an SSLHandshakeException or, once the
        // server has already sent its close/alert, as a broken-pipe SocketException — both are handshake
        // failures.
        try (JettyServer secure = startWebServer(false)) {
            assertThatThrownBy(() -> handshakeWithClientCert(secure.port, UNTRUSTED_CLIENT_CERT, UNTRUSTED_CLIENT_KEY))
                    .as("an untrusted client cert must be rejected when insecure=false")
                    .isInstanceOf(IOException.class);
            handshakeWithClientCert(secure.port, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY);
        }
        // Insecure (insecure=true): both untrusted and trusted client certs are accepted (trust-all).
        try (JettyServer insecure = startWebServer(true)) {
            handshakeWithClientCert(insecure.port, UNTRUSTED_CLIENT_CERT, UNTRUSTED_CLIENT_KEY);
            handshakeWithClientCert(insecure.port, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY);
        }
    }

    /**
     * PIP-478: a factory that supplies its {@code SSLContext} together with an {@code SSLParameters} companion
     * drives the synthesized server {@link SslContextFactory.Server} — its enabled protocols/ciphers and its
     * client-auth mode are mapped from the companion, the latter authoritatively (merge rule 4).
     */
    @Test
    public void serverFactoryMapsCompanionProtocolsAndClientAuth() throws Exception {
        SSLParameters companion = new SSLParameters();
        companion.setProtocols(new String[] {"TLSv1.2"});
        companion.setCipherSuites(new String[] {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"});
        companion.setNeedClientAuth(true);

        FileBasedTlsFactory delegate = new FileBasedTlsFactory(
                Map.of(TlsPurpose.WEB, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString())),
                FileBasedTlsFactorySettings.builder().build());
        CompanionFactory factory = new CompanionFactory(delegate, companion);
        factory.initialize(initContext()).join();

        // Consumer config asks for no protocol/cipher restriction and only optional client auth; the companion
        // overrides all three.
        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        SslContextFactory.Server sslContextFactory = reloadable.sslContextFactory();
        try {
            assertThat(sslContextFactory.getIncludeProtocols()).containsExactly("TLSv1.2");
            assertThat(sslContextFactory.getIncludeCipherSuites())
                    .containsExactly("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
            assertThat(sslContextFactory.getNeedClientAuth()).isTrue();
        } finally {
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * T4: the companion's protocol restriction and {@code needClientAuth} are not just mapped onto the
     * synthesized Jetty {@link SslContextFactory.Server} getters — they decide a real handshake against a live
     * server. A trusted client certificate over the allowed protocol succeeds; a client presenting no
     * certificate is rejected (needClientAuth); a client offering only a disallowed protocol is rejected.
     */
    @Test
    public void serverCompanionNeedClientAuthAndProtocolEnforcedAtHandshake() throws Exception {
        SSLParameters companion = new SSLParameters();
        companion.setProtocols(new String[] {"TLSv1.2"});
        companion.setNeedClientAuth(true);

        FileBasedTlsFactory delegate = new FileBasedTlsFactory(
                Map.of(TlsPurpose.WEB, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString())),
                FileBasedTlsFactorySettings.builder().build());
        CompanionFactory factory = new CompanionFactory(delegate, companion);
        factory.initialize(initContext()).join();

        // Consumer config asks for no protocol restriction and only optional client auth; the companion wins.
        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server, reloadable.sslContextFactory());
        connector.setPort(0);
        server.setConnectors(new ServerConnector[] {connector});
        server.start();
        int port = connector.getLocalPort();
        try {
            // A trusted client certificate over the allowed protocol completes the handshake.
            handshakeWithClientCert(port, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY);
            // The companion's needClientAuth rejects a client that presents no certificate.
            assertThatThrownBy(() -> handshakeWithoutClientCert(port))
                    .as("needClientAuth from the companion rejects a client with no certificate")
                    .isInstanceOf(IOException.class);
            // The companion's TLSv1.2-only restriction rejects a client offering only TLSv1.3.
            assertThatThrownBy(() -> handshakeWithClientCert(port, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY,
                    "TLSv1.3"))
                    .as("a TLSv1.3-only client cannot handshake with the companion-pinned TLSv1.2 server")
                    .isInstanceOf(IOException.class);
        } finally {
            reloadable.subscription().dispose();
            server.stop();
            factory.close();
        }
    }

    /**
     * PIP-478 (pip-478.md:736): the {@code SSLParameters} companion is re-requested on <em>every</em>
     * {@code SSLContext} delivery, not just at build time — so engine policy (here the server client-auth mode)
     * rotates with material. A companion that flips {@code needClientAuth} on rotation must be reflected in the
     * synthesized Jetty {@link SslContextFactory.Server} after the reload, and dropping the companion must
     * revert client-auth to the consumer default rather than leaving the prior value stuck.
     *
     * <p>The companion is resolved on the <em>same single-thread scheduler</em> that runs the poll deliveries
     * (see {@link CompanionFactory}'s executor): this reproduces the self-deadlock hazard a synchronous
     * {@code join} on the delivery thread would hit. The reload path re-requests the companion asynchronously,
     * so the rotation still applies (the assertions are awaited rather than read synchronously).
     */
    @Test
    public void serverCompanionClientAuthRotatesWithMaterialOnReload() throws Exception {
        // Build-time companion: no client auth. This overrides the consumer default (optional client auth).
        SSLParameters noClientAuth = new SSLParameters();
        noClientAuth.setNeedClientAuth(false);

        FileBasedTlsFactory delegate = new FileBasedTlsFactory(
                Map.of(TlsPurpose.WEB, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString())),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());
        // Dispatch companion creation onto the shared scheduler (the delivery thread), so a join inside the
        // reload callback would deadlock; the async re-request must keep the rotation working.
        CompanionFactory factory = new CompanionFactory(delegate, noClientAuth, scheduler);
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        SslContextFactory.Server sslContextFactory = reloadable.sslContextFactory();
        // Start standalone (no full Jetty server needed) so the reload path (isStarted()) runs on delivery.
        sslContextFactory.start();
        try {
            assertThat(sslContextFactory.getNeedClientAuth())
                    .as("the build-time companion (needClientAuth=false) is authoritative")
                    .isFalse();

            // Flip the companion to require client auth, then rotate material to trigger an SSLContext delivery.
            SSLParameters needClientAuth = new SSLParameters();
            needClientAuth.setNeedClientAuth(true);
            factory.setCompanion(needClientAuth);
            rotateWebMaterial();

            // The reload re-requests the companion and re-applies it under Jetty's lock, so the new
            // needClientAuth takes effect on the synthesized factory.
            Awaitility.await().atMost(Duration.ofSeconds(15)).until(sslContextFactory::getNeedClientAuth);
            assertThat(sslContextFactory.getNeedClientAuth())
                    .as("the companion is re-requested on delivery, so client-auth rotates with material")
                    .isTrue();

            // Drop the companion entirely and rotate again: client-auth must revert to the consumer default
            // (optional client auth => wantClientAuth, needClientAuth cleared) instead of staying stuck at need.
            factory.setCompanion(null);
            rotateWebMaterial();
            Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> !sslContextFactory.getNeedClientAuth());
            assertThat(sslContextFactory.getNeedClientAuth())
                    .as("dropping the companion reverts needClientAuth to the consumer default")
                    .isFalse();
            assertThat(sslContextFactory.getWantClientAuth())
                    .as("the consumer default (optional client auth) is restored")
                    .isTrue();
        } finally {
            sslContextFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * PIP-478 generation guard: two rotation companions that complete OUT OF ORDER must leave the NEWER
     * rotation's context and baseline applied, never regressing to the older one. Each rotation captures a
     * strictly increasing delivery generation; when the older rotation's companion completes last, its
     * reload is dropped because a newer generation has already been published (mirroring
     * {@code TlsContextAcquisition.SynthesizingSubscription}'s generation-guarded {@code publish}). Without the
     * guard the stale companion would pin the synthesized {@link SslContextFactory.Server} to the older
     * context/baseline until the next rotation.
     */
    @Test
    public void serverRotationOutOfOrderCompanionKeepsNewerContextAndBaseline() throws Exception {
        SSLContext initial = JdkSslContexts.createSslContext(false, CA, BROKER_CERT, BROKER_KEY, null);
        SSLContext contextA = JdkSslContexts.createSslContext(false, CA, BROKER_CERT, BROKER_KEY, null);
        SSLContext contextB = JdkSslContexts.createSslContext(false, CA, PROXY_CERT, PROXY_KEY, null);
        ScriptableFactory factory = new ScriptableFactory(initial, null);

        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        SslContextFactory.Server sslContextFactory = reloadable.sslContextFactory();
        // Start standalone so the reload path (isStarted()) runs on each rotation delivery.
        sslContextFactory.start();
        try {
            factory.deferCompanions();
            // Two rotations: A (older, generation N) then B (newer, generation N+1).
            factory.deliver(contextA);
            factory.deliver(contextB);

            SSLParameters baselineA = new SSLParameters();
            baselineA.setProtocols(new String[] {"TLSv1.2"});
            SSLParameters baselineB = new SSLParameters();
            baselineB.setProtocols(new String[] {"TLSv1.3"});

            // Complete the NEWER rotation's companion first, then the older one; the generation guard must
            // drop the stale (older) result so the newer context/baseline stays applied.
            factory.completeCompanion(1, baselineB);
            factory.completeCompanion(0, baselineA);

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(sslContextFactory.getIncludeProtocols())
                        .as("the newer rotation's baseline wins despite the older companion completing last")
                        .containsExactly("TLSv1.3");
                assertThat(sslContextFactory.getSslContext())
                        .as("the newer rotation's context stays pinned")
                        .isSameAs(contextB);
            });
        } finally {
            sslContextFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * PIP-478 last-good retention: a rotation whose companion re-request fails transiently must retain the
     * last successfully-resolved baseline (seeded from the build-time companion) rather than downgrading to
     * consumer defaults. Here the build companion pins TLSv1.2 and {@code needClientAuth}; the rotation's
     * companion then fails, and the reload must still apply the rotated context while keeping client-auth and
     * the protocol restriction — the guarantee {@code SynthesizingSubscription} provides via its retained
     * {@code lastBaseline}. Without it, the failed companion would silently drop client-auth/protocols.
     */
    @Test
    public void serverRotationCompanionFailureRetainsLastGoodBaseline() throws Exception {
        SSLContext initial = JdkSslContexts.createSslContext(false, CA, BROKER_CERT, BROKER_KEY, null);
        SSLContext rotated = JdkSslContexts.createSslContext(false, CA, PROXY_CERT, PROXY_KEY, null);
        SSLParameters lastGood = new SSLParameters();
        lastGood.setProtocols(new String[] {"TLSv1.2"});
        lastGood.setNeedClientAuth(true);
        ScriptableFactory factory = new ScriptableFactory(initial, lastGood);

        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        SslContextFactory.Server sslContextFactory = reloadable.sslContextFactory();
        sslContextFactory.start();
        try {
            // The build-time companion is authoritative and seeds the retained last-good baseline.
            assertThat(sslContextFactory.getNeedClientAuth()).isTrue();
            assertThat(sslContextFactory.getIncludeProtocols()).containsExactly("TLSv1.2");

            factory.deferCompanions();
            // Rotate material; the companion re-request for this rotation fails transiently.
            factory.deliver(rotated);
            factory.failCompanion(0, new IllegalStateException("companion temporarily unavailable"));

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(sslContextFactory.getSslContext())
                        .as("the rotated context is applied even though the companion failed")
                        .isSameAs(rotated);
                assertThat(sslContextFactory.getNeedClientAuth())
                        .as("a transient companion failure retains the last-good client-auth")
                        .isTrue();
                assertThat(sslContextFactory.getIncludeProtocols())
                        .as("a transient companion failure retains the last-good protocol restriction")
                        .containsExactly("TLSv1.2");
            });
        } finally {
            sslContextFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * PIP-478 companion revert, cipher suites: a companion that pins cipher suites on one delivery and drops them
     * on the next must revert to the consumer default (here: no restriction) rather than leaving the stale
     * allowlist pinned on the long-lived {@link SslContextFactory.Server}, which persists setter state across
     * {@code reload()}. This is the same guarantee already covered for client-auth in
     * {@link #serverCompanionClientAuthRotatesWithMaterialOnReload}, extended to the cipher/protocol members.
     */
    @Test
    public void serverDroppedCompanionRevertsCipherSuitesToConsumerDefault() throws Exception {
        SSLContext initial = JdkSslContexts.createSslContext(false, CA, BROKER_CERT, BROKER_KEY, null);
        SSLContext rotated = JdkSslContexts.createSslContext(false, CA, PROXY_CERT, PROXY_KEY, null);
        SSLParameters buildCompanion = new SSLParameters();
        buildCompanion.setCipherSuites(new String[] {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"});
        buildCompanion.setProtocols(new String[] {"TLSv1.2"});
        ScriptableFactory factory = new ScriptableFactory(initial, buildCompanion);

        // The consumer configures neither ciphers nor protocols, so the defaults are "no cipher restriction" and
        // the {TLSv1.3, TLSv1.2} protocol floor.
        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        SslContextFactory.Server sslContextFactory = reloadable.sslContextFactory();
        sslContextFactory.start();
        try {
            assertThat(sslContextFactory.getIncludeCipherSuites())
                    .containsExactly("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
            assertThat(sslContextFactory.getIncludeProtocols()).containsExactly("TLSv1.2");

            // Rotate with a companion that supplies nothing at all (empty()), modelling a delivery that
            // legitimately drops the previously pinned members.
            factory.deferCompanions();
            factory.deliver(rotated);
            factory.completeCompanion(0, null);

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(sslContextFactory.getIncludeCipherSuites())
                        .as("a dropped companion reverts cipher suites to the consumer default (unrestricted)")
                        .isEmpty();
                assertThat(sslContextFactory.getIncludeProtocols())
                        .as("a dropped companion reverts protocols to the consumer default floor")
                        .containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
            });
        } finally {
            sslContextFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /** The client path is symmetric: a dropped companion must not leave a stale cipher allowlist pinned. */
    @Test
    public void clientDroppedCompanionRevertsCipherSuitesToConsumerDefault() throws Exception {
        SSLContext initial = JdkSslContexts.createSslContext(false, CA, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY, null);
        SSLContext rotated = JdkSslContexts.createSslContext(false, CA, USER1_CLIENT_CERT, USER1_CLIENT_KEY, null);
        SSLParameters buildCompanion = new SSLParameters();
        buildCompanion.setCipherSuites(new String[] {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"});
        buildCompanion.setProtocols(new String[] {"TLSv1.2"});
        ScriptableFactory factory = new ScriptableFactory(initial, buildCompanion);

        JettyTlsFactory.ReloadableClientTls reloadable = JettyTlsFactory.createReloadingClientFactory(
                factory, TlsPurpose.BROKER_CLIENT, SAME_THREAD, null, true);
        SslContextFactory.Client clientFactory = reloadable.sslContextFactory();
        clientFactory.start();
        try {
            assertThat(clientFactory.getIncludeCipherSuites())
                    .containsExactly("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
            assertThat(clientFactory.getIncludeProtocols()).containsExactly("TLSv1.2");

            factory.deferCompanions();
            factory.deliver(rotated);
            factory.completeCompanion(0, null);

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(clientFactory.getIncludeCipherSuites())
                        .as("a dropped companion reverts cipher suites to the consumer default (unrestricted)")
                        .isEmpty();
                assertThat(clientFactory.getIncludeProtocols())
                        .as("a dropped companion reverts protocols to the consumer default floor")
                        .containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
            });
        } finally {
            clientFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * The reload must not run on the thread that delivered the rotation.
     *
     * <p>For the default factory the companion request completes synchronously — {@code SSLParameters} is
     * an unsupported class, so {@code createInstance} returns an already-completed future — which means a
     * plain {@code whenComplete} would run the whole Jetty reload inline, on a delivery thread that is
     * inside the factory's {@code synchronized} fan-out. That put the source monitor at the head of a
     * source -> coordinator -> Jetty lock chain and broke the delivery contract's "cheap non-blocking
     * store". Dispatching to the supplied executor is what keeps that from happening.
     */
    @Test
    public void rotationReloadRunsOnTheSuppliedExecutorNotTheDeliveryThread() throws Exception {
        SSLContext initial = JdkSslContexts.createSslContext(false, CA, BROKER_CERT, BROKER_KEY, null);
        SSLContext rotated = JdkSslContexts.createSslContext(false, CA, PROXY_CERT, PROXY_KEY, null);
        // The companion resolves synchronously here (no deferCompanions()), which is exactly the default
        // factory's behaviour: SSLParameters is an unsupported class, so createInstance returns an
        // already-completed future. That is the case where an inline whenComplete would run the whole Jetty
        // reload on the delivery thread.
        ScriptableFactory factory = new ScriptableFactory(initial, null);
        HoldingExecutor reloadExecutor = new HoldingExecutor();
        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, reloadExecutor, null, false, false, null, null);
        SslContextFactory.Server target = reloadable.sslContextFactory();
        target.start();
        try {
            // The first delivery bypasses the executor (it fires synchronously during subscribe and only
            // sets the context), so holding from here is race-free.
            reloadExecutor.hold();
            factory.deliver(rotated);

            // The discriminator: the delivery call has returned, and the rotation has NOT been applied,
            // because publish is parked on the executor. Under an inline whenComplete the reload would
            // already have run inside deliver(...) and this would be `rotated`.
            assertThat(target.getSslContext())
                    .as("the reload must be dispatched, not run inline on the delivery thread")
                    .isSameAs(initial);
            assertThat(reloadExecutor.heldCount()).as("exactly one reload was dispatched").isEqualTo(1);

            reloadExecutor.runHeld();
            assertThat(target.getSslContext()).as("released reload applies the rotation").isSameAs(rotated);
            reloadable.subscription().dispose();
        } finally {
            target.stop();
            factory.close();
        }
    }

    /** An {@link Executor} that can park submitted tasks so a test can observe that dispatch happened. */
    private static final class HoldingExecutor implements Executor {
        private volatile boolean holding;
        private final List<Runnable> held = new CopyOnWriteArrayList<>();

        void hold() {
            holding = true;
        }

        int heldCount() {
            return held.size();
        }

        void runHeld() {
            holding = false;
            List<Runnable> tasks = List.copyOf(held);
            held.clear();
            tasks.forEach(Runnable::run);
        }

        @Override
        public void execute(Runnable command) {
            if (holding) {
                held.add(command);
            } else {
                command.run();
            }
        }
    }

    /**
     * PIP-478: a rotation delivered BEFORE the connector starts must still refresh the companion. Only the very
     * first (pre-fetched, synchronous) delivery reuses the build-time baseline; a later delivery that lands while
     * the factory is still unstarted re-requests the companion and applies it directly (Jetty's {@code reload()}
     * is for a live factory), so the started factory runs the rotation's engine policy — protocols, cipher suites
     * and client-auth — rather than the stale build-time one.
     */
    @Test
    public void preStartRotationRefreshesCompanionBeforeStart() throws Exception {
        SSLContext initial = JdkSslContexts.createSslContext(false, CA, BROKER_CERT, BROKER_KEY, null);
        SSLContext rotated = JdkSslContexts.createSslContext(false, CA, PROXY_CERT, PROXY_KEY, null);
        // No build-time companion: the consumer defaults (the {TLSv1.3, TLSv1.2} floor, unrestricted ciphers and
        // optional client auth) are what a stale baseline would leave in force.
        ScriptableFactory factory = new ScriptableFactory(initial, null);

        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        SslContextFactory.Server sslContextFactory = reloadable.sslContextFactory();
        try {
            assertThat(sslContextFactory.getNeedClientAuth())
                    .as("the consumer default before any rotation is optional client auth")
                    .isFalse();

            // Rotate while still unstarted, with a companion that tightens all three members.
            SSLParameters tightened = new SSLParameters();
            tightened.setProtocols(new String[] {"TLSv1.2"});
            tightened.setCipherSuites(new String[] {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"});
            tightened.setNeedClientAuth(true);
            factory.deferCompanions();
            factory.deliver(rotated);
            factory.completeCompanion(0, tightened);

            // Only now does the connector start it.
            sslContextFactory.start();

            assertThat(sslContextFactory.getSslContext())
                    .as("the pre-start rotation's context is the one the factory starts with")
                    .isSameAs(rotated);
            assertThat(sslContextFactory.getIncludeProtocols())
                    .as("the pre-start rotation's companion protocols are in force after start")
                    .containsExactly("TLSv1.2");
            assertThat(sslContextFactory.getIncludeCipherSuites())
                    .as("the pre-start rotation's companion cipher suites are in force after start")
                    .containsExactly("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
            assertThat(sslContextFactory.getNeedClientAuth())
                    .as("the pre-start rotation's companion client-auth is in force after start")
                    .isTrue();
        } finally {
            sslContextFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * PIP-478 generation guard, client path: the same shared reload coordinator drives the synthesized
     * {@link SslContextFactory.Client}, so an out-of-order companion completion must likewise leave the newer
     * rotation's context/protocols applied (client-auth is a server concept and is not mapped here).
     */
    @Test
    public void clientRotationOutOfOrderCompanionKeepsNewerContextAndProtocols() throws Exception {
        SSLContext initial = JdkSslContexts.createSslContext(false, CA, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY, null);
        SSLContext contextA = JdkSslContexts.createSslContext(false, CA, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY, null);
        SSLContext contextB = JdkSslContexts.createSslContext(false, CA, USER1_CLIENT_CERT, USER1_CLIENT_KEY, null);
        ScriptableFactory factory = new ScriptableFactory(initial, null);

        JettyTlsFactory.ReloadableClientTls reloadable = JettyTlsFactory.createReloadingClientFactory(
                factory, TlsPurpose.BROKER_CLIENT, SAME_THREAD, null, true);
        SslContextFactory.Client clientFactory = reloadable.sslContextFactory();
        clientFactory.start();
        try {
            factory.deferCompanions();
            factory.deliver(contextA);
            factory.deliver(contextB);

            SSLParameters baselineA = new SSLParameters();
            baselineA.setProtocols(new String[] {"TLSv1.2"});
            SSLParameters baselineB = new SSLParameters();
            baselineB.setProtocols(new String[] {"TLSv1.3"});

            factory.completeCompanion(1, baselineB);
            factory.completeCompanion(0, baselineA);

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(clientFactory.getIncludeProtocols())
                        .as("the newer rotation's baseline wins despite the older companion completing last")
                        .containsExactly("TLSv1.3");
                assertThat(clientFactory.getSslContext())
                        .as("the newer rotation's context stays pinned")
                        .isSameAs(contextB);
            });
        } finally {
            clientFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    // Rotate the WEB server material (cert.pem/key.pem) to the other certificate signed by the same CA
    // (broker<->proxy), with a future mtime so the file-based factory's poll observes the change and re-delivers
    // the SSLContext. Toggling on the current content guarantees each call is a real change.
    private void rotateWebMaterial() throws Exception {
        boolean toProxy = certSerial(tempDir.resolve("cert.pem").toString()).equals(certSerial(BROKER_CERT));
        Files.copy(Paths.get(toProxy ? PROXY_CERT : BROKER_CERT), tempDir.resolve("cert.pem"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(toProxy ? PROXY_KEY : BROKER_KEY), tempDir.resolve("key.pem"),
                StandardCopyOption.REPLACE_EXISTING);
        long later = System.currentTimeMillis() + 5000;
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(later));
        Files.setLastModifiedTime(tempDir.resolve("key.pem"), FileTime.fromMillis(later));
    }

    /**
     * With no configured protocols and no factory companion, the synthesized Jetty server/client factories
     * pin the {@code {TLSv1.3, TLSv1.2}} floor (matching the native Netty path) rather than deferring to the
     * provider default.
     */
    @Test
    public void defaultProtocolsPinnedWhenUnconfigured() throws Exception {
        FileBasedTlsFactory factory = new FileBasedTlsFactory(
                Map.of(TlsPurpose.WEB, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString())),
                FileBasedTlsFactorySettings.builder().build());
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableServerTls server = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        JettyTlsFactory.ReloadableClientTls client = JettyTlsFactory.createReloadingClientFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, true);
        try {
            assertThat(server.sslContextFactory().getIncludeProtocols())
                    .containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
            assertThat(client.sslContextFactory().getIncludeProtocols())
                    .containsExactlyInAnyOrder("TLSv1.3", "TLSv1.2");
        } finally {
            server.subscription().dispose();
            client.subscription().dispose();
            factory.close();
        }
    }

    /**
     * PIP-478: the synthesized client {@link SslContextFactory.Client} maps the companion's enabled
     * protocols/ciphers (client-auth is a server concept and is not mapped on the client factory).
     */
    @Test
    public void clientFactoryMapsCompanionProtocols() throws Exception {
        Path clientCert = tempDir.resolve("clientcert.pem");
        Path clientKey = tempDir.resolve("clientkey.pem");
        Files.copy(Paths.get(TRUSTED_CLIENT_CERT), clientCert, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(TRUSTED_CLIENT_KEY), clientKey, StandardCopyOption.REPLACE_EXISTING);

        SSLParameters companion = new SSLParameters();
        companion.setProtocols(new String[] {"TLSv1.2"});

        FileBasedTlsFactory delegate = new FileBasedTlsFactory(
                Map.of(TlsPurpose.BROKER_CLIENT, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        clientCert.toString(), clientKey.toString())),
                FileBasedTlsFactorySettings.builder().build());
        CompanionFactory factory = new CompanionFactory(delegate, companion);
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableClientTls reloadable = JettyTlsFactory.createReloadingClientFactory(
                factory, TlsPurpose.BROKER_CLIENT, SAME_THREAD, null, true);
        try {
            assertThat(reloadable.sslContextFactory().getIncludeProtocols()).containsExactly("TLSv1.2");
        } finally {
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * PIP-478: {@link SslContextFactory.Client} is a well-known SPI class — a factory that supplies it
     * natively has its instance used verbatim by {@code createReloadingClientFactory} (unstarted, the
     * factory owns reload), and the framework does <em>not</em> fall back to synthesizing one from an
     * {@code SSLContext} subscription. Disposing the returned handle propagates to the factory's handle.
     */
    @Test
    public void clientFactoryHonorsFactorySuppliedNativeJettyClient() {
        SslContextFactory.Client nativeClient = new SslContextFactory.Client();
        NativeJettyFactory factory = new NativeJettyFactory(SslContextFactory.Client.class, nativeClient);
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableClientTls reloadable = JettyTlsFactory.createReloadingClientFactory(
                factory, TlsPurpose.BROKER_CLIENT, SAME_THREAD, null, true);
        try {
            assertThat(reloadable.sslContextFactory())
                    .as("the factory-supplied native Jetty client factory is used verbatim")
                    .isSameAs(nativeClient);
            assertThat(factory.sslContextSubscribed)
                    .as("no SSLContext synthesis fallback when the factory supplies the Jetty client natively")
                    .isFalse();
        } finally {
            reloadable.subscription().dispose();
            assertThat(factory.handleDisposed).as("disposing the reloadable disposes the factory handle").isTrue();
            factory.close();
        }
    }

    /**
     * PIP-478: the Server variant is symmetric — a factory that supplies {@link SslContextFactory.Server}
     * natively has its instance used verbatim by {@code createReloadingServerFactory}, with no synthesis
     * fallback and no consumer config overlaid.
     */
    @Test
    public void serverFactoryHonorsFactorySuppliedNativeJettyServer() {
        SslContextFactory.Server nativeServer = new SslContextFactory.Server();
        NativeJettyFactory factory = new NativeJettyFactory(SslContextFactory.Server.class, nativeServer);
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, SAME_THREAD, null, false, false, null, null);
        try {
            assertThat(reloadable.sslContextFactory())
                    .as("the factory-supplied native Jetty server factory is used verbatim")
                    .isSameAs(nativeServer);
            assertThat(factory.sslContextSubscribed)
                    .as("no SSLContext synthesis fallback when the factory supplies the Jetty server natively")
                    .isFalse();
        } finally {
            reloadable.subscription().dispose();
            assertThat(factory.handleDisposed).as("disposing the reloadable disposes the factory handle").isTrue();
            factory.close();
        }
    }

    /**
     * A factory that supplies a single Jetty factory ({@link SslContextFactory.Server} or {@code .Client})
     * natively from a fixed instance and returns {@code empty()} for everything else, recording whether the
     * framework fell back to an {@code SSLContext} subscription (the synthesis path) and whether its handle
     * was disposed.
     */
    private static final class NativeJettyFactory implements PulsarTlsFactory {
        private final Class<?> jettyClass;
        private final SslContextFactory instance;
        volatile boolean sslContextSubscribed;
        volatile boolean handleDisposed;

        NativeJettyFactory(Class<?> jettyClass, SslContextFactory instance) {
            this.jettyClass = jettyClass;
            this.instance = instance;
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass) {
            if (instanceClass == jettyClass) {
                return CompletableFuture.completedFuture(Optional.of((TlsHandle<T>) nativeHandle()));
            }
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            // The synthesis fallback subscribes to the JDK SSLContext; record it so a test can assert it did
            // NOT run when the factory supplied the Jetty factory natively.
            if (instanceClass == SSLContext.class) {
                sslContextSubscribed = true;
            }
            return CompletableFuture.completedFuture(Optional.empty());
        }

        private TlsHandle<SslContextFactory> nativeHandle() {
            return new TlsHandle<>() {
                @Override
                public SslContextFactory get() {
                    return instance;
                }

                @Override
                public void dispose() {
                    handleDisposed = true;
                }
            };
        }

        @Override
        public void close() {
        }
    }

    /**
     * A factory that delegates every request to a {@link FileBasedTlsFactory} except the {@code SSLParameters}
     * companion, which it supplies from a fixed instance (the file-based factory returns {@code empty()} for it).
     */
    private static final class CompanionFactory implements PulsarTlsFactory {
        private final FileBasedTlsFactory delegate;
        // Volatile so a test can flip the companion between deliveries and assert the reload re-requests it.
        private volatile SSLParameters companion;
        // When non-null, companion creation is dispatched onto this executor instead of completing inline. The
        // rotation test passes the same single-thread scheduler that runs the poll deliveries, reproducing the
        // self-deadlock hazard: a framework that joined on the companion from the delivery thread would wait on
        // that thread itself. null leaves companion creation synchronous (the other companion tests).
        private final Executor companionExecutor;

        CompanionFactory(FileBasedTlsFactory delegate, SSLParameters companion) {
            this(delegate, companion, null);
        }

        CompanionFactory(FileBasedTlsFactory delegate, SSLParameters companion, Executor companionExecutor) {
            this.delegate = delegate;
            this.companion = companion;
            this.companionExecutor = companionExecutor;
        }

        void setCompanion(SSLParameters companion) {
            this.companion = companion;
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return delegate.initialize(context);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass) {
            if (instanceClass == SSLParameters.class) {
                if (companionExecutor != null) {
                    // Resolve on the (shared, single-thread) executor so the future completes off the caller's
                    // thread: a framework that joined on it from the delivery thread would deadlock.
                    return CompletableFuture.supplyAsync(() -> resolveCompanion(purpose, instanceClass),
                            companionExecutor);
                }
                return CompletableFuture.completedFuture(resolveCompanion(purpose, instanceClass));
            }
            return delegate.createInstance(purpose, instanceClass);
        }

        @SuppressWarnings("unchecked")
        private <T> Optional<TlsHandle<T>> resolveCompanion(TlsPurpose purpose, Class<T> instanceClass) {
            SSLParameters current = companion;
            // A null companion models a factory that supplies none: empty() so the consumer default applies
            // (used by the reload rotation test to drop the companion mid-flight).
            if (current == null) {
                return delegate.createInstance(purpose, instanceClass).join();
            }
            return Optional.of((TlsHandle<T>) companionHandle(current));
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return delegate.createInstance(purpose, instanceClass, onLoadOrReload);
        }

        private TlsHandle<SSLParameters> companionHandle(SSLParameters value) {
            return new TlsHandle<>() {
                @Override
                public SSLParameters get() {
                    return value;
                }

                @Override
                public void dispose() {
                }
            };
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * A scriptable factory for the reload-ordering tests. It supplies the {@code SSLContext} subscription and
     * the {@code SSLParameters} companion under full test control and no native Jetty factory (forcing the
     * synthesized, self-reloading path). The build-time companion is resolved inline (seeding the coordinator's
     * last-good baseline); after {@link #deferCompanions()} each rotation's companion re-request is parked as an
     * incomplete future the test {@link #completeCompanion completes} — in any order — or {@link #failCompanion
     * fails}, so out-of-order completion and transient failures are reproduced deterministically.
     */
    private static final class ScriptableFactory implements PulsarTlsFactory {
        private final SSLContext initialContext;
        private final SSLParameters buildCompanion;
        private volatile Consumer<SSLContext> deliveryCallback;
        private volatile boolean deferCompanions;
        private final List<CompletableFuture<Optional<TlsHandle<SSLParameters>>>> pendingCompanions =
                new CopyOnWriteArrayList<>();

        ScriptableFactory(SSLContext initialContext, SSLParameters buildCompanion) {
            this.initialContext = initialContext;
            this.buildCompanion = buildCompanion;
        }

        // Start parking each rotation's companion re-request (build-time resolution has already happened inline).
        void deferCompanions() {
            deferCompanions = true;
        }

        // Simulate a poll delivery of rotated material on the (serial) delivery thread.
        void deliver(SSLContext context) {
            deliveryCallback.accept(context);
        }

        // Resolve the index-th parked rotation companion (in call order); null models the factory supplying none.
        void completeCompanion(int index, SSLParameters baseline) {
            requireCompanionRequested(index);
            pendingCompanions.get(index).complete(baseline == null ? Optional.empty()
                    : Optional.of(companionHandle(baseline)));
        }

        // Fail the index-th parked rotation companion, reproducing a transient companion-fetch error.
        void failCompanion(int index, Throwable error) {
            requireCompanionRequested(index);
            pendingCompanions.get(index).completeExceptionally(error);
        }

        // A rotation that never re-requests its companion is itself the defect these tests guard against, so
        // report it as a named assertion failure instead of an IndexOutOfBounds from the harness.
        private void requireCompanionRequested(int index) {
            assertThat(pendingCompanions)
                    .as("the rotation must have re-requested its SSLParameters companion (index %s)", index)
                    .hasSizeGreaterThan(index);
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass) {
            if (instanceClass == SSLParameters.class) {
                if (deferCompanions) {
                    CompletableFuture<Optional<TlsHandle<SSLParameters>>> pending = new CompletableFuture<>();
                    pendingCompanions.add(pending);
                    return (CompletableFuture<Optional<TlsHandle<T>>>) (CompletableFuture<?>) pending;
                }
                Optional<TlsHandle<SSLParameters>> companion = buildCompanion == null ? Optional.empty()
                        : Optional.of(companionHandle(buildCompanion));
                return CompletableFuture.completedFuture((Optional<TlsHandle<T>>) (Optional<?>) companion);
            }
            // No native Jetty factory: force the synthesized path.
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            if (instanceClass == SSLContext.class) {
                deliveryCallback = (Consumer<SSLContext>) onLoadOrReload;
                // The first delivery fires synchronously during subscribe (before start), mirroring the SPI.
                deliveryCallback.accept(initialContext);
                return CompletableFuture.completedFuture(Optional.of((TlsHandle<T>) sslContextHandle()));
            }
            return CompletableFuture.completedFuture(Optional.empty());
        }

        private TlsHandle<SSLContext> sslContextHandle() {
            return new TlsHandle<>() {
                @Override
                public SSLContext get() {
                    return initialContext;
                }

                @Override
                public void dispose() {
                }
            };
        }

        private static TlsHandle<SSLParameters> companionHandle(SSLParameters value) {
            return new TlsHandle<>() {
                @Override
                public SSLParameters get() {
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

    /** A running Jetty HTTPS server (WEB purpose, optional client auth) and its resources. */
    private final class JettyServer implements AutoCloseable {
        private final Server server;
        private final FileBasedTlsFactory factory;
        private final JettyTlsFactory.ReloadableServerTls reloadable;
        private final int port;

        JettyServer(boolean allowInsecureConnection) throws Exception {
            // The trust gate lives in the WEB SSLContext's trust managers (built from the policy's insecure
            // flag), which the framework hands to Jetty via setSslContext — Jetty's own setTrustAll is inert
            // on that path. So the policy's allowInsecureConnection is what actually scopes client-cert trust.
            factory = new FileBasedTlsFactory(
                    Map.of(TlsPurpose.WEB, TlsPolicy.builder()
                            .format(TlsPolicy.Format.PEM)
                            .trustCertsFilePath(CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                            .allowInsecureConnection(allowInsecureConnection)
                            .enableHostnameVerification(false)
                            .build()),
                    FileBasedTlsFactorySettings.builder().build());
            factory.initialize(initContext()).join();
            // Optional client auth (requireTrustedClientCert=false); TLSv1.2 so an untrusted-cert rejection
            // surfaces synchronously in the client handshake rather than as a post-handshake alert.
            reloadable = JettyTlsFactory.createReloadingServerFactory(factory, TlsPurpose.WEB, SAME_THREAD,
                    null, false, allowInsecureConnection, null, Set.of("TLSv1.2"));
            server = new Server();
            ServerConnector connector = new ServerConnector(server, reloadable.sslContextFactory());
            connector.setPort(0);
            server.setConnectors(new ServerConnector[] {connector});
            server.start();
            port = connector.getLocalPort();
        }

        @Override
        public void close() throws Exception {
            reloadable.subscription().dispose();
            server.stop();
            factory.close();
        }
    }

    private JettyServer startWebServer(boolean allowInsecureConnection) throws Exception {
        return new JettyServer(allowInsecureConnection);
    }

    // Connect presenting a client certificate over TLSv1.2 and complete the handshake (throws on rejection).
    // The key manager is forced to always present the given certificate regardless of the server's advertised
    // acceptable-CA list, so an untrusted certificate is actually sent (and therefore rejected) rather than
    // silently withheld by the default JSSE key manager.
    private void handshakeWithClientCert(int port, String clientCert, String clientKey) throws Exception {
        handshakeWithClientCert(port, clientCert, clientKey, "TLSv1.2");
    }

    private void handshakeWithClientCert(int port, String clientCert, String clientKey, String protocol)
            throws Exception {
        SSLContext clientContext = forcingClientContext(clientCert, clientKey);
        SSLSocketFactory socketFactory = clientContext.getSocketFactory();
        try (SSLSocket socket = (SSLSocket) socketFactory.createSocket("localhost", port)) {
            socket.setEnabledProtocols(new String[] {protocol});
            socket.setSoTimeout(10000);
            socket.startHandshake();
        }
    }

    // Connect trusting the CA but presenting no client certificate; a needClientAuth server rejects this.
    private void handshakeWithoutClientCert(int port) throws Exception {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        X509Certificate[] ca = PemReader.loadCertificatesFromPemFile(CA);
        for (int i = 0; i < ca.length; i++) {
            trustStore.setCertificateEntry("ca-" + i, ca[i]);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(trustStore);
        SSLContext clientContext = SSLContext.getInstance("TLS");
        clientContext.init(null, tmf.getTrustManagers(), null);
        SSLSocketFactory socketFactory = clientContext.getSocketFactory();
        try (SSLSocket socket = (SSLSocket) socketFactory.createSocket("localhost", port)) {
            socket.setEnabledProtocols(new String[] {"TLSv1.2"});
            socket.setSoTimeout(10000);
            socket.startHandshake();
        }
    }

    private static SSLContext forcingClientContext(String clientCert, String clientKey) throws Exception {
        X509Certificate[] chain = PemReader.loadCertificatesFromPemFile(clientCert);
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(clientKey);
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        X509Certificate[] ca = PemReader.loadCertificatesFromPemFile(CA);
        for (int i = 0; i < ca.length; i++) {
            trustStore.setCertificateEntry("ca-" + i, ca[i]);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(trustStore);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[] {new ForcingKeyManager(chain, key)}, tmf.getTrustManagers(), null);
        return context;
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

    private static BigInteger serverCertSerial(SSLContext clientContext, int port) throws Exception {
        SSLSocketFactory socketFactory = clientContext.getSocketFactory();
        try (SSLSocket socket = (SSLSocket) socketFactory.createSocket("localhost", port)) {
            socket.setSoTimeout(10000);
            socket.startHandshake();
            X509Certificate leaf = (X509Certificate) socket.getSession().getPeerCertificates()[0];
            return leaf.getSerialNumber();
        }
    }

    private static BigInteger certSerial(String pemPath) throws Exception {
        return PemReader.loadCertificatesFromPemFile(pemPath)[0].getSerialNumber();
    }

    private TlsFactoryInitContext initContext() {
        Executor direct = Runnable::run;
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return Map.of();
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return scheduler;
            }

            @Override
            public Executor blockingExecutor() {
                return direct;
            }

            @Override
            public Clock clock() {
                return Clock.systemUTC();
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return OpenTelemetry.noop();
            }
        };
    }

    private static String resource(String name) {
        return new File(Resources.getResource(name).getPath()).getAbsolutePath();
    }
}
