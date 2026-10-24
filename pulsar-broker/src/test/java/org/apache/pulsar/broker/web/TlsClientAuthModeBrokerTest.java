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
package org.apache.pulsar.broker.web;

import static org.assertj.core.api.Assertions.assertThat;
import java.net.Socket;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.tls.TlsClientAuthMode;
import org.apache.pulsar.common.util.tls.PemReader;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The broker's binary and web TLS listeners send a TLS {@code CertificateRequest} only when client certificates
 * are required or used, following {@code tlsClientAuthMode}.
 */
@Test(groups = "broker")
public class TlsClientAuthModeBrokerTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
    }

    @DataProvider(name = "protocols")
    public Object[][] protocols() {
        return new Object[][] {{"TLSv1.2"}, {"TLSv1.3"}};
    }

    @Test(dataProvider = "protocols")
    public void noCertificateRequestWithoutTlsAuthentication(String protocol) throws Exception {
        // Authentication is disabled, so nothing uses a client certificate and the default mode is NONE.
        assertThat(clientCertificateRequested(pulsar.getBrokerListenPortTls().orElseThrow(), protocol))
                .as("binary listener").isFalse();
        assertThat(clientCertificateRequested(pulsar.getListenPortHTTPS().orElseThrow(), protocol))
                .as("web listener").isFalse();
    }

    @Test(dataProvider = "protocols")
    public void certificateRequestedWithOptionalClientAuth(String protocol) throws Exception {
        restartBroker(conf -> conf.setTlsClientAuthMode(TlsClientAuthMode.OPTIONAL));
        assertThat(clientCertificateRequested(pulsar.getBrokerListenPortTls().orElseThrow(), protocol))
                .as("binary listener").isTrue();
        assertThat(clientCertificateRequested(pulsar.getListenPortHTTPS().orElseThrow(), protocol))
                .as("web listener").isTrue();
    }

    // Completes a TLS handshake that presents no client certificate and reports whether the server asked for
    // one: the client key manager is consulted only on a CertificateRequest.
    private static boolean clientCertificateRequested(int port, String protocol) throws Exception {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", PemReader.loadCertificatesFromPemFile(CA_CERT_FILE_PATH)[0]);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(trustStore);
        RecordingKeyManager keyManager = new RecordingKeyManager();
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[] {keyManager}, tmf.getTrustManagers(), null);
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket("localhost", port)) {
            socket.setEnabledProtocols(new String[] {protocol});
            socket.startHandshake();
        }
        return keyManager.consulted.get();
    }

    /** A key manager without identities that records whether the server requested a client certificate. */
    private static final class RecordingKeyManager extends X509ExtendedKeyManager {
        private final AtomicBoolean consulted = new AtomicBoolean();

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return null;
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            consulted.set(true);
            return null;
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            consulted.set(true);
            return null;
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
            return null;
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return null;
        }
    }
}
