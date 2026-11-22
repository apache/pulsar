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
import com.google.common.io.Resources;
import java.io.File;
import java.net.InetAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import org.apache.pulsar.common.util.tls.JcaProviders;
import org.apache.pulsar.common.util.tls.JdkSslContexts;
import org.apache.pulsar.common.util.tls.PemReader;
import org.awaitility.Awaitility;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.ssl.SslConnection;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.SkipException;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarSslConnectionFactoryTest {
    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");

    /**
     * A client that resets the connection without a TLS close_notify must not leave the server's Conscrypt engine
     * open. Otherwise its native SSL object is released only by the engine's finalizer, which can race with the
     * NativeSsl finalizer and crash the JVM.
     */
    @Test
    public void closesConscryptEngineWhenClientResetsConnection() throws Exception {
        if (JcaProviders.CONSCRYPT_PROVIDER == null) {
            throw new SkipException("Conscrypt is not available on this platform");
        }
        SSLContext serverContext = JdkSslContexts.createSslContextWithProvider(false,
                PemReader.loadCertificatesFromPemFile(CA), PemReader.loadCertificatesFromPemFile(BROKER_CERT),
                PemReader.loadPrivateKeyFromPemFile(BROKER_KEY), JcaProviders.CONSCRYPT_PROVIDER);
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setSslContext(serverContext);

        Server server = new Server();
        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory();
        ServerConnector connector = new ServerConnector(server,
                new PulsarSslConnectionFactory(sslContextFactory, httpConnectionFactory.getProtocol()),
                httpConnectionFactory);
        connector.setHost(InetAddress.getLoopbackAddress().getHostAddress());
        connector.setPort(0);
        CompletableFuture<SslConnection> closedConnection = new CompletableFuture<>();
        connector.addEventListener(new Connection.Listener() {
            @Override
            public void onClosed(Connection connection) {
                if (connection instanceof SslConnection sslConnection) {
                    closedConnection.complete(sslConnection);
                }
            }
        });
        server.addConnector(connector);
        server.start();
        try {
            SSLContext clientContext = JdkSslContexts.createSslContext(false,
                    PemReader.loadCertificatesFromPemFile(CA), null, null);
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), connector.getLocalPort())) {
                SSLSocket sslSocket = (SSLSocket) clientContext.getSocketFactory()
                        .createSocket(socket, "localhost", connector.getLocalPort(), false);
                sslSocket.startHandshake();
                // Reset the TCP connection without sending a close_notify.
                socket.setSoLinger(true, 0);
            }

            SslConnection sslConnection = closedConnection.get(30, TimeUnit.SECONDS);
            SSLEngine engine = sslConnection.getSSLEngine();
            assertThat(PulsarSslConnectionFactory.isConscryptEngine(engine)).isTrue();
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(engine.isInboundDone()).as("inbound side of the server engine is closed").isTrue());
        } finally {
            server.stop();
        }
    }

    @Test
    public void doesNotCloseEnginesOfOtherProviders() throws Exception {
        SSLEngine engine = SSLContext.getDefault().createSSLEngine();
        assertThat(PulsarSslConnectionFactory.isConscryptEngine(engine)).isFalse();
    }

    private static String resource(String name) {
        return new File(Resources.getResource(name).getPath()).getAbsolutePath();
    }
}
