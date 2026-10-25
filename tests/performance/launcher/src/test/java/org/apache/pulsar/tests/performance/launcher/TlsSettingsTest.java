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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TlsSettingsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void isDisabledWithoutTheSection() throws Exception {
        ClusterSettings settings = ClusterSettings.read(mapper, mapper.readTree("""
                brokers:
                  replicas: 2
                  bootstrap: all
                bookies:
                  replicas: 1
                """));

        assertThat(settings.tls()).isEqualTo(TlsSettings.DISABLED);
        assertThat(settings.tls().trustCertsFilePath()).isNull();
        assertThat(settings.tls().withBrokerTlsSettings(Map.of("a", "b"))).isEqualTo(Map.of("a", "b"));
        assertThat(settings.serviceUrl("iot-1"))
                .isEqualTo("pulsar://iot-1-pulsar-broker-0:6650,iot-1-pulsar-broker-1:6650");
    }

    @Test
    public void connectsTheWorkloadsToTheBrokersTlsPort() throws Exception {
        ClusterSettings settings = ClusterSettings.read(mapper, mapper.readTree("""
                brokers:
                  replicas: 2
                  bootstrap: all
                  env:
                    tlsProtocols: TLSv1.3
                bookies:
                  replicas: 1
                tls:
                  enabled: true
                remote: {}
                """));

        assertThat(settings.tls()).isEqualTo(new TlsSettings(true, TlsSettings.RSA));
        assertThat(settings.serviceUrl("iot-1"))
                .isEqualTo("pulsar+ssl://iot-1-pulsar-broker-0:6651,iot-1-pulsar-broker-1:6651");
        assertThat(settings.remote().serviceUrl("iot-1", settings.tls()))
                .isEqualTo("pulsar+ssl://iot-1-remote-pulsar-broker-0:6651,iot-1-remote-pulsar-broker-1:6651");
        assertThat(settings.tls().trustCertsFilePath()).isEqualTo("/pulsar/performance-tls/ca.cert.pem");
        assertThat(settings.tls().withBrokerTlsSettings(settings.brokers().env())).containsExactly(
                Map.entry("brokerServicePortTls", "6651"),
                Map.entry("tlsCertificateFilePath", "/pulsar/performance-tls/broker.cert.pem"),
                Map.entry("tlsKeyFilePath", "/pulsar/performance-tls/broker.key-pk8.pem"),
                Map.entry("tlsTrustCertsFilePath", "/pulsar/performance-tls/ca.cert.pem"),
                Map.entry("tlsProtocols", "TLSv1.3"));
    }

    @Test
    public void readsTheSettings() throws Exception {
        assertThat(TlsSettings.read(mapper.readTree("{enabled: true, keyAlgorithm: EC}")))
                .isEqualTo(new TlsSettings(true, TlsSettings.EC));
        assertThat(TlsSettings.read(mapper.readTree("{enabled: false}")).enabled()).isFalse();
        // As an environment override or --set gives it
        assertThat(TlsSettings.read(mapper.readTree("{enabled: \"true\"}")).enabled()).isTrue();

        assertThatThrownBy(() -> TlsSettings.read(mapper.readTree("true")))
                .hasMessageContaining("cluster.tls must be a mapping");
        assertThatThrownBy(() -> TlsSettings.read(mapper.readTree("{enabled: yes please}")))
                .hasMessageContaining("cluster.tls.enabled must be true or false");
        assertThatThrownBy(() -> TlsSettings.read(mapper.readTree("{enabled: true, keyAlgorithm: DSA}")))
                .hasMessageContaining("cluster.tls.keyAlgorithm must be one of [RSA, EC]");
        assertThatThrownBy(() -> TlsSettings.read(mapper.readTree("{enabled: true, clientAuth: true}")))
                .hasMessageContaining("cluster.tls.clientAuth isn't a setting");
    }

    @DataProvider
    public Object[][] keyAlgorithms() {
        return new Object[][] {{TlsSettings.RSA}, {TlsSettings.EC}};
    }

    @Test(dataProvider = "keyAlgorithms")
    public void writesACertificateOfTheBrokersNamesThatTheCaSigns(String keyAlgorithm) throws Exception {
        Path directory = Files.createTempDirectory("tls-settings-test").resolve("tls");
        TlsSettings tls = new TlsSettings(true, keyAlgorithm);

        tls.writeCertificates(directory, List.of("iot-1-pulsar-broker-0", "pulsar-broker-0", "pulsar-broker-0"));

        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        X509Certificate ca;
        X509Certificate broker;
        try (InputStream in = Files.newInputStream(directory.resolve(TlsSettings.CA_CERTIFICATE))) {
            ca = (X509Certificate) factory.generateCertificate(in);
        }
        try (InputStream in = Files.newInputStream(directory.resolve(TlsSettings.BROKER_CERTIFICATE))) {
            broker = (X509Certificate) factory.generateCertificate(in);
        }
        broker.verify(ca.getPublicKey());
        ca.verify(ca.getPublicKey());
        assertThat(ca.getBasicConstraints()).isGreaterThanOrEqualTo(0);
        assertThat(broker.getBasicConstraints()).isEqualTo(-1);
        assertThat(broker.getPublicKey().getAlgorithm()).isEqualTo(keyAlgorithm);
        assertThat(broker.getSubjectAlternativeNames()).containsExactly(
                List.of(2, "iot-1-pulsar-broker-0"), List.of(2, "pulsar-broker-0"));
        assertThat(Files.readString(directory.resolve(TlsSettings.BROKER_KEY))).startsWith("-----BEGIN PRIVATE KEY");

        // A client that verifies the broker's name, as Pulsar's clients do by default, accepts the names
        handshake(directory, "iot-1-pulsar-broker-0");
        handshake(directory, "pulsar-broker-0");
        assertThatThrownBy(() -> handshake(directory, "pulsar-broker-1"))
                .isInstanceOf(SSLHandshakeException.class);
    }

    /** A TLS handshake of a client that connects to the broker {@code hostName} with the written files. */
    private static void handshake(Path directory, String hostName) throws Exception {
        SSLContext server = ((JdkSslContext) SslContextBuilder.forServer(
                directory.resolve(TlsSettings.BROKER_CERTIFICATE).toFile(),
                directory.resolve(TlsSettings.BROKER_KEY).toFile()).sslProvider(SslProvider.JDK).build()).context();
        SSLContext client = ((JdkSslContext) SslContextBuilder.forClient()
                .trustManager(directory.resolve(TlsSettings.CA_CERTIFICATE).toFile())
                .sslProvider(SslProvider.JDK).build()).context();
        try (SSLServerSocket serverSocket = (SSLServerSocket) server.getServerSocketFactory()
                .createServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> accepted = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.getOutputStream().write(1);
                    socket.getInputStream().read();
                } catch (Exception e) {
                    // The client's failure is the result
                }
            });
            try (Socket plain = new Socket(InetAddress.getLoopbackAddress(), serverSocket.getLocalPort());
                 SSLSocket socket = (SSLSocket) client.getSocketFactory().createSocket(plain, hostName,
                         serverSocket.getLocalPort(), true)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                OutputStream out = socket.getOutputStream();
                assertThat(socket.getInputStream().read()).isEqualTo(1);
                out.write(1);
                out.flush();
            }
            accepted.get(10, TimeUnit.SECONDS);
        }
    }
}
