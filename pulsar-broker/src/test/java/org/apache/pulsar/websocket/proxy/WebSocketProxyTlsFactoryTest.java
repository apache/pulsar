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
package org.apache.pulsar.websocket.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import org.apache.pulsar.broker.tls.CountingWebTlsFactory;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.common.util.tls.JdkSslContexts;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.utils.ResourceUtils;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.annotations.Test;

/**
 * PIP-478 (T5): the standalone WebSocket proxy serves its WEB (HTTPS) listener through a custom
 * {@link org.apache.pulsar.tls.PulsarTlsFactory} selected via {@code tlsFactoryClassName}. A counting
 * factory proves the new SPI path is exercised (its {@code createInstance} is invoked while building the WEB
 * SslContextFactory) and a successful HTTPS request against the {@code /status.html} endpoint proves the
 * synthesized web TLS listener actually works. No broker is started — the proxy's Jetty web server alone.
 */
@Test(groups = "websocket")
public class WebSocketProxyTlsFactoryTest {

    private static final String CA = ResourceUtils.getAbsolutePath("certificate-authority/certs/ca.cert.pem");
    private static final String CERT =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.cert.pem");
    private static final String KEY =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.key-pk8.pem");

    @Test
    public void webListenerServedThroughCustomTlsFactory() throws Exception {
        int before = CountingWebTlsFactory.createInstanceCount();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setClusterName("test");
        config.setWebServicePort(Optional.empty());
        config.setWebServicePortTls(Optional.of(0));
        config.setTlsFactoryClassName(CountingWebTlsFactory.class.getName());
        config.setTlsFactoryConfig("trust=" + CA + ",cert=" + CERT + ",key=" + KEY);
        config.setStatusFilePath(CA);

        ProxyServer proxyServer = new ProxyServer(config);
        proxyServer.addRestResource("/", VipStatus.ATTRIBUTE_STATUS_FILE_PATH, CA, VipStatus.class);
        proxyServer.start();
        try {
            int port = proxyServer.getListenPortHTTPS().orElseThrow();
            ContentResponse response = httpsGet("https://localhost:" + port + "/status.html");
            assertThat(response.getStatus()).isEqualTo(200);
            assertThat(response.getContentAsString()).isEqualTo("OK");
            assertThat(CountingWebTlsFactory.createInstanceCount())
                    .as("the WEB purpose was served through the custom tlsFactoryClassName")
                    .isGreaterThan(before);
        } finally {
            proxyServer.stop();
        }
    }

    private static ContentResponse httpsGet(String url) throws Exception {
        SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();
        sslContextFactory.setSslContext(JdkSslContexts.createSslContext(
                false, PemReader.loadCertificatesFromPemFile(CA), null));
        HttpClient httpClient = new HttpClient();
        httpClient.setSslContextFactory(sslContextFactory);
        httpClient.start();
        try {
            return httpClient.newRequest(url).send();
        } finally {
            httpClient.stop();
        }
    }
}
