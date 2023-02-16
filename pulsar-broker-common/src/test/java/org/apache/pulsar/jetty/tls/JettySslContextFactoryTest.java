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

import com.google.common.io.Resources;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pulsar.common.util.SecurityUtility;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.annotations.Test;

@Slf4j
public class JettySslContextFactoryTest {

    @Test
    public void testJettyTlsServerTls() throws Exception {
        Server server = new Server();
        List<ServerConnector> connectors = new ArrayList<>();
        SslContextFactory factory = JettySslContextFactory.createServerSslContext(
                null,
                false,
                Resources.getResource("ssl/my-ca/ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/server-ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/server-key.pem").getPath(),
                true,
                null,
                null,
                600);

        ServerConnector connector = new ServerConnector(server, factory);
        connector.setPort(0);
        connectors.add(connector);
        server.setConnectors(connectors.toArray(new ServerConnector[0]));
        server.start();
        // client connect
        HttpClientBuilder httpClientBuilder = HttpClients.custom();
        RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
        registryBuilder.register("https",
                new SSLConnectionSocketFactory(getClientSslContext(), new NoopHostnameVerifier()));
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registryBuilder.build());
        httpClientBuilder.setConnectionManager(cm);
        CloseableHttpClient httpClient = httpClientBuilder.build();
        HttpGet httpGet = new HttpGet("https://localhost:" + connector.getLocalPort());
        httpClient.execute(httpGet);
        httpClient.close();
        server.stop();
    }

    @Test(expectedExceptions = SSLHandshakeException.class)
    public void testJettyTlsServerInvalidTlsProtocol() throws Exception {
        Server server = new Server();
        List<ServerConnector> connectors = new ArrayList<>();
        SslContextFactory factory = JettySslContextFactory.createServerSslContext(
                null,
                false,
                Resources.getResource("ssl/my-ca/ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/server-ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/server-key.pem").getPath(),
                true,
                null,
                new HashSet<String>() {
                    {
                        this.add("TLSv1.3");
                    }
                },
                600);
        factory.setHostnameVerifier((s, sslSession) -> true);
        ServerConnector connector = new ServerConnector(server, factory);
        connector.setPort(0);
        connectors.add(connector);
        server.setConnectors(connectors.toArray(new ServerConnector[0]));
        server.start();
        // client connect
        HttpClientBuilder httpClientBuilder = HttpClients.custom();
        RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
        registryBuilder.register("https", new SSLConnectionSocketFactory(getClientSslContext(),
                new String[]{"TLSv1.2"}, null, new NoopHostnameVerifier()));
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registryBuilder.build());
        httpClientBuilder.setConnectionManager(cm);
        CloseableHttpClient httpClient = httpClientBuilder.build();
        HttpGet httpGet = new HttpGet("https://localhost:" + connector.getLocalPort());
        httpClient.execute(httpGet);
        httpClient.close();
        server.stop();
    }

    @Test(expectedExceptions = SSLHandshakeException.class)
    public void testJettyTlsServerInvalidCipher() throws Exception {
        Server server = new Server();
        List<ServerConnector> connectors = new ArrayList<>();
        SslContextFactory factory = JettySslContextFactory.createServerSslContext(
                null,
                false,
                Resources.getResource("ssl/my-ca/ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/server-ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/server-key.pem").getPath(),
                true,
                new HashSet<String>() {
                    {
                        this.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
                    }
                },
                new HashSet<String>() {
                    {
                        this.add("TLSv1.2");
                    }
                },
                600);

        factory.setHostnameVerifier((s, sslSession) -> true);
        ServerConnector connector = new ServerConnector(server, factory);
        connector.setPort(0);
        connectors.add(connector);
        server.setConnectors(connectors.toArray(new ServerConnector[0]));
        server.start();
        // client connect
        HttpClientBuilder httpClientBuilder = HttpClients.custom();
        RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
        registryBuilder.register("https", new SSLConnectionSocketFactory(getClientSslContext(),
                new String[]{"TLSv1.2"}, new String[]{"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"},
                new NoopHostnameVerifier()));
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registryBuilder.build());
        httpClientBuilder.setConnectionManager(cm);
        CloseableHttpClient httpClient = httpClientBuilder.build();
        HttpGet httpGet = new HttpGet("https://localhost:" + connector.getLocalPort());
        httpClient.execute(httpGet);
        httpClient.close();
        server.stop();
    }

    private static SSLContext getClientSslContext() throws GeneralSecurityException, IOException {
        return SecurityUtility.createSslContext(
                false,
                Resources.getResource("ssl/my-ca/ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/client-ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/client-key.pem").getPath(),
                null
        );
    }
}
