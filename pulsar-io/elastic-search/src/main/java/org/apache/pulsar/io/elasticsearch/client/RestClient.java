/**
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
package org.apache.pulsar.io.elasticsearch.client;

import com.google.common.base.Strings;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConfig;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConnectionException;
import org.apache.pulsar.io.elasticsearch.ElasticSearchSslConfig;
import org.elasticsearch.client.RestClientBuilder;

public abstract class RestClient implements Closeable {

    protected final ElasticSearchConfig config;
    protected final ConfigCallback configCallback;
    private final ScheduledExecutorService executorService;

    public RestClient(ElasticSearchConfig elasticSearchConfig, BulkProcessor.Listener bulkProcessorListener) {
        this.config = elasticSearchConfig;
        this.configCallback = new ConfigCallback();

        // idle+expired connection evictor thread
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.executorService.scheduleAtFixedRate(() -> {
                    configCallback.connectionManager.closeExpiredConnections();
                    configCallback.connectionManager.closeIdleConnections(
                            config.getConnectionIdleTimeoutInMs(), TimeUnit.MILLISECONDS);
                },
                config.getConnectionIdleTimeoutInMs(),
                config.getConnectionIdleTimeoutInMs(),
                TimeUnit.MILLISECONDS
        );
    }

    public abstract boolean indexExists(String index) throws IOException;
    public abstract boolean createIndex(String index) throws IOException;
    public abstract boolean deleteIndex(String index) throws IOException;

    public abstract boolean indexDocument(String index, String documentId, String documentSource) throws IOException;
    public abstract boolean deleteDocument(String index, String documentId) throws IOException;

    public abstract long totalHits(String index) throws IOException;

    public abstract BulkProcessor getBulkProcessor();

    public class ConfigCallback implements RestClientBuilder.HttpClientConfigCallback,
            org.opensearch.client.RestClientBuilder.HttpClientConfigCallback {
        final NHttpClientConnectionManager connectionManager;
        final CredentialsProvider credentialsProvider;

        public ConfigCallback() {
            this.connectionManager = buildConnectionManager(RestClient.this.config);
            this.credentialsProvider = buildCredentialsProvider(RestClient.this.config);
        }

        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder builder) {
            builder.setMaxConnPerRoute(config.getBulkConcurrentRequests());
            builder.setMaxConnTotal(config.getBulkConcurrentRequests());
            builder.setConnectionManager(connectionManager);

            if (this.credentialsProvider != null) {
                builder.setDefaultCredentialsProvider(credentialsProvider);
            }
            return builder;
        }

        public NHttpClientConnectionManager buildConnectionManager(ElasticSearchConfig config) {
            try {
                IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                        .setConnectTimeout(config.getConnectTimeoutInMs())
                        .setSoTimeout(config.getSocketTimeoutInMs())
                        .build();
                ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
                PoolingNHttpClientConnectionManager connManager;
                if (config.getSsl().isEnabled()) {
                    ElasticSearchSslConfig sslConfig = config.getSsl();
                    HostnameVerifier hostnameVerifier = config.getSsl().isHostnameVerification()
                            ? SSLConnectionSocketFactory.getDefaultHostnameVerifier()
                            : new NoopHostnameVerifier();
                    String[] cipherSuites = null;
                    if (!Strings.isNullOrEmpty(sslConfig.getCipherSuites())) {
                        cipherSuites = sslConfig.getCipherSuites().split(",");
                    }
                    String[] protocols = null;
                    if (!Strings.isNullOrEmpty(sslConfig.getProtocols())) {
                        protocols = sslConfig.getProtocols().split(",");
                    }
                    Registry<SchemeIOSessionStrategy> registry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                            .register("http", NoopIOSessionStrategy.INSTANCE)
                            .register("https", new SSLIOSessionStrategy(
                                    buildSslContext(config),
                                    protocols,
                                    cipherSuites,
                                    hostnameVerifier))
                            .build();
                    connManager = new PoolingNHttpClientConnectionManager(ioReactor, registry);
                } else {
                    connManager = new PoolingNHttpClientConnectionManager(ioReactor);
                }
                return connManager;
            } catch (Exception e) {
                throw new ElasticSearchConnectionException(e);
            }
        }

        private SSLContext buildSslContext(ElasticSearchConfig config) throws NoSuchAlgorithmException,
                KeyManagementException, CertificateException, KeyStoreException,
                IOException, UnrecoverableKeyException {
            ElasticSearchSslConfig sslConfig = config.getSsl();
            SSLContextBuilder sslContextBuilder = SSLContexts.custom();
            if (!Strings.isNullOrEmpty(sslConfig.getProvider())) {
                sslContextBuilder.setProvider(sslConfig.getProvider());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getProtocols())) {
                sslContextBuilder.setProtocol(sslConfig.getProtocols());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getTruststorePath())
                    && !Strings.isNullOrEmpty(sslConfig.getTruststorePassword())) {
                sslContextBuilder.loadTrustMaterial(new File(sslConfig.getTruststorePath()),
                        sslConfig.getTruststorePassword().toCharArray());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getKeystorePath())
                    && !Strings.isNullOrEmpty(sslConfig.getKeystorePassword())) {
                sslContextBuilder.loadKeyMaterial(new File(sslConfig.getKeystorePath()),
                        sslConfig.getKeystorePassword().toCharArray(),
                        sslConfig.getKeystorePassword().toCharArray());
            }
            return sslContextBuilder.build();
        }

        private CredentialsProvider buildCredentialsProvider(ElasticSearchConfig config) {
            if (StringUtils.isEmpty(config.getUsername()) || StringUtils.isEmpty(config.getPassword())) {
                return null;
            }
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            return credentialsProvider;
        }
    }

    protected HttpHost[] getHttpHosts() {
        final String url = config.getElasticSearchUrl();
        return Arrays.stream(url.split(",")).map(host -> {
            try {
                URL hostUrl = new URL(host);
                return new HttpHost(hostUrl.getHost(), hostUrl.getPort(),
                        hostUrl.getProtocol());
            } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid elasticSearch url :" + host);
            }
        }).toArray(HttpHost[]::new);
    }

    protected abstract void closeClient();

    @Override
    public void close() {
        executorService.shutdown();
        closeClient();
    }
}
