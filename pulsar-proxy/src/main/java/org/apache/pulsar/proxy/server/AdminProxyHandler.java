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
package org.apache.pulsar.proxy.server;

import static org.apache.commons.lang3.StringUtils.isBlank;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpRequest;
import org.eclipse.jetty.client.ProtocolHandlers;
import org.eclipse.jetty.client.RedirectProtocolHandler;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AdminProxyHandler extends ProxyServlet {

    private static final Logger LOG = LoggerFactory.getLogger(AdminProxyHandler.class);

    private static final String ORIGINAL_PRINCIPAL_HEADER = "X-Original-Principal";

    public static final String INIT_PARAM_REQUEST_BUFFER_SIZE = "requestBufferSize";

    private static final Set<String> functionRoutes = new HashSet<>(Arrays.asList(
        "/admin/v3/function",
        "/admin/v2/function",
        "/admin/function",
        "/admin/v3/source",
        "/admin/v2/source",
        "/admin/source",
        "/admin/v3/sink",
        "/admin/v2/sink",
        "/admin/sink",
        "/admin/v2/worker",
        "/admin/v2/worker-stats",
        "/admin/worker",
        "/admin/worker-stats"
    ));

    private final ProxyConfiguration config;
    private final BrokerDiscoveryProvider discoveryProvider;
    private final Authentication proxyClientAuthentication;
    private final String brokerWebServiceUrl;
    private final String functionWorkerWebServiceUrl;
    private PulsarSslFactory pulsarSslFactory;
    private ScheduledExecutorService sslContextRefresher;

    AdminProxyHandler(ProxyConfiguration config, BrokerDiscoveryProvider discoveryProvider,
                      Authentication proxyClientAuthentication) {
        this.config = config;
        this.discoveryProvider = discoveryProvider;
        this.proxyClientAuthentication = proxyClientAuthentication;
        this.brokerWebServiceUrl = config.isTlsEnabledWithBroker() ? config.getBrokerWebServiceURLTLS()
                : config.getBrokerWebServiceURL();
        this.functionWorkerWebServiceUrl = config.isTlsEnabledWithBroker() ? config.getFunctionWorkerWebServiceURLTLS()
                : config.getFunctionWorkerWebServiceURL();
        if (config.isTlsEnabledWithBroker()) {
            this.pulsarSslFactory = createPulsarSslFactory();
            this.sslContextRefresher = Executors.newSingleThreadScheduledExecutor(
                    new ExecutorProvider.ExtendedThreadFactory("pulsar-proxy-admin-handler-ssl-refresh"));
            if (config.getTlsCertRefreshCheckDurationSec() > 0) {
                this.sslContextRefresher.scheduleWithFixedDelay(this::refreshSslContext,
                        config.getTlsCertRefreshCheckDurationSec(), config.getTlsCertRefreshCheckDurationSec(),
                        TimeUnit.SECONDS);
            }
        }
        super.setTimeout(config.getHttpProxyTimeout());
    }

    @Override
    protected HttpClient createHttpClient() throws ServletException {
        ServletConfig config = getServletConfig();

        HttpClient client = newHttpClient();

        client.setFollowRedirects(true);

        // Must not store cookies, otherwise cookies of different clients will mix.
        client.setCookieStore(new HttpCookieStore.Empty());

        Executor executor;
        String value = config.getInitParameter("maxThreads");
        if (value == null || "-".equals(value)) {
            executor = (Executor) getServletContext().getAttribute("org.eclipse.jetty.server.Executor");
            if (executor == null) {
                throw new IllegalStateException("No server executor for proxy");
            }
        } else {
            QueuedThreadPool qtp = new QueuedThreadPool(Integer.parseInt(value));
            String servletName = config.getServletName();
            int dot = servletName.lastIndexOf('.');
            if (dot >= 0) {
                servletName = servletName.substring(dot + 1);
            }
            qtp.setName(servletName);
            executor = qtp;
        }

        client.setExecutor(executor);

        value = config.getInitParameter("maxConnections");
        if (value == null) {
            value = "256";
        }
        client.setMaxConnectionsPerDestination(Integer.parseInt(value));

        value = config.getInitParameter("idleTimeout");
        if (value == null) {
            value = "30000";
        }
        client.setIdleTimeout(Long.parseLong(value));

        value = config.getInitParameter(INIT_PARAM_REQUEST_BUFFER_SIZE);
        if (value != null) {
            client.setRequestBufferSize(Integer.parseInt(value));
        }

        value = config.getInitParameter("responseBufferSize");
        if (value != null){
            client.setResponseBufferSize(Integer.parseInt(value));
        }

        try {
            client.start();

            // Content must not be decoded, otherwise the client gets confused.
            // Allow encoded content, such as "Content-Encoding: gzip", to pass through without decoding it.
            client.getContentDecoderFactories().clear();

            // Pass traffic to the client, only intercept what's necessary.
            ProtocolHandlers protocolHandlers = client.getProtocolHandlers();
            protocolHandlers.clear();
            protocolHandlers.put(new RedirectProtocolHandler(client));

            return client;
        } catch (Exception x) {
            throw new ServletException(x);
        }
    }


    // This class allows the request body to be replayed, the default implementation
    // does not
    protected class ReplayableProxyContentProvider extends ProxyInputStreamContentProvider {
        static final int MIN_REPLAY_BODY_BUFFER_SIZE = 64;
        private boolean bodyBufferAvailable = false;
        private boolean bodyBufferMaxSizeReached = false;
        private final ByteArrayOutputStream bodyBuffer;
        private final long httpInputMaxReplayBufferSize;

        protected ReplayableProxyContentProvider(HttpServletRequest request, HttpServletResponse response,
                                                 Request proxyRequest, InputStream input,
                                                 int httpInputMaxReplayBufferSize) {
            super(request, response, proxyRequest, input);
            bodyBuffer = new ByteArrayOutputStream(
                    Math.min(Math.max(request.getContentLength(), MIN_REPLAY_BODY_BUFFER_SIZE),
                            httpInputMaxReplayBufferSize));
            this.httpInputMaxReplayBufferSize = httpInputMaxReplayBufferSize;
        }

        @Override
        public Iterator<ByteBuffer> iterator() {
            if (bodyBufferAvailable) {
                return Collections.singleton(ByteBuffer.wrap(bodyBuffer.toByteArray())).iterator();
            } else {
                bodyBufferAvailable = true;
                return super.iterator();
            }
        }

        @Override
        protected ByteBuffer onRead(byte[] buffer, int offset, int length) {
            if (!bodyBufferMaxSizeReached) {
                if (bodyBuffer.size() + length < httpInputMaxReplayBufferSize) {
                    bodyBuffer.write(buffer, offset, length);
                } else {
                    bodyBufferMaxSizeReached = true;
                    bodyBufferAvailable = false;
                    bodyBuffer.reset();
                }
            }
            return super.onRead(buffer, offset, length);
        }
    }

    private static class JettyHttpClient extends HttpClient {
        private static final int NUMBER_OF_SELECTOR_THREADS = 1;

        public JettyHttpClient() {
            super(new HttpClientTransportOverHTTP(NUMBER_OF_SELECTOR_THREADS), null);
        }

        public JettyHttpClient(SslContextFactory sslContextFactory) {
            super(new HttpClientTransportOverHTTP(NUMBER_OF_SELECTOR_THREADS), sslContextFactory);
        }

        /**
         * Ensure the Authorization header is carried over after a 307 redirect
         * from brokers.
         */
        @Override
        protected Request copyRequest(HttpRequest oldRequest, URI newURI) {
            String authorization = oldRequest.getHeaders().get(HttpHeader.AUTHORIZATION);
            Request newRequest = super.copyRequest(oldRequest, newURI);
            if (authorization != null) {
                newRequest.header(HttpHeader.AUTHORIZATION, authorization);
            }

            return newRequest;
        }

    }

    @Override
    protected ContentProvider proxyRequestContent(HttpServletRequest request,
                                                  HttpServletResponse response, Request proxyRequest)
            throws IOException {
        return new ReplayableProxyContentProvider(request, response, proxyRequest, request.getInputStream(),
                config.getHttpInputMaxReplayBufferSize());
    }

    @Override
    protected HttpClient newHttpClient() {
        try {
            if (config.isTlsEnabledWithBroker()) {
                try {
                    SslContextFactory contextFactory = new Client(this.pulsarSslFactory);
                    if (!config.isTlsHostnameVerificationEnabled()) {
                        contextFactory.setEndpointIdentificationAlgorithm(null);
                    }
                    return new JettyHttpClient(contextFactory);
                } catch (Exception e) {
                    LOG.error("new jetty http client exception ", e);
                    throw new PulsarClientException.InvalidConfigurationException(e.getMessage());
                }
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        // return an unauthenticated client, every request will fail.
        return new JettyHttpClient();
    }

    @Override
    protected String rewriteTarget(HttpServletRequest request) {
        StringBuilder url = new StringBuilder();

        boolean isFunctionsRestRequest = false;
        String requestUri = request.getRequestURI();
        for (String routePrefix : functionRoutes) {
            if (requestUri.startsWith(routePrefix)) {
                isFunctionsRestRequest = true;
                break;
            }
        }

        if (isFunctionsRestRequest && !isBlank(functionWorkerWebServiceUrl)) {
            url.append(functionWorkerWebServiceUrl);
        } else if (isBlank(brokerWebServiceUrl)) {
            try {
                ServiceLookupData availableBroker = discoveryProvider.nextBroker();

                if (config.isTlsEnabledWithBroker()) {
                    url.append(availableBroker.getWebServiceUrlTls());
                } else {
                    url.append(availableBroker.getWebServiceUrl());
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}:{}] Selected active broker is {}", request.getRemoteAddr(), request.getRemotePort(),
                            url);
                }
            } catch (Exception e) {
                LOG.warn("[{}:{}] Failed to get next active broker {}", request.getRemoteAddr(),
                        request.getRemotePort(), e.getMessage(), e);
                return null;
            }
        } else {
            url.append(brokerWebServiceUrl);
        }

        if (url.lastIndexOf("/") == url.length() - 1) {
            url.deleteCharAt(url.lastIndexOf("/"));
        }
        url.append(requestUri);

        String query = request.getQueryString();
        if (query != null) {
            url.append("?").append(query);
        }

        URI rewrittenUrl = URI.create(url.toString()).normalize();

        if (!validateDestination(rewrittenUrl.getHost(), rewrittenUrl.getPort())) {
            return null;
        }

        return rewrittenUrl.toString();
    }

    @Override
    protected void addProxyHeaders(HttpServletRequest clientRequest, Request proxyRequest) {
        super.addProxyHeaders(clientRequest, proxyRequest);
        String user = (String) clientRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
        if (user != null) {
            proxyRequest.header(ORIGINAL_PRINCIPAL_HEADER, user);
        }
    }

    private static class Client extends SslContextFactory.Client {

        private final PulsarSslFactory sslFactory;

        public Client(PulsarSslFactory sslFactory) {
            super();
            this.sslFactory = sslFactory;
        }

        @Override
        public SSLContext getSslContext() {
            return this.sslFactory.getInternalSslContext();
        }
    }

    protected PulsarSslConfiguration buildSslConfiguration(AuthenticationDataProvider authData) {
        return PulsarSslConfiguration.builder()
                .tlsProvider(config.getBrokerClientSslProvider())
                .tlsKeyStoreType(config.getBrokerClientTlsKeyStoreType())
                .tlsKeyStorePath(config.getBrokerClientTlsKeyStore())
                .tlsKeyStorePassword(config.getBrokerClientTlsKeyStorePassword())
                .tlsTrustStoreType(config.getBrokerClientTlsTrustStoreType())
                .tlsTrustStorePath(config.getBrokerClientTlsTrustStore())
                .tlsTrustStorePassword(config.getBrokerClientTlsTrustStorePassword())
                .tlsCiphers(config.getBrokerClientTlsCiphers())
                .tlsProtocols(config.getBrokerClientTlsProtocols())
                .tlsTrustCertsFilePath(config.getBrokerClientTrustCertsFilePath())
                .tlsCertificateFilePath(config.getBrokerClientCertificateFilePath())
                .tlsKeyFilePath(config.getBrokerClientKeyFilePath())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(false)
                .tlsEnabledWithKeystore(config.isBrokerClientTlsEnabledWithKeyStore())
                .tlsCustomParams(config.getBrokerClientSslFactoryPluginParams())
                .authData(authData)
                .serverMode(false)
                .isHttps(true)
                .build();
    }

    protected PulsarSslFactory createPulsarSslFactory() {
        try {
            try {
                AuthenticationDataProvider authData = proxyClientAuthentication.getAuthData();
                PulsarSslConfiguration pulsarSslConfiguration = buildSslConfiguration(authData);
                PulsarSslFactory sslFactory =
                        (PulsarSslFactory) Class.forName(config.getBrokerClientSslFactoryPlugin())
                                .getConstructor().newInstance();
                sslFactory.initialize(pulsarSslConfiguration);
                sslFactory.createInternalSslContext();
                return sslFactory;
            } catch (Exception e) {
                LOG.error("Failed to create Pulsar SSLFactory ", e);
                throw new PulsarClientException.InvalidConfigurationException(e.getMessage());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void refreshSslContext() {
        try {
            this.pulsarSslFactory.update();
        } catch (Exception e) {
            LOG.error("Failed to refresh SSL context", e);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        if (this.sslContextRefresher != null) {
            this.sslContextRefresher.shutdownNow();
        }
    }
}
