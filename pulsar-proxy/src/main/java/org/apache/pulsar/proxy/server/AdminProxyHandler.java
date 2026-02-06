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
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;
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
import org.eclipse.jetty.util.ssl.SslContextFactory;
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

    AdminProxyHandler(ProxyConfiguration config, BrokerDiscoveryProvider discoveryProvider,
                      Authentication proxyClientAuthentication) {
        this.config = config;
        this.discoveryProvider = discoveryProvider;
        this.proxyClientAuthentication = proxyClientAuthentication;
        this.brokerWebServiceUrl = config.isTlsEnabledWithBroker() ? config.getBrokerWebServiceURLTLS()
                : config.getBrokerWebServiceURL();
        this.functionWorkerWebServiceUrl = config.isTlsEnabledWithBroker() ? config.getFunctionWorkerWebServiceURLTLS()
                : config.getFunctionWorkerWebServiceURL();

    }

    @Override
    protected HttpClient createHttpClient() throws ServletException {
        HttpClient httpClient = super.createHttpClient();
        customizeHttpClient(httpClient);
        return httpClient;
    }

    protected void customizeHttpClient(HttpClient httpClient) {
        httpClient.setFollowRedirects(true);

        ProtocolHandlers protocolHandlers = httpClient.getProtocolHandlers();
        if (protocolHandlers != null) {
            protocolHandlers.put(new RedirectProtocolHandler(httpClient));
        }

        setTimeout(config.getHttpProxyTimeout());
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
                    X509Certificate[] trustCertificates = SecurityUtility
                        .loadCertificatesFromPemFile(config.getBrokerClientTrustCertsFilePath());

                    SSLContext sslCtx;
                    AuthenticationDataProvider authData =
                            proxyClientAuthentication.getAuthData(URI.create(getWebServiceUrl()).getHost());
                    if (config.isBrokerClientTlsEnabledWithKeyStore()) {
                        KeyStoreParams params = authData.hasDataForTls() ? authData.getTlsKeyStoreParams() : null;
                        sslCtx = KeyStoreSSLContext.createClientSslContext(
                                config.getBrokerClientSslProvider(),
                                params != null ? params.getKeyStoreType() : null,
                                params != null ? params.getKeyStorePath() : null,
                                params != null ? params.getKeyStorePassword() : null,
                                config.isTlsAllowInsecureConnection(),
                                config.getBrokerClientTlsTrustStoreType(),
                                config.getBrokerClientTlsTrustStore(),
                                config.getBrokerClientTlsTrustStorePassword(),
                                config.getBrokerClientTlsCiphers(),
                                config.getBrokerClientTlsProtocols());
                    } else {
                        if (authData.hasDataForTls()) {
                            sslCtx = SecurityUtility.createSslContext(
                                    config.isTlsAllowInsecureConnection(),
                                    trustCertificates,
                                    authData.getTlsCertificates(),
                                    authData.getTlsPrivateKey(),
                                    config.getBrokerClientSslProvider()
                            );
                        } else {
                            sslCtx = SecurityUtility.createSslContext(
                                    config.isTlsAllowInsecureConnection(),
                                    trustCertificates,
                                    config.getBrokerClientSslProvider()
                            );
                        }
                    }

                    SslContextFactory contextFactory = new SslContextFactory.Client();
                    contextFactory.setSslContext(sslCtx);
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

    private String getWebServiceUrl() throws PulsarServerException {
        if (isBlank(brokerWebServiceUrl)) {
            ServiceLookupData availableBroker = discoveryProvider.nextBroker();
            if (config.isTlsEnabledWithBroker()) {
                return availableBroker.getWebServiceUrlTls();
            } else {
                return availableBroker.getWebServiceUrl();
            }
        } else {
            return brokerWebServiceUrl;
        }
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
        } else {
            try {
                url.append(getWebServiceUrl());
                if (LOG.isDebugEnabled() && isBlank(brokerWebServiceUrl)) {
                    LOG.debug("[{}:{}] Selected active broker is {}", request.getRemoteAddr(), request.getRemotePort(),
                            url);
                }
            } catch (Exception e) {
                LOG.warn("[{}:{}] Failed to get next active broker {}", request.getRemoteAddr(),
                        request.getRemotePort(), e.getMessage(), e);
                return null;
            }
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
}
