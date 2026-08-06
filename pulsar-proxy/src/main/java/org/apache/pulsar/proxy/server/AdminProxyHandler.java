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
import io.opentelemetry.api.OpenTelemetry;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.tls.TlsFactorySupport;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.jetty.tls.JettyTlsFactory;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.ProtocolHandlers;
import org.eclipse.jetty.client.RedirectProtocolHandler;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.client.transport.HttpClientTransportOverHTTP;
import org.eclipse.jetty.ee10.proxy.ProxyServlet;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@CustomLog
class AdminProxyHandler extends ProxyServlet {
    private static final long serialVersionUID = 1L;

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
    // PIP-478 broker-client TLS SPI factory (the only path since the PIP-337 removal).
    private PulsarTlsFactory brokerClientTlsFactory;
    // PIP-478: the handle backing the Jetty HttpClient's SslContextFactory.Client — the BROKER_CLIENT
    // SSLContext subscription that reloads a synthesized client on rotation, or the native-instance handle
    // when a custom factory supplies the Jetty client directly. Disposed in destroy().
    private TlsHandle<?> brokerClientTlsSubscription;
    // PIP-478: the OpenTelemetry root threaded into the BROKER_CLIENT-purpose TlsFactoryInitContext so
    // pulsar.tls.reload emits; OpenTelemetry.noop() when unset.
    private final OpenTelemetry openTelemetry;
    private ScheduledExecutorService sslContextRefresher;

    AdminProxyHandler(ProxyConfiguration config, BrokerDiscoveryProvider discoveryProvider,
                      Authentication proxyClientAuthentication) {
        this(config, discoveryProvider, proxyClientAuthentication, OpenTelemetry.noop());
    }

    AdminProxyHandler(ProxyConfiguration config, BrokerDiscoveryProvider discoveryProvider,
                      Authentication proxyClientAuthentication, OpenTelemetry openTelemetry) {
        this.config = config;
        this.discoveryProvider = discoveryProvider;
        this.proxyClientAuthentication = proxyClientAuthentication;
        this.openTelemetry = openTelemetry;
        this.brokerWebServiceUrl = config.isTlsEnabledWithBroker() ? config.getBrokerWebServiceURLTLS()
                : config.getBrokerWebServiceURL();
        this.functionWorkerWebServiceUrl = config.isTlsEnabledWithBroker() ? config.getFunctionWorkerWebServiceURLTLS()
                : config.getFunctionWorkerWebServiceURL();
        if (config.isTlsEnabledWithBroker()) {
            this.sslContextRefresher = Executors.newSingleThreadScheduledExecutor(
                    new ExecutorProvider.ExtendedThreadFactory("pulsar-proxy-admin-handler-ssl-refresh"));
            this.brokerClientTlsFactory = createBrokerClientTlsFactory();
        }
    }

    // PIP-478: build+initialize the broker-client PulsarTlsFactory (BROKER_CLIENT purpose). The factory owns
    // rotation internally; newHttpClient() builds a self-reloading SslContextFactory.Client that swaps the
    // context on rotation, so a long-lived admin HttpClient picks up rotated broker-client material.
    private PulsarTlsFactory createBrokerClientTlsFactory() {
        try {
            PulsarTlsFactory factory = TlsFactorySupport.createFactory(config.getBrokerClientTlsFactoryClassName(),
                    null, () -> ProxyTlsFactories.brokerClientFactory(config, proxyClientAuthentication));
            TlsFactoryInitContext initContext = TlsFactorySupport.initContext(
                    TlsFactorySupport.parseFactoryConfig(config.getBrokerClientTlsFactoryConfig()),
                    sslContextRefresher, sslContextRefresher, openTelemetry);
            TlsFactorySupport.initializeBlocking(factory, initContext);
            return factory;
        } catch (Exception e) {
            log.error().exception(e).log("Failed to create Pulsar TLS factory");
            throw new RuntimeException(e);
        }
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
            protocolHandlers.put(new NonAbortingRedirectProtocolHandler(httpClient));
        }

        httpClient.setIdleTimeout(config.getHttpProxyIdleTimeout());

        setTimeout(config.getHttpProxyTimeout());
    }

    /**
     * A {@link RedirectProtocolHandler} that does not abort the in-flight request when a redirect
     * response is received.
     *
     * <p>Jetty's default {@link RedirectProtocolHandler#onSuccess(Response)} aborts a request that
     * still has a body to send when a redirect status is received, raising
     * {@code HttpRequestException: "Aborting request after receiving a NNN response"}. When a broker
     * returns a 307 (to redirect an admin request to the bundle-owner broker) before the proxy has
     * finished streaming the request body, that abort can race ahead of the redirect continuation in
     * {@link RedirectProtocolHandler#onComplete} and surface to the proxy as a spurious HTTP 502 Bad
     * Gateway. The redirect itself is driven by {@code onComplete} from the response (its status and
     * {@code Location} header) and does not depend on the abort, so skipping it lets the redirect
     * always be followed on a fresh request (with the body replayed by
     * {@link ReplayableProxyContentProvider}), which is the behavior this proxy needs.
     */
    static class NonAbortingRedirectProtocolHandler extends RedirectProtocolHandler {
        NonAbortingRedirectProtocolHandler(HttpClient client) {
            super(client);
        }

        @Override
        public void onSuccess(Response response) {
            // Intentionally do NOT abort the request here. The redirect is followed in onComplete();
            // aborting the in-flight request only races a 502 to the proxy when the broker returns a
            // redirect before the request body has finished sending.
        }
    }

    // This class allows the request body to be replayed, the default implementation
    // does not
    protected class ReplayableProxyContentProvider extends ProxyInputStreamRequestContent {
        static final int MIN_REPLAY_BODY_BUFFER_SIZE = 64;
        private boolean eofReached = false;
        private boolean bodyBufferMaxSizeReached = false;
        private final ByteBuffer bodyBuffer;

        protected ReplayableProxyContentProvider(HttpServletRequest request, HttpServletResponse response,
                                                 Request proxyRequest, InputStream input,
                                                 int httpInputMaxReplayBufferSize) {
            super(request, response, proxyRequest, input);
            bodyBuffer = ByteBuffer.allocate(
                    Math.min(Math.max(request.getContentLength(), MIN_REPLAY_BODY_BUFFER_SIZE),
                            httpInputMaxReplayBufferSize));
        }

        @Override
        public Content.Chunk read() {
            Content.Chunk chunk;
            if (!eofReached) {
                chunk = super.read();
                ByteBuffer srcBuffer = chunk.getByteBuffer();
                if (chunk.isLast() && BufferUtil.isTheEmptyBuffer(srcBuffer)) {
                    eofReached = true;
                    bodyBuffer.flip();
                }
                if (srcBuffer != null && !bodyBufferMaxSizeReached) {
                    if (bodyBuffer.remaining() >= srcBuffer.remaining()) {
                        srcBuffer.mark();
                        bodyBuffer.put(srcBuffer);
                        srcBuffer.reset();
                    } else {
                        bodyBufferMaxSizeReached = true;
                        bodyBuffer.clear();
                    }
                }
            } else {
                if (!bodyBufferMaxSizeReached) {
                    chunk = Content.Chunk.from(bodyBuffer.slice(), true);
                } else {
                    chunk = super.read();
                }
            }
            return chunk;
        }

        public boolean rewind() {
            return true;
        }
    }

    private static class JettyHttpClient extends HttpClient {
        private static final int NUMBER_OF_SELECTOR_THREADS = 1;

        public JettyHttpClient() {
            super(new HttpClientTransportOverHTTP(NUMBER_OF_SELECTOR_THREADS));
        }

        public JettyHttpClient(SslContextFactory.Client sslContextFactory) {
            super(new HttpClientTransportOverHTTP(NUMBER_OF_SELECTOR_THREADS));
            setSslContextFactory(sslContextFactory);
        }

        /**
         * Ensure the Authorization header is carried over after a 307 redirect
         * from brokers.
         */
        @Override
        protected Request copyRequest(Request oldRequest, URI newURI) {
            String authorization = oldRequest.getHeaders().get(HttpHeader.AUTHORIZATION);
            Request newRequest = super.copyRequest(oldRequest, newURI);
            if (authorization != null) {
                newRequest.headers(
                        mutable -> mutable.ensureField(new HttpField(HttpHeader.AUTHORIZATION, authorization)));
            }
            return newRequest;
        }

    }

    @Override
    protected Request.Content proxyRequestContent(HttpServletRequest request,
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
                    // PIP-478: the BROKER_CLIENT Jetty client factory — a custom brokerClientTlsFactory
                    // may supply it natively (owning its reload and endpoint identification), otherwise the
                    // framework synthesizes a self-reloading one subscribed to the BROKER_CLIENT SSLContext so
                    // rotated broker-client material reaches new connections. Hostname verification is applied
                    // on the synthesized path only; a native client owns it.
                    JettyTlsFactory.ReloadableClientTls reloadable = JettyTlsFactory.createReloadingClientFactory(
                            this.brokerClientTlsFactory, TlsPurpose.BROKER_CLIENT,
                            config.getBrokerClientSslProvider(), config.isTlsHostnameVerificationEnabled());
                    // Replace any prior subscription (newHttpClient may be invoked more than once).
                    disposeBrokerClientTlsSubscription();
                    this.brokerClientTlsSubscription = reloadable.subscription();
                    return new JettyHttpClient(reloadable.sslContextFactory());
                } catch (Exception e) {
                    log.error().exception(e).log("new jetty http client exception");
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
                if (isBlank(brokerWebServiceUrl)) {
                    log.debug()
                            .attr("remoteAddr", request.getRemoteAddr())
                            .attr("remotePort", request.getRemotePort())
                            .attr("broker", url)
                            .log("Selected active broker");
                }
            } catch (Exception e) {
                log.warn()
                        .attr("remoteAddr", request.getRemoteAddr())
                        .attr("remotePort", request.getRemotePort())
                        .exception(e)
                        .log("Failed to get next active broker");
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
            proxyRequest.headers(mutable -> mutable.ensureField(new HttpField(ORIGINAL_PRINCIPAL_HEADER, user)));
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        // PIP-478: dispose the BROKER_CLIENT SSLContext subscription driving the reloading Jetty
        // client factory, then close the broker-client TLS factory if the new path was used.
        disposeBrokerClientTlsSubscription();
        if (this.brokerClientTlsFactory != null) {
            this.brokerClientTlsFactory.close();
            this.brokerClientTlsFactory = null;
        }
        if (this.sslContextRefresher != null) {
            this.sslContextRefresher.shutdownNow();
        }
    }

    private void disposeBrokerClientTlsSubscription() {
        TlsHandle<?> subscription = this.brokerClientTlsSubscription;
        if (subscription != null) {
            this.brokerClientTlsSubscription = null;
            subscription.dispose();
        }
    }
}
