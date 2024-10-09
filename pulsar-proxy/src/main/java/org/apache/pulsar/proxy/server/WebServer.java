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

import static org.apache.pulsar.proxy.server.AdminProxyHandler.INIT_PARAM_REQUEST_BUFFER_SIZE;
import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.jetty.JettyStatisticsCollector;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.broker.web.JettyRequestLogFactory;
import org.apache.pulsar.broker.web.JsonMapperProvider;
import org.apache.pulsar.broker.web.RateLimitingFilter;
import org.apache.pulsar.broker.web.WebExecutorThreadPool;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.apache.pulsar.jetty.tls.JettySslContextFactory;
import org.apache.pulsar.proxy.stats.PulsarProxyOpenTelemetry;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ConnectionLimit;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ProxyConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.QoSFilter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages web-service startup/stop on jetty server.
 *
 */
public class WebServer {
    private static final String MATCH_ALL = "/*";

    private final Server server;
    private final WebExecutorThreadPool webServiceExecutor;
    private final AuthenticationService authenticationService;
    private final List<String> servletPaths = new ArrayList<>();
    private final List<Handler> handlers = new ArrayList<>();
    private final ProxyConfiguration config;
    protected int externalServicePort;
    private URI serviceURI = null;

    private ServerConnector connector;
    private ServerConnector connectorTls;

    private ScheduledExecutorService sslRefreshScheduledExecutor;
    private PulsarSslFactory sslFactory;

    private final FilterInitializer filterInitializer;

    public WebServer(ProxyConfiguration config, AuthenticationService authenticationService) {
        this.webServiceExecutor = new WebExecutorThreadPool(config.getHttpNumThreads(), "pulsar-external-web",
                config.getHttpServerThreadPoolQueueSize());
        this.server = new Server(webServiceExecutor);
        if (config.getMaxHttpServerConnections() > 0) {
            server.addBean(new ConnectionLimit(config.getMaxHttpServerConnections(), server));
        }
        this.authenticationService = authenticationService;
        this.config = config;

        List<ServerConnector> connectors = new ArrayList<>();

        HttpConfiguration httpConfig = new HttpConfiguration();
        if (config.isWebServiceTrustXForwardedFor()) {
            httpConfig.addCustomizer(new ForwardedRequestCustomizer());
        }
        httpConfig.setOutputBufferSize(config.getHttpOutputBufferSize());
        httpConfig.setRequestHeaderSize(config.getHttpMaxRequestHeaderSize());

        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);
        if (config.getWebServicePort().isPresent()) {
            this.externalServicePort = config.getWebServicePort().get();
            List<ConnectionFactory> connectionFactories = new ArrayList<>();
            if (config.isWebServiceHaProxyProtocolEnabled()) {
                connectionFactories.add(new ProxyConnectionFactory());
            }
            connectionFactories.add(httpConnectionFactory);
            connector = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
            connector.setHost(config.getBindAddress());
            connector.setPort(externalServicePort);
            connectors.add(connector);
        }
        if (config.getWebServicePortTls().isPresent()) {
            try {
                this.sslRefreshScheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                        new ExecutorProvider.ExtendedThreadFactory("pulsar-proxy-web-server-tls-refresh"));
                PulsarSslConfiguration sslConfiguration = buildSslConfiguration(config);
                this.sslFactory = (PulsarSslFactory) Class.forName(config.getSslFactoryPlugin())
                        .getConstructor().newInstance();
                this.sslFactory.initialize(sslConfiguration);
                this.sslFactory.createInternalSslContext();
                if (config.getTlsCertRefreshCheckDurationSec() > 0) {
                    sslRefreshScheduledExecutor.scheduleWithFixedDelay(this::refreshSslContext,
                            config.getTlsCertRefreshCheckDurationSec(),
                            config.getTlsCertRefreshCheckDurationSec(), TimeUnit.SECONDS);
                }
                SslContextFactory sslCtxFactory =
                        JettySslContextFactory.createSslContextFactory(config.getTlsProvider(),
                                sslFactory, config.isTlsRequireTrustedClientCertOnConnect(),
                                config.getWebServiceTlsCiphers(), config.getWebServiceTlsProtocols());
                List<ConnectionFactory> connectionFactories = new ArrayList<>();
                if (config.isWebServiceHaProxyProtocolEnabled()) {
                    connectionFactories.add(new ProxyConnectionFactory());
                }
                connectionFactories.add(new SslConnectionFactory(sslCtxFactory, httpConnectionFactory.getProtocol()));
                connectionFactories.add(httpConnectionFactory);
                // org.eclipse.jetty.server.AbstractConnectionFactory.getFactories contains similar logic
                // this is needed for TLS authentication
                if (httpConfig.getCustomizer(SecureRequestCustomizer.class) == null) {
                    httpConfig.addCustomizer(new SecureRequestCustomizer());
                }
                connectorTls = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
                connectorTls.setPort(config.getWebServicePortTls().get());
                connectorTls.setHost(config.getBindAddress());
                connectors.add(connectorTls);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.stream().forEach(c -> c.setAcceptQueueSize(config.getHttpServerAcceptQueueSize()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));

        filterInitializer = new FilterInitializer(config, authenticationService);
    }

    public URI getServiceUri() {
        return serviceURI;
    }

    private static class FilterInitializer {
        private final List<FilterHolder> filterHolders = new ArrayList<>();
        private final FilterHolder authenticationFilterHolder;

        FilterInitializer(ProxyConfiguration config, AuthenticationService authenticationService) {
            if (config.getMaxConcurrentHttpRequests() > 0) {
                FilterHolder filterHolder = new FilterHolder(QoSFilter.class);
                filterHolder.setInitParameter("maxRequests", String.valueOf(config.getMaxConcurrentHttpRequests()));
                filterHolders.add(filterHolder);
            }

            if (config.isHttpRequestsLimitEnabled()) {
                filterHolders.add(new FilterHolder(
                        new RateLimitingFilter(config.getHttpRequestsMaxPerSecond(),
                                OpenTelemetry.noop().getMeter(PulsarProxyOpenTelemetry.INSTRUMENTATION_SCOPE_NAME))));
            }

            if (config.isAuthenticationEnabled()) {
                authenticationFilterHolder = new FilterHolder(new AuthenticationFilter(authenticationService));
                filterHolders.add(authenticationFilterHolder);
            } else {
                authenticationFilterHolder = null;
            }
        }

        public void addFilters(ServletContextHandler context, boolean requiresAuthentication) {
            for (FilterHolder filterHolder : filterHolders) {
                if (requiresAuthentication || filterHolder != authenticationFilterHolder) {
                    context.addFilter(filterHolder,
                            MATCH_ALL, EnumSet.allOf(DispatcherType.class));
                }
            }
        }
    }

    public void addServlet(String basePath, ServletHolder servletHolder) {
        addServlet(basePath, servletHolder, Collections.emptyList());
    }

    public void addServlet(String basePath, ServletHolder servletHolder, List<Pair<String, Object>> attributes) {
        addServlet(basePath, servletHolder, attributes, true);
    }

    public void addServlet(String basePath, ServletHolder servletHolder,
                           List<Pair<String, Object>> attributes, boolean requireAuthentication) {
        addServlet(basePath, servletHolder, attributes, requireAuthentication, true);
    }

    private void addServlet(String basePath, ServletHolder servletHolder,
            List<Pair<String, Object>> attributes, boolean requireAuthentication, boolean checkForExistingPaths) {
        popularServletParams(servletHolder, config);

        if (checkForExistingPaths) {
            Optional<String> existingPath = servletPaths.stream().filter(p -> p.startsWith(basePath)).findFirst();
            if (existingPath.isPresent()) {
                throw new IllegalArgumentException(
                        String.format("Cannot add servlet at %s, path %s already exists", basePath,
                                existingPath.get()));
            }
        }
        servletPaths.add(basePath);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(basePath);
        context.addServlet(servletHolder, MATCH_ALL);
        for (Pair<String, Object> attribute : attributes) {
            context.setAttribute(attribute.getLeft(), attribute.getRight());
        }

        filterInitializer.addFilters(context, requireAuthentication);

        handlers.add(context);
    }

    private static void popularServletParams(ServletHolder servletHolder, ProxyConfiguration config) {
        int requestBufferSize = -1;
        try {
            requestBufferSize = Integer.parseInt(servletHolder.getInitParameter(INIT_PARAM_REQUEST_BUFFER_SIZE));
        } catch (NumberFormatException nfe){
            log.warn("The init-param {} is invalidated, because it is not a number", INIT_PARAM_REQUEST_BUFFER_SIZE);
        }
        if (requestBufferSize > 0 || config.getHttpMaxRequestHeaderSize() > 0) {
            int v = Math.max(requestBufferSize, config.getHttpMaxRequestHeaderSize());
            servletHolder.setInitParameter(INIT_PARAM_REQUEST_BUFFER_SIZE, String.valueOf(v));
        }
    }

    /**
     * Add a REST resource to the servlet context with authentication coverage.
     *
     * @see WebServer#addRestResource(String, String, Object, Class, boolean)
     *
     * @param basePath             The base path for the resource.
     * @param attribute            An attribute associated with the resource.
     * @param attributeValue       The value of the attribute.
     * @param resourceClass        The class representing the resource.
     */
    public void addRestResource(String basePath, String attribute, Object attributeValue, Class<?> resourceClass) {
        addRestResource(basePath, attribute, attributeValue, resourceClass, true);
    }

    /**
     * Add a REST resource to the servlet context.
     *
     * @param basePath             The base path for the resource.
     * @param attribute            An attribute associated with the resource.
     * @param attributeValue       The value of the attribute.
     * @param resourceClass        The class representing the resource.
     * @param requireAuthentication A boolean indicating whether authentication is required for this resource.
     */
    public void addRestResource(String basePath, String attribute, Object attributeValue,
                                Class<?> resourceClass, boolean requireAuthentication) {
        ResourceConfig config = new ResourceConfig();
        config.register(resourceClass);
        config.register(JsonMapperProvider.class);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        // This method has not historically checked for existing paths, so we don't check here either. The
        // method call is added to reduce code duplication.
        addServlet(basePath, servletHolder, Collections.singletonList(Pair.of(attribute, attributeValue)),
                requireAuthentication, false);
    }

    public int getExternalServicePort() {
        return externalServicePort;
    }

    public void start() throws Exception {
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        boolean showDetailedAddresses = config.getWebServiceLogDetailedAddresses() != null
                ? config.getWebServiceLogDetailedAddresses() :
                (config.isWebServiceHaProxyProtocolEnabled() || config.isWebServiceTrustXForwardedFor());
        requestLogHandler.setRequestLog(JettyRequestLogFactory.createRequestLogger(showDetailedAddresses, server));
        handlers.add(0, new ContextHandlerCollection());
        handlers.add(requestLogHandler);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));

        HandlerCollection handlerCollection = new HandlerCollection();
        handlerCollection.setHandlers(new Handler[] { contexts, new DefaultHandler(), requestLogHandler });

        // Metrics handler
        StatisticsHandler stats = new StatisticsHandler();
        stats.setHandler(handlerCollection);
        try {
            new JettyStatisticsCollector(stats).register();
        } catch (IllegalArgumentException e) {
            // Already registered. Eg: in unit tests
        }

        server.setHandler(stats);

        try {
            server.start();

            Arrays.stream(server.getConnectors())
                .filter(c -> c instanceof ServerConnector)
                .findFirst().ifPresent(c -> {
                        WebServer.this.externalServicePort = ((ServerConnector) c).getPort();
                    });

            // server reports URI of first servlet, we want to strip that path off
            URI reportedURI = server.getURI();
            serviceURI = new URI(reportedURI.getScheme(),
                                 null,
                                 reportedURI.getHost(),
                                 reportedURI.getPort(),
                                 null, null, null);
        } catch (Exception e) {
            List<Integer> ports = new ArrayList<>();
            for (Connector c : server.getConnectors()) {
                if (c instanceof ServerConnector) {
                    ports.add(((ServerConnector) c).getPort());
                }
            }
            throw new IOException("Failed to start HTTP server on ports " + ports, e);
        }

        log.info("Server started at end point {}", getServiceUri());
    }

    public void stop() throws Exception {
        if (this.sslRefreshScheduledExecutor != null) {
            this.sslRefreshScheduledExecutor.shutdownNow();
        }
        server.stop();
        webServiceExecutor.stop();
        log.info("Server stopped successfully");
    }

    public boolean isStarted() {
        return server.isStarted();
    }

    public Optional<Integer> getListenPortHTTP() {
        if (connector != null) {
            return Optional.of(connector.getLocalPort());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Integer> getListenPortHTTPS() {
        if (connectorTls != null) {
            return Optional.of(connectorTls.getLocalPort());
        } else {
            return Optional.empty();
        }
    }

    protected PulsarSslConfiguration buildSslConfiguration(ProxyConfiguration config) {
        return PulsarSslConfiguration.builder()
                .tlsProvider(config.getTlsProvider())
                .tlsKeyStoreType(config.getTlsKeyStoreType())
                .tlsKeyStorePath(config.getTlsKeyStore())
                .tlsKeyStorePassword(config.getTlsKeyStorePassword())
                .tlsTrustStoreType(config.getTlsTrustStoreType())
                .tlsTrustStorePath(config.getTlsTrustStore())
                .tlsTrustStorePassword(config.getTlsTrustStorePassword())
                .tlsCiphers(config.getTlsCiphers())
                .tlsProtocols(config.getTlsProtocols())
                .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
                .tlsCertificateFilePath(config.getTlsCertificateFilePath())
                .tlsKeyFilePath(config.getTlsKeyFilePath())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(config.isTlsRequireTrustedClientCertOnConnect())
                .tlsEnabledWithKeystore(config.isTlsEnabledWithKeyStore())
                .tlsCustomParams(config.getSslFactoryPluginParams())
                .authData(null)
                .serverMode(true)
                .isHttps(true)
                .build();
    }

    protected void refreshSslContext() {
        try {
            this.sslFactory.update();
        } catch (Exception e) {
            log.error("Failed to refresh SSL context", e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WebServer.class);
}
