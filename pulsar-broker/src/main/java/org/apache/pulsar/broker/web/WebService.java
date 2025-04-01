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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.jetty.JettyStatisticsCollector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.intercept.BrokerInterceptors;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.apache.pulsar.jetty.tls.JettySslContextFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ConnectionLimit;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ProxyConnectionFactory;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.QoSFilter;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web Service embedded into Pulsar.
 */
public class WebService implements AutoCloseable {

    private static final String MATCH_ALL = "/*";

    public static final String ATTRIBUTE_PULSAR_NAME = "pulsar";
    public static final String HANDLER_CACHE_CONTROL = "max-age=3600";

    private final PulsarService pulsar;
    private final Server server;
    private final List<Handler> handlers;
    @Deprecated
    private final WebExecutorStats executorStats;
    private final WebExecutorThreadPoolStats webExecutorThreadPoolStats;
    private final WebExecutorThreadPool webServiceExecutor;

    private final ServerConnector httpConnector;
    private final ServerConnector httpsConnector;
    private final FilterInitializer filterInitializer;
    private JettyStatisticsCollector jettyStatisticsCollector;
    private PulsarSslFactory sslFactory;
    private ScheduledFuture<?> sslContextRefreshTask;

    @Getter
    private static final DynamicSkipUnknownPropertyHandler sharedUnknownPropertyHandler =
            new DynamicSkipUnknownPropertyHandler();

    public void updateHttpRequestsFailOnUnknownPropertiesEnabled(boolean httpRequestsFailOnUnknownPropertiesEnabled){
        sharedUnknownPropertyHandler
                .setSkipUnknownProperty(!httpRequestsFailOnUnknownPropertiesEnabled);
    }

    public WebService(PulsarService pulsar) throws PulsarServerException {
        this.handlers = new ArrayList<>();
        this.pulsar = pulsar;
        ServiceConfiguration config = pulsar.getConfiguration();
        this.webServiceExecutor = new WebExecutorThreadPool(
                config.getNumHttpServerThreads(),
                "pulsar-web",
                config.getHttpServerThreadPoolQueueSize());
        this.executorStats = WebExecutorStats.getStats(webServiceExecutor);
        this.webExecutorThreadPoolStats =
                new WebExecutorThreadPoolStats(pulsar.getOpenTelemetry().getMeter(), webServiceExecutor);
        this.server = new Server(webServiceExecutor);
        if (config.getMaxHttpServerConnections() > 0) {
            server.addBean(new ConnectionLimit(config.getMaxHttpServerConnections(), server));
        }
        List<ServerConnector> connectors = new ArrayList<>();

        Optional<Integer> port = config.getWebServicePort();
        HttpConfiguration httpConfig = new HttpConfiguration();
        if (config.isWebServiceTrustXForwardedFor()) {
            httpConfig.addCustomizer(new ForwardedRequestCustomizer());
        }
        httpConfig.setRequestHeaderSize(pulsar.getConfig().getHttpMaxRequestHeaderSize());
        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);
        if (port.isPresent()) {
            List<ConnectionFactory> connectionFactories = new ArrayList<>();
            if (config.isWebServiceHaProxyProtocolEnabled()) {
                connectionFactories.add(new ProxyConnectionFactory());
            }
            connectionFactories.add(httpConnectionFactory);
            httpConnector = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
            httpConnector.setPort(port.get());
            httpConnector.setHost(pulsar.getBindAddress());
            connectors.add(httpConnector);
        } else {
            httpConnector = null;
        }

        Optional<Integer> tlsPort = config.getWebServicePortTls();
        if (tlsPort.isPresent()) {
            try {
                PulsarSslConfiguration sslConfiguration = buildSslConfiguration(config);
                this.sslFactory = (PulsarSslFactory) Class.forName(config.getSslFactoryPlugin())
                        .getConstructor().newInstance();
                this.sslFactory.initialize(sslConfiguration);
                this.sslFactory.createInternalSslContext();
                if (config.getTlsCertRefreshCheckDurationSec() > 0) {
                    this.sslContextRefreshTask = this.pulsar.getExecutor()
                            .scheduleWithFixedDelay(this::refreshSslContext,
                                    config.getTlsCertRefreshCheckDurationSec(),
                                    config.getTlsCertRefreshCheckDurationSec(),
                                    TimeUnit.SECONDS);
                }
                SslContextFactory sslCtxFactory =
                        JettySslContextFactory.createSslContextFactory(config.getWebServiceTlsProvider(),
                                this.sslFactory, config.isTlsRequireTrustedClientCertOnConnect(),
                                config.getTlsCiphers(), config.getTlsProtocols());
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
                httpsConnector = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
                httpsConnector.setPort(tlsPort.get());
                httpsConnector.setHost(pulsar.getBindAddress());
                connectors.add(httpsConnector);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        } else {
            httpsConnector = null;
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.forEach(c -> c.setAcceptQueueSize(config.getHttpServerAcceptQueueSize()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));

        filterInitializer = new FilterInitializer(pulsar);
        // Whether to reject requests with unknown attributes.
        sharedUnknownPropertyHandler.setSkipUnknownProperty(!config.isHttpRequestsFailOnUnknownPropertiesEnabled());
    }

    public void addRestResources(String basePath, boolean requiresAuthentication, Map<String, Object> attributeMap,
                                 boolean useSharedJsonMapperProvider, String... javaPackages) {
        ResourceConfig config = new ResourceConfig();
        for (String javaPackage : javaPackages) {
            config.packages(false, javaPackage);
        }
        addResourceServlet(basePath, requiresAuthentication, attributeMap, config, useSharedJsonMapperProvider);
    }

    public void addRestResource(String basePath, boolean requiresAuthentication, Map<String, Object> attributeMap,
                                boolean useSharedJsonMapperProvider, Class<?>... resourceClasses) {
        ResourceConfig config = new ResourceConfig();
        for (Class<?> resourceClass : resourceClasses) {
            config.register(resourceClass);
        }
        addResourceServlet(basePath, requiresAuthentication, attributeMap, config, useSharedJsonMapperProvider);
    }

    private void addResourceServlet(String basePath, boolean requiresAuthentication, Map<String, Object> attributeMap,
                                    ResourceConfig config, boolean useSharedJsonMapperProvider) {
        if (useSharedJsonMapperProvider){
            JsonMapperProvider jsonMapperProvider = new JsonMapperProvider(sharedUnknownPropertyHandler);
            config.register(jsonMapperProvider);
            config.register(UnrecognizedPropertyExceptionMapper.class);
        } else {
            config.register(JsonMapperProvider.class);
        }
        config.register(MultiPartFeature.class);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        addServlet(basePath, servletHolder, requiresAuthentication, attributeMap);
    }

    private static class FilterInitializer {
        private final List<FilterHolder> filterHolders = new ArrayList<>();
        private final FilterHolder authenticationFilterHolder;
        FilterInitializer(PulsarService pulsarService) {
            ServiceConfiguration config = pulsarService.getConfiguration();

            if (config.getMaxConcurrentHttpRequests() > 0) {
                FilterHolder filterHolder = new FilterHolder(QoSFilter.class);
                filterHolder.setInitParameter("maxRequests", String.valueOf(config.getMaxConcurrentHttpRequests()));
                filterHolders.add(filterHolder);
            }

            if (config.isHttpRequestsLimitEnabled()) {
                filterHolders.add(new FilterHolder(
                        new RateLimitingFilter(config.getHttpRequestsMaxPerSecond(),
                                pulsarService.getOpenTelemetry().getMeter())));
            }

            // wait until the PulsarService is ready to serve incoming requests
            filterHolders.add(
                    new FilterHolder(new WaitUntilPulsarServiceIsReadyForIncomingRequestsFilter(pulsarService)));

            boolean brokerInterceptorEnabled = pulsarService.getBrokerInterceptor() != null;
            if (brokerInterceptorEnabled) {
                ExceptionHandler handler = new ExceptionHandler();
                // Enable PreInterceptFilter only when interceptors are enabled
                filterHolders.add(
                        new FilterHolder(new PreInterceptFilter(pulsarService.getBrokerInterceptor(), handler)));
                // The `ProcessHandlerFilter` is used to overwrite `doFilter` method, which cannot be called multiple
                // times inside one `Filter`, so we cannot use one `ProcessHandlerFilter` with a `BrokerInterceptors` to
                // hold all interceptors, instead we need to create a `ProcessHandlerFilter` for each `interceptor`.
                if (pulsarService.getBrokerInterceptor() instanceof BrokerInterceptors) {
                    for (BrokerInterceptor interceptor: ((BrokerInterceptors) pulsarService.getBrokerInterceptor())
                            .getInterceptors().values()) {
                        filterHolders.add(new FilterHolder(new ProcessHandlerFilter(interceptor)));
                    }
                } else {
                    filterHolders.add(new FilterHolder(new ProcessHandlerFilter(pulsarService.getBrokerInterceptor())));
                }
            }

            if (config.isAuthenticationEnabled()) {
                authenticationFilterHolder = new FilterHolder(new AuthenticationFilter(
                        pulsarService.getBrokerService().getAuthenticationService()));
                filterHolders.add(authenticationFilterHolder);
            } else {
                authenticationFilterHolder = null;
            }

            if (config.isDisableHttpDebugMethods()) {
                filterHolders.add(new FilterHolder(new DisableDebugHttpMethodFilter(config)));
            }

            if (config.getHttpMaxRequestSize() > 0) {
                filterHolders.add(new FilterHolder(
                        new MaxRequestSizeFilter(
                                config.getHttpMaxRequestSize())));
            }

            if (brokerInterceptorEnabled) {
                filterHolders.add(new FilterHolder(new ResponseHandlerFilter(pulsarService)));
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

        // Filter that waits until the PulsarService is ready to serve incoming requests
        private static class WaitUntilPulsarServiceIsReadyForIncomingRequestsFilter implements Filter {
            private final PulsarService pulsarService;

            public WaitUntilPulsarServiceIsReadyForIncomingRequestsFilter(PulsarService pulsarService) {
                this.pulsarService = pulsarService;
            }

            @Override
            public void init(FilterConfig filterConfig) throws ServletException {

            }

            @Override
            public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                    throws IOException, ServletException {
                try {
                    // Wait until the PulsarService is ready to serve incoming requests
                    pulsarService.waitUntilReadyForIncomingRequests();
                } catch (ExecutionException e) {
                    ((HttpServletResponse) response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                            "PulsarService failed to start.");
                    return;
                } catch (InterruptedException e) {
                    ((HttpServletResponse) response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                            "PulsarService is not ready.");
                    return;
                }
                chain.doFilter(request, response);
            }

            @Override
            public void destroy() {

            }
        }
    }

    public void addServlet(String path, ServletHolder servletHolder, boolean requiresAuthentication,
                           Map<String, Object> attributeMap) {
        ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        // Notice: each context path should be unique, but there's nothing here to verify that
        servletContextHandler.setContextPath(path);
        servletContextHandler.addServlet(servletHolder, MATCH_ALL);
        if (attributeMap != null) {
            attributeMap.forEach(servletContextHandler::setAttribute);
        }
        filterInitializer.addFilters(servletContextHandler, requiresAuthentication);

        handlers.add(servletContextHandler);
    }

    public void addStaticResources(String basePath, String resourcePath) {
        ContextHandler capHandler = new ContextHandler();
        capHandler.setContextPath(basePath);
        ResourceHandler resHandler = new ResourceHandler();
        resHandler.setBaseResource(Resource.newClassPathResource(resourcePath));
        resHandler.setEtags(true);
        resHandler.setCacheControl(WebService.HANDLER_CACHE_CONTROL);
        capHandler.setHandler(resHandler);
        handlers.add(capHandler);
    }

    public void start() throws PulsarServerException {
        try {
            RequestLogHandler requestLogHandler = new RequestLogHandler();
            boolean showDetailedAddresses = pulsar.getConfiguration().getWebServiceLogDetailedAddresses() != null
                    ? pulsar.getConfiguration().getWebServiceLogDetailedAddresses() :
                    (pulsar.getConfiguration().isWebServiceHaProxyProtocolEnabled()
                            || pulsar.getConfiguration().isWebServiceTrustXForwardedFor());
            RequestLog requestLogger = JettyRequestLogFactory.createRequestLogger(showDetailedAddresses, server);
            requestLogHandler.setRequestLog(requestLogger);
            handlers.add(0, new ContextHandlerCollection());
            handlers.add(requestLogHandler);

            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));

            Handler handlerForContexts = GzipHandlerUtil.wrapWithGzipHandler(contexts,
                    pulsar.getConfig().getHttpServerGzipCompressionExcludedPaths());
            HandlerCollection handlerCollection = new HandlerCollection();
            handlerCollection.setHandlers(new Handler[] {handlerForContexts, new DefaultHandler(), requestLogHandler});

            // Metrics handler
            StatisticsHandler stats = new StatisticsHandler();
            stats.setHandler(handlerCollection);
            try {
                jettyStatisticsCollector = new JettyStatisticsCollector(stats);
                jettyStatisticsCollector.register();
            } catch (IllegalArgumentException e) {
                // Already registered. Eg: in unit tests
            }

            server.setHandler(stats);
            server.start();

            if (httpConnector != null) {
                log.info("HTTP Service started at http://{}:{}", httpConnector.getHost(), httpConnector.getLocalPort());
                pulsar.getConfiguration().setWebServicePort(Optional.of(httpConnector.getLocalPort()));
            } else {
                log.info("HTTP Service disabled");
            }

            if (httpsConnector != null) {
                log.info("HTTPS Service started at https://{}:{}", httpsConnector.getHost(),
                        httpsConnector.getLocalPort());
                pulsar.getConfiguration().setWebServicePortTls(Optional.of(httpsConnector.getLocalPort()));
            } else {
                log.info("HTTPS Service disabled");
            }
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    @Override
    public void close() throws PulsarServerException {
        try {
            server.stop();
            // unregister statistics from Prometheus client's default CollectorRegistry singleton
            // to prevent memory leaks in tests
            if (jettyStatisticsCollector != null) {
                try {
                    CollectorRegistry.defaultRegistry.unregister(jettyStatisticsCollector);
                } catch (Exception e) {
                    // ignore any exception happening in unregister
                    // exception will be thrown for 2. instance of WebService in tests since
                    // the register supports a single JettyStatisticsCollector
                }
                jettyStatisticsCollector = null;
            }
            webServiceExecutor.join();
            if (this.sslContextRefreshTask != null) {
                this.sslContextRefreshTask.cancel(true);
            }
            webExecutorThreadPoolStats.close();
            this.executorStats.close();
            log.info("Web service closed");
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    public Optional<Integer> getListenPortHTTP() {
        if (httpConnector != null) {
            return Optional.of(httpConnector.getLocalPort());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Integer> getListenPortHTTPS() {
        if (httpsConnector != null) {
            return Optional.of(httpsConnector.getLocalPort());
        } else {
            return Optional.empty();
        }
    }

    protected PulsarSslConfiguration buildSslConfiguration(ServiceConfiguration serviceConfig) {
        return PulsarSslConfiguration.builder()
                .tlsKeyStoreType(serviceConfig.getTlsKeyStoreType())
                .tlsKeyStorePath(serviceConfig.getTlsKeyStore())
                .tlsKeyStorePassword(serviceConfig.getTlsKeyStorePassword())
                .tlsTrustStoreType(serviceConfig.getTlsTrustStoreType())
                .tlsTrustStorePath(serviceConfig.getTlsTrustStore())
                .tlsTrustStorePassword(serviceConfig.getTlsTrustStorePassword())
                .tlsCiphers(serviceConfig.getTlsCiphers())
                .tlsProtocols(serviceConfig.getTlsProtocols())
                .tlsTrustCertsFilePath(serviceConfig.getTlsTrustCertsFilePath())
                .tlsCertificateFilePath(serviceConfig.getTlsCertificateFilePath())
                .tlsKeyFilePath(serviceConfig.getTlsKeyFilePath())
                .allowInsecureConnection(serviceConfig.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(serviceConfig.isTlsRequireTrustedClientCertOnConnect())
                .tlsEnabledWithKeystore(serviceConfig.isTlsEnabledWithKeyStore())
                .tlsCustomParams(serviceConfig.getSslFactoryPluginParams())
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

    private static final Logger log = LoggerFactory.getLogger(WebService.class);
}
