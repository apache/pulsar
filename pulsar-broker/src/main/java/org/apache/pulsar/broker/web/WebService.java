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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.DispatcherType;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.jetty.tls.JettySslContextFactory;
import org.eclipse.jetty.server.ConnectionLimit;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
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
    private final WebExecutorStats executorStats;
    private final WebExecutorThreadPool webServiceExecutor;

    private final ServerConnector httpConnector;
    private final ServerConnector httpsConnector;
    private final FilterInitializer filterInitializer;
    private JettyStatisticsCollector jettyStatisticsCollector;

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
        this.server = new Server(webServiceExecutor);
        if (config.getMaxHttpServerConnections() > 0) {
            server.addBean(new ConnectionLimit(config.getMaxHttpServerConnections(), server));
        }
        List<ServerConnector> connectors = new ArrayList<>();

        Optional<Integer> port = config.getWebServicePort();
        if (port.isPresent()) {
            httpConnector = new ServerConnector(server);
            httpConnector.setPort(port.get());
            httpConnector.setHost(pulsar.getBindAddress());
            connectors.add(httpConnector);
        } else {
            httpConnector = null;
        }

        Optional<Integer> tlsPort = config.getWebServicePortTls();
        if (tlsPort.isPresent()) {
            try {
                SslContextFactory sslCtxFactory;
                if (config.isTlsEnabledWithKeyStore()) {
                    sslCtxFactory = JettySslContextFactory.createServerSslContextWithKeystore(
                            config.getWebServiceTlsProvider(),
                            config.getTlsKeyStoreType(),
                            config.getTlsKeyStore(),
                            config.getTlsKeyStorePassword(),
                            config.isTlsAllowInsecureConnection(),
                            config.getTlsTrustStoreType(),
                            config.getTlsTrustStore(),
                            config.getTlsTrustStorePassword(),
                            config.isTlsRequireTrustedClientCertOnConnect(),
                            config.getWebServiceTlsCiphers(),
                            config.getWebServiceTlsProtocols(),
                            config.getTlsCertRefreshCheckDurationSec()
                    );
                } else {
                    sslCtxFactory = JettySslContextFactory.createServerSslContext(
                            config.getWebServiceTlsProvider(),
                            config.isTlsAllowInsecureConnection(),
                            config.getTlsTrustCertsFilePath(),
                            config.getTlsCertificateFilePath(),
                            config.getTlsKeyFilePath(),
                            config.isTlsRequireTrustedClientCertOnConnect(),
                            config.getWebServiceTlsCiphers(),
                            config.getWebServiceTlsProtocols(),
                            config.getTlsCertRefreshCheckDurationSec());
                }
                httpsConnector = new ServerConnector(server, sslCtxFactory);
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
                        new RateLimitingFilter(config.getHttpRequestsMaxPerSecond())));
            }

            if (!config.getBrokerInterceptors().isEmpty()
                    || !config.isDisableBrokerInterceptors()) {
                ExceptionHandler handler = new ExceptionHandler();
                // Enable PreInterceptFilter only when interceptors are enabled
                filterHolders.add(
                        new FilterHolder(new PreInterceptFilter(pulsarService.getBrokerInterceptor(), handler)));
                filterHolders.add(new FilterHolder(new ProcessHandlerFilter(pulsarService)));
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

            filterHolders.add(new FilterHolder(new ResponseHandlerFilter(pulsarService)));
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

    public void addServlet(String path, ServletHolder servletHolder, boolean requiresAuthentication,
                           Map<String, Object> attributeMap) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        // Notice: each context path should be unique, but there's nothing here to verify that
        context.setContextPath(path);
        context.addServlet(servletHolder, MATCH_ALL);
        if (attributeMap != null) {
            attributeMap.forEach((key, value) -> {
                context.setAttribute(key, value);
            });
        }
        filterInitializer.addFilters(context, requiresAuthentication);
        handlers.add(context);
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
            requestLogHandler.setRequestLog(JettyRequestLogFactory.createRequestLogger());
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
                jettyStatisticsCollector = new JettyStatisticsCollector(stats);
                jettyStatisticsCollector.register();
            } catch (IllegalArgumentException e) {
                // Already registered. Eg: in unit tests
            }
            handlers.add(stats);

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

    private static final Logger log = LoggerFactory.getLogger(WebService.class);
}
