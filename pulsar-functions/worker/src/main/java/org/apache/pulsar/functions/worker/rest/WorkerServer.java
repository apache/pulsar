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
package org.apache.pulsar.functions.worker.rest;

import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.jetty.JettyStatisticsCollector;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.broker.web.JettyRequestLogFactory;
import org.apache.pulsar.broker.web.RateLimitingFilter;
import org.apache.pulsar.broker.web.WebExecutorThreadPool;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.apache.pulsar.functions.worker.PulsarWorkerOpenTelemetry;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerStatsApiV2Resource;
import org.apache.pulsar.jetty.tls.JettySslContextFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ConnectionLimit;
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

@Slf4j
public class WorkerServer {

    private final WorkerConfig workerConfig;
    private final WorkerService workerService;
    private final AuthenticationService authenticationService;
    private static final String MATCH_ALL = "/*";
    private final WebExecutorThreadPool webServerExecutor;
    private Server server;

    private ServerConnector httpConnector;
    private ServerConnector httpsConnector;

    private final FilterInitializer filterInitializer;
    private PulsarSslFactory sslFactory;
    private ScheduledExecutorService scheduledExecutorService;

    public WorkerServer(WorkerService workerService, AuthenticationService authenticationService) {
        this.workerConfig = workerService.getWorkerConfig();
        this.workerService = workerService;
        this.authenticationService = authenticationService;
        this.webServerExecutor = new WebExecutorThreadPool(this.workerConfig.getNumHttpServerThreads(), "function-web",
                this.workerConfig.getHttpServerThreadPoolQueueSize());
        this.filterInitializer = new FilterInitializer(workerConfig, authenticationService);
        init();
    }

    public void start() throws Exception {
        server.start();
        log.info("Worker Server started at {}", server.getURI());
    }

    private void init() {
        server = new Server(webServerExecutor);
        if (workerConfig.getMaxHttpServerConnections() > 0) {
            server.addBean(new ConnectionLimit(workerConfig.getMaxHttpServerConnections(), server));
        }

        HttpConfiguration httpConfig = new HttpConfiguration();
        if (workerConfig.isWebServiceTrustXForwardedFor()) {
            httpConfig.addCustomizer(new ForwardedRequestCustomizer());
        }
        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);

        List<ServerConnector> connectors = new ArrayList<>();
        if (this.workerConfig.getWorkerPort() != null) {
            log.info("Configuring http server on port={}", this.workerConfig.getWorkerPort());
            List<ConnectionFactory> connectionFactories = new ArrayList<>();
            if (workerConfig.isWebServiceHaProxyProtocolEnabled()) {
                connectionFactories.add(new ProxyConnectionFactory());
            }
            connectionFactories.add(httpConnectionFactory);
            httpConnector = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
            httpConnector.setPort(this.workerConfig.getWorkerPort());
            connectors.add(httpConnector);
        }

        List<Handler> handlers = new ArrayList<>(4);
        handlers.add(newServletContextHandler("/admin",
            new ResourceConfig(Resources.getApiV2Resources()), workerService, filterInitializer));
        handlers.add(newServletContextHandler("/admin/v2",
            new ResourceConfig(Resources.getApiV2Resources()), workerService, filterInitializer));
        handlers.add(newServletContextHandler("/admin/v3",
            new ResourceConfig(Resources.getApiV3Resources()), workerService, filterInitializer));
        // don't require auth for metrics or config routes
        handlers.add(newServletContextHandler("/",
            new ResourceConfig(Resources.getRootResources()), workerService,
            workerConfig.isAuthenticateMetricsEndpoint(), filterInitializer));

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        boolean showDetailedAddresses = workerConfig.getWebServiceLogDetailedAddresses() != null
                ? workerConfig.getWebServiceLogDetailedAddresses() :
                (workerConfig.isWebServiceHaProxyProtocolEnabled() || workerConfig.isWebServiceTrustXForwardedFor());
        requestLogHandler.setRequestLog(JettyRequestLogFactory.createRequestLogger(showDetailedAddresses, server));
        handlers.add(0, new ContextHandlerCollection());
        handlers.add(requestLogHandler);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));
        HandlerCollection handlerCollection = new HandlerCollection();
        handlerCollection.setHandlers(new Handler[]{contexts, new DefaultHandler(), requestLogHandler});

        // Metrics handler
        StatisticsHandler stats = new StatisticsHandler();
        stats.setHandler(handlerCollection);
        try {
            new JettyStatisticsCollector(stats).register();
        } catch (IllegalArgumentException e) {
            // Already registered. Eg: in unit tests
        }
        handlers.add(stats);
        server.setHandler(stats);

        if (this.workerConfig.getTlsEnabled()) {
            log.info("Configuring https server on port={}", this.workerConfig.getWorkerPortTls());
            try {
                PulsarSslConfiguration sslConfiguration = buildSslConfiguration(workerConfig);
                this.sslFactory = new DefaultPulsarSslFactory();
                this.sslFactory.initialize(sslConfiguration);
                this.sslFactory.createInternalSslContext();
                this.scheduledExecutorService = Executors
                        .newSingleThreadScheduledExecutor(new ExecutorProvider
                                .ExtendedThreadFactory("functions-worker-web-ssl-refresh"));
                this.scheduledExecutorService.scheduleWithFixedDelay(this::refreshSslContext,
                        workerConfig.getTlsCertRefreshCheckDurationSec(),
                        workerConfig.getTlsCertRefreshCheckDurationSec(),
                        TimeUnit.SECONDS);
                SslContextFactory sslCtxFactory =
                        JettySslContextFactory.createSslContextFactory(this.workerConfig.getTlsProvider(),
                                this.sslFactory, this.workerConfig.isTlsRequireTrustedClientCertOnConnect(),
                                this.workerConfig.getWebServiceTlsCiphers(),
                                this.workerConfig.getWebServiceTlsProtocols());
                List<ConnectionFactory> connectionFactories = new ArrayList<>();
                if (workerConfig.isWebServiceHaProxyProtocolEnabled()) {
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
                httpsConnector.setPort(this.workerConfig.getWorkerPortTls());
                connectors.add(httpsConnector);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.forEach(c -> c.setAcceptQueueSize(workerConfig.getHttpServerAcceptQueueSize()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    private static class FilterInitializer {
        private final List<FilterHolder> filterHolders = new ArrayList<>();
        private final FilterHolder authenticationFilterHolder;

        FilterInitializer(WorkerConfig config, AuthenticationService authenticationService) {
            if (config.getMaxConcurrentHttpRequests() > 0) {
                FilterHolder filterHolder = new FilterHolder(QoSFilter.class);
                filterHolder.setInitParameter("maxRequests", String.valueOf(config.getMaxConcurrentHttpRequests()));
                filterHolders.add(filterHolder);
            }

            if (config.isHttpRequestsLimitEnabled()) {
                filterHolders.add(new FilterHolder(
                        new RateLimitingFilter(config.getHttpRequestsMaxPerSecond(),
                                OpenTelemetry.noop().getMeter(PulsarWorkerOpenTelemetry.INSTRUMENTATION_SCOPE_NAME))));
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

    static ServletContextHandler newServletContextHandler(String contextPath,
                                                                 ResourceConfig config,
                                                                 WorkerService workerService,
                                                                 FilterInitializer filterInitializer) {
        return newServletContextHandler(contextPath, config, workerService, true, filterInitializer);
    }

    static ServletContextHandler newServletContextHandler(String contextPath,
                                                                 ResourceConfig config,
                                                                 WorkerService workerService,
                                                                 boolean requireAuthentication,
                                                                 FilterInitializer filterInitializer) {
        final ServletContextHandler contextHandler =
                new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

        contextHandler.setAttribute(FunctionApiResource.ATTRIBUTE_FUNCTION_WORKER, workerService);
        contextHandler.setAttribute(WorkerApiV2Resource.ATTRIBUTE_WORKER_SERVICE, workerService);
        contextHandler.setAttribute(WorkerStatsApiV2Resource.ATTRIBUTE_WORKERSTATS_SERVICE, workerService);
        contextHandler.setContextPath(contextPath);

        final ServletHolder apiServlet =
                new ServletHolder(new ServletContainer(config));
        contextHandler.addServlet(apiServlet, MATCH_ALL);

        filterInitializer.addFilters(contextHandler, requireAuthentication);

        return contextHandler;
    }

    public void stop() {
        if (this.server != null) {
            try {
                this.server.stop();
                this.server.destroy();
            } catch (Exception e) {
                log.error("Failed to stop function web-server ", e);
            }
        }
        if (this.webServerExecutor != null && this.webServerExecutor.isRunning()) {
            try {
                this.webServerExecutor.stop();
            } catch (Exception e) {
                log.warn("Error stopping function web-server executor", e);
            }
        }
        if (this.scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdownNow();
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

    protected void refreshSslContext() {
        try {
            this.sslFactory.update();
        } catch (Exception e) {
            log.error("Failed to refresh SSL context", e);
        }
    }

    protected PulsarSslConfiguration buildSslConfiguration(WorkerConfig config) {
        return PulsarSslConfiguration.builder()
                .tlsKeyStoreType(config.getTlsKeyStoreType())
                .tlsKeyStorePath(config.getTlsKeyStore())
                .tlsKeyStorePassword(config.getTlsKeyStorePassword())
                .tlsTrustStoreType(config.getTlsTrustStoreType())
                .tlsTrustStorePath(config.getTlsTrustStore())
                .tlsTrustStorePassword(config.getTlsTrustStorePassword())
                .tlsCiphers(config.getWebServiceTlsCiphers())
                .tlsProtocols(config.getWebServiceTlsProtocols())
                .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
                .tlsCertificateFilePath(config.getTlsCertificateFilePath())
                .tlsKeyFilePath(config.getTlsKeyFilePath())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(config.isTlsRequireTrustedClientCertOnConnect())
                .tlsEnabledWithKeystore(config.isTlsEnabledWithKeyStore())
                .serverMode(true)
                .isHttps(true)
                .build();
    }
}
