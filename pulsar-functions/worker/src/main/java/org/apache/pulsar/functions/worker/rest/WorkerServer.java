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
import jakarta.servlet.DispatcherType;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.CustomLog;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.tls.TlsFactorySupport;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.broker.web.JettyRequestLogFactory;
import org.apache.pulsar.broker.web.RateLimitingFilter;
import org.apache.pulsar.broker.web.WebExecutorThreadPool;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.functions.worker.PulsarWorkerOpenTelemetry;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerStatsApiV2Resource;
import org.apache.pulsar.jetty.metrics.JettyStatisticsCollector;
import org.apache.pulsar.jetty.tls.JettyTlsFactory;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NetworkConnectionLimit;
import org.eclipse.jetty.server.ProxyConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.QoSHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

@CustomLog
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
    private ScheduledExecutorService scheduledExecutorService;
    // PIP-478 TLS SPI factory (the only server TLS path since the PIP-337 removal).
    private PulsarTlsFactory tlsFactory;
    private JettyTlsFactory.ReloadableServerTls reloadableServerTls;

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
        log.info().attr("uri", server.getURI())
                .log("Worker Server started");
    }

    private void init() {
        server = new Server(webServerExecutor);
        if (workerConfig.getMaxHttpServerConnections() > 0) {
            server.addBean(new NetworkConnectionLimit(workerConfig.getMaxHttpServerConnections(), server));
        }

        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setUriCompliance(UriCompliance.LEGACY);
        if (workerConfig.isWebServiceTrustXForwardedFor()) {
            httpConfig.addCustomizer(new ForwardedRequestCustomizer());
        }
        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);

        List<ServerConnector> connectors = new ArrayList<>();
        if (this.workerConfig.getWorkerPort() != null) {
            log.info().attr("port", this.workerConfig.getWorkerPort())
                    .log("Configuring http server");
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

        boolean showDetailedAddresses = workerConfig.getWebServiceLogDetailedAddresses() != null
                ? workerConfig.getWebServiceLogDetailedAddresses() :
                (workerConfig.isWebServiceHaProxyProtocolEnabled() || workerConfig.isWebServiceTrustXForwardedFor());
        server.setRequestLog(JettyRequestLogFactory.createRequestLogger(showDetailedAddresses, server));


        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(handlers);
        Handler.Collection handlerCollection = new Handler.Sequence();
        handlerCollection.setHandlers(contexts, new DefaultHandler());

        // Metrics handler
        StatisticsHandler stats = new StatisticsHandler();
        stats.setHandler(handlerCollection);
        try {
            new JettyStatisticsCollector(stats).register();
        } catch (IllegalArgumentException e) {
            // Already registered. Eg: in unit tests
        }

        Handler serverHandler = stats;
        if (workerConfig.getMaxConcurrentHttpRequests() > 0) {
            QoSHandler qoSHandler = new QoSHandler(serverHandler);
            qoSHandler.setMaxRequestCount(workerConfig.getMaxConcurrentHttpRequests());
            serverHandler = qoSHandler;
        }
        server.setHandler(serverHandler);

        if (this.workerConfig.getTlsEnabled()) {
            log.info().attr("port", this.workerConfig.getWorkerPortTls())
                    .log("Configuring https server");
            try {
                this.scheduledExecutorService = Executors
                        .newSingleThreadScheduledExecutor(new ExecutorProvider
                                .ExtendedThreadFactory("functions-worker-web-ssl-refresh"));
                // PIP-478: the functions worker web listener uses the PulsarTlsFactory SPI (the built-in
                // file-based factory by default, or a custom tlsFactoryClassName).
                SslContextFactory.Server sslCtxFactory = createTlsFactoryWebServer(workerConfig);
                List<ConnectionFactory> connectionFactories = new ArrayList<>();
                if (workerConfig.isWebServiceHaProxyProtocolEnabled()) {
                    connectionFactories.add(new ProxyConnectionFactory());
                }
                connectionFactories.add(new SslConnectionFactory(sslCtxFactory, httpConnectionFactory.getProtocol()));
                connectionFactories.add(httpConnectionFactory);
                // org.eclipse.jetty.server.AbstractConnectionFactory.getFactories contains similar logic
                // this is needed for TLS authentication
                if (httpConfig.getCustomizer(SecureRequestCustomizer.class) == null) {
                    // disable SNI host check for backwards compatibility with Jetty 9.x
                    boolean sniHostCheck = false;
                    httpConfig.addCustomizer(new SecureRequestCustomizer(sniHostCheck));
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
        // Allow %2F-encoded path separators; Jetty 12 ee10 rejects ambiguous URIs at the servlet layer by
        // default (PIP-472 / Jetty 12).
        contextHandler.getServletHandler().setDecodeAmbiguousURIs(true);

        filterInitializer.addFilters(contextHandler, requireAuthentication);

        return contextHandler;
    }

    public void stop() {
        if (this.server != null) {
            try {
                this.server.stop();
                this.server.destroy();
            } catch (Exception e) {
                log.error().exception(e).log("Failed to stop function web-server");
            }
        }
        if (this.webServerExecutor != null && this.webServerExecutor.isRunning()) {
            try {
                this.webServerExecutor.stop();
            } catch (Exception e) {
                log.warn().exception(e)
                        .log("Error stopping function web-server executor");
            }
        }
        if (this.scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdownNow();
        }
        // PIP-478: dispose the TLS factory subscription and close the factory, if the new path was used.
        if (this.reloadableServerTls != null) {
            this.reloadableServerTls.subscription().dispose();
            this.reloadableServerTls = null;
        }
        if (this.tlsFactory != null) {
            this.tlsFactory.close();
            this.tlsFactory = null;
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

    // PIP-478: the OpenTelemetry root for the WEB-purpose TlsFactoryInitContext, so pulsar.tls.reload emits
    // for the worker web listener; OpenTelemetry.noop() when no PulsarWorkerService OTel handle is available.
    private OpenTelemetry workerOpenTelemetry() {
        if (workerService instanceof PulsarWorkerService pulsarWorkerService
                && pulsarWorkerService.getOpenTelemetry() != null) {
            return pulsarWorkerService.getOpenTelemetry().getOpenTelemetry();
        }
        return OpenTelemetry.noop();
    }

    // PIP-478: build the PulsarTlsFactory for the WEB purpose and drive a vanilla Jetty
    // SslContextFactory.Server via the SSLContext subscription (no cert refresh task).
    private SslContextFactory.Server createTlsFactoryWebServer(WorkerConfig config) throws Exception {
        this.tlsFactory = TlsFactorySupport.createFactory(config.getTlsFactoryClassName(), null,
                () -> buildDefaultWebTlsFactory(config));
        // Once the factory is created it owns live resources (cert watchers, reload work). A failure in any
        // subsequent step rethrows out of init()/the constructor without returning a WorkerServer, so stop()
        // is never reachable — dispose/close the partial state here to avoid leaking it.
        try {
            TlsFactoryInitContext initContext = TlsFactorySupport.initContext(
                    TlsFactorySupport.parseFactoryConfig(config.getTlsFactoryConfig()),
                    scheduledExecutorService, scheduledExecutorService, workerOpenTelemetry());
            TlsFactorySupport.initializeBlocking(this.tlsFactory, initContext);
            this.reloadableServerTls = JettyTlsFactory.createReloadingServerFactory(this.tlsFactory, TlsPurpose.WEB,
                    config.getTlsProvider(), config.isTlsRequireTrustedClientCertOnConnect(),
                    config.isTlsAllowInsecureConnection(), config.getWebServiceTlsCiphers(),
                    config.getWebServiceTlsProtocols());
            return this.reloadableServerTls.sslContextFactory();
        } catch (Exception e) {
            if (this.reloadableServerTls != null) {
                this.reloadableServerTls.subscription().dispose();
                this.reloadableServerTls = null;
            }
            if (this.tlsFactory != null) {
                this.tlsFactory.close();
                this.tlsFactory = null;
            }
            throw e;
        }
    }

    private static PulsarTlsFactory buildDefaultWebTlsFactory(WorkerConfig config) {
        TlsPolicy.Builder policyBuilder = TlsPolicy.builder()
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .protocols(toList(config.getWebServiceTlsProtocols()))
                .ciphers(toList(config.getWebServiceTlsCiphers()))
                // PIP-478: pin the JSSE (SSLContext) provider for the functions-worker web listener (Goal #5). A
                // non-engine tlsProvider value (e.g. Conscrypt) is also routed here for v4 keystore parity,
                // mirroring the broker's two-axis split.
                .jsseProvider(TlsFactorySupport.resolveJsseProvider(config.getJsseProvider(), config.getTlsProvider()));
        if (config.isTlsEnabledWithKeyStore()) {
            policyBuilder.format(TlsPolicy.Format.KEYSTORE)
                    .keyStoreType(config.getTlsKeyStoreType())
                    .trustStoreType(config.getTlsTrustStoreType())
                    .keyStorePath(config.getTlsKeyStore())
                    .keyStorePassword(config.getTlsKeyStorePassword())
                    .trustStorePath(config.getTlsTrustStore())
                    .trustStorePassword(config.getTlsTrustStorePassword());
        } else {
            policyBuilder.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(config.getTlsTrustCertsFilePath())
                    .certificateFilePath(config.getTlsCertificateFilePath())
                    .keyFilePath(config.getTlsKeyFilePath());
        }
        Map<TlsPurpose, TlsPolicy> policies = Map.of(TlsPurpose.WEB, policyBuilder.build());
        long refresh = config.getTlsCertRefreshCheckDurationSec();
        // The Jetty web path uses a JDK SSLContext, so the Netty engine selection is irrelevant here.
        FileBasedTlsFactorySettings settings = FileBasedTlsFactorySettings.builder()
                .requireTrustedClientCert(config.isTlsRequireTrustedClientCertOnConnect())
                .refreshIntervalSeconds(refresh <= 0 ? FileBasedTlsFactorySettings.DEFAULT_REFRESH_INTERVAL_SECONDS
                        : (int) Math.min(refresh, Integer.MAX_VALUE))
                .build();
        return new FileBasedTlsFactory(policies, settings);
    }

    private static List<String> toList(Set<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
