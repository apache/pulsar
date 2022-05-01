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
package org.apache.pulsar.functions.worker.rest;

import io.prometheus.client.jetty.JettyStatisticsCollector;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.broker.web.RateLimitingFilter;
import org.apache.pulsar.broker.web.JettyRequestLogFactory;
import org.apache.pulsar.broker.web.WebExecutorThreadPool;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerStatsApiV2Resource;
import org.apache.pulsar.jetty.tls.JettySslContextFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import javax.servlet.DispatcherType;

@Slf4j
public class WorkerServer {

    private final WorkerConfig workerConfig;
    private final WorkerService workerService;
    private final AuthenticationService authenticationService;
    private static final String MATCH_ALL = "/*";
    private static final int MAX_CONCURRENT_REQUESTS = 1024;
    private final WebExecutorThreadPool webServerExecutor;
    private Server server;

    private ServerConnector httpConnector;
    private ServerConnector httpsConnector;

    public WorkerServer(WorkerService workerService, AuthenticationService authenticationService) {
        this.workerConfig = workerService.getWorkerConfig();
        this.workerService = workerService;
        this.authenticationService = authenticationService;
        this.webServerExecutor = new WebExecutorThreadPool(this.workerConfig.getNumHttpServerThreads(), "function-web");
        init();
    }

    public void start() throws Exception {
        server.start();
        log.info("Worker Server started at {}", server.getURI());
    }

    private void init() {
        server = new Server(webServerExecutor);

        List<ServerConnector> connectors = new ArrayList<>();
        httpConnector = new ServerConnector(server, 1, 1);
        httpConnector.setPort(this.workerConfig.getWorkerPort());
        connectors.add(httpConnector);

        List<Handler> handlers = new ArrayList<>(4);
        handlers.add(newServletContextHandler("/admin",
            new ResourceConfig(Resources.getApiV2Resources()), workerService, authenticationService));
        handlers.add(newServletContextHandler("/admin/v2",
            new ResourceConfig(Resources.getApiV2Resources()), workerService, authenticationService));
        handlers.add(newServletContextHandler("/admin/v3",
            new ResourceConfig(Resources.getApiV3Resources()), workerService, authenticationService));
        // don't require auth for metrics or config routes
        handlers.add(newServletContextHandler("/",
            new ResourceConfig(Resources.getRootResources()), workerService,
            workerConfig.isAuthenticateMetricsEndpoint(), authenticationService));

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
            new JettyStatisticsCollector(stats).register();
        } catch (IllegalArgumentException e) {
            // Already registered. Eg: in unit tests
        }
        handlers.add(stats);
        server.setHandler(stats);

        if (this.workerConfig.getTlsEnabled()) {
            try {
                SslContextFactory sslCtxFactory;
                if (workerConfig.isTlsEnabledWithKeyStore()) {
                    sslCtxFactory = JettySslContextFactory.createServerSslContextWithKeystore(
                            workerConfig.getTlsProvider(),
                            workerConfig.getTlsKeyStoreType(),
                            workerConfig.getTlsKeyStore(),
                            workerConfig.getTlsKeyStorePassword(),
                            workerConfig.isTlsAllowInsecureConnection(),
                            workerConfig.getTlsTrustStoreType(),
                            workerConfig.getTlsTrustStore(),
                            workerConfig.getTlsTrustStorePassword(),
                            workerConfig.isTlsRequireTrustedClientCertOnConnect(),
                            workerConfig.getWebServiceTlsCiphers(),
                            workerConfig.getWebServiceTlsProtocols(),
                            workerConfig.getTlsCertRefreshCheckDurationSec()
                    );
                } else {
                    sslCtxFactory = JettySslContextFactory.createServerSslContext(
                            workerConfig.getTlsProvider(),
                            workerConfig.isTlsAllowInsecureConnection(),
                            workerConfig.getTlsTrustCertsFilePath(),
                            workerConfig.getTlsCertificateFilePath(),
                            workerConfig.getTlsKeyFilePath(),
                            workerConfig.isTlsRequireTrustedClientCertOnConnect(),
                            workerConfig.getWebServiceTlsCiphers(),
                            workerConfig.getWebServiceTlsProtocols(),
                            workerConfig.getTlsCertRefreshCheckDurationSec()
                    );
                }
                httpsConnector = new ServerConnector(server, sslCtxFactory);
                httpsConnector.setPort(this.workerConfig.getWorkerPortTls());
                connectors.add(httpsConnector);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.forEach(c -> c.setAcceptQueueSize(MAX_CONCURRENT_REQUESTS / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public static ServletContextHandler newServletContextHandler(String contextPath,
                                                                 ResourceConfig config,
                                                                 WorkerService workerService,
                                                                 AuthenticationService authenticationService) {
        return newServletContextHandler(contextPath, config, workerService, true, authenticationService);
    }

    public static ServletContextHandler newServletContextHandler(String contextPath,
                                                                 ResourceConfig config,
                                                                 WorkerService workerService,
                                                                 boolean requireAuthentication,
                                                                 AuthenticationService authenticationService) {
        final ServletContextHandler contextHandler =
                new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

        contextHandler.setAttribute(FunctionApiResource.ATTRIBUTE_FUNCTION_WORKER, workerService);
        contextHandler.setAttribute(WorkerApiV2Resource.ATTRIBUTE_WORKER_SERVICE, workerService);
        contextHandler.setAttribute(WorkerStatsApiV2Resource.ATTRIBUTE_WORKERSTATS_SERVICE, workerService);
        contextHandler.setContextPath(contextPath);

        final ServletHolder apiServlet =
                new ServletHolder(new ServletContainer(config));
        contextHandler.addServlet(apiServlet, "/*");
        if (workerService.getWorkerConfig().isAuthenticationEnabled() && requireAuthentication) {
            FilterHolder filter = new FilterHolder(new AuthenticationFilter(authenticationService));
            contextHandler.addFilter(filter, MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

        if (workerService.getWorkerConfig().isHttpRequestsLimitEnabled()) {
            contextHandler.addFilter(
                    new FilterHolder(
                            new RateLimitingFilter(workerService.getWorkerConfig().getHttpRequestsMaxPerSecond())),
                    MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

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
}
