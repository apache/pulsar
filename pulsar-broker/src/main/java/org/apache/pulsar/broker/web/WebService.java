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
package org.apache.pulsar.broker.web;

import com.google.common.collect.Lists;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.jetty.JettyStatisticsCollector;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.DispatcherType;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.jetty.tls.JettySslContextFactory;
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
    public final int maxConcurrentRequests;

    private final ServerConnector httpConnector;
    private final ServerConnector httpsConnector;
    private JettyStatisticsCollector jettyStatisticsCollector;

    public WebService(PulsarService pulsar) throws PulsarServerException {
        this.handlers = Lists.newArrayList();
        this.pulsar = pulsar;
        this.webServiceExecutor = new WebExecutorThreadPool(
                pulsar.getConfiguration().getNumHttpServerThreads(),
                "pulsar-web");
        this.executorStats = WebExecutorStats.getStats(webServiceExecutor);
        this.server = new Server(webServiceExecutor);
        this.maxConcurrentRequests = pulsar.getConfiguration().getMaxConcurrentHttpRequests();
        List<ServerConnector> connectors = new ArrayList<>();

        Optional<Integer> port = pulsar.getConfiguration().getWebServicePort();
        if (port.isPresent()) {
            httpConnector = new PulsarServerConnector(server, 1, 1);
            httpConnector.setPort(port.get());
            httpConnector.setHost(pulsar.getBindAddress());
            connectors.add(httpConnector);
        } else {
            httpConnector = null;
        }

        Optional<Integer> tlsPort = pulsar.getConfiguration().getWebServicePortTls();
        if (tlsPort.isPresent()) {
            try {
                SslContextFactory sslCtxFactory;
                ServiceConfiguration config = pulsar.getConfiguration();
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
                httpsConnector = new PulsarServerConnector(server, 1, 1, sslCtxFactory);
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
        connectors.forEach(c -> c.setAcceptQueueSize(maxConcurrentRequests / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public void addRestResources(String basePath, String javaPackages, boolean requiresAuthentication,
                                 Map<String, Object> attributeMap) {
        ResourceConfig config = new ResourceConfig();
        config.packages("jersey.config.server.provider.packages", javaPackages);
        config.register(JsonMapperProvider.class);
        config.register(MultiPartFeature.class);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        addServlet(basePath, servletHolder, requiresAuthentication, attributeMap);
    }

    public void addServlet(String path, ServletHolder servletHolder, boolean requiresAuthentication,
                           Map<String, Object> attributeMap) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(path);
        context.addServlet(servletHolder, MATCH_ALL);
        if (attributeMap != null) {
            attributeMap.forEach((key, value) -> {
                context.setAttribute(key, value);
            });
        }

        if (!pulsar.getConfig().getBrokerInterceptors().isEmpty()
                || !pulsar.getConfig().isDisableBrokerInterceptors()) {
            ExceptionHandler handler = new ExceptionHandler();
            // Enable PreInterceptFilter only when interceptors are enabled
            context.addFilter(new FilterHolder(new PreInterceptFilter(pulsar.getBrokerInterceptor(), handler)),
                    MATCH_ALL, EnumSet.allOf(DispatcherType.class));
            context.addFilter(new FilterHolder(new ProcessHandlerFilter(pulsar)),
                    MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

        if (requiresAuthentication && pulsar.getConfiguration().isAuthenticationEnabled()) {
            FilterHolder filter = new FilterHolder(new AuthenticationFilter(
                    pulsar.getBrokerService().getAuthenticationService()));
            context.addFilter(filter, MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

        if (pulsar.getConfig().isDisableHttpDebugMethods()) {
            FilterHolder filter = new FilterHolder(new DisableDebugHttpMethodFilter(pulsar.getConfig()));
            context.addFilter(filter, MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

        if (pulsar.getConfiguration().isHttpRequestsLimitEnabled()) {
            context.addFilter(
                    new FilterHolder(new RateLimitingFilter(pulsar.getConfiguration().getHttpRequestsMaxPerSecond())),
                    MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

        if (pulsar.getConfig().getHttpMaxRequestSize() > 0) {
            context.addFilter(new FilterHolder(
                    new MaxRequestSizeFilter(
                            pulsar.getConfig().getHttpMaxRequestSize())),
                    MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

        FilterHolder responseFilter = new FilterHolder(new ResponseHandlerFilter(pulsar));
        context.addFilter(responseFilter, MATCH_ALL, EnumSet.allOf(DispatcherType.class));
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
