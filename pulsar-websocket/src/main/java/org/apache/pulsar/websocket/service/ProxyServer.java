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
package org.apache.pulsar.websocket.service;

import com.google.common.collect.Lists;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import java.util.stream.Collectors;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.websocket.DeploymentException;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.web.JsonMapperProvider;
import org.apache.pulsar.broker.web.JettyRequestLogFactory;
import org.apache.pulsar.broker.web.WebExecutorThreadPool;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.SecurityUtility;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyServer {
    private final Server server;
    private final List<Handler> handlers = Lists.newArrayList();
    private final WebSocketProxyConfiguration conf;
    private final WebExecutorThreadPool executorService;

    private ServerConnector connector;
    private ServerConnector connectorTls;

    public ProxyServer(WebSocketProxyConfiguration config)
            throws PulsarClientException, MalformedURLException, PulsarServerException {
        this.conf = config;
        executorService = new WebExecutorThreadPool(config.getNumHttpServerThreads(), "pulsar-websocket-web");
        this.server = new Server(executorService);
        List<ServerConnector> connectors = new ArrayList<>();

        if (config.getWebServicePort().isPresent()) {
			connector = new ServerConnector(server);
			connector.setPort(config.getWebServicePort().get());
			connectors.add(connector);
        }
        // TLS enabled connector
        if (config.getWebServicePortTls().isPresent()) {
            try {
                SslContextFactory sslCtxFactory = SecurityUtility.createSslContextFactory(
                        config.isTlsAllowInsecureConnection(),
                        config.getTlsTrustCertsFilePath(),
                        config.getTlsCertificateFilePath(),
                        config.getTlsKeyFilePath(),
                        config.isTlsRequireTrustedClientCertOnConnect(),
                        true,
                        config.getTlsCertRefreshCheckDurationSec());
                connectorTls = new ServerConnector(server, -1, -1, sslCtxFactory);
                connectorTls.setPort(config.getWebServicePortTls().get());
                connectors.add(connectorTls);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }

        // Limit number of concurrent HTTP connections to avoid getting out of
        // file descriptors
        connectors.stream().forEach(c -> c.setAcceptQueueSize(1024 / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public void addWebSocketServlet(String basePath, Servlet socketServlet)
            throws ServletException, DeploymentException {
        ServletHolder servletHolder = new ServletHolder("ws-events", socketServlet);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(basePath);
        context.addServlet(servletHolder, "/*");
        handlers.add(context);
    }

    public void addRestResources(String basePath, String javaPackages, String attribute, Object attributeValue) {
        ResourceConfig config = new ResourceConfig();
        config.packages("jersey.config.server.provider.packages", javaPackages);
        config.register(JsonMapperProvider.class);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(basePath);
        context.addServlet(servletHolder, "/*");
        context.setAttribute(attribute, attributeValue);
        handlers.add(context);
    }

    public void start() throws PulsarServerException {
        log.info("Starting web socket proxy at port {}", Arrays.stream(server.getConnectors())
                .map(ServerConnector.class::cast).map(ServerConnector::getPort).map(Object::toString)
                .collect(Collectors.joining(",")));
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        requestLogHandler.setRequestLog(JettyRequestLogFactory.createRequestLogger());
        handlers.add(0, new ContextHandlerCollection());
        handlers.add(requestLogHandler);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));

        HandlerCollection handlerCollection = new HandlerCollection();
        handlerCollection.setHandlers(new Handler[] { contexts, new DefaultHandler(), requestLogHandler });
        server.setHandler(handlerCollection);

        try {
            server.start();
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    public void stop() throws Exception {
        server.stop();
        executorService.stop();
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

    private static final Logger log = LoggerFactory.getLogger(ProxyServer.class);
}
