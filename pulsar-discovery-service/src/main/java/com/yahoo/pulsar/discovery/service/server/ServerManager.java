/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.discovery.service.server;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import javax.net.ssl.SSLContext;

import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.common.util.SecurityUtility;
import com.yahoo.pulsar.discovery.service.DiscoveryService;
import com.yahoo.pulsar.discovery.service.RestException;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Manages web-service startup/stop on jetty server.
 *
 */
public class ServerManager {
    private final Server server;
    private final ExecutorService webServiceExecutor;
    private final List<Handler> handlers = Lists.newArrayList();
    protected final int externalServicePort;

    public ServerManager(ServiceConfig config) {
        this.webServiceExecutor = Executors.newFixedThreadPool(32, new DefaultThreadFactory("pulsar-external-web"));
        this.server = new Server(new ExecutorThreadPool(webServiceExecutor));
        this.externalServicePort = config.getWebServicePort();

        List<ServerConnector> connectors = Lists.newArrayList();

        ServerConnector connector = new ServerConnector(server, 1, 1);
        connector.setPort(externalServicePort);
        connectors.add(connector);

        if (config.isTlsEnabled()) {
            SslContextFactory sslCtxFactory = new SslContextFactory();
            try {
                SSLContext sslCtx = SecurityUtility.createSslContext(false, null, config.getTlsCertificateFilePath(),
                        config.getTlsKeyFilePath());
                sslCtxFactory.setSslContext(sslCtx);
            } catch (GeneralSecurityException e) {
                throw new RestException(e);
            }
            sslCtxFactory.setWantClientAuth(true);
            ServerConnector tlsConnector = new ServerConnector(server, 1, 1, sslCtxFactory);
            tlsConnector.setPort(config.getWebServicePortTls());
            connectors.add(tlsConnector);
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.stream().forEach(c -> c.setAcceptQueueSize(1024 / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public URI getServiceUri() {
        return this.server.getURI();
    }

    public void addRestResources(String basePath, String javaPackages) {
        ServletHolder servletHolder = new ServletHolder(ServletContainer.class);
        servletHolder.setInitParameter("jersey.config.server.provider.packages", javaPackages);
        addServlet(basePath, servletHolder);
    }

    public void addServlet(String path, ServletHolder servletHolder) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(path);
        context.addServlet(servletHolder, "/*");
        handlers.add(context);
    }

    public int getExternalServicePort() {
        return externalServicePort;
    }

    protected void start() throws Exception {
        try {
            RequestLogHandler requestLogHandler = new RequestLogHandler();
            Slf4jRequestLog requestLog = new Slf4jRequestLog();
            requestLog.setExtended(true);
            requestLog.setLogTimeZone("GMT");
            requestLog.setLogLatency(true);
            requestLogHandler.setRequestLog(requestLog);
            handlers.add(0, new ContextHandlerCollection());
            handlers.add(requestLogHandler);

            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));

            HandlerCollection handlerCollection = new HandlerCollection();
            handlerCollection.setHandlers(new Handler[] { contexts, new DefaultHandler(), requestLogHandler });
            server.setHandler(handlerCollection);

            server.start();

        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public void stop() throws Exception {
        server.stop();
        webServiceExecutor.shutdown();
        log.info("Server stopped successfully");
    }

    public void start(List<String> resources) throws Exception {
        if (resources != null) {
            resources.forEach(r -> this.addRestResources("/", DiscoveryService.class.getPackage().getName()));
        }
        this.start();
        log.info("Server started at end point {}", getServiceUri().toString());
    }

    private static final Logger log = LoggerFactory.getLogger(ServerManager.class);
}
