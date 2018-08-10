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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import javax.servlet.DispatcherType;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import java.net.BindException;
import java.net.URI;
import java.security.GeneralSecurityException;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.DefaultThreadFactory;

@Slf4j
public class WorkerServer {

    private final WorkerConfig workerConfig;
    private final WorkerService workerService;
    private static final String MATCH_ALL = "/*";
    private static final int NUM_ACCEPTORS = 16;
    private static final int MAX_CONCURRENT_REQUESTS = 1024;
    private final ExecutorService webServerExecutor;
    private Server server;

    private static String getErrorMessage(Server server, int port, Exception ex) {
        if (ex instanceof BindException) {
            final URI uri = server.getURI();
            return String.format("%s http://%s:%d", ex.getMessage(), uri.getHost(), port);
        }

        return ex.getMessage();
    }

    public WorkerServer(WorkerService workerService) {
        this.workerConfig = workerService.getWorkerConfig();
        this.workerService = workerService;
        this.webServerExecutor = Executors.newFixedThreadPool(NUM_ACCEPTORS, new DefaultThreadFactory("function-web"));
        init();
    }

    public void start() throws Exception {
        server.start();
        log.info("Worker Server started at {}", server.getURI());
    }
    
    private void init() {
        server = new Server(new ExecutorThreadPool(webServerExecutor));

        List<ServerConnector> connectors = new ArrayList<>();
        ServerConnector connector = new ServerConnector(server, 1, 1);
        connector.setPort(this.workerConfig.getWorkerPort());
        connectors.add(connector);

        List<Handler> handlers = new ArrayList<>(3);
        handlers.add(
                newServletContextHandler("/admin", new ResourceConfig(Resources.getApiResources()), workerService));
        handlers.add(
                newServletContextHandler("/admin/v2", new ResourceConfig(Resources.getApiResources()), workerService));
        handlers.add(newServletContextHandler("/", new ResourceConfig(Resources.getRootResources()), workerService));

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));
        HandlerCollection handlerCollection = new HandlerCollection();
        handlerCollection.setHandlers(new Handler[] { contexts, new DefaultHandler() });
        server.setHandler(handlerCollection);

        if (this.workerConfig.isTlsEnabled()) {
            try {
                SslContextFactory sslCtxFactory = SecurityUtility.createSslContextFactory(
                        this.workerConfig.isTlsAllowInsecureConnection(), this.workerConfig.getTlsTrustCertsFilePath(),
                        this.workerConfig.getTlsCertificateFilePath(), this.workerConfig.getTlsKeyFilePath(),
                        this.workerConfig.isTlsRequireTrustedClientCertOnConnect());
                ServerConnector tlsConnector = new ServerConnector(server, 1, 1, sslCtxFactory);
                tlsConnector.setPort(this.workerConfig.getWorkerPortTls());
                connectors.add(tlsConnector);
            } catch (GeneralSecurityException e) {
                throw new RuntimeException(e);
            }
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.forEach(c -> c.setAcceptQueueSize(MAX_CONCURRENT_REQUESTS / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public static ServletContextHandler newServletContextHandler(String contextPath, ResourceConfig config, WorkerService workerService) {
        final ServletContextHandler contextHandler =
                new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

        contextHandler.setAttribute(FunctionApiResource.ATTRIBUTE_FUNCTION_WORKER, workerService);
        contextHandler.setContextPath(contextPath);

        final ServletHolder apiServlet =
                new ServletHolder(new ServletContainer(config));
        contextHandler.addServlet(apiServlet, "/*");
        if (workerService.getWorkerConfig().isAuthenticationEnabled()) {
            FilterHolder filter = new FilterHolder(new AuthenticationFilter(workerService.getAuthenticationService()));
            contextHandler.addFilter(filter, MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

        return contextHandler;
    }
    
    @VisibleForTesting
    public void stop() {
        if (this.server != null) {
            try {
                this.server.destroy();
            } catch (Exception e) {
                log.error("Failed to stop function web-server ", e);
            }
        }
        if (this.webServerExecutor != null && !this.webServerExecutor.isShutdown()) {
            this.webServerExecutor.shutdown();
        }
    }
    
}
