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

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.DispatcherType;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.Lists;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Web Service embedded into Pulsar
 */
public class WebService implements AutoCloseable {

    private static final String MATCH_ALL = "/*";

    public static final String ATTRIBUTE_PULSAR_NAME = "pulsar";
    public static final String HANDLER_CACHE_CONTROL = "max-age=3600";
    public static final int NUM_ACCEPTORS = 32; // make it configurable?
    public static final int MAX_CONCURRENT_REQUESTS = 1024; // make it configurable?

    private final PulsarService pulsar;
    private final Server server;
    private final List<Handler> handlers;
    private final ExecutorService webServiceExecutor;

    public WebService(PulsarService pulsar) throws PulsarServerException {
        this.handlers = Lists.newArrayList();
        this.pulsar = pulsar;
        this.webServiceExecutor = Executors.newFixedThreadPool(WebService.NUM_ACCEPTORS, new DefaultThreadFactory("pulsar-web"));
        this.server = new Server(new ExecutorThreadPool(webServiceExecutor));
        List<ServerConnector> connectors = new ArrayList<>();

        ServerConnector connector = new PulsarServerConnector(server, 1, 1);
        connector.setPort(pulsar.getConfiguration().getWebServicePort());
        connector.setHost(pulsar.getBindAddress());
        connectors.add(connector);

        if (pulsar.getConfiguration().isTlsEnabled()) {
            SslContextFactory sslCtxFactory = new SslContextFactory();

            try {
                sslCtxFactory.setSslContext(
                        SecurityUtility.createSslContext(
                            pulsar.getConfiguration().isTlsAllowInsecureConnection(),
                            pulsar.getConfiguration().getTlsTrustCertsFilePath(),
                            pulsar.getConfiguration().getTlsCertificateFilePath(),
                            pulsar.getConfiguration().getTlsKeyFilePath()));
            } catch (GeneralSecurityException e) {
                throw new PulsarServerException(e);
            }

            sslCtxFactory.setWantClientAuth(true);
            ServerConnector tlsConnector = new PulsarServerConnector(server, 1, 1, sslCtxFactory);
            tlsConnector.setPort(pulsar.getConfiguration().getWebServicePortTls());
            tlsConnector.setHost(pulsar.getBindAddress());
            connectors.add(tlsConnector);
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.forEach(c -> c.setAcceptQueueSize(WebService.MAX_CONCURRENT_REQUESTS / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public void addRestResources(String basePath, String javaPackages, boolean requiresAuthentication, Map<String,Object> attributeMap) {
        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(ObjectMapperFactory.create());
        ResourceConfig config = new ResourceConfig();
        config.packages("jersey.config.server.provider.packages", javaPackages);
        config.register(provider);
        config.register(MultiPartFeature.class);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        addServlet(basePath, servletHolder, requiresAuthentication, attributeMap);
    }

    public void addServlet(String path, ServletHolder servletHolder, boolean requiresAuthentication, Map<String,Object> attributeMap) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(path);
        context.addServlet(servletHolder, MATCH_ALL);
        if (attributeMap != null) {
            attributeMap.forEach((key, value) -> {
                context.setAttribute(key, value);
            });
        }

        if (requiresAuthentication && pulsar.getConfiguration().isAuthenticationEnabled()) {
            FilterHolder filter = new FilterHolder(new AuthenticationFilter(pulsar));
            context.addFilter(filter, MATCH_ALL, EnumSet.allOf(DispatcherType.class));
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
            Slf4jRequestLog requestLog = new Slf4jRequestLog();
            requestLog.setExtended(true);
            requestLog.setLogTimeZone(TimeZone.getDefault().getID());
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

            log.info("Web Service started at {}", pulsar.getWebServiceAddress());
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    @Override
    public void close() throws PulsarServerException {
        try {
            server.stop();
            webServiceExecutor.shutdown();
            log.info("Web service closed");
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WebService.class);
}
