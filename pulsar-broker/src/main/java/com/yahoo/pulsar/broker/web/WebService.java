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
package com.yahoo.pulsar.broker.web;

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.servlet.DispatcherType;

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
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.common.util.SecurityUtility;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Web Service embedded into Pulsar
 */
public class WebService implements AutoCloseable {
    private final ServiceConfiguration config;
    private final PulsarService pulsar;
    private final Server server;
    private final List<Handler> handlers = Lists.newArrayList();
    private final ExecutorService webServiceExecutor;

    /**
     * The set of path regexes on which the ApiVersionFilter is installed if needed
     */
    private static final List<Pattern> API_VERSION_FILTER_PATTERNS = ImmutableList.of(Pattern.compile("^/lookup.*") // V2
                                                                                                                    // lookups
    );

    public WebService(ServiceConfiguration config, PulsarService pulsar) throws PulsarServerException {
        this.config = config;
        this.pulsar = pulsar;
        this.webServiceExecutor = Executors.newFixedThreadPool(32, new DefaultThreadFactory("pulsar-web"));
        this.server = new Server(new ExecutorThreadPool(webServiceExecutor));
        List<ServerConnector> connectors = new ArrayList<>();

        ServerConnector connector = new PulsarServerConnector(server, 1, 1);
        connector.setPort(config.getWebServicePort());
        connectors.add(connector);

        if (config.isTlsEnabled()) {
            SslContextFactory sslCtxFactory = new SslContextFactory();

            try {
                SSLContext sslCtx = SecurityUtility.createSslContext(config.isTlsAllowInsecureConnection(),
                        config.getTlsTrustCertsFilePath(), config.getTlsCertificateFilePath(),
                        config.getTlsKeyFilePath());
                sslCtxFactory.setSslContext(sslCtx);
            } catch (GeneralSecurityException e) {
                throw new PulsarServerException(e);
            }

            sslCtxFactory.setWantClientAuth(true);
            ServerConnector tlsConnector = new PulsarServerConnector(server, 1, 1, sslCtxFactory);
            tlsConnector.setPort(config.getWebServicePortTls());
            connectors.add(tlsConnector);
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.stream().forEach(c -> c.setAcceptQueueSize(1024 / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public void addRestResources(String basePath, String javaPackages, boolean requiresAuthentication) {
        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(ObjectMapperFactory.create());
        ResourceConfig config = new ResourceConfig();
        config.packages("jersey.config.server.provider.packages", javaPackages);
        config.register(provider);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        addServlet(basePath, servletHolder, requiresAuthentication);
    }

    public void addServlet(String path, ServletHolder servletHolder, boolean requiresAuthentication) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(path);
        context.addServlet(servletHolder, "/*");
        context.setAttribute("pulsar", pulsar);
        context.setAttribute("config", config);

        if (requiresAuthentication && config.isAuthenticationEnabled()) {
            FilterHolder filter = new FilterHolder(new AuthenticationFilter(pulsar));
            context.addFilter(filter, "/*", EnumSet.allOf(DispatcherType.class));
        }

        log.info("Servlet path: '{}' -- Enable client version check: {} -- shouldCheckApiVersionOnPath: {}", path,
                config.isClientLibraryVersionCheckEnabled(), shouldCheckApiVersionOnPath(path));
        if (config.isClientLibraryVersionCheckEnabled() && shouldCheckApiVersionOnPath(path)) {
            // Add the ApiVersionFilter to reject request from deprecated
            // clients.
            FilterHolder holder = new FilterHolder(
                    new ApiVersionFilter(pulsar, config.isClientLibraryVersionCheckAllowUnversioned()));
            context.addFilter(holder, "/*", EnumSet.allOf(DispatcherType.class));
            log.info("Enabling ApiVersionFilter");
        }

        handlers.add(context);
    }

    public void addStaticResources(String basePath, String resourcePath) {
        ContextHandler capHandler = new ContextHandler();
        capHandler.setContextPath(basePath);
        ResourceHandler resHandler = new ResourceHandler();
        resHandler.setBaseResource(Resource.newClassPathResource(resourcePath));
        resHandler.setEtags(true);
        resHandler.setCacheControl("max-age=3600");
        capHandler.setHandler(resHandler);
        handlers.add(capHandler);
    }

    /**
     * Checks to see if the given path matches any of the api version filter paths.
     *
     * @param path
     *            the path to check
     * @return true if the ApiVersionFilter can be installed on the path
     */
    private boolean shouldCheckApiVersionOnPath(String path) {
        for (Pattern filterPattern : API_VERSION_FILTER_PATTERNS) {
            Matcher matcher = filterPattern.matcher(path);
            if (matcher.matches()) {
                return true;
            }
        }
        return false;
    }

    public void start() throws PulsarServerException {
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
