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
package org.apache.pulsar.discovery.service.server;

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import javax.servlet.Servlet;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages web-service startup/stop on jetty server.
 *
 */
public class ServerManager {
    private final Server server;
    private final ExecutorThreadPool webServiceExecutor;
    private final List<Handler> handlers = Lists.newArrayList();

    private final ServerConnector connector;
    private final ServerConnector connectorTls;

    public ServerManager(ServiceConfig config) {
        this.webServiceExecutor = new ExecutorThreadPool();
        this.webServiceExecutor.setName("pulsar-external-web");
        this.server = new Server(webServiceExecutor);

        List<ServerConnector> connectors = Lists.newArrayList();

        if (config.getWebServicePort().isPresent()) {
            connector = new ServerConnector(server, 1, 1);
            connector.setPort(config.getWebServicePort().get());
            connectors.add(connector);
        } else {
            connector = null;
        }

        if (config.getWebServicePortTls().isPresent()) {
            try {
                SslContextFactory sslCtxFactory;
                if (config.isTlsEnabledWithKeyStore()) {
                    sslCtxFactory = KeyStoreSSLContext.createSslContextFactory(
                            config.getTlsProvider(),
                            config.getTlsKeyStoreType(),
                            config.getTlsKeyStore(),
                            config.getTlsKeyStorePassword(),
                            config.isTlsAllowInsecureConnection(),
                            config.getTlsTrustStoreType(),
                            config.getTlsTrustStore(),
                            config.getTlsTrustStorePassword(),
                            config.isTlsRequireTrustedClientCertOnConnect(),
                            config.getTlsCertRefreshCheckDurationSec()
                    );
                } else {
                    sslCtxFactory = SecurityUtility.createSslContextFactory(
                            config.isTlsAllowInsecureConnection(),
                            config.getTlsTrustCertsFilePath(),
                            config.getTlsCertificateFilePath(),
                            config.getTlsKeyFilePath(),
                            config.isTlsRequireTrustedClientCertOnConnect(),
                            true,
                            config.getTlsCertRefreshCheckDurationSec());
                }
                connectorTls = new ServerConnector(server, 1, 1, sslCtxFactory);
                connectorTls.setPort(config.getWebServicePortTls().get());
                connectors.add(connectorTls);
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            connectorTls = null;
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.stream().forEach(c -> c.setAcceptQueueSize(1024 / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public URI getServiceUri() {
        return this.server.getURI();
    }

    public void addServlet(String path, Class<? extends Servlet> servlet, Map<String, String> initParameters) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(path);

        ServletHolder holder = new ServletHolder(servlet);
        holder.setInitParameters(initParameters);
        context.addServlet(holder, path);
        handlers.add(context);
    }

    public void start() throws Exception {
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

        log.info("Server started at end point {}", getServiceUri());
    }

    public void stop() throws Exception {
        server.stop();
        webServiceExecutor.stop();
        log.info("Server stopped successfully");
    }

	public boolean isStarted() {
		return server.isStarted();
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

    private static final Logger log = LoggerFactory.getLogger(ServerManager.class);
}
