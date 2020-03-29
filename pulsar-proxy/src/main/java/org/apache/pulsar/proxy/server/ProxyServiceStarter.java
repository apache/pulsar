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
package org.apache.pulsar.proxy.server;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.pulsar.common.stats.JvmMetrics.getJvmDirectMemoryUsed;
import static org.slf4j.bridge.SLF4JBridgeHandler.install;
import static org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger;

import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import io.netty.util.internal.PlatformDependent;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Gauge.Child;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.proxy.stats.ProxyStats;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;


/**
 * Starts an instance of the Pulsar ProxyService
 *
 */
public class ProxyServiceStarter {

    @Parameter(names = { "-c", "--config" }, description = "Configuration file path", required = true)
    private String configFile;

    @Parameter(names = { "-zk", "--zookeeper-servers" }, description = "Local zookeeper connection string")
    private String zookeeperServers = "";

    @Deprecated
    @Parameter(names = { "-gzk", "--global-zookeeper-servers" }, description = "Global zookeeper connection string")
    private String globalZookeeperServers = "";

    @Parameter(names = { "-cs", "--configuration-store-servers" },
        description = "Configuration store connection string")
    private String configurationStoreServers = "";

    @Parameter(names = { "-h", "--help" }, description = "Show this help message")
    private boolean help = false;

    public ProxyServiceStarter(String[] args) throws Exception {
        // setup handlers
        removeHandlersForRootLogger();
        install();

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            System.out.println(String.format("%s [%s] error Uncaught exception in thread %s: %s", dateFormat.format(new Date()), thread.getContextClassLoader(), thread.getName(), exception.getMessage()));
        });

        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(this);
            jcommander.parse(args);
            if (help || isBlank(configFile)) {
                jcommander.usage();
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            System.exit(-1);
        }

        // load config file
        final ProxyConfiguration config = PulsarConfigurationLoader.create(configFile, ProxyConfiguration.class);

        if (!isBlank(zookeeperServers)) {
            // Use zookeeperServers from command line
            config.setZookeeperServers(zookeeperServers);
        }

        if (!isBlank(globalZookeeperServers)) {
            // Use globalZookeeperServers from command line
            config.setConfigurationStoreServers(globalZookeeperServers);
        }
        if (!isBlank(configurationStoreServers)) {
            // Use configurationStoreServers from command line
            config.setConfigurationStoreServers(configurationStoreServers);
        }

        if ((isBlank(config.getBrokerServiceURL()) && isBlank(config.getBrokerServiceURLTLS()))
                || config.isAuthorizationEnabled()) {
            checkArgument(!isEmpty(config.getZookeeperServers()), "zookeeperServers must be provided");
            checkArgument(!isEmpty(config.getConfigurationStoreServers()),
                    "configurationStoreServers must be provided");
        }

        if ((!config.isTlsEnabledWithBroker() && isBlank(config.getBrokerWebServiceURL()))
                || (config.isTlsEnabledWithBroker() && isBlank(config.getBrokerWebServiceURLTLS()))) {
            checkArgument(!isEmpty(config.getZookeeperServers()), "zookeeperServers must be provided");
        }

        AuthenticationService authenticationService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(config));
        // create proxy service
        ProxyService proxyService = new ProxyService(config, authenticationService);
        // create a web-service
        final WebServer server = new WebServer(config, authenticationService);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                proxyService.close();
                server.stop();
            } catch (Exception e) {
                log.warn("server couldn't stop gracefully {}", e.getMessage(), e);
            }
        }));

        proxyService.start();

        // Setup metrics
        DefaultExports.initialize();

        // Report direct memory from Netty counters
        Gauge.build("jvm_memory_direct_bytes_used", "-").create().setChild(new Child() {
            @Override
            public double get() {
                return getJvmDirectMemoryUsed();
            }
        }).register(CollectorRegistry.defaultRegistry);

        Gauge.build("jvm_memory_direct_bytes_max", "-").create().setChild(new Child() {
            @Override
            public double get() {
                return PlatformDependent.maxDirectMemory();
            }
        }).register(CollectorRegistry.defaultRegistry);

        addWebServerHandlers(server, config, proxyService, proxyService.getDiscoveryProvider());

        // start web-service
        server.start();
    }

    public static void main(String[] args) throws Exception {
        new ProxyServiceStarter(args);
    }

    public static void addWebServerHandlers(WebServer server,
                                     ProxyConfiguration config,
                                     ProxyService service,
                                     BrokerDiscoveryProvider discoveryProvider) {
        server.addServlet("/metrics", new ServletHolder(MetricsServlet.class), Collections.emptyList(), config.isAuthenticateMetricsEndpoint());
        server.addRestResources("/", VipStatus.class.getPackage().getName(),
                VipStatus.ATTRIBUTE_STATUS_FILE_PATH, config.getStatusFilePath());
        server.addRestResources("/proxy-stats", ProxyStats.class.getPackage().getName(), ProxyStats.ATTRIBUTE_PULSAR_PROXY_NAME, service);

        AdminProxyHandler adminProxyHandler = new AdminProxyHandler(config, discoveryProvider);
        ServletHolder servletHolder = new ServletHolder(adminProxyHandler);
        servletHolder.setInitParameter("preserveHost", "true");
        server.addServlet("/admin", servletHolder);
        server.addServlet("/lookup", servletHolder);

        for (ProxyConfiguration.HttpReverseProxyConfig revProxy : config.getHttpReverseProxyConfigs()) {
            log.debug("Adding reverse proxy with config {}", revProxy);
            ServletHolder proxyHolder = new ServletHolder(ProxyServlet.Transparent.class);
            proxyHolder.setInitParameter("proxyTo", revProxy.getProxyTo());
            proxyHolder.setInitParameter("prefix", "/");
            server.addServlet(revProxy.getPath(), proxyHolder);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyServiceStarter.class);

}
