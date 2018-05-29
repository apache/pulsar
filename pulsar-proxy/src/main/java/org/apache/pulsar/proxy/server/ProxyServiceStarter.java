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
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Thread.setDefaultUncaughtExceptionHandler;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.slf4j.bridge.SLF4JBridgeHandler.install;
import static org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.pulsar.common.configuration.VipStatus;

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
        setDefaultUncaughtExceptionHandler((thread, exception) -> {
            log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
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
            checkArgument(!isEmpty(config.getConfigurationStoreServers()),
                    "configurationStoreServers must be provided");
            checkArgument(!isEmpty(config.getClusterName()), "clusterName must be provided");
        }

        java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        // create proxy service
        ProxyService proxyService = new ProxyService(config);
        // create a web-service
        final WebServer server = new WebServer(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                proxyService.close();
                server.stop();
            } catch (Exception e) {
                log.warn("server couldn't stop gracefully {}", e.getMessage(), e);
            }
        }));

        proxyService.start();

        // Get broker URL to proxy http requests
        String brokerWebServiceUrl = null;

        if (config.isTlsEnabledWithBroker()) {
            brokerWebServiceUrl = config.getBrokerWebServiceURLTLS();
        } else {
            brokerWebServiceUrl = config.getBrokerWebServiceURL();
        }

        if (isEmpty(brokerWebServiceUrl)) {
            try {
                ClusterData clusterData = proxyService.getDiscoveryProvider()
                        .getClusterMetadata(config.getClusterName()).get(30L, TimeUnit.SECONDS);
                if (config.isTlsEnabledWithBroker()) {
                    brokerWebServiceUrl = clusterData.getServiceUrlTls();
                } else {
                    brokerWebServiceUrl = clusterData.getServiceUrl();
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof NoSuchElementException) {
                    log.error("Metadata of {} cluster does not exist in configuration store", config.getClusterName());
                }
                throw e;
            }
        }

        if (isEmpty(brokerWebServiceUrl)) {
            throw new RuntimeException("Could not get broker url to proxy http requests");
        }

        // Setup metrics
        DefaultExports.initialize();
        server.addServlet("/metrics", new ServletHolder(MetricsServlet.class));
        server.addRestResources("/", VipStatus.class.getPackage().getName(),
                VipStatus.ATTRIBUTE_STATUS_FILE_PATH, config.getStatusFilePath());

        server.addProxyServlet("/admin", new AdminProxyHandler(config), brokerWebServiceUrl);
        server.addProxyServlet("/lookup", new AdminProxyHandler(config), brokerWebServiceUrl);

        // start web-service
        server.start();
    }

    public static void main(String[] args) throws Exception {
        new ProxyServiceStarter(args);
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyServiceStarter.class);

}
