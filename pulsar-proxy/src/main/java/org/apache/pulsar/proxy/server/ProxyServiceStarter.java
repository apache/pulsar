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
import static java.lang.Thread.setDefaultUncaughtExceptionHandler;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.slf4j.bridge.SLF4JBridgeHandler.install;
import static org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger;

import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * Starts an instance of the Pulsar ProxyService
 *
 */
public class ProxyServiceStarter {

    @Parameter(names = { "-c", "--config" }, description = "Configuration file path", required = true)
    private String configFile;

    @Parameter(names = { "-zk", "--zookeeper-servers" }, description = "Local zookeeper connection string")
    private String zookeeperServers = "";

    @Parameter(names = { "-gzk", "--global-zookeeper-servers" }, description = "Global zookeeper connection string")
    private String globalZookeeperServers = "";

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
            config.setGlobalZookeeperServers(globalZookeeperServers);
        }

        checkArgument(!isEmpty(config.getZookeeperServers()), "zookeeperServers must be provided");
        checkArgument(!isEmpty(config.getGlobalZookeeperServers()), "globalZookeeperServers must be provided");

        // create broker service
        ProxyService discoveryService = new ProxyService(config);
        // create a web-service
        final WebServer server = new WebServer(config);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    discoveryService.close();
                    server.stop();
                } catch (Exception e) {
                    log.warn("server couldn't stop gracefully {}", e.getMessage(), e);
                }
            }
        });

        discoveryService.start();

        // Setup metrics
        DefaultExports.initialize();
        server.addServlet("/metrics", new ServletHolder(MetricsServlet.class));

        // start web-service
        server.start();
    }

    public static void main(String[] args) throws Exception {
        new ProxyServiceStarter(args);
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyServiceStarter.class);

}
