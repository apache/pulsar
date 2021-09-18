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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.slf4j.bridge.SLF4JBridgeHandler.install;
import static org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.core.util.datetime.FixedDateFormat;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.discovery.service.DiscoveryService;
import org.apache.pulsar.discovery.service.web.DiscoveryServiceServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Starts jetty server and initialize {@link DiscoveryServiceServlet} web-service
 *
 */
public class DiscoveryServiceStarter {
    private static class Arguments {
        @Parameter(description = "config file")
        private String configFile = "";

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static void checkConfig(ServiceConfig config) {
        checkArgument(!isEmpty(config.getZookeeperServers()), "zookeeperServers must be provided");
        checkArgument(!isEmpty(config.getConfigurationStoreServers()),  "configuration-store Servers must be provided");
    }

    public static void init(String configFile) throws Exception {
        // setup handlers
        removeHandlersForRootLogger();
        install();

        DateFormat dateFormat = new SimpleDateFormat(
            FixedDateFormat.FixedFormat.ISO8601_OFFSET_DATE_TIME_HHMM.getPattern());
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            System.out.println(String.format("%s [%s] error Uncaught exception in thread %s: %s", dateFormat.format(new Date()), thread.getContextClassLoader(), thread.getName(), exception.getMessage()));
        });

        // load config file
        final ServiceConfig config = PulsarConfigurationLoader.create(configFile, ServiceConfig.class);
        checkConfig(config);

        // create Discovery service
        DiscoveryService discoveryService = new DiscoveryService(config);
        // create a web-service
        final ServerManager server = new ServerManager(config);

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
        startWebService(server, config);
    }

    protected static void startWebService(ServerManager server, ServiceConfig config) throws Exception {
        // add servlet
        Map<String, String> initParameters = new TreeMap<>();
        initParameters.put("zookeeperServers", config.getZookeeperServers());
        server.addServlet("/*", DiscoveryServiceServlet.class, initParameters);

        // start web-service
        server.start();
        log.info("Discovery service is started at {}", server.getServiceUri().toString());
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(arguments);
            jcommander.parse(args);
            if (arguments.help) {
                jcommander.usage();
                return;
            }
            if (arguments.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("discovery", arguments);
                cmd.run(null);
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            return;
        }

        checkArgument(args.length == 1, "Need to specify a configuration file");

        try {
            // load config file and start server
            init(args[0]);
        } catch (Exception e) {
            log.error("Failed to start discovery service.", e);
            Runtime.getRuntime().halt(1);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DiscoveryServiceStarter.class);

}
