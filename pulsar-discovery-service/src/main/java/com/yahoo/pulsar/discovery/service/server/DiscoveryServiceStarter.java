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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.yahoo.pulsar.common.util.FieldParser.update;
import static java.lang.Thread.setDefaultUncaughtExceptionHandler;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.slf4j.bridge.SLF4JBridgeHandler.install;
import static org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.discovery.service.DiscoveryServiceServlet;

/**
 *
 * Starts jetty server and initialize {@link DiscoveryServiceServlet} web-service
 *
 */
public class DiscoveryServiceStarter {

    public static void init(String configFile) throws Exception {
        // setup handlers
        removeHandlersForRootLogger();
        install();
        setDefaultUncaughtExceptionHandler((thread, exception) -> {
            log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
        });

        // load config file
        final ServiceConfig config = load(configFile);
        // create a web-service
        final ServerManager server = new ServerManager(config);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (Exception e) {
                    log.warn("server couldn't stop gracefully {}", e.getMessage(), e);
                }
            }
        });

        // add servlet
        Map<String, String> initParameters = new TreeMap<>();
        initParameters.put("zookeeperServers", config.getZookeeperServers());
        server.addServlet("/*", DiscoveryServiceServlet.class, initParameters);

        // start web-service
        server.start();
        log.info("Discovery service is started at {}", server.getServiceUri().toString());
    }

    public static void main(String[] args) {
        checkArgument(args.length == 1, "Need to specify a configuration file");

        try {
            // load config file and start server
            init(args[0]);
        } catch (Exception e) {
            log.error("Failed to start discovery service.", e);
            Runtime.getRuntime().halt(1);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static ServiceConfig load(String configFile) throws IOException, IllegalArgumentException {
        final InputStream inStream = new FileInputStream(configFile);
        try {
            checkNotNull(inStream, "Unbable to read config file " + configFile);
            ServiceConfig config = new ServiceConfig();
            Properties properties = new Properties();
            properties.load(inStream);
            update((Map) properties, config);
            checkArgument(!isEmpty(config.getZookeeperServers()), "zookeeperServers must be provided");
            return config;
        } finally {
            if (inStream != null) {
                inStream.close();
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DiscoveryServiceStarter.class);

}
