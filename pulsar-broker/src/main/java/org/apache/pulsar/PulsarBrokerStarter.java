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
package org.apache.pulsar;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.create;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.isComplete;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.ea.agentloader.AgentLoader;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.util.Arrays;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.aspectj.weaver.loadtime.Agent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class PulsarBrokerStarter {

    private static ServiceConfiguration loadConfig(String configFile) throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        ServiceConfiguration config = create((new FileInputStream(configFile)), ServiceConfiguration.class);
        // it validates provided configuration is completed
        isComplete(config);
        return config;
    }

    private static class BookieArguments {

        @Parameter(names = {"-rb", "--run-bookie"}, description = "Run Bookie together with broker")
        private boolean runBookie = false;

        @Parameter(names = {"-ra", "--run-bookie-autorecovery"}, description = "Run Bookie Autorecovery together with broker")
        private boolean runBookieAutoRecovery = false;

        @Parameter(names = {"-bc", "--bookie-conf"}, description = "Configuration file for Bookie")
        private String bookieConfigFile;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;
    }

    private static ServerConfiguration readBookieConfFile(String bookieConfigFile) throws IllegalArgumentException {
        ServerConfiguration bookieConf = new ServerConfiguration();
        try {
            bookieConf.loadConf(new File(bookieConfigFile).toURI().toURL());
            bookieConf.validate();
            log.info("Using bookie configuration file {}", bookieConfigFile);
        } catch (MalformedURLException e) {
            log.error("Could not open configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException("Could not open configuration file");
        } catch (ConfigurationException e) {
            log.error("Malformed configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException("Malformed configuration file");
        }
        return bookieConf;
    }

    private static class PulsarBookieStarter {
        private final BookieServer bookieServer;
        private final AutoRecoveryMain autoRecoveryMain;
        private final StatsProvider bookieStatsProvider;
        private final ServerConfiguration bookieConfig;

        PulsarBookieStarter(String[] args) throws Exception{
            BookieArguments bookieArguments = new BookieArguments();
            JCommander jcommander = new JCommander(bookieArguments);
            jcommander.setProgramName("PulsarBrokerStarter <broker.conf>");

            // parse args by jcommander
            jcommander.parse(args);
            if (bookieArguments.help) {
                jcommander.usage();
                System.exit(-1);
            }
            if ((bookieArguments.runBookie || bookieArguments.runBookieAutoRecovery)
                && isBlank(bookieArguments.bookieConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("No configuration file for Bookie");
            }

            // init stats provider
            if (bookieArguments.runBookie || bookieArguments.runBookieAutoRecovery) {
                checkState(isNotBlank(bookieArguments.bookieConfigFile),
                    "No configuration file for Bookie");
                bookieConfig = readBookieConfFile(bookieArguments.bookieConfigFile);
                Class<? extends StatsProvider> statsProviderClass = bookieConfig.getStatsProviderClass();
                bookieStatsProvider = ReflectionUtils.newInstance(statsProviderClass);
            } else {
                bookieConfig = null;
                bookieStatsProvider = null;
            }

            // init bookie server
            if (bookieArguments.runBookie) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie");
                checkNotNull(bookieStatsProvider, "No Stats Provider for Bookie");
                bookieServer = new BookieServer(bookieConfig, bookieStatsProvider.getStatsLogger(""));
            } else {
                bookieServer = null;
            }

            // init bookie AutorecoveryMain
            if (bookieArguments.runBookieAutoRecovery) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie Autorecovery");
                autoRecoveryMain = new AutoRecoveryMain(bookieConfig);
            } else {
                autoRecoveryMain = null;
            }
        }

        public void start() throws Exception {
            if (bookieStatsProvider != null) {
                bookieStatsProvider.start(bookieConfig);
                log.info("started bookieStatsProvider.");
            }
            if (bookieServer != null) {
                bookieServer.start();
                log.info("started bookieServer.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.start();
                log.info("started bookie autoRecoveryMain.");
            }
        }

        public void join() throws InterruptedException {
            if (bookieServer != null) {
                bookieServer.join();
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.join();
            }
        }

        public void shutdown() {
            if (bookieStatsProvider != null) {
                bookieStatsProvider.stop();
            }
            if (bookieServer != null) {
                bookieServer.shutdown();
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.shutdown();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Need to specify a configuration file");
        }

        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
        });

        String configFile = args[0];
        ServiceConfiguration config = loadConfig(configFile);

        // load aspectj-weaver agent for instrumentation
        AgentLoader.loadAgentClass(Agent.class.getName(), null);

        PulsarBookieStarter bookieStarter = new PulsarBookieStarter(Arrays.copyOfRange(args, 1, args.length));
        bookieStarter.start();

        @SuppressWarnings("resource")
        final PulsarService service = new PulsarService(config);
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                service.getShutdownService().run();
                log.info("Shut down broker service successfully.");
                bookieStarter.shutdown();
            })
        );

        try {
            service.start();
            log.info("PulsarService started");
        } catch (PulsarServerException e) {
            log.error("Failed to start pulsar service.", e);

            Runtime.getRuntime().halt(1);
        }

        service.waitUntilClosed();

        bookieStarter.join();
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStarter.class);
}
