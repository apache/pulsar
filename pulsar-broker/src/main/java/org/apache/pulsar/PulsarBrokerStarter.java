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

import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.create;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.isComplete;

import com.ea.agentloader.AgentLoader;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

    private static final Options OPTS = new Options();
    static {
        OPTS.addOption("b", "bookie", false, "Run Bookie Server together");
        OPTS.addOption("a", "bookieautorecovery", false, "Run Bookie Auto Recovery together");
        OPTS.addOption("c", "bookieconf", true, "Configuration file for Bookie");
        OPTS.addOption("h", "help", false, "Print help message");
    }

    /**
     * Print usage.
     */
    private static void printUsage() {
        HelpFormatter hf = new HelpFormatter();
        String header = "\n"
            + "PulsarBrokerStarter is a command to start Pulsar Broker. \n"
            + "User could use option to choose starting bookie together with broker or not.\n\n";
        String footer = "\nHere is an example:\n"
            + "\tPulsarBrokerStarter broker.conf --runbookie --runbookierecovery --bookieconf bookkeeper.conf\n\n ";
        hf.printHelp("PulsarBrokerStarter <pulsar_config_file>  ", header, OPTS, footer, true);
    }

    private static CommandLine readCommandLine(String[] args) throws ParseException {
        BasicParser parser = new BasicParser();
        return parser.parse(OPTS, args);
    }

    private static boolean runBookieTogether(CommandLine cmdLine) throws IllegalArgumentException {
        if (cmdLine.hasOption('b') && cmdLine.hasOption('c')) {
            return true;
        } else if(cmdLine.hasOption('b')) {
            printUsage();
            throw new IllegalArgumentException("No configuration file for bookie");
        } else {
            return false;
        }
    }

    private static boolean runBookieAutoRecoveryTogether(CommandLine cmdLine) throws IllegalArgumentException {
        if (cmdLine.hasOption('a') && cmdLine.hasOption('c')) {
            return true;
        } else if(cmdLine.hasOption('a')) {
            printUsage();
            throw new IllegalArgumentException("No configuration file for bookie auto recovery");
        } else {
            return false;
        }
    }

    private static ServerConfiguration readBookieConfFile(CommandLine cmdLine) throws IllegalArgumentException {
        if (!cmdLine.hasOption('c')) {
            throw new IllegalArgumentException("No configuration file");
        }

        String bookieConfigFile = cmdLine.getOptionValue("c");
        ServerConfiguration bookieConf = new ServerConfiguration();
        try {
            bookieConf.loadConf(new File(bookieConfigFile).toURI().toURL());
            bookieConf.validate();
            log.info("Using bookie configuration file {}", bookieConfigFile);
        } catch (MalformedURLException e) {
            log.error("Could not open configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException();
        } catch (ConfigurationException e) {
            log.error("Malformed configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException();
        }

        return bookieConf;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            throw new IllegalArgumentException("Need to specify a configuration file");
        }

        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
        });

        String configFile = args[0];
        ServiceConfiguration config = loadConfig(configFile);

        CommandLine cmdLine = readCommandLine(args);
        boolean runBookie = runBookieTogether(cmdLine);
        boolean runBookieAutoRecovery = runBookieAutoRecoveryTogether(cmdLine);
        final BookieServer bookieServer;
        final AutoRecoveryMain autoRecoveryMain;
        final StatsProvider bookieStatsProvider;
        ServerConfiguration bookieConfig = null;

        if (runBookie || runBookieAutoRecovery) {
            bookieConfig = readBookieConfFile(cmdLine);
            Class<? extends StatsProvider> statsProviderClass = bookieConfig.getStatsProviderClass();
            bookieStatsProvider = ReflectionUtils.newInstance(statsProviderClass);
            bookieStatsProvider.start(bookieConfig);
        } else {
            bookieStatsProvider = null;
        }

        if (runBookie) {
            // start Bookie
            bookieServer = new BookieServer(bookieConfig, bookieStatsProvider.getStatsLogger(""));
            bookieServer.start();
        } else {
            bookieServer = null;
        }

        if (runBookieAutoRecovery) {
            // start Bookie AutoRecovery
            autoRecoveryMain = new AutoRecoveryMain(bookieConfig);
            autoRecoveryMain.start();
        } else {
            autoRecoveryMain = null;
        }

        // load aspectj-weaver agent for instrumentation
        AgentLoader.loadAgentClass(Agent.class.getName(), null);

        @SuppressWarnings("resource")
        final PulsarService service = new PulsarService(config);
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                service.getShutdownService().run();
                log.info("Shut down broker service successfully");
                if (bookieServer != null) {
                    bookieServer.shutdown();
                    log.info("Shut down bookie server successfully");
                }
                if (autoRecoveryMain != null) {
                    autoRecoveryMain.shutdown();
                    log.info("Shutdown AutoRecoveryMain successfully");
                }
                if (bookieStatsProvider != null) {
                    bookieStatsProvider.stop();
                }
            }
            )
        );

        try {
            service.start();
            log.info("PulsarService started");
        } catch (PulsarServerException e) {
            log.error("Failed to start pulsar service.", e);

            Runtime.getRuntime().halt(1);
        }

        service.waitUntilClosed();

        if (bookieServer != null) {
            bookieServer.join();
        }
        if (autoRecoveryMain != null) {
            autoRecoveryMain.join();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStarter.class);
}
