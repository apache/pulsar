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
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.internal.PlatformDependent;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.util.datetime.FixedDateFormat;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.WorkerServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class PulsarBrokerStarter {

    private static ServiceConfiguration loadConfig(String configFile) throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            ServiceConfiguration config = create(inputStream, ServiceConfiguration.class);
            // it validates provided configuration is completed
            isComplete(config);
            return config;
        }
    }

    @VisibleForTesting
    @Parameters(commandDescription = "Options")
    private static class StarterArguments {
        @Parameter(names = {"-c", "--broker-conf"}, description = "Configuration file for Broker")
        private String brokerConfigFile =
                Paths.get("").toAbsolutePath().normalize().toString() + "/conf/broker.conf";

        @Parameter(names = {"-rb", "--run-bookie"}, description = "Run Bookie together with Broker")
        private boolean runBookie = false;

        @Parameter(names = {"-ra", "--run-bookie-autorecovery"},
                description = "Run Bookie Autorecovery together with broker")
        private boolean runBookieAutoRecovery = false;

        @Parameter(names = {"-bc", "--bookie-conf"}, description = "Configuration file for Bookie")
        private String bookieConfigFile =
                Paths.get("").toAbsolutePath().normalize().toString() + "/conf/bookkeeper.conf";

        @Parameter(names = {"-rfw", "--run-functions-worker"}, description = "Run functions worker with Broker")
        private boolean runFunctionsWorker = false;

        @Parameter(names = {"-fwc", "--functions-worker-conf"}, description = "Configuration file for Functions Worker")
        private String fnWorkerConfigFile =
                Paths.get("").toAbsolutePath().normalize().toString() + "/conf/functions_worker.yml";

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
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

        if (bookieConf.getMaxPendingReadRequestPerThread() < bookieConf.getRereplicationEntryBatchSize()) {
            throw new IllegalArgumentException(
                "rereplicationEntryBatchSize should be smaller than " + "maxPendingReadRequestPerThread");
        }
        return bookieConf;
    }

    private static boolean argsContains(String[] args, String arg) {
        return Arrays.asList(args).contains(arg);
    }

    private static class BrokerStarter {
        private final ServiceConfiguration brokerConfig;
        private final PulsarService pulsarService;
        private final LifecycleComponent bookieServer;
        private volatile CompletableFuture<Void> bookieStartFuture;
        private final AutoRecoveryMain autoRecoveryMain;
        private final StatsProvider bookieStatsProvider;
        private final ServerConfiguration bookieConfig;
        private final WorkerService functionsWorkerService;
        private final WorkerConfig workerConfig;

        BrokerStarter(String[] args) throws Exception {
            StarterArguments starterArguments = new StarterArguments();
            JCommander jcommander = new JCommander(starterArguments);
            jcommander.setProgramName("PulsarBrokerStarter");

            // parse args by JCommander
            jcommander.parse(args);
            if (starterArguments.help) {
                jcommander.usage();
                System.exit(-1);
            }

            if (starterArguments.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("broker", starterArguments);
                cmd.run(null);
                System.exit(-1);
            }

            // init broker config
            if (isBlank(starterArguments.brokerConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("Need to specify a configuration file for broker");
            } else {
                brokerConfig = loadConfig(starterArguments.brokerConfigFile);
            }

            int maxFrameSize = brokerConfig.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING;
            if (maxFrameSize >= PlatformDependent.estimateMaxDirectMemory()) {
                throw new IllegalArgumentException("Max message size need smaller than jvm directMemory");
            }

            if (!NamespaceBundleSplitAlgorithm.AVAILABLE_ALGORITHMS.containsAll(
                    brokerConfig.getSupportedNamespaceBundleSplitAlgorithms())) {
                throw new IllegalArgumentException(
                        "The given supported namespace bundle split algorithm has unavailable algorithm. "
                                + "Available algorithms are " + NamespaceBundleSplitAlgorithm.AVAILABLE_ALGORITHMS);
            }

            if (!brokerConfig.getSupportedNamespaceBundleSplitAlgorithms().contains(
                    brokerConfig.getDefaultNamespaceBundleSplitAlgorithm())) {
                throw new IllegalArgumentException("Supported namespace bundle split algorithms "
                        + "must contains the default namespace bundle split algorithm");
            }

            // init functions worker
            if (starterArguments.runFunctionsWorker || brokerConfig.isFunctionsWorkerEnabled()) {
                workerConfig = PulsarService.initializeWorkerConfigFromBrokerConfig(
                    brokerConfig, starterArguments.fnWorkerConfigFile
                );
                functionsWorkerService = WorkerServiceLoader.load(workerConfig);
            } else {
                workerConfig = null;
                functionsWorkerService = null;
            }

            // init pulsar service
            pulsarService = new PulsarService(brokerConfig,
                                              workerConfig,
                                              Optional.ofNullable(functionsWorkerService),
                                              (exitCode) -> {
                                                  log.info("Halting broker process with code {}",
                                                           exitCode);
                                                  Runtime.getRuntime().halt(exitCode);
                                              });

            // if no argument to run bookie in cmd line, read from pulsar config
            if (!argsContains(args, "-rb") && !argsContains(args, "--run-bookie")) {
                checkState(!starterArguments.runBookie,
                        "runBookie should be false if has no argument specified");
                starterArguments.runBookie = brokerConfig.isEnableRunBookieTogether();
            }
            if (!argsContains(args, "-ra") && !argsContains(args, "--run-bookie-autorecovery")) {
                checkState(!starterArguments.runBookieAutoRecovery,
                        "runBookieAutoRecovery should be false if has no argument specified");
                starterArguments.runBookieAutoRecovery = brokerConfig.isEnableRunBookieAutoRecoveryTogether();
            }

            if ((starterArguments.runBookie || starterArguments.runBookieAutoRecovery)
                && isBlank(starterArguments.bookieConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("No configuration file for Bookie");
            }

            // init stats provider
            if (starterArguments.runBookie || starterArguments.runBookieAutoRecovery) {
                checkState(isNotBlank(starterArguments.bookieConfigFile),
                    "No configuration file for Bookie");
                bookieConfig = readBookieConfFile(starterArguments.bookieConfigFile);
                Class<? extends StatsProvider> statsProviderClass = bookieConfig.getStatsProviderClass();
                bookieStatsProvider = ReflectionUtils.newInstance(statsProviderClass);
            } else {
                bookieConfig = null;
                bookieStatsProvider = null;
            }

            // init bookie server
            if (starterArguments.runBookie) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie");
                checkNotNull(bookieStatsProvider, "No Stats Provider for Bookie");
                bookieServer = org.apache.bookkeeper.server.Main
                        .buildBookieServer(new BookieConfiguration(bookieConfig));
            } else {
                bookieServer = null;
            }

            // init bookie AutorecoveryMain
            if (starterArguments.runBookieAutoRecovery) {
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
                bookieStartFuture = ComponentStarter.startComponent(bookieServer);
                log.info("started bookieServer.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.start();
                log.info("started bookie autoRecoveryMain.");
            }

            pulsarService.start();
            log.info("PulsarService started.");
        }

        public void join() throws InterruptedException {
            pulsarService.waitUntilClosed();

            try {
                pulsarService.close();
            } catch (PulsarServerException e) {
                throw new RuntimeException();
            }

            if (bookieStartFuture != null) {
                bookieStartFuture.join();
                bookieStartFuture = null;
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.join();
            }
        }

        public void shutdown() throws Exception {
            if (null != functionsWorkerService) {
                functionsWorkerService.stop();
                log.info("Shut down functions worker service successfully.");
            }

            pulsarService.close();
            log.info("Shut down broker service successfully.");

            if (bookieStatsProvider != null) {
                bookieStatsProvider.stop();
                log.info("Shut down bookieStatsProvider successfully.");
            }
            if (bookieServer != null) {
                bookieServer.close();
                log.info("Shut down bookieServer successfully.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.shutdown();
                log.info("Shut down autoRecoveryMain successfully.");
            }
        }
    }


    public static void main(String[] args) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat(
            FixedDateFormat.FixedFormat.ISO8601_OFFSET_DATE_TIME_HHMM.getPattern());
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            System.out.println(String.format("%s [%s] error Uncaught exception in thread %s: %s",
                    dateFormat.format(new Date()), thread.getContextClassLoader(),
                    thread.getName(), exception.getMessage()));
            exception.printStackTrace(System.out);
        });

        BrokerStarter starter = new BrokerStarter(args);
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                try {
                    starter.shutdown();
                } catch (Throwable t) {
                    log.error("Error while shutting down Pulsar service", t);
                }
            }, "pulsar-service-shutdown")
        );

        PulsarByteBufAllocator.registerOOMListener(oomException -> {
            if (starter.brokerConfig.isSkipBrokerShutdownOnOOM()) {
                log.error("-- Received OOM exception: {}", oomException.getMessage(), oomException);
            } else {
                log.error("-- Shutting down - Received OOM exception: {}", oomException.getMessage(), oomException);
                starter.pulsarService.shutdownNow();
            }
        });

        try {
            starter.start();
        } catch (Throwable t) {
            log.error("Failed to start pulsar service.", t);
            LogManager.shutdown();
            Runtime.getRuntime().halt(1);
        } finally {
            starter.join();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStarter.class);
}
