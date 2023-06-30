/*
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

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
import java.io.FileInputStream;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.CmdGenerateDocs;

@Slf4j
public class PulsarStandaloneStarter extends PulsarStandalone {

    private static final String PULSAR_CONFIG_FILE = "pulsar.config.file";

    @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
    private boolean generateDocs = false;

    public PulsarStandaloneStarter(String[] args) throws Exception {

        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(this);
            jcommander.parse(args);
            if (this.isHelp()) {
                jcommander.usage();
                System.exit(0);
            }
            if (Strings.isNullOrEmpty(this.getConfigFile())) {
                String configFile = System.getProperty(PULSAR_CONFIG_FILE);
                if (Strings.isNullOrEmpty(configFile)) {
                    throw new IllegalArgumentException(
                            "Config file not specified. Please use -c, --config-file or -Dpulsar.config.file to "
                                    + "specify the config file.");
                }
                this.setConfigFile(configFile);
            }
            if (this.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("standalone", this);
                cmd.run(null);
                System.exit(0);
            }

            if (this.isNoBroker() && this.isOnlyBroker()) {
                log.error("Only one option is allowed between '--no-broker' and '--only-broker'");
                jcommander.usage();
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            log.error(e.getMessage());
            System.exit(1);
        }

        try (FileInputStream inputStream = new FileInputStream(this.getConfigFile())) {
            this.config = PulsarConfigurationLoader.create(
                    inputStream, ServiceConfiguration.class);
        }

        if (this.getAdvertisedAddress() != null) {
            // Use advertised address from command line
            config.setAdvertisedAddress(this.getAdvertisedAddress());
        } else if (isBlank(config.getAdvertisedAddress()) && isBlank(config.getAdvertisedListeners())) {
            // Use advertised address as local hostname
            config.setAdvertisedAddress("localhost");
        } else {
            // Use advertised or advertisedListeners address from config file
        }

        // Set ZK server's host to localhost
        // Priority: args > conf > default
        if (!argsContains(args, "--zookeeper-port")) {
            if (StringUtils.isNotBlank(config.getMetadataStoreUrl())) {
                String[] metadataStoreUrl = config.getMetadataStoreUrl().split(",")[0].split(":");
                if (metadataStoreUrl.length == 2) {
                    this.setZkPort(Integer.parseInt(metadataStoreUrl[1]));
                } else if ((metadataStoreUrl.length == 3)){
                    String zkPort = metadataStoreUrl[2];
                    if (zkPort.contains("/")) {
                        this.setZkPort(Integer.parseInt(zkPort.substring(0, zkPort.lastIndexOf("/"))));
                    } else {
                        this.setZkPort(Integer.parseInt(zkPort));
                    }
                }
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (fnWorkerService != null) {
                    fnWorkerService.stop();
                }

                if (broker != null) {
                    broker.close();
                }

                if (bkEnsemble != null) {
                    bkEnsemble.stop();
                }
            } catch (Exception e) {
                log.error("Shutdown failed: {}", e.getMessage(), e);
            } finally {
                LogManager.shutdown();
            }
        }));
    }

    private static boolean argsContains(String[] args, String arg) {
        return Arrays.asList(args).contains(arg);
    }

    public static void main(String[] args) throws Exception {
        // Start standalone
        PulsarStandaloneStarter standalone = new PulsarStandaloneStarter(args);
        try {
            standalone.start();
        } catch (Throwable th) {
            log.error("Failed to start pulsar service.", th);
            LogManager.shutdown();
            Runtime.getRuntime().exit(1);
        }

    }
}
