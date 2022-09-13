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

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.FileInputStream;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarStandaloneStarter extends PulsarStandalone {
    @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
    private boolean generateDocs = false;

    private static final Logger log = LoggerFactory.getLogger(PulsarStandaloneStarter.class);

    public PulsarStandaloneStarter(String[] args) throws Exception {

        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(this);
            jcommander.parse(args);
            if (this.isHelp() || isBlank(this.getConfigFile())) {
                jcommander.usage();
                return;
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
            return;
        }

        try (FileInputStream inputStream = new FileInputStream(this.getConfigFile())) {
            this.config = PulsarConfigurationLoader.create(
                    inputStream, ServiceConfiguration.class);
        }

        String zkServers = "127.0.0.1";

        if (this.getAdvertisedAddress() != null) {
            // Use advertised address from command line
            config.setAdvertisedAddress(this.getAdvertisedAddress());
            zkServers = this.getAdvertisedAddress();
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

        final String metadataStoreUrl =
                ZKMetadataStore.ZK_SCHEME_IDENTIFIER + zkServers + ":" + this.getZkPort();
        config.setMetadataStoreUrl(metadataStoreUrl);
        config.setConfigurationMetadataStoreUrl(metadataStoreUrl);

        config.setRunningStandalone(true);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
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

                    LogManager.shutdown();
                } catch (Exception e) {
                    log.error("Shutdown failed: {}", e.getMessage(), e);
                }
            }
        });
    }

    private static boolean argsContains(String[] args, String arg) {
        return Arrays.asList(args).contains(arg);
    }

    public static void main(String args[]) throws Exception {
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
