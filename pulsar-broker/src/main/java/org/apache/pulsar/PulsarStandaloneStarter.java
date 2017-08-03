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

import java.io.FileInputStream;
import java.net.URL;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.aspectj.weaver.loadtime.Agent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.ea.agentloader.AgentLoader;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class PulsarStandaloneStarter {

    PulsarService broker;
    PulsarAdmin admin;
    LocalBookkeeperEnsemble bkEnsemble;
    ServiceConfiguration config;

    @Parameter(names = { "-c", "--config" }, description = "Configuration file path", required = true)
    private String configFile;

    @Parameter(names = { "--wipe-data" }, description = "Clean up previous ZK/BK data")
    private boolean wipeData = false;

    @Parameter(names = { "--num-bookies" }, description = "Number of local Bookies")
    private int numOfBk = 1;

    @Parameter(names = { "--zookeeper-port" }, description = "Local zookeeper's port")
    private int zkPort = 2181;

    @Parameter(names = { "--bookkeeper-port" }, description = "Local bookies base port")
    private int bkPort = 3181;

    @Parameter(names = { "--zookeeper-dir" }, description = "Local zooKeeper's data directory")
    private String zkDir = "data/standalone/zookeeper";

    @Parameter(names = { "--bookkeeper-dir" }, description = "Local bookies base data directory")
    private String bkDir = "data/standalone/bookkeeper";

    @Parameter(names = { "--no-broker" }, description = "Only start ZK and BK services, no broker")
    private boolean noBroker = false;

    @Parameter(names = { "--only-broker" }, description = "Only start Pulsar broker service (no ZK, BK)")
    private boolean onlyBroker = false;

    @Parameter(names = { "-a", "--advertised-address" }, description = "Standalone broker advertised address")
    private String advertisedAddress = null;

    @Parameter(names = { "-h", "--help" }, description = "Show this help message")
    private boolean help = false;

    private static final Logger log = LoggerFactory.getLogger(PulsarStandaloneStarter.class);

    public PulsarStandaloneStarter(String[] args) throws Exception {

        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(this);
            jcommander.parse(args);
            if (help || isBlank(configFile)) {
                jcommander.usage();
                return;
            }

            if (noBroker && onlyBroker) {
                log.error("Only one option is allowed between '--no-broker' and '--only-broker'");
                jcommander.usage();
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            return;
        }

        this.config = PulsarConfigurationLoader.create((new FileInputStream(configFile)), ServiceConfiguration.class);
        PulsarConfigurationLoader.isComplete(config);
        // Set ZK server's host to localhost
        config.setZookeeperServers("127.0.0.1:" + zkPort);
        config.setGlobalZookeeperServers("127.0.0.1:" + zkPort);

        if (advertisedAddress != null) {
            // Use advertised address from command line
            config.setAdvertisedAddress(advertisedAddress);
        } else if (isBlank(config.getAdvertisedAddress())) {
            // Use advertised address as local hostname
            config.setAdvertisedAddress(ServiceConfigurationUtils.unsafeLocalhostResolve());
        } else {
            // Use advertised address from config file
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    if (broker != null) {
                        broker.close();
                    }

                    if (bkEnsemble != null) {
                        bkEnsemble.stop();
                    }
                } catch (Exception e) {
                    log.error("Shutdown failed: {}", e.getMessage());
                }
            }
        });
    }

    void start() throws Exception {

        if (config == null) {
            System.exit(1);
        }

        log.debug("--- setup PulsarStandaloneStarter ---");

        if (!onlyBroker) {
            // Start LocalBookKeeper
            bkEnsemble = new LocalBookkeeperEnsemble(numOfBk, zkPort, bkPort, zkDir, bkDir, wipeData);
            bkEnsemble.startStandalone();
        }

        if (noBroker) {
            return;
        }

        // load aspectj-weaver agent for instrumentation
        AgentLoader.loadAgentClass(Agent.class.getName(), null);

        // Start Broker
        broker = new PulsarService(config);
        broker.start();

        // Create a sample namespace
        URL webServiceUrl = new URL(
                String.format("http://%s:%d", config.getAdvertisedAddress(), config.getWebServicePort()));
        String brokerServiceUrl = String.format("pulsar://%s:%d", config.getAdvertisedAddress(),
                config.getBrokerServicePort());
        admin = new PulsarAdmin(webServiceUrl, config.getBrokerClientAuthenticationPlugin(),
                config.getBrokerClientAuthenticationParameters());
        String property = "sample";
        String cluster = config.getClusterName();
        String namespace = property + "/" + cluster + "/ns1";
        try {
            ClusterData clusterData = new ClusterData(webServiceUrl.toString(), null /* serviceUrlTls */,
                    brokerServiceUrl, null /* brokerServiceUrlTls */);
            if (!admin.clusters().getClusters().contains(cluster)) {
                admin.clusters().createCluster(cluster, clusterData);
            } else {
                admin.clusters().updateCluster(cluster, clusterData);
            }

            if (!admin.properties().getProperties().contains(property)) {
                admin.properties().createProperty(property,
                        new PropertyAdmin(Lists.newArrayList(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }

            if (!admin.namespaces().getNamespaces(property).contains(namespace)) {
                admin.namespaces().createNamespace(namespace);
            }
        } catch (PulsarAdminException e) {
            log.info(e.getMessage());
        }

        log.debug("--- setup completed ---");
    }

    public static void main(String args[]) throws Exception {
        // Start standalone
        PulsarStandaloneStarter standalone = new PulsarStandaloneStarter(args);
        standalone.start();
    }
}
