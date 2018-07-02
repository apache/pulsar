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
package org.apache.pulsar.tests.topologies;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.tests.containers.PulsarContainer.CS_PORT;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.containers.BKContainer;
import org.apache.pulsar.tests.containers.BrokerContainer;
import org.apache.pulsar.tests.containers.CSContainer;
import org.apache.pulsar.tests.containers.ProxyContainer;
import org.apache.pulsar.tests.containers.PulsarContainer;
import org.apache.pulsar.tests.containers.ZKContainer;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.Network;

/**
 * Pulsar Cluster in containers.
 */
@Slf4j
public class PulsarCluster {

    public static final String ADMIN_SCRIPT = "/pulsar/bin/pulsar-admin";
    public static final String CLIENT_SCRIPT = "/pulsar/bin/pulsar-client";

    /**
     * Pulsar Cluster Spec.
     *
     * @param spec pulsar cluster spec.
     * @return the built pulsar cluster
     */
    public static PulsarCluster forSpec(PulsarClusterSpec spec) {
        return new PulsarCluster(spec);
    }

    private final PulsarClusterSpec spec;
    @Getter
    private final String clusterName;
    private final Network network;
    private final ZKContainer zkContainer;
    private final CSContainer csContainer;
    private final Map<String, BKContainer> bookieContainers;
    private final Map<String, BrokerContainer> brokerContainers;
    private final ProxyContainer proxyContainer;

    private PulsarCluster(PulsarClusterSpec spec) {
        this.spec = spec;
        this.clusterName = spec.clusterName();
        this.network = Network.newNetwork();
        this.zkContainer = new ZKContainer(clusterName)
            .withNetwork(network)
            .withNetworkAliases(ZKContainer.NAME)
            .withEnv("clusterName", clusterName)
            .withEnv("zkServers", ZKContainer.NAME)
            .withEnv("configurationStore", CSContainer.NAME + ":" + CS_PORT)
            .withEnv("pulsarNode", "pulsar-broker-0");

        this.csContainer = new CSContainer(clusterName)
            .withNetwork(network)
            .withNetworkAliases(CSContainer.NAME);
        this.bookieContainers = Maps.newTreeMap();
        this.brokerContainers = Maps.newTreeMap();
        this.proxyContainer = new ProxyContainer(clusterName, "pulsar-proxy")
            .withNetwork(network)
            .withNetworkAliases("pulsar-proxy")
            .withEnv("zookeeperServers", ZKContainer.NAME)
            .withEnv("configurationStoreServers", CSContainer.NAME + ":" + CS_PORT)
            .withEnv("clusterName", clusterName);
    }

    public String getPlainTextServiceUrl() {
        return proxyContainer.getPlainTextServiceUrl();
    }

    public String getHttpServiceUrl() {
        return proxyContainer.getHttpServiceUrl();
    }

    public void start() throws Exception {
        // start the local zookeeper
        zkContainer.start();
        log.info("Successfully started local zookeeper container.");

        // start the configuration store
        csContainer.start();
        log.info("Successfully started configuration store container.");

        // init the cluster
        zkContainer.execCmd(
            "bin/init-cluster.sh");
        log.info("Successfully initialized the cluster.");

        // create bookies
        bookieContainers.putAll(
            runNumContainers("bookie", spec.numBookies(), (name) -> new BKContainer(clusterName, name)
                .withNetwork(network)
                .withNetworkAliases(name)
                .withEnv("zkServers", ZKContainer.NAME)
                .withEnv("useHostNameAsBookieID", "true")
                .withEnv("clusterName", clusterName)
            )
        );

        // create brokers
        brokerContainers.putAll(
            runNumContainers("broker", spec.numBrokers(), (name) -> new BrokerContainer(clusterName, name)
                .withNetwork(network)
                .withNetworkAliases(name)
                .withEnv("zookeeperServers", ZKContainer.NAME)
                .withEnv("configurationStoreServers", CSContainer.NAME + ":" + CS_PORT)
                .withEnv("clusterName", clusterName)
                .withEnv("brokerServiceCompactionMonitorIntervalInSeconds", "1")
            )
        );

        // create proxy
        proxyContainer.start();
        log.info("Successfully started pulsar proxy.");

        log.info("Pulsar cluster {} is up running:", clusterName);
        log.info("\tBinary Service Url : {}", getPlainTextServiceUrl());
        log.info("\tHttp Service Url : {}", getHttpServiceUrl());
    }

    private static <T extends PulsarContainer> Map<String, T> runNumContainers(String serviceName,
                                                                               int numContainers,
                                                                               Function<String, T> containerCreator) {
        List<CompletableFuture<?>> startFutures = Lists.newArrayList();
        Map<String, T> containers = Maps.newTreeMap();
        for (int i = 0; i < numContainers; i++) {
            String name = "pulsar-" + serviceName + "-" + i;
            T container = containerCreator.apply(name);
            containers.put(name, container);
            startFutures.add(CompletableFuture.runAsync(() -> container.start()));
        }
        CompletableFuture.allOf(startFutures.toArray(new CompletableFuture[startFutures.size()])).join();
        log.info("Successfully started {} {} containers", numContainers, serviceName);
        return containers;
    }

    public void stop() {
        proxyContainer.stop();
        brokerContainers.values().forEach(BrokerContainer::stop);
        bookieContainers.values().forEach(BKContainer::stop);
        csContainer.stop();
        zkContainer.stop();
        try {
            network.close();
        } catch (Exception e) {
            log.info("Failed to shutdown network for pulsar cluster {}", clusterName, e);
        }
    }

    public BrokerContainer getAnyBroker() {
        List<BrokerContainer> brokerList = Lists.newArrayList();
        brokerList.addAll(brokerContainers.values());
        Collections.shuffle(brokerList);
        checkArgument(!brokerList.isEmpty(), "No broker is alive");
        return brokerList.get(0);
    }

    public Collection<BrokerContainer> getBrokers() {
        return brokerContainers.values();
    }

    public ExecResult runAdminCommandOnAnyBroker(String...commands) throws Exception {
        BrokerContainer container = getAnyBroker();
        String[] cmds = new String[commands.length + 1];
        cmds[0] = ADMIN_SCRIPT;
        System.arraycopy(commands, 0, cmds, 1, commands.length);
        return container.execCmd(cmds);
    }

    public ExecResult createNamespace(String nsName) throws Exception {
        return runAdminCommandOnAnyBroker(
            "namespaces", "create", "public/" + nsName,
            "--clusters", clusterName);
    }

    public ExecResult enableDeduplication(String nsName, boolean enabled) throws Exception {
        return runAdminCommandOnAnyBroker(
            "namespaces", "set-deduplication", "public/" + nsName,
            enabled ? "--enable" : "--disable");
    }
}
