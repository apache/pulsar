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
package org.apache.pulsar.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a shell for the user to dictate how simulation clients should incur load.
 */
public class LoadSimulationController {
    private static final Logger log = LoggerFactory.getLogger(LoadSimulationController.class);
    private static final String QUOTA_ROOT = "/loadbalance/resource-quota/namespace";
    private static final String BUNDLE_DATA_ROOT = "/loadbalance/bundle-data";

    // Input streams for each client to send commands through.
    private final DataInputStream[] inputStreams;

    // Output streams for each client to receive information from.
    private final DataOutputStream[] outputStreams;

    // client host names.
    private final String[] clients;

    // Port clients are listening on.
    private final int clientPort;

    // The ZooKeeper cluster to run on.
    private final String cluster;

    private final Random random;

    private static final ExecutorService threadPool = Executors.newCachedThreadPool();

    // JCommander arguments for starting a controller via main.
    @Parameters(commandDescription = "Provides a shell for the user to dictate how simulation clients should "
            + "incur load.")
    private static class MainArguments {
        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "--cluster" }, description = "Cluster to test on", required = true)
        String cluster;

        @Parameter(names = { "--clients" }, description = "Comma separated list of client hostnames", required = true)
        String clientHostNames;

        @Parameter(names = { "--client-port" }, description = "Port that the clients are listening on", required = true)
        int clientPort;
    }

    // JCommander arguments for accepting user input.
    private static class ShellArguments {
        @Parameter(description = "Command arguments:\n" + "trade tenant namespace topic\n"
                + "change tenant namespace topic\n" + "stop tenant namespace topic\n"
                + "trade_group tenant group_name num_namespaces\n" + "change_group tenant group_name\n"
                + "stop_group tenant group_name\n" + "script script_name\n" + "copy tenant_name source_zk target_zk\n"
                + "stream source_zk\n" + "simulate zk\n", required = true)
        List<String> commandArguments;

        @Parameter(names = { "--rand-rate" }, description = "Choose message rate uniformly randomly from the next two "
                + "comma separated values (overrides --rate)")
        String rangeString = "";

        @Parameter(names = { "--rate" }, description = "Messages per second")
        double rate = 1;

        @Parameter(names = { "--rate-multiplier" }, description = "Multiplier to use for copying or streaming rates")
        double rateMultiplier = 1;

        @Parameter(names = { "--separation" }, description = "Separation time in ms for trade_group actions "
                + "(0 for no separation)")
        int separation = 0;

        @Parameter(names = { "--size" }, description = "Message size in bytes")
        int size = 1024;

        @Parameter(names = { "--topics-per-namespace" }, description = "Number of topics to create per namespace in "
                + "trade_group (total number of topics is num_namespaces X num_topics)")
        int topicsPerNamespace = 1;
    }

    // In stream mode, the BrokerWatcher watches the /loadbalance/broker zpath and adds LoadReportWatchers accordingly
    // when new brokers come up.
    private class BrokerWatcher implements Watcher {
        private final ZooKeeper zkClient;

        // Currently observed brokers.
        private final Set<String> brokers;

        // Shell arguments to configure streaming with.
        private final ShellArguments arguments;

        private BrokerWatcher(final ZooKeeper zkClient, final ShellArguments arguments) {
            this.zkClient = zkClient;
            this.arguments = arguments;
            brokers = new HashSet<>();
            // Observe the currently active brokers and put a watch on the broker root.
            process(null);
        }

        // Add load report watchers for newly observed brokers.
        public synchronized void process(final WatchedEvent event) {
            try {
                final List<String> currentBrokers = zkClient.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT, this);
                for (final String broker : currentBrokers) {
                    if (!brokers.contains(broker)) {
                        new LoadReportWatcher(String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, broker),
                                zkClient, arguments);
                        brokers.add(broker);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    // In stream mode, the LoadReportWatcher watches the /loadbalance/broker children and adds or modifies topics with
    // suitable rates based on the most recent message rate and throughput information.
    private class LoadReportWatcher implements Watcher {
        private final ZooKeeper zkClient;
        private final String path;
        private final ShellArguments arguments;

        public LoadReportWatcher(final String path, final ZooKeeper zkClient, final ShellArguments arguments) {
            this.path = path;
            this.zkClient = zkClient;
            this.arguments = arguments;
            // Get initial topics and set this up as a watch by calling process.
            process(null);
        }

        // Update the message rate information for the bundles in a recently changed load report.
        public synchronized void process(final WatchedEvent event) {
            try {
                // Get the load report and put this back as a watch.
                final LoadReport loadReport = ObjectMapperFactory.getThreadLocal()
                        .readValue(zkClient.getData(path, this, null), LoadReport.class);
                for (final Map.Entry<String, NamespaceBundleStats> entry : loadReport.getBundleStats().entrySet()) {
                    final String bundle = entry.getKey();
                    final String namespace = bundle.substring(0, bundle.lastIndexOf('/'));
                    final String topic = String.format("%s/%s", namespace, "t");
                    final NamespaceBundleStats stats = entry.getValue();

                    // Approximate total message rate via average between in/out.
                    final double messageRate = arguments.rateMultiplier * (stats.msgRateIn + stats.msgRateOut) / 2;

                    // size = throughput / rate.
                    final int messageSize = (int) Math.ceil(arguments.rateMultiplier
                            * (stats.msgThroughputIn + stats.msgThroughputOut) / (2 * messageRate));

                    arguments.rate = messageRate;
                    arguments.size = messageSize;
                    // Try to modify the topic if it already exists. Otherwise, create it.
                    changeOrCreate(arguments, topic);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Create a LoadSimulationController with the given JCommander arguments.
     *
     * @param arguments
     *            Arguments to create from.
     */
    public LoadSimulationController(final MainArguments arguments) throws Exception {
        random = new Random();
        clientPort = arguments.clientPort;
        cluster = arguments.cluster;
        clients = arguments.clientHostNames.split(",");
        final Socket[] sockets = new Socket[clients.length];
        inputStreams = new DataInputStream[clients.length];
        outputStreams = new DataOutputStream[clients.length];
        log.info("Found {} clients:", clients.length);
        for (int i = 0; i < clients.length; ++i) {
            sockets[i] = new Socket(clients[i], clientPort);
            inputStreams[i] = new DataInputStream(sockets[i].getInputStream());
            outputStreams[i] = new DataOutputStream(sockets[i].getOutputStream());
            log.info("Connected to {}", clients[i]);
        }
    }

    // Check that the expected number of application arguments matches the
    // actual number of application arguments.
    private boolean checkAppArgs(final int numAppArgs, final int numRequired) {
        if (numAppArgs != numRequired) {
            log.info("ERROR: Wrong number of application arguments (found {}, required {})", numAppArgs, numRequired);
            return false;
        }
        return true;
    }

    // Recursively acquire all resource quotas by getting the ZK children of the given path and calling this function
    // on the children if there are any, or getting the data from this ZNode otherwise.
    private void getResourceQuotas(final String path, final ZooKeeper zkClient,
            final Map<String, ResourceQuota>[] threadLocalMaps) throws Exception {
        final List<String> children = zkClient.getChildren(path, false);
        if (children.isEmpty()) {
            threadLocalMaps[random.nextInt(clients.length)].put(path, ObjectMapperFactory.getThreadLocal()
                    .readValue(zkClient.getData(path, false, null), ResourceQuota.class));
        } else {
            for (final String child : children) {
                getResourceQuotas(String.format("%s/%s", path, child), zkClient, threadLocalMaps);
            }
        }
    }

    // Initialize a BundleData from a resource quota and configurations and modify the quota accordingly.
    private BundleData initializeBundleData(final ResourceQuota quota, final ShellArguments arguments) {
        final double messageRate = (quota.getMsgRateIn() + quota.getMsgRateOut()) / 2;
        final int messageSize = (int) Math.ceil((quota.getBandwidthIn() + quota.getBandwidthOut()) / (2 * messageRate));
        arguments.rate = messageRate * arguments.rateMultiplier;
        arguments.size = messageSize;
        final NamespaceBundleStats startingStats = new NamespaceBundleStats();

        // Modify the original quota so that new rates are set.
        final double modifiedRate = messageRate * arguments.rateMultiplier;
        final double modifiedBandwidth = (quota.getBandwidthIn() + quota.getBandwidthOut()) * arguments.rateMultiplier
                / 2;
        quota.setMsgRateIn(modifiedRate);
        quota.setMsgRateOut(modifiedRate);
        quota.setBandwidthIn(modifiedBandwidth);
        quota.setBandwidthOut(modifiedBandwidth);

        // Assume modified memory usage is comparable to the rate multiplier times the original usage.
        quota.setMemory(quota.getMemory() * arguments.rateMultiplier);
        startingStats.msgRateIn = quota.getMsgRateIn();
        startingStats.msgRateOut = quota.getMsgRateOut();
        startingStats.msgThroughputIn = quota.getBandwidthIn();
        startingStats.msgThroughputOut = quota.getBandwidthOut();
        final BundleData bundleData = new BundleData(10, 1000, startingStats);
        // Assume there is ample history for the bundle.
        bundleData.getLongTermData().setNumSamples(1000);
        bundleData.getShortTermData().setNumSamples(10);
        return bundleData;
    }

    // Makes a topic string from a tenant name, namespace name, and topic
    // name.
    private String makeTopic(final String tenant, final String namespace, final String topic) {
        return String.format("persistent://%s/%s/%s/%s", tenant, cluster, namespace, topic);
    }

    // Write options that are common to modifying and creating topics.
    private void writeProducerOptions(final DataOutputStream outputStream, final ShellArguments arguments,
            final String topic) throws Exception {
        if (!arguments.rangeString.isEmpty()) {
            // If --rand-rate was specified, extract the bounds by splitting on
            // the comma and parsing the resulting
            // doubles.
            final String[] splits = arguments.rangeString.split(",");
            if (splits.length != 2) {
                log.error("Argument to --rand-rate should be two comma-separated values");
                return;
            }
            final double first = Double.parseDouble(splits[0]);
            final double second = Double.parseDouble(splits[1]);
            final double min = Math.min(first, second);
            final double max = Math.max(first, second);
            arguments.rate = random.nextDouble() * (max - min) + min;
        }
        outputStream.writeUTF(topic);
        outputStream.writeInt(arguments.size);
        outputStream.writeDouble(arguments.rate);
    }

    // Change producer settings for a given topic and JCommander arguments.
    private void change(final ShellArguments arguments, final String topic, final int client) throws Exception {
        outputStreams[client].write(LoadSimulationClient.CHANGE_COMMAND);
        writeProducerOptions(outputStreams[client], arguments, topic);
        outputStreams[client].flush();
    }

    // Change an existing topic, or create it if it does not exist.
    private int changeOrCreate(final ShellArguments arguments, final String topic) throws Exception {
        final int client = find(topic);
        if (client == -1) {
            trade(arguments, topic, random.nextInt(clients.length));
        } else {
            change(arguments, topic, client);
        }
        return client;
    }

    // Find a topic and change it if it exists.
    private int changeIfExists(final ShellArguments arguments, final String topic) throws Exception {
        final int client = find(topic);
        if (client != -1) {
            change(arguments, topic, client);
        }
        return client;
    }

    // Attempt to find a topic on the clients.
    private int find(final String topic) throws Exception {
        int clientWithTopic = -1;
        for (int i = 0; i < clients.length; ++i) {
            outputStreams[i].write(LoadSimulationClient.FIND_COMMAND);
            outputStreams[i].writeUTF(topic);
        }
        for (int i = 0; i < clients.length; ++i) {
            if (inputStreams[i].readBoolean()) {
                clientWithTopic = i;
            }
        }
        return clientWithTopic;
    }

    // Trade using the arguments parsed via JCommander and the topic name.
    private synchronized void trade(final ShellArguments arguments, final String topic, final int client)
            throws Exception {
        // Decide which client to send to randomly to preserve statelessness of
        // the controller.
        outputStreams[client].write(LoadSimulationClient.TRADE_COMMAND);
        writeProducerOptions(outputStreams[client], arguments, topic);
        outputStreams[client].flush();
    }

    // Handle the command line arguments associated with the change command.
    private void handleChange(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        // Change expects three application arguments: tenant name, namespace name, and topic name.
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String topic = makeTopic(commandArguments.get(1), commandArguments.get(2),
                    commandArguments.get(3));
            if (changeIfExists(arguments, topic) == -1) {
                log.info("Topic {} not found", topic);
            }
        }
    }

    // Handle the command line arguments associated with the copy command.
    private void handleCopy(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        // Copy accepts 3 application arguments: Tenant name, source ZooKeeper and target ZooKeeper connect strings.
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String tenantName = commandArguments.get(1);
            final String sourceZKConnectString = commandArguments.get(2);
            final String targetZKConnectString = commandArguments.get(3);
            final ZooKeeper sourceZKClient = new ZooKeeper(sourceZKConnectString, 5000, null);
            final ZooKeeper targetZKClient = new ZooKeeper(targetZKConnectString, 5000, null);
            // Make a map for each thread to speed up the ZooKeeper writing process.
            final Map<String, ResourceQuota>[] threadLocalMaps = new Map[clients.length];
            for (int i = 0; i < clients.length; ++i) {
                threadLocalMaps[i] = new HashMap<>();
            }
            getResourceQuotas(QUOTA_ROOT, sourceZKClient, threadLocalMaps);
            final List<Future> futures = new ArrayList<>(clients.length);
            int i = 0;
            log.info("Copying...");
            for (final Map<String, ResourceQuota> bundleToQuota : threadLocalMaps) {
                final int j = i;
                futures.add(threadPool.submit(() -> {
                    for (final Map.Entry<String, ResourceQuota> entry : bundleToQuota.entrySet()) {
                        final String bundle = entry.getKey();
                        final ResourceQuota quota = entry.getValue();
                        // Simulation will send messages in and out at about the same rate, so just make the rate the
                        // average of in and out.

                        final int tenantStart = QUOTA_ROOT.length() + 1;
                        final int clusterStart = bundle.indexOf('/', tenantStart) + 1;
                        final String sourceTenant = bundle.substring(tenantStart, clusterStart - 1);
                        final int namespaceStart = bundle.indexOf('/', clusterStart) + 1;
                        final String sourceCluster = bundle.substring(clusterStart, namespaceStart - 1);
                        final String namespace = bundle.substring(namespaceStart, bundle.lastIndexOf('/'));
                        final String keyRangeString = bundle.substring(bundle.lastIndexOf('/') + 1);
                        // To prevent duplicate node issues for same namespace names in different clusters/tenants.
                        final String manglePrefix = String.format("%s-%s-%s", sourceCluster, sourceTenant,
                                keyRangeString);
                        final String mangledNamespace = String.format("%s-%s", manglePrefix, namespace);
                        final BundleData bundleData = initializeBundleData(quota, arguments);
                        final String oldAPITargetPath = String.format(
                                "/loadbalance/resource-quota/namespace/%s/%s/%s/0x00000000_0xffffffff", tenantName,
                                cluster, mangledNamespace);
                        final String newAPITargetPath = String.format(
                                "/loadbalance/bundle-data/%s/%s/%s/0x00000000_0xffffffff", tenantName, cluster,
                                mangledNamespace);
                        try {
                            ZkUtils.createFullPathOptimistic(targetZKClient, oldAPITargetPath,
                                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(quota),
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        } catch (KeeperException.NodeExistsException e) {
                            // Ignore already created nodes.
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        // Put the bundle data in the new ZooKeeper.
                        try {
                            ZkUtils.createFullPathOptimistic(targetZKClient, newAPITargetPath,
                                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(bundleData),
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        } catch (KeeperException.NodeExistsException e) {
                            // Ignore already created nodes.
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        try {
                            trade(arguments, makeTopic(tenantName, mangledNamespace, "t"), j);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }));
                ++i;
            }
            for (final Future future : futures) {
                future.get();
            }
            sourceZKClient.close();
            targetZKClient.close();
        }
    }

    // Handle the command line arguments associated with the simulate command.
    private void handleSimulate(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        checkAppArgs(commandArguments.size() - 1, 1);
        final ZooKeeper zkClient = new ZooKeeper(commandArguments.get(1), 5000, null);
        // Make a map for each thread to speed up the ZooKeeper writing process.
        final Map<String, ResourceQuota>[] threadLocalMaps = new Map[clients.length];
        for (int i = 0; i < clients.length; ++i) {
            threadLocalMaps[i] = new HashMap<>();
        }
        getResourceQuotas(QUOTA_ROOT, zkClient, threadLocalMaps);
        final List<Future> futures = new ArrayList<>(clients.length);
        int i = 0;
        log.info("Simulating...");
        for (final Map<String, ResourceQuota> bundleToQuota : threadLocalMaps) {
            final int j = i;
            futures.add(threadPool.submit(() -> {
                for (final Map.Entry<String, ResourceQuota> entry : bundleToQuota.entrySet()) {
                    final String bundle = entry.getKey();
                    final String newAPIPath = bundle.replace(QUOTA_ROOT, BUNDLE_DATA_ROOT);
                    final ResourceQuota quota = entry.getValue();
                    final int tenantStart = QUOTA_ROOT.length() + 1;
                    final String topic = String.format("persistent://%s/t", bundle.substring(tenantStart));
                    final BundleData bundleData = initializeBundleData(quota, arguments);
                    // Put the bundle data in the new ZooKeeper.
                    try {
                        ZkUtils.createFullPathOptimistic(zkClient, newAPIPath,
                                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(bundleData),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {
                        try {
                            zkClient.setData(newAPIPath,
                                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(bundleData), -1);
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        trade(arguments, topic, j);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }));
            ++i;
        }
        for (final Future future : futures) {
            future.get();
        }
        zkClient.close();
    }

    // Handle the command line arguments associated with the stop command.
    private void handleStop(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        // Stop expects three application arguments: tenant name, namespace
        // name, and topic name.
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String topic = makeTopic(commandArguments.get(1), commandArguments.get(2),
                    commandArguments.get(3));
            for (DataOutputStream outputStream : outputStreams) {
                outputStream.write(LoadSimulationClient.STOP_COMMAND);
                outputStream.writeUTF(topic);
                outputStream.flush();
            }
        }
    }

    // Handle the command line arguments associated with the stream command.
    private void handleStream(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        // Stream accepts 1 application argument: ZooKeeper connect string.
        if (checkAppArgs(commandArguments.size() - 1, 1)) {
            final String zkConnectString = commandArguments.get(1);
            final ZooKeeper zkClient = new ZooKeeper(zkConnectString, 5000, null);
            new BrokerWatcher(zkClient, arguments);
            // This controller will now stream rate changes from the given ZK.
            // Users wishing to stop this should Ctrl + C and use another
            // Controller to send new commands.
            while (true) {}
        }
    }

    // Handle the command line arguments associated with the trade command.
    private void handleTrade(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        // Trade expects three application arguments: tenant, namespace, and
        // topic.
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String topic = makeTopic(commandArguments.get(1), commandArguments.get(2),
                    commandArguments.get(3));
            trade(arguments, topic, random.nextInt(clients.length));
        }
    }

    // Handle the command line arguments associated with the group change command.
    private void handleGroupChange(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        // Group change expects two application arguments: tenant name and group
        // name.
        if (checkAppArgs(commandArguments.size() - 1, 2)) {
            final String tenant = commandArguments.get(1);
            final String group = commandArguments.get(2);
            for (DataOutputStream outputStream : outputStreams) {
                outputStream.write(LoadSimulationClient.CHANGE_GROUP_COMMAND);
                outputStream.writeUTF(tenant);
                outputStream.writeUTF(group);
                outputStream.writeInt(arguments.size);
                outputStream.writeDouble(arguments.rate);
                outputStream.flush();
            }
        }
    }

    // Handle the command line arguments associated with the group stop command.
    private void handleGroupStop(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        // Group stop requires two application arguments: tenant name and group
        // name.
        if (checkAppArgs(commandArguments.size() - 1, 2)) {
            final String tenant = commandArguments.get(1);
            final String group = commandArguments.get(2);
            for (DataOutputStream outputStream : outputStreams) {
                outputStream.write(LoadSimulationClient.STOP_GROUP_COMMAND);
                outputStream.writeUTF(tenant);
                outputStream.writeUTF(group);
                outputStream.flush();
            }
        }
    }

    // Handle the command line arguments associated with the group trade command.
    private void handleGroupTrade(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        // Group trade expects 3 application arguments: tenant name, group name,
        // and number of namespaces.
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String tenant = commandArguments.get(1);
            final String group = commandArguments.get(2);
            final int numNamespaces = Integer.parseInt(commandArguments.get(3));
            for (int i = 0; i < numNamespaces; ++i) {
                for (int j = 0; j < arguments.topicsPerNamespace; ++j) {
                    // For each namespace and topic pair, create the namespace
                    // by using the group name and the
                    // namespace index, and then create the topic by using the
                    // topic index. Then just call trade.
                    final String topic = makeTopic(tenant, String.format("%s-%d", group, i),
                            Integer.toString(j));
                    trade(arguments, topic, random.nextInt(clients.length));
                    Thread.sleep(arguments.separation);
                }
            }
        }
    }

    /**
     * Read the user-submitted arguments as commands to send to clients.
     *
     * @param args
     *            Arguments split on whitespace from user input.
     */
    private void read(final String[] args) {
        // Don't attempt to process blank input.
        if (args.length > 0 && !(args.length == 1 && args[0].isEmpty())) {
            final ShellArguments arguments = new ShellArguments();
            final JCommander jc = new JCommander(arguments);
            try {
                jc.parse(args);
                final String command = arguments.commandArguments.get(0);
                switch (command) {
                case "trade":
                    handleTrade(arguments);
                    break;
                case "change":
                    handleChange(arguments);
                    break;
                case "stop":
                    handleStop(arguments);
                    break;
                case "trade_group":
                    handleGroupTrade(arguments);
                    break;
                case "change_group":
                    handleGroupChange(arguments);
                    break;
                case "stop_group":
                    handleGroupStop(arguments);
                    break;
                case "script":
                    // Read input from the given script instead of stdin until
                    // the script has executed completely.
                    final List<String> commandArguments = arguments.commandArguments;
                    checkAppArgs(commandArguments.size() - 1, 1);
                    final String scriptName = commandArguments.get(1);
                    final BufferedReader scriptReader = new BufferedReader(
                            new InputStreamReader(new FileInputStream(Paths.get(scriptName).toFile())));
                    String line = scriptReader.readLine();
                    while (line != null) {
                        read(line.split("\\s+"));
                        line = scriptReader.readLine();
                    }
                    scriptReader.close();
                    break;
                case "copy":
                    handleCopy(arguments);
                    break;
                case "stream":
                    handleStream(arguments);
                    break;
                case "simulate":
                    handleSimulate(arguments);
                    break;
                case "quit":
                case "exit":
                    PerfClientUtils.exit(0);
                    break;
                default:
                    log.info("ERROR: Unknown command \"{}\"", command);
                }
            } catch (ParameterException ex) {
                ex.printStackTrace();
                jc.usage();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Create a shell for the user to send commands to clients.
     */
    public void run() throws Exception {
        BufferedReader inReader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            // Print the very simple prompt.
            System.out.println();
            System.out.print("> ");
            read(inReader.readLine().split("\\s+"));
        }
    }

    /**
     * Start a controller with command line arguments.
     *
     * @param args
     *            Arguments to pass in.
     */
    public static void main(String[] args) throws Exception {
        final MainArguments arguments = new MainArguments();
        final JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf simulation-controller");
        try {
            jc.parse(args);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            jc.usage();
            PerfClientUtils.exit(-1);
        }
        (new LoadSimulationController(arguments)).run();
    }
}
