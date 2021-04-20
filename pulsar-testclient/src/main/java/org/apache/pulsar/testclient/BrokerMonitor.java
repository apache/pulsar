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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pulsar.broker.TimeAverageBrokerData;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.testclient.utils.FixedColumnLengthTableMaker;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * Monitors brokers and prints to the console information about their system resource usages, their topic and bundle
 * counts, their message rates, and other metrics.
 */
public class BrokerMonitor {
    private static final Logger log = LoggerFactory.getLogger(BrokerMonitor.class);

    private static final String BROKER_ROOT = "/loadbalance/brokers";
    private static final int ZOOKEEPER_TIMEOUT_MILLIS = 30000;
    private static final int GLOBAL_STATS_PRINT_PERIOD_MILLIS = 60000;
    private final ZooKeeper zkClient;
    private static final Gson gson = new Gson();

    // Fields common for message rows.
    private static final List<Object> MESSAGE_FIELDS = Arrays.asList("MSG/S IN", "MSG/S OUT", "TOTAL", "KB/S IN",
            "KB/S OUT", "TOTAL");

    // Fields common for system rows.
    private static final List<Object> SYSTEM_FIELDS = Arrays.asList("CPU %", "MEMORY %", "DIRECT %", "BW IN %",
            "BW OUT %", "MAX %");

    private static final Object[] SYSTEM_ROW = makeSystemRow("SYSTEM");
    private static final Object[] COUNT_ROW = { "COUNT", "TOPIC", "BUNDLE", "PRODUCER", "CONSUMER", "BUNDLE +",
            "BUNDLE -" };
    private static final Object[] LATEST_ROW = makeMessageRow("LATEST");
    private static final Object[] SHORT_ROW = makeMessageRow("SHORT");
    private static final Object[] LONG_ROW = makeMessageRow("LONG");
    private static final Object[] RAW_SYSTEM_ROW = makeSystemRow("RAW SYSTEM");
    private static final Object[] ALLOC_SYSTEM_ROW = makeSystemRow("ALLOC SYSTEM");
    private static final Object[] RAW_MESSAGE_ROW = makeMessageRow("RAW MSG");
    private static final Object[] ALLOC_MESSAGE_ROW = makeMessageRow("ALLOC MSG");
    private static final Object[] GLOBAL_HEADER = { "BROKER", "BUNDLE", "MSG/S", "LONG/S", "KB/S", "MAX %" };

    private final Map<String, Object> loadData;

    private static final FixedColumnLengthTableMaker localTableMaker = new FixedColumnLengthTableMaker();
    static {
        // Makes the table length about 120.
        localTableMaker.elementLength = 14;
        localTableMaker.decimalFormatter = "%.2f";
    }

    private static final FixedColumnLengthTableMaker globalTableMaker = new FixedColumnLengthTableMaker();
    static {
        globalTableMaker.decimalFormatter = "%.2f";
        globalTableMaker.topBorder = '*';
        globalTableMaker.bottomBorder = '*';
        // Make broker column substantially longer than other columns.
        globalTableMaker.lengthFunction = column -> column == 0 ? 60 : 12;
    }

    // Take advantage of repeated labels in message rows.
    private static Object[] makeMessageRow(final String firstElement) {
        final List<Object> result = new ArrayList<>();
        result.add(firstElement);
        result.addAll(MESSAGE_FIELDS);
        return result.toArray();
    }

    // Take advantage of repeated labels in system rows.
    private static Object[] makeSystemRow(final String firstElement) {
        final List<Object> result = new ArrayList<>();
        result.add(firstElement);
        result.addAll(SYSTEM_FIELDS);
        return result.toArray();
    }

    // Helper method to initialize rows.
    private static void initRow(final Object[] row, final Object... elements) {
        System.arraycopy(elements, 0, row, 1, elements.length);
    }

    // Helper method to initialize rows which hold message data.
    private static void initMessageRow(final Object[] row, final double messageRateIn, final double messageRateOut,
            final double messageThroughputIn, final double messageThroughputOut) {
        initRow(row, messageRateIn, messageRateOut, messageRateIn + messageRateOut, messageThroughputIn / 1024,
                messageThroughputOut / 1024, (messageThroughputIn + messageThroughputOut) / 1024);
    }

    // Prints out the global load data.
    private void printGlobalData() {
        synchronized (loadData) {
            // 1 header row, 1 total row, and loadData.size() rows for brokers.
            Object[][] rows = new Object[loadData.size() + 2][];
            rows[0] = GLOBAL_HEADER;
            int totalBundles = 0;
            double totalThroughput = 0;
            double totalMessageRate = 0;
            double totalLongTermMessageRate = 0;
            double maxMaxUsage = 0;
            int i = 1;
            for (final Map.Entry<String, Object> entry : loadData.entrySet()) {
                final String broker = entry.getKey();
                final Object data = entry.getValue();
                rows[i] = new Object[GLOBAL_HEADER.length];
                rows[i][0] = broker;
                int numBundles;
                double messageRate;
                double longTermMessageRate;
                double messageThroughput;
                double maxUsage;
                if (data instanceof LoadReport) {
                    final LoadReport loadReport = (LoadReport) data;
                    numBundles = loadReport.getNumBundles();
                    messageRate = loadReport.getMsgRateIn() + loadReport.getMsgRateOut();
                    longTermMessageRate = loadReport.getAllocatedMsgRateIn() + loadReport.getAllocatedMsgRateOut();
                    messageThroughput = (loadReport.getAllocatedBandwidthIn() + loadReport.getAllocatedBandwidthOut())
                            / 1024;
                    final SystemResourceUsage systemResourceUsage = loadReport.getSystemResourceUsage();
                    maxUsage = Math.max(
                            Math.max(
                                    Math.max(systemResourceUsage.getCpu().percentUsage(),
                                            systemResourceUsage.getMemory().percentUsage()),
                                    Math.max(systemResourceUsage.getDirectMemory().percentUsage(),
                                            systemResourceUsage.getBandwidthIn().percentUsage())),
                            systemResourceUsage.getBandwidthOut().percentUsage());
                } else if (data instanceof LocalBrokerData) {
                    final LocalBrokerData localData = (LocalBrokerData) data;
                    numBundles = localData.getNumBundles();
                    messageRate = localData.getMsgRateIn() + localData.getMsgRateOut();
                    final String timeAveragePath = ModularLoadManagerImpl.TIME_AVERAGE_BROKER_ZPATH + "/" + broker;
                    try {
                        final TimeAverageBrokerData timeAverageData = gson.fromJson(
                                new String(zkClient.getData(timeAveragePath, false, null)), TimeAverageBrokerData.class);
                        longTermMessageRate = timeAverageData.getLongTermMsgRateIn() + timeAverageData.getLongTermMsgRateOut();
                    } catch ( Exception x ) {
                        throw new RuntimeException(x);
                    }
                    messageThroughput = (localData.getMsgThroughputIn() + localData.getMsgThroughputOut()) / 1024;
                    maxUsage = localData.getMaxResourceUsage();
                } else {
                    throw new AssertionError("Unreachable code");
                }

                rows[i][1] = numBundles;
                rows[i][2] = messageRate;
                rows[i][3] = messageThroughput;
                rows[i][4] = longTermMessageRate;
                rows[i][5] = maxUsage;

                totalBundles += numBundles;
                totalMessageRate += messageRate;
                totalLongTermMessageRate += longTermMessageRate;
                totalThroughput += messageThroughput;
                maxMaxUsage = Math.max(maxUsage, maxMaxUsage);
                ++i;
            }
            final int finalRow = loadData.size() + 1;
            rows[finalRow] = new Object[GLOBAL_HEADER.length];
            rows[finalRow][0] = "TOTAL";
            rows[finalRow][1] = totalBundles;
            rows[finalRow][2] = totalMessageRate;
            rows[finalRow][3] = totalLongTermMessageRate;
            rows[finalRow][4] = totalThroughput;
            rows[finalRow][5] = maxMaxUsage;
            final String table = globalTableMaker.make(rows);
            log.info("Overall Broker Data:\n{}", table);
        }
    }

    // This watcher initializes data watchers whenever a new broker is found.
    private class BrokerWatcher implements Watcher {
        private final ZooKeeper zkClient;
        private Set<String> brokers;

        private BrokerWatcher(final ZooKeeper zkClient) {
            this.zkClient = zkClient;
            this.brokers = Collections.emptySet();
        }

        /**
         * Creates a watch for a newly acquired broker so that its data is printed whenever it is updated.
         * 
         * @param event
         *            The watched event.
         */
        public synchronized void process(final WatchedEvent event) {
            try {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    updateBrokers(event.getPath());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        // Inform the user of any broker gains and losses and put watches on newly acquired brokers.
        private synchronized void updateBrokers(final String path) {
            final Set<String> newBrokers = new HashSet<>();
            try {
                newBrokers.addAll(zkClient.getChildren(path, this));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            for (String oldBroker : brokers) {
                if (!newBrokers.contains(oldBroker)) {
                    log.info("Lost broker: " + oldBroker);
                    synchronized (loadData) {
                        // Stop including lost broker in global stats.
                        loadData.remove(oldBroker);
                    }
                }
            }
            for (String newBroker : newBrokers) {
                if (!brokers.contains(newBroker)) {
                    log.info("Gained broker: " + newBroker);
                    final BrokerDataWatcher brokerDataWatcher = new BrokerDataWatcher(zkClient);
                    brokerDataWatcher.printData(path + "/" + newBroker);
                }
            }
            this.brokers = newBrokers;
        }
    }

    // This watcher prints tabular data for a broker after its ZNode is updated.
    private class BrokerDataWatcher implements Watcher {
        private final ZooKeeper zkClient;

        private BrokerDataWatcher(final ZooKeeper zkClient) {
            this.zkClient = zkClient;
        }

        // Given the path to a broker ZNode, return the broker host name.
        private String brokerNameFromPath(final String path) {
            return path.substring(path.lastIndexOf('/') + 1);
        }

        /**
         * Print the local and historical broker data in a tabular format, and put this back as a watcher.
         * 
         * @param event
         *            The watched event.
         */
        public synchronized void process(final WatchedEvent event) {
            try {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    printData(event.getPath());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private double percentUsage(final double usage, final double limit) {
            return limit > 0 && usage >= 0 ? 100 * Math.min(1, usage / limit) : 0;
        }

        // Decide whether this broker is running SimpleLoadManagerImpl or ModularLoadManagerImpl and then print the data
        // accordingly.
        private synchronized void printData(final String path) {
            final String broker = brokerNameFromPath(path);
            String jsonString;
            try {
                jsonString = new String(zkClient.getData(path, this, null));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            // Use presence of the String "allocated" to determine if this is using SimpleLoadManagerImpl.
            if (jsonString.contains("allocated")) {
                printLoadReport(broker, gson.fromJson(jsonString, LoadReport.class));
            } else {
                final LocalBrokerData localBrokerData = gson.fromJson(jsonString, LocalBrokerData.class);
                final String timeAveragePath = ModularLoadManagerImpl.TIME_AVERAGE_BROKER_ZPATH + "/" + broker;
                try {
                    final TimeAverageBrokerData timeAverageData = gson.fromJson(
                            new String(zkClient.getData(timeAveragePath, false, null)), TimeAverageBrokerData.class);
                    printBrokerData(broker, localBrokerData, timeAverageData);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // Print the load report in a tabular form for a broker running SimpleLoadManagerImpl.
        private synchronized void printLoadReport(final String broker, final LoadReport loadReport) {
            loadData.put(broker, loadReport);

            // Initialize the constant rows.
            final Object[][] rows = new Object[10][];

            rows[0] = COUNT_ROW;
            rows[2] = RAW_SYSTEM_ROW;
            rows[4] = ALLOC_SYSTEM_ROW;
            rows[6] = RAW_MESSAGE_ROW;
            rows[8] = ALLOC_MESSAGE_ROW;

            // First column is a label, so start at the second column at index 1.
            // Client count row.
            rows[1] = new Object[COUNT_ROW.length];
            initRow(rows[1], loadReport.getNumTopics(), loadReport.getNumBundles(), loadReport.getNumProducers(),
                    loadReport.getNumConsumers(), loadReport.getBundleGains().size(),
                    loadReport.getBundleLosses().size());

            // Raw system row.
            final SystemResourceUsage systemResourceUsage = loadReport.getSystemResourceUsage();
            final ResourceUsage cpu = systemResourceUsage.getCpu();
            final ResourceUsage memory = systemResourceUsage.getMemory();
            final ResourceUsage directMemory = systemResourceUsage.getDirectMemory();
            final ResourceUsage bandwidthIn = systemResourceUsage.getBandwidthIn();
            final ResourceUsage bandwidthOut = systemResourceUsage.getBandwidthOut();
            final double maxUsage = Math.max(
                    Math.max(Math.max(cpu.percentUsage(), memory.percentUsage()),
                            Math.max(directMemory.percentUsage(), bandwidthIn.percentUsage())),
                    bandwidthOut.percentUsage());
            rows[3] = new Object[RAW_SYSTEM_ROW.length];
            initRow(rows[3], cpu.percentUsage(), memory.percentUsage(), directMemory.percentUsage(),
                    bandwidthIn.percentUsage(), bandwidthOut.percentUsage(), maxUsage);

            // Allocated system row.
            rows[5] = new Object[ALLOC_SYSTEM_ROW.length];
            final double allocatedCpuUsage = percentUsage(loadReport.getAllocatedCPU(), cpu.limit);
            final double allocatedMemoryUsage = percentUsage(loadReport.getAllocatedMemory(), memory.limit);
            final double allocatedBandwidthInUsage = percentUsage(loadReport.getAllocatedBandwidthIn(),
                    bandwidthIn.limit);
            final double allocatedBandwidthOutUsage = percentUsage(loadReport.getAllocatedBandwidthOut(),
                    bandwidthOut.limit);
            final double maxAllocatedUsage = Math.max(
                    Math.max(Math.max(allocatedCpuUsage, allocatedMemoryUsage), allocatedBandwidthInUsage),
                    allocatedBandwidthOutUsage);
            initRow(rows[5], allocatedCpuUsage, allocatedMemoryUsage, null, allocatedBandwidthInUsage,
                    allocatedBandwidthOutUsage, maxAllocatedUsage);

            // Raw message row.
            rows[7] = new Object[RAW_MESSAGE_ROW.length];
            initMessageRow(rows[7], loadReport.getMsgRateIn(), loadReport.getMsgRateOut(), bandwidthIn.usage,
                    bandwidthOut.usage);

            // Allocated message row.
            rows[9] = new Object[ALLOC_MESSAGE_ROW.length];
            initMessageRow(rows[9], loadReport.getAllocatedMsgRateIn(), loadReport.getAllocatedMsgRateOut(),
                    loadReport.getAllocatedBandwidthIn(), loadReport.getAllocatedBandwidthOut());

            final String table = localTableMaker.make(rows);
            log.info("\nLoad Report for {}:\n{}\n", broker, table);
        }

        // Print the broker data in a tabular form for a broker using ModularLoadManagerImpl.
        private synchronized void printBrokerData(final String broker, final LocalBrokerData localBrokerData,
                final TimeAverageBrokerData timeAverageData) {
            loadData.put(broker, localBrokerData);

            // Initialize the constant rows.
            final Object[][] rows = new Object[10][];
            rows[0] = SYSTEM_ROW;
            rows[2] = COUNT_ROW;
            rows[4] = LATEST_ROW;
            rows[6] = SHORT_ROW;
            rows[8] = LONG_ROW;

            // First column is a label, so start at the second column at index 1.
            // System row.
            rows[1] = new Object[SYSTEM_ROW.length];
            initRow(rows[1], localBrokerData.getCpu().percentUsage(), localBrokerData.getMemory().percentUsage(),
                    localBrokerData.getDirectMemory().percentUsage(), localBrokerData.getBandwidthIn().percentUsage(),
                    localBrokerData.getBandwidthOut().percentUsage(), localBrokerData.getMaxResourceUsage() * 100);

            // Count row.
            rows[3] = new Object[COUNT_ROW.length];
            initRow(rows[3], localBrokerData.getNumTopics(), localBrokerData.getNumBundles(),
                    localBrokerData.getNumProducers(), localBrokerData.getNumConsumers(),
                    localBrokerData.getLastBundleGains().size(), localBrokerData.getLastBundleLosses().size());

            // Latest message data row.
            rows[5] = new Object[LATEST_ROW.length];
            initMessageRow(rows[5], localBrokerData.getMsgRateIn(), localBrokerData.getMsgRateOut(),
                    localBrokerData.getMsgThroughputIn(), localBrokerData.getMsgThroughputOut());

            // Short-term message data row.
            rows[7] = new Object[SHORT_ROW.length];
            initMessageRow(rows[7], timeAverageData.getShortTermMsgRateIn(), timeAverageData.getShortTermMsgRateOut(),
                    timeAverageData.getShortTermMsgThroughputIn(), timeAverageData.getShortTermMsgThroughputOut());

            // Long-term message data row.
            rows[9] = new Object[LONG_ROW.length];
            initMessageRow(rows[9], timeAverageData.getLongTermMsgRateIn(), timeAverageData.getLongTermMsgRateOut(),
                    timeAverageData.getLongTermMsgThroughputIn(), timeAverageData.getLongTermMsgThroughputOut());

            final String table = localTableMaker.make(rows);
            log.info("\nBroker Data for {}:\n{}\n", broker, table);
        }
    }

    // JCommander arguments class.
    @Parameters(commandDescription = "Monitors brokers and prints to the console information about their system resource usages," +
            "\ntheir topic and bundle counts, their message rates, and other metrics.")
    private static class Arguments {
        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "--connect-string" }, description = "Zookeeper connect string", required = true)
        public String connectString = null;
    }

    /**
     * Create a broker monitor from the given ZooKeeper client.
     * 
     * @param zkClient
     *            Client to create this from.
     */
    public BrokerMonitor(final ZooKeeper zkClient) {
        loadData = new ConcurrentHashMap<>();
        this.zkClient = zkClient;
    }

    /**
     * Start the broker monitoring procedure.
     */
    public void start() {
        try {
            final BrokerWatcher brokerWatcher = new BrokerWatcher(zkClient);
            brokerWatcher.updateBrokers(BROKER_ROOT);
            while (true) {
                Thread.sleep(GLOBAL_STATS_PRINT_PERIOD_MILLIS);
                printGlobalData();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Run a monitor from command line arguments.
     * 
     * @param args
     *            Arguments for the monitor.
     */
    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        final JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf monitor-brokers");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            PerfClientUtils.exit(-1);
        }
        final ZooKeeper zkClient = new ZooKeeper(arguments.connectString, ZOOKEEPER_TIMEOUT_MILLIS, null);
        final BrokerMonitor monitor = new BrokerMonitor(zkClient);
        monitor.start();
    }
}
