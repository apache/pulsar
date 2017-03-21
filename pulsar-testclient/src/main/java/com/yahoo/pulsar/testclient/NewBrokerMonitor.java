package com.yahoo.pulsar.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;
import com.yahoo.pulsar.broker.loadbalance.impl.NewLoadManagerImpl;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class NewBrokerMonitor {
    private static final String BROKER_ROOT = "/loadbalance/new-brokers";
    private static final int ZOOKEEPER_TIMEOUT_MILLIS = 5000;
    private final ZooKeeper zkClient;
    private static final Gson gson = new Gson();

    private static class BrokerWatcher implements Watcher {
        public final ZooKeeper zkClient;
        public Set<String> brokers;

        public BrokerWatcher(final ZooKeeper zkClient) {
            this.zkClient = zkClient;
            this.brokers = Collections.EMPTY_SET;
        }

        public synchronized void process(final WatchedEvent event) {
            try {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    updateBrokers(event.getPath());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public synchronized void updateBrokers(final String path) {
            final Set<String> newBrokers = new HashSet<>();
            try {
                newBrokers.addAll(zkClient.getChildren(path, this));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            for (String oldBroker: brokers) {
                if (!newBrokers.contains(oldBroker)) {
                    System.out.println("Lost broker: " + oldBroker);
                }
            }
            for (String newBroker: newBrokers) {
                if (!brokers.contains(newBroker)) {
                    System.out.println("Gained broker: " + newBroker);
                    final BrokerDataWatcher brokerDataWatcher = new BrokerDataWatcher(zkClient);
                    brokerDataWatcher.printBrokerData(path + "/" + newBroker);
                }
            }
            this.brokers = newBrokers;
        }
    }

    private static class BrokerDataWatcher implements Watcher {
        private final ZooKeeper zkClient;

        public BrokerDataWatcher(final ZooKeeper zkClient) {
            this.zkClient = zkClient;
        }

        public static String brokerNameFromPath(final String path) {
            return path.substring(path.lastIndexOf('/') + 1);
        }

        public synchronized void process(final WatchedEvent event) {
            try {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    final String broker = brokerNameFromPath(event.getPath());
                    printBrokerData(event.getPath());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private static void printMessageData(final double msgThroughputIn, final double msgThroughputOut,
                                             final double msgRateIn, final double msgRateOut) {
            System.out.format("Message Throughput In: %.2f KB/s\n", msgThroughputIn / 1024);
            System.out.format("Message Throughput Out: %.2f KB/s\n", msgThroughputOut / 1024);
            System.out.format("Message Rate In: %.2f msgs/s\n", msgRateIn);
            System.out.format("Message Rate Out: %.2f msgs/s\n", msgRateOut);
        }

        public synchronized void printBrokerData(final String brokerPath) {
            final String broker = brokerNameFromPath(brokerPath);
            final String timeAveragePath = NewLoadManagerImpl.TIME_AVERAGE_BROKER_ZPATH + "/" + broker;
            LocalBrokerData localBrokerData;
            try {
                localBrokerData = gson.fromJson(new String(zkClient.getData(brokerPath, this, null)), LocalBrokerData.class);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            System.out.println("\nBroker Data for " + broker + ":");
            System.out.println("---------------");


            System.out.println("\nNum Topics: " + localBrokerData.getNumTopics());
            System.out.println("Num Bundles: " + localBrokerData.getNumBundles());
            System.out.println("Num Consumers: " + localBrokerData.getNumConsumers());
            System.out.println("Num Producers: " + localBrokerData.getNumProducers());

            System.out.println(String.format("\nCPU: %.2f%%", localBrokerData.getCpu().percentUsage()));

            System.out.println(String.format("Memory: %.2f%%", localBrokerData.getMemory().percentUsage()));

            System.out.println(String.format("Direct Memory: %.2f%%", localBrokerData.getDirectMemory().percentUsage()));

            System.out.println("\nLatest Data:\n");
            printMessageData(localBrokerData.getMsgThroughputIn(), localBrokerData.getMsgThroughputOut(),
                    localBrokerData.getMsgRateIn(), localBrokerData.getMsgRateOut());

            TimeAverageBrokerData timeAverageData;
            try {
                timeAverageData = gson.fromJson(new String(zkClient.getData(timeAveragePath, null, null)),
                        TimeAverageBrokerData.class);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            System.out.println("\nShort Term Data:\n");
            printMessageData(timeAverageData.getShortTermMsgThroughputIn(),
                    timeAverageData.getShortTermMsgThroughputOut(), timeAverageData.getShortTermMsgRateIn(),
                    timeAverageData.getShortTermMsgRateOut());

            System.out.println("\nLong Term Data:\n");
            printMessageData(timeAverageData.getLongTermMsgThroughputIn(),
                    timeAverageData.getLongTermMsgThroughputOut(), timeAverageData.getLongTermMsgRateIn(),
                    timeAverageData.getLongTermMsgRateOut());


            System.out.println();
            if (!localBrokerData.getLastBundleGains().isEmpty()) {
                for (String bundle: localBrokerData.getLastBundleGains()) {
                    System.out.println("Gained Bundle: " + bundle);
                }
                System.out.println();
            }
            if (!localBrokerData.getLastBundleLosses().isEmpty()) {
                for (String bundle: localBrokerData.getLastBundleLosses()) {
                    System.out.println("Lost Bundle: " + bundle);
                }
                System.out.println();
            }
        }
    }

    static class Arguments {
        @Parameter(names = {"--connect-string"}, description = "Zookeeper connect string", required = true)
        public String connectString = null;
    }

    public NewBrokerMonitor(final ZooKeeper zkClient) {
        this.zkClient = zkClient;
    }

    private void start() {
        try {
            final BrokerWatcher brokerWatcher = new BrokerWatcher(zkClient);
            brokerWatcher.updateBrokers(BROKER_ROOT);
            while (true) {}
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String[] args) {
        try {
            final Arguments arguments = new Arguments();
            final JCommander jc = new JCommander(arguments);
            jc.parse(args);
            final ZooKeeper zkClient = new ZooKeeper(arguments.connectString, ZOOKEEPER_TIMEOUT_MILLIS, null);
            final NewBrokerMonitor monitor = new NewBrokerMonitor(zkClient);
            monitor.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
