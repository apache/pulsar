/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.testclient;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;
import com.yahoo.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModularLoadManagerBrokerMonitor {
    private static final Logger log = LoggerFactory.getLogger(ModularLoadManagerImpl.class);
    private static final String BROKER_ROOT = "/loadbalance/brokers";
    private static final int ZOOKEEPER_TIMEOUT_MILLIS = 5000;
    private final ZooKeeper zkClient;
    private static final Gson gson = new Gson();

    private static class BrokerWatcher implements Watcher {
        public final ZooKeeper zkClient;
        public Set<String> brokers;

        public BrokerWatcher(final ZooKeeper zkClient) {
            this.zkClient = zkClient;
            this.brokers = Collections.emptySet();
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
            for (String oldBroker : brokers) {
                if (!newBrokers.contains(oldBroker)) {
                    log.info("Lost broker: " + oldBroker);
                }
            }
            for (String newBroker : newBrokers) {
                if (!brokers.contains(newBroker)) {
                    log.info("Gained broker: " + newBroker);
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
            log.info(String.format("Message Throughput In: %.2f KB/s", msgThroughputIn / 1024));
            log.info(String.format("Message Throughput Out: %.2f KB/s", msgThroughputOut / 1024));
            log.info(String.format("Message Rate In: %.2f msgs/s", msgRateIn));
            log.info(String.format("Message Rate Out: %.2f msgs/s", msgRateOut));
        }

        public synchronized void printBrokerData(final String brokerPath) {
            final String broker = brokerNameFromPath(brokerPath);
            final String timeAveragePath = ModularLoadManagerImpl.TIME_AVERAGE_BROKER_ZPATH + "/" + broker;
            LocalBrokerData localBrokerData;
            try {
                localBrokerData = gson.fromJson(new String(zkClient.getData(brokerPath, this, null)),
                        LocalBrokerData.class);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            log.info("Broker Data for " + broker + ":");
            log.info("---------------");

            log.info("Num Topics: " + localBrokerData.getNumTopics());
            log.info("Num Bundles: " + localBrokerData.getNumBundles());
            log.info("Num Consumers: " + localBrokerData.getNumConsumers());
            log.info("Num Producers: " + localBrokerData.getNumProducers());

            log.info(String.format("CPU: %.2f%%", localBrokerData.getCpu().percentUsage()));

            log.info(String.format("Memory: %.2f%%", localBrokerData.getMemory().percentUsage()));

            log.info(String.format("Direct Memory: %.2f%%", localBrokerData.getDirectMemory().percentUsage()));

            log.info("Latest Data:");
            printMessageData(localBrokerData.getMsgThroughputIn(), localBrokerData.getMsgThroughputOut(),
                    localBrokerData.getMsgRateIn(), localBrokerData.getMsgRateOut());

            TimeAverageBrokerData timeAverageData;
            try {
                timeAverageData = gson.fromJson(new String(zkClient.getData(timeAveragePath, null, null)),
                        TimeAverageBrokerData.class);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            log.info("Short Term Data:");
            printMessageData(timeAverageData.getShortTermMsgThroughputIn(),
                    timeAverageData.getShortTermMsgThroughputOut(), timeAverageData.getShortTermMsgRateIn(),
                    timeAverageData.getShortTermMsgRateOut());

            log.info("Long Term Data:");
            printMessageData(timeAverageData.getLongTermMsgThroughputIn(),
                    timeAverageData.getLongTermMsgThroughputOut(), timeAverageData.getLongTermMsgRateIn(),
                    timeAverageData.getLongTermMsgRateOut());

            if (!localBrokerData.getLastBundleGains().isEmpty()) {
                for (String bundle : localBrokerData.getLastBundleGains()) {
                    log.info("Gained Bundle: " + bundle);
                }
            }
            if (!localBrokerData.getLastBundleLosses().isEmpty()) {
                for (String bundle : localBrokerData.getLastBundleLosses()) {
                    log.info("Lost Bundle: " + bundle);
                }
            }
        }
    }

    static class Arguments {
        @Parameter(names = { "--connect-string" }, description = "Zookeeper connect string", required = true)
        public String connectString = null;
    }

    public ModularLoadManagerBrokerMonitor(final ZooKeeper zkClient) {
        this.zkClient = zkClient;
    }

    private void start() {
        try {
            final BrokerWatcher brokerWatcher = new BrokerWatcher(zkClient);
            brokerWatcher.updateBrokers(BROKER_ROOT);
            while (true) {
            }
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
            final ModularLoadManagerBrokerMonitor monitor = new ModularLoadManagerBrokerMonitor(zkClient);
            monitor.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
