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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * To use the monitor, simply start one via pulsar-perf monitor --connect-string <zk hostname>:<zk port> You will then
 * receive updates in LoadReports as they occur.
 */
public class SimpleLoadManagerBrokerMonitor {
    private static final Logger log = LoggerFactory.getLogger(SimpleLoadManagerBrokerMonitor.class);
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
                    final LoadReportWatcher loadReportWatcher = new LoadReportWatcher(zkClient);
                    loadReportWatcher.printLoadReport(path + "/" + newBroker);
                }
            }
            this.brokers = newBrokers;
        }
    }

    private static class LoadReportWatcher implements Watcher {
        private final ZooKeeper zkClient;

        public LoadReportWatcher(final ZooKeeper zkClient) {
            this.zkClient = zkClient;
        }

        public synchronized void process(final WatchedEvent event) {
            try {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    printLoadReport(event.getPath());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public synchronized void printLoadReport(final String path) {
            final String brokerName = path.substring(path.lastIndexOf('/') + 1);
            LoadReport loadReport;
            try {
                loadReport = gson.fromJson(new String(zkClient.getData(path, this, null)), LoadReport.class);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            final SystemResourceUsage resourceUsage = loadReport.getSystemResourceUsage();

            log.info("Load Report for " + brokerName + ":");
            log.info("---------------");

            log.info("Num Topics: " + loadReport.getNumTopics());
            log.info("Num Bundles: " + loadReport.getNumBundles());

            log.info(String.format("Raw CPU: %.2f%%", resourceUsage.getCpu().percentUsage()));
            log.info(String.format("Allocated CPU: %.2f%%",
                    percentUsage(loadReport.getAllocatedCPU(), resourceUsage.getCpu().limit)));
            log.info(String.format("Preallocated CPU: %.2f%%",
                    percentUsage(loadReport.getPreAllocatedCPU(), resourceUsage.getCpu().limit)));

            log.info(String.format("Raw Memory: %.2f%%", resourceUsage.getMemory().percentUsage()));
            log.info(String.format("Allocated Memory: %.2f%%",
                    percentUsage(loadReport.getAllocatedMemory(), resourceUsage.getMemory().limit)));
            log.info(String.format("Preallocated Memory: %.2f%%",
                    percentUsage(loadReport.getPreAllocatedMemory(), resourceUsage.getMemory().limit)));

            log.info(String.format("Raw Bandwidth In: %.2f%%", resourceUsage.getBandwidthIn().percentUsage()));
            log.info(String.format("Allocated Bandwidth In: %.2f%%",
                    percentUsage(loadReport.getAllocatedBandwidthIn(), resourceUsage.getBandwidthIn().limit)));
            log.info(String.format("Preallocated Bandwidth In: %.2f%%",
                    percentUsage(loadReport.getPreAllocatedBandwidthIn(), resourceUsage.getBandwidthIn().limit)));

            log.info(String.format("Raw Bandwidth Out: %.2f%%", resourceUsage.getBandwidthOut().percentUsage()));
            log.info(String.format("Allocated Bandwidth Out: %.2f%%",
                    percentUsage(loadReport.getAllocatedBandwidthOut(), resourceUsage.getBandwidthOut().limit)));
            log.info(String.format("Preallocated Bandwidth Out: %.2f%%",
                    percentUsage(loadReport.getPreAllocatedBandwidthOut(), resourceUsage.getBandwidthOut().limit)));

            log.info(String.format("Direct Memory: %.2f%%", resourceUsage.getDirectMemory().percentUsage()));

            log.info(String.format("Messages In Per Second: %.2f", loadReport.getMsgRateIn()));
            log.info(String.format("Messages Out Per Second: %.2f", loadReport.getMsgRateOut()));
            log.info(String.format("Preallocated Messages In Per Second: %.2f", loadReport.getPreAllocatedMsgRateIn()));
            log.info(String.format("Preallocated Out Per Second: %.2f", loadReport.getPreAllocatedMsgRateOut()));

            if (!loadReport.getBundleGains().isEmpty()) {
                for (String bundle : loadReport.getBundleGains()) {
                    log.info("Gained Bundle: " + bundle);
                }
            }
            if (!loadReport.getBundleLosses().isEmpty()) {
                for (String bundle : loadReport.getBundleLosses()) {
                    log.info("Lost Bundle: " + bundle);
                }
            }
        }
    }

    static class Arguments {
        @Parameter(names = { "--connect-string" }, description = "Zookeeper connect string", required = true)
        public String connectString = null;
    }

    public SimpleLoadManagerBrokerMonitor(final ZooKeeper zkClient) {
        this.zkClient = zkClient;
    }

    private static double percentUsage(final double usage, final double limit) {
        return limit > 0 && usage >= 0 ? 100 * Math.min(1, usage / limit) : 0;
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
            final SimpleLoadManagerBrokerMonitor monitor = new SimpleLoadManagerBrokerMonitor(zkClient);
            monitor.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
