package com.yahoo.pulsar.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;

/**
 * To use the monitor, simply start one via pulsar-perf monitor --connect-string
 * <zk hostname>:<zk port> You will then receive updates in LoadReports as they
 * occur.
 */
public class SimpleLoadManagerBrokerMonitor {
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
					System.out.println("Lost broker: " + oldBroker);
				}
			}
			for (String newBroker : newBrokers) {
				if (!brokers.contains(newBroker)) {
					System.out.println("Gained broker: " + newBroker);
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

			System.out.println("\nLoad Report for " + brokerName + ":");
			System.out.println("---------------");

			System.out.println("\nNum Topics: " + loadReport.getNumTopics());
			System.out.println("Num Bundles: " + loadReport.getNumBundles());

			System.out.format("\nRaw CPU: %.2f%%\n", resourceUsage.getCpu().percentUsage());
			System.out.println(String.format("Allocated CPU: %.2f%%",
					percentUsage(loadReport.getAllocatedCPU(), resourceUsage.getCpu().limit)));
			System.out.println(String.format("Preallocated CPU: %.2f%%",
					percentUsage(loadReport.getPreAllocatedCPU(), resourceUsage.getCpu().limit)));

			System.out.format("\nRaw Memory: %.2f%%\n", resourceUsage.getMemory().percentUsage());
			System.out.println(String.format("Allocated Memory: %.2f%%",
					percentUsage(loadReport.getAllocatedMemory(), resourceUsage.getMemory().limit)));
			System.out.println(String.format("Preallocated Memory: %.2f%%",
					percentUsage(loadReport.getPreAllocatedMemory(), resourceUsage.getMemory().limit)));

			System.out.format("\nRaw Bandwidth In: %.2f%%\n", resourceUsage.getBandwidthIn().percentUsage());
			System.out.println(String.format("Allocated Bandwidth In: %.2f%%",
					percentUsage(loadReport.getAllocatedBandwidthIn(), resourceUsage.getBandwidthIn().limit)));
			System.out.println(String.format("Preallocated Bandwidth In: %.2f%%",
					percentUsage(loadReport.getPreAllocatedBandwidthIn(), resourceUsage.getBandwidthIn().limit)));

			System.out.format("\nRaw Bandwidth Out: %.2f%%\n", resourceUsage.getBandwidthOut().percentUsage());
			System.out.println(String.format("Allocated Bandwidth Out: %.2f%%",
					percentUsage(loadReport.getAllocatedBandwidthOut(), resourceUsage.getBandwidthOut().limit)));
			System.out.println(String.format("Preallocated Bandwidth Out: %.2f%%",
					percentUsage(loadReport.getPreAllocatedBandwidthOut(), resourceUsage.getBandwidthOut().limit)));

			System.out.format("\nDirect Memory: %.2f%%\n", resourceUsage.getDirectMemory().percentUsage());

			System.out.format("Messages In Per Second: %.2f\n", loadReport.getMsgRateIn());
			System.out.format("Messages Out Per Second: %.2f\n", loadReport.getMsgRateOut());
			System.out.format("Preallocated Messages In Per Second: %.2f\n", loadReport.getPreAllocatedMsgRateIn());
			System.out.format("Preallocated Out Per Second: %.2f\n", loadReport.getPreAllocatedMsgRateOut());
			System.out.println();
			if (!loadReport.getBundleGains().isEmpty()) {
				for (String bundle : loadReport.getBundleGains()) {
					System.out.println("Gained Bundle: " + bundle);
				}
				System.out.println();
			}
			if (!loadReport.getBundleLosses().isEmpty()) {
				for (String bundle : loadReport.getBundleLosses()) {
					System.out.println("Lost Bundle: " + bundle);
				}
				System.out.println();
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
