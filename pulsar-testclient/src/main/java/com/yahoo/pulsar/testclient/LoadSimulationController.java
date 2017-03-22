package com.yahoo.pulsar.testclient;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.common.policies.data.ResourceQuota;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;
import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;

/**
 * To use: 1. Delegate a list of server machines which act as zookeeper clients.
 * 2. Choose a port for those machines. 3. On each of these machines, get them
 * to listen via pulsar-perf simulation-server --port <chosen port>
 * --service-url <broker service url> 4. Start the controller with pulsar-perf
 * simulation-controller --cluster <cluster name> --servers <comma separated
 * list of <hostname>:<port> --server-port <chosen port> 5. You will get a shell
 * on the controller, where you can use the commands trade, change, stop,
 * trade_group, change_group, stop_group. You can enter "help" to see the syntax
 * for the commands. Note that tenant, namespace, and topic refer to
 * persistent://cluster/tenant/namespace/topic/bundle. For instance, to start
 * trading for topic with destination
 * persistent://mycluster/mytenant/mynamespace/mytopic/bundle at rate 200
 * msgs/s, you would type "trade mytenant mynamespace mytopic --rate 200". The
 * group commands also refer to a "group_name" parameter. This is a string that
 * is prefixed to the namespaces when trade_group is invoked so they may be
 * identified by other group commands. At the moment, groups may not be modified
 * after they have been created via trade_group.
 *
 */
public class LoadSimulationController {
	private final static String QUOTA_ROOT = "/loadbalance/resource-quota/namespace";

	// Input streams for each server to send commands through.
	private final DataInputStream[] inputStreams;

	// Output streams for each server to receive information from.
	private final DataOutputStream[] outputStreams;

	// Server host names.
	private final String[] servers;

	// Port servers are listening on.
	private final int serverPort;

	// The ZooKeeper cluster to run on.
	private final String cluster;

	private final Random random;

	// JCommander arguments for starting a controller via main.
	private static class MainArguments {
		@Parameter(names = { "--cluster" }, description = "Cluster to test on", required = true)
		String cluster;

		@Parameter(names = { "--servers" }, description = "Comma separated list of server hostnames", required = true)
		String serverHostNames;

		@Parameter(names = { "--server-port" }, description = "Port that the servers are listening on", required = true)
		int serverPort;
	}

	// JCommander arguments for accepting user input.
	private static class ShellArguments {
		@Parameter(description = "Command arguments:\n" + "trade tenant namespace topic\n"
				+ "change tenant namespace topic\n" + "stop tenant namespace topic\n"
				+ "trade_group tenant group_name num_namespaces\n" + "change_group tenant group_name\n"
				+ "stop_group tenant group_name\n" + "script script_name\n" + "copy tenant_name source_zk target_zk\n"
				+ "stream source_zk\n", required = true)
		List<String> commandArguments;

		@Parameter(names = { "--rand-rate" }, description = "Choose message rate uniformly randomly from the next two "
				+ "comma separated values (overrides --rate)")
		String rangeString = "";

		@Parameter(names = { "--rate" }, description = "Messages per second")
		double rate = 1;

		@Parameter(names = { "--rate-multiplier" }, description = "Multiplier to use for copying or streaming rates")
		double rateMultiplier = 1;

		@Parameter(names = { "--size" }, description = "Message size in bytes")
		int size = 1024;

		@Parameter(names = { "--separation" }, description = "Separation time in ms for trade_group actions "
				+ "(0 for no separation)")
		int separation = 0;

		@Parameter(names = { "--topics-per-namespace" }, description = "Number of topics to create per namespace in "
				+ "trade_group (total number of topics is num_namespaces X num_topics)")
		int topicsPerNamespace = 1;
	}

	// In stream mode, the BrokerWatcher watches the /loadbalance/broker zpath
	// and adds LoadReportWatchers
	// accordingly when new brokers come up.
	private class BrokerWatcher implements Watcher {
		private final ZooKeeper zkClient;
		private final Set<String> brokers;
		private final String path;
		private final ShellArguments arguments;

		public BrokerWatcher(final String path, final ZooKeeper zkClient, final ShellArguments arguments) {
			this.path = path;
			this.zkClient = zkClient;
			this.arguments = arguments;
			brokers = new HashSet<>();
			process(null);
		}

		public synchronized void process(final WatchedEvent event) {
			try {
				final List<String> currentBrokers = zkClient.getChildren(path, this);
				for (final String broker : currentBrokers) {
					if (!brokers.contains(broker)) {
						new LoadReportWatcher(String.format("%s/%s", path, broker), zkClient, arguments);
						brokers.add(broker);
					}
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	// In stream mode, the LoadReportWatcher watches the /loadbalance/broker
	// children and adds or modifies topics
	// with suitable rates based on the most recent message rate and throughput
	// information.
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

		public synchronized void process(final WatchedEvent event) {
			try {
				// Get the load report and put this back as a watch.
				final LoadReport loadReport = ObjectMapperFactory.getThreadLocal()
						.readValue(zkClient.getData(path, this, null), LoadReport.class);
				for (final Map.Entry<String, NamespaceBundleStats> entry : loadReport.getBundleStats().entrySet()) {
					final String bundle = entry.getKey();
					final String namespace = bundle.substring(0, bundle.lastIndexOf('/'));
					final String destination = String.format("%s/%s", namespace, "t");
					final NamespaceBundleStats stats = entry.getValue();

					// Approximate total message rate via average between
					// in/out.
					final double messageRate = arguments.rateMultiplier * (stats.msgRateIn + stats.msgRateOut) / 2;

					// size = throughput / rate.
					final int messageSize = (int) Math.ceil(arguments.rateMultiplier
							* (stats.msgThroughputIn + stats.msgThroughputOut) / (2 * messageRate));

					final ShellArguments tradeArguments = new ShellArguments();
					arguments.rate = messageRate;
					arguments.size = messageSize;
					// Try to modify the topic if it already exists. Otherwise,
					// create it.
					if (!change(tradeArguments, destination)) {
						trade(tradeArguments, destination);
					}
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
		serverPort = arguments.serverPort;
		cluster = arguments.cluster;
		servers = arguments.serverHostNames.split(",");
		final Socket[] sockets = new Socket[servers.length];
		inputStreams = new DataInputStream[servers.length];
		outputStreams = new DataOutputStream[servers.length];
		System.out.format("Found %d servers:\n", servers.length);
		for (int i = 0; i < servers.length; ++i) {
			sockets[i] = new Socket(servers[i], serverPort);
			inputStreams[i] = new DataInputStream(sockets[i].getInputStream());
			outputStreams[i] = new DataOutputStream(sockets[i].getOutputStream());
			System.out.format("Connected to %s\n", servers[i]);
		}
	}

	// Check that the expected number of application arguments matches the
	// actual number of application arguments.
	private boolean checkAppArgs(final int numAppArgs, final int numRequired) {
		if (numAppArgs != numRequired) {
			System.out.format("ERROR: Wrong number of application arguments (found %d, required %d)\n", numAppArgs,
					numRequired);
			return false;
		}
		return true;
	}

	// Makes a destination string from a tenant name, namespace name, and topic
	// name.
	private String makeDestination(final String tenant, final String namespace, final String topic) {
		return String.format("persistent://%s/%s/%s/%s", cluster, tenant, namespace, topic);
	}

	// Write options that are common to modifying and creating topics.
	private void writeProducerOptions(final DataOutputStream outputStream, final ShellArguments arguments,
			final String destination) throws Exception {
		if (!arguments.rangeString.isEmpty()) {
			// If --rand-rate was specified, extract the bounds by splitting on
			// the comma and parsing the resulting
			// doubles.
			final String[] splits = arguments.rangeString.split(",");
			if (splits.length != 2) {
				System.out.println("ERROR: Argument to --rand-rate should be a two comma-separated values");
				return;
			}
			final double first = Double.parseDouble(splits[0]);
			final double second = Double.parseDouble(splits[1]);
			final double min = Math.min(first, second);
			final double max = Math.max(first, second);
			arguments.rate = random.nextDouble() * (max - min) + min;
		}
		outputStream.writeUTF(destination);
		outputStream.writeInt(arguments.size);
		outputStream.writeDouble(arguments.rate);
	}

	// Trade using the arguments parsed via JCommander and the destination name.
	private synchronized void trade(final ShellArguments arguments, final String destination) throws Exception {
		// Decide which server to send to randomly to preserve statelessness of
		// the controller.
		final int i = random.nextInt(servers.length);
		System.out.println("Sending trade request to " + servers[i]);
		outputStreams[i].write(LoadSimulationServer.TRADE_COMMAND);
		writeProducerOptions(outputStreams[i], arguments, destination);
		outputStreams[i].flush();
		if (inputStreams[i].read() != -1) {
			System.out.println("Created producer and consumer for " + destination);
		} else {
			System.out.format("ERROR: Socket to %s closed\n", servers[i]);
		}
	}

	private void handleTrade(final ShellArguments arguments) throws Exception {
		final List<String> commandArguments = arguments.commandArguments;
		// Trade expects three application arguments: tenant, namespace, and
		// topic.
		if (checkAppArgs(commandArguments.size() - 1, 3)) {
			final String destination = makeDestination(commandArguments.get(1), commandArguments.get(2),
					commandArguments.get(3));
			trade(arguments, destination);
		}
	}

	// Change producer settings for a given destination and JCommander
	// arguments.
	// Returns true if the topic was found and false otherwise.
	private synchronized boolean change(final ShellArguments arguments, final String destination) throws Exception {
		System.out.println("Searching for server with topic " + destination);
		for (DataOutputStream outputStream : outputStreams) {
			outputStream.write(LoadSimulationServer.CHANGE_COMMAND);
			writeProducerOptions(outputStream, arguments, destination);
			outputStream.flush();
		}
		boolean foundTopic = false;
		for (int i = 0; i < servers.length; ++i) {
			int readValue;
			switch (readValue = inputStreams[i].read()) {
			case LoadSimulationServer.FOUND_TOPIC:
				System.out.format("Found topic %s on server %s\n", destination, servers[i]);
				foundTopic = true;
				break;
			case LoadSimulationServer.NO_SUCH_TOPIC:
				break;
			case -1:
				System.out.format("ERROR: Socket to %s closed\n", servers[i]);
				break;
			default:
				System.out.println("ERROR: Unknown response signal received: " + readValue);
			}
		}
		return foundTopic;
	}

	private void handleChange(final ShellArguments arguments) throws Exception {
		final List<String> commandArguments = arguments.commandArguments;
		// Change expects three application arguments: tenant name, namespace
		// name, and topic name.
		if (checkAppArgs(commandArguments.size() - 1, 3)) {
			final String destination = makeDestination(commandArguments.get(1), commandArguments.get(2),
					commandArguments.get(3));
			if (!change(arguments, destination)) {
				System.out.format("ERROR: Topic %s not found\n", destination);
			}
		}
	}

	private void handleStop(final ShellArguments arguments) throws Exception {
		final List<String> commandArguments = arguments.commandArguments;
		// Stop expects three application arguments: tenant name, namespace
		// name, and topic name.
		if (checkAppArgs(commandArguments.size() - 1, 3)) {
			final String destination = makeDestination(commandArguments.get(1), commandArguments.get(2),
					commandArguments.get(3));
			System.out.println("Searching for server with topic " + destination);
			for (DataOutputStream outputStream : outputStreams) {
				outputStream.write(LoadSimulationServer.STOP_COMMAND);
				outputStream.writeUTF(destination);
				outputStream.flush();
			}
			boolean foundTopic = false;
			for (int i = 0; i < servers.length; ++i) {
				int readValue;
				switch (readValue = inputStreams[i].read()) {
				case LoadSimulationServer.FOUND_TOPIC:
					System.out.format("Found topic %s on server %s\n", destination, servers[i]);
					foundTopic = true;
					break;
				case LoadSimulationServer.NO_SUCH_TOPIC:
					break;
				case LoadSimulationServer.REDUNDANT_COMMAND:
					System.out.format("ERROR: Topic %s already stopped on %s\n", destination, servers[i]);
					foundTopic = true;
					break;
				case -1:
					System.out.format("ERROR: Socket to %s closed\n", servers[i]);
					break;
				default:
					System.out.println("ERROR: Unknown response signal received: " + readValue);
				}
			}
			if (!foundTopic) {
				System.out.format("ERROR: Topic %s not found\n", destination);
			}
		}
	}

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
					final String destination = makeDestination(tenant, String.format("%s-%d", group, i),
							Integer.toString(j));
					trade(arguments, destination);
					Thread.sleep(arguments.separation);
				}
			}
		}
	}

	private void handleGroupChange(final ShellArguments arguments) throws Exception {
		final List<String> commandArguments = arguments.commandArguments;
		// Group change expects two application arguments: tenant name and group
		// name.
		if (checkAppArgs(commandArguments.size() - 1, 2)) {
			final String tenant = commandArguments.get(1);
			final String group = commandArguments.get(2);
			for (DataOutputStream outputStream : outputStreams) {
				outputStream.write(LoadSimulationServer.CHANGE_GROUP_COMMAND);
				outputStream.writeUTF(tenant);
				outputStream.writeUTF(group);
				outputStream.writeInt(arguments.size);
				outputStream.writeDouble(arguments.rate);
				outputStream.flush();
			}
			accumulateAndReport(tenant, group);
		}
	}

	// Report the number of topics found belonging to the given tenant and
	// group.
	private void accumulateAndReport(final String tenant, final String group) throws Exception {
		int numFound = 0;
		for (int i = 0; i < servers.length; ++i) {
			final int foundOnServer = inputStreams[i].readInt();
			if (foundOnServer == -1) {
				System.out.format("ERROR: Socket to %s closed\n", servers[i]);
			} else if (foundOnServer == 0) {
				System.out.format("Found no topics belonging to tenant %s and group %s on %s\n", tenant, group,
						servers[i]);
			} else if (foundOnServer > 0) {
				System.out.format("Found %d topics belonging to tenant %s and group %s on %s\n", foundOnServer, tenant,
						group, servers[i]);
				numFound += foundOnServer;
			} else {
				System.out.format("ERROR: Negative value %d received for topic count on %s\n", foundOnServer,
						servers[i]);
			}
		}
		if (numFound == 0) {
			System.out.format("ERROR: Found no topics belonging to tenant %s and group %s\n", tenant, group);
		} else {
			System.out.format("Found %d topics belonging to tenant %s and group %s\n", numFound, tenant, group);
		}
	}

	private void handleGroupStop(final ShellArguments arguments) throws Exception {
		final List<String> commandArguments = arguments.commandArguments;
		// Group stop requires two application arguments: tenant name and group
		// name.
		if (checkAppArgs(commandArguments.size() - 1, 2)) {
			final String tenant = commandArguments.get(1);
			final String group = commandArguments.get(2);
			for (DataOutputStream outputStream : outputStreams) {
				outputStream.write(LoadSimulationServer.STOP_GROUP_COMMAND);
				outputStream.writeUTF(tenant);
				outputStream.writeUTF(group);
				outputStream.flush();
			}
			accumulateAndReport(tenant, group);
		}
	}

	// Recursively acquire all resource quotas by getting the ZK children of the
	// given path and calling this function
	// on the children if there are any, or getting the data from this ZNode
	// otherwise.
	private void getResourceQuotas(final String path, final ZooKeeper zkClient,
			final Map<String, ResourceQuota> bundleToQuota) throws Exception {
		final List<String> children = zkClient.getChildren(path, false);
		if (children.isEmpty()) {
			bundleToQuota.put(path, ObjectMapperFactory.getThreadLocal().readValue(zkClient.getData(path, false, null),
					ResourceQuota.class));
		} else {
			for (final String child : children) {
				getResourceQuotas(String.format("%s/%s", path, child), zkClient, bundleToQuota);
			}
		}
	}

	private void handleStream(final ShellArguments arguments) throws Exception {
		final List<String> commandArguments = arguments.commandArguments;
		// Stream accepts 1 application argument: ZooKeeper connect string.
		if (checkAppArgs(commandArguments.size() - 1, 1)) {
			final String zkConnectString = commandArguments.get(1);
			final ZooKeeper zkClient = new ZooKeeper(zkConnectString, 5000, null);
			new BrokerWatcher("/loadbalance/brokers", zkClient, arguments);
			// This controller will now stream rate changes from the given ZK.
			// Users wishing to stop this should Ctrl + C and use another
			// Controller to send new commands.
			while (true)
				;
		}
	}

	private void handleCopy(final ShellArguments arguments) throws Exception {
		final List<String> commandArguments = arguments.commandArguments;
		// Copy accepts 3 application arguments: Tenant name, source ZooKeeper
		// and target ZooKeeper connect strings.
		if (checkAppArgs(commandArguments.size() - 1, 3)) {
			final String tenantName = commandArguments.get(1);
			final String sourceZKConnectString = commandArguments.get(2);
			final String targetZKConnectString = commandArguments.get(3);
			final ZooKeeper sourceZKClient = new ZooKeeper(sourceZKConnectString, 5000, null);
			final ZooKeeper targetZKClient = new ZooKeeper(targetZKConnectString, 5000, null);
			final Map<String, ResourceQuota> bundleToQuota = new HashMap<>();
			getResourceQuotas(QUOTA_ROOT, sourceZKClient, bundleToQuota);
			for (final Map.Entry<String, ResourceQuota> entry : bundleToQuota.entrySet()) {
				final String bundle = entry.getKey();
				final ResourceQuota quota = entry.getValue();
				// Simulation will send messages in and out at about the same
				// rate, so just make the rate the average
				// of in and out.
				final double messageRate = (quota.getMsgRateIn() + quota.getMsgRateOut()) / 2;
				final int messageSize = (int) Math
						.ceil((quota.getBandwidthIn() + quota.getBandwidthOut()) / messageRate);
				final int clusterStart = QUOTA_ROOT.length() + 1;
				final int tenantStart = bundle.indexOf('/', clusterStart) + 1;
				final String sourceCluster = bundle.substring(clusterStart, tenantStart - 1);
				final int namespaceStart = bundle.indexOf('/', tenantStart) + 1;
				final String sourceTenant = bundle.substring(tenantStart, namespaceStart - 1);
				final String namespace = bundle.substring(namespaceStart, bundle.lastIndexOf('/'));
				final String keyRangeString = bundle.substring(bundle.lastIndexOf('/') + 1);
				// To prevent duplicate node issues for same namespace names in
				// different clusters/tenants.
				final String manglePrefix = String.format("%s-%s-%s", sourceCluster, sourceTenant, keyRangeString);
				final String mangledNamespace = String.format("%s-%s", manglePrefix, namespace);
				arguments.rate = messageRate * arguments.rateMultiplier;
				arguments.size = messageSize;
				final NamespaceBundleStats startingStats = new NamespaceBundleStats();

				// Modify the original quota so that new rates are set.
				quota.setMsgRateIn(quota.getMsgRateIn() * arguments.rateMultiplier);
				quota.setMsgRateOut(quota.getMsgRateOut() * arguments.rateMultiplier);
				quota.setBandwidthIn(quota.getBandwidthIn() * arguments.rateMultiplier);
				quota.setBandwidthOut(quota.getBandwidthOut() * arguments.rateMultiplier);

				// Assume modified memory usage is comparable to the rate
				// multiplier times the original usage.
				quota.setMemory(quota.getMemory() * arguments.rateMultiplier);
				startingStats.msgRateIn = quota.getMsgRateIn();
				startingStats.msgRateOut = quota.getMsgRateOut();
				startingStats.msgThroughputIn = quota.getBandwidthIn();
				startingStats.msgThroughputOut = quota.getBandwidthOut();
				final BundleData bundleData = new BundleData(10, 1000, startingStats);
				// Assume there is ample history for topic.
				bundleData.getLongTermData().setNumSamples(1000);
				bundleData.getShortTermData().setNumSamples(1000);
				final String oldAPITargetPath = String.format(
						"/loadbalance/resource-quota/namespace/%s/%s/%s/0x00000000_0xffffffff", cluster, tenantName,
						mangledNamespace);
				final String newAPITargetPath = String.format("/loadbalance/bundle-data/%s/%s/%s/0x00000000_0xffffffff",
						cluster, tenantName, mangledNamespace);
				System.out.format("Copying %s to %s\n", bundle, oldAPITargetPath);
				ZkUtils.createFullPathOptimistic(targetZKClient, oldAPITargetPath,
						ObjectMapperFactory.getThreadLocal().writeValueAsBytes(quota), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				System.out.format("Creating new API data at %s\n", newAPITargetPath);
				// Put the quota in the new ZooKeeper.
				ZkUtils.createFullPathOptimistic(targetZKClient, newAPITargetPath, bundleData.getJsonBytes(),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				trade(arguments, makeDestination(tenantName, mangledNamespace, "t"));
			}
			sourceZKClient.close();
			targetZKClient.close();
		}
	}

	public void read(final String[] args) {
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
				case "quit":
				case "exit":
					System.exit(0);
					break;
				default:
					System.out.format("ERROR: Unknown command \"%s\"\n", command);
				}
			} catch (ParameterException ex) {
				ex.printStackTrace();
				jc.usage();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public void run() throws Exception {
		BufferedReader inReader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			// Print the very simple prompt.
			System.out.println();
			System.out.print("> ");
			read(inReader.readLine().split("\\s+"));
		}
	}

	public static void main(String[] args) throws Exception {
		final MainArguments arguments = new MainArguments();
		final JCommander jc = new JCommander(arguments);
		try {
			jc.parse(args);
		} catch (Exception ex) {
			jc.usage();
			ex.printStackTrace();
			System.exit(1);
		}
		(new LoadSimulationController(arguments)).run();
	}
}
