package com.yahoo.pulsar.testclient;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.commons.lang.SystemUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.RateLimiter;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.MessageListener;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * LoadSimulationServer is used to simulate client load by maintaining producers
 * and consumers for topics. Instances of this class are controlled across a
 * network via LoadSimulationController.
 */
public class LoadSimulationServer {
	// Values for command responses.
	public static final byte FOUND_TOPIC = 0;
	public static final byte NO_SUCH_TOPIC = 1;
	public static final byte REDUNDANT_COMMAND = 2;

	// Values for command encodings.
	public static final byte CHANGE_COMMAND = 0;
	public static final byte STOP_COMMAND = 1;
	public static final byte TRADE_COMMAND = 2;
	public static final byte CHANGE_GROUP_COMMAND = 3;
	public static final byte STOP_GROUP_COMMAND = 4;

	private final ExecutorService executor;
	private final Map<Integer, byte[]> payloadCache;
	private final Map<String, TradeUnit> topicsToTradeUnits;
	private final PulsarClient client;
	private final ProducerConfiguration producerConf;
	private final ConsumerConfiguration consumerConf;
	private final ClientConfiguration clientConf;
	private final int port;

	// A TradeUnit is a Consumer and Producer pair. The rate of message
	// consumption as well as size may be changed at
	// any time, and the TradeUnit may also be stopped.
	private static class TradeUnit {
		Future<Producer> producerFuture;
		Future<Consumer> consumerFuture;
		final AtomicBoolean stop;
		final RateLimiter rateLimiter;

		// Creating a byte[] for every message is stressful for a client
		// machine, so in order to ensure that any
		// message size may be sent/changed while reducing object creation, the
		// byte[] is wrapped in an AtomicReference.
		final AtomicReference<byte[]> payload;
		final ProducerConfiguration producerConf;
		final PulsarClient client;
		final String topic;
		final Map<Integer, byte[]> payloadCache;

		public TradeUnit(final TradeConfiguration tradeConf, final PulsarClient client,
				final ProducerConfiguration producerConf, final ConsumerConfiguration consumerConf,
				final Map<Integer, byte[]> payloadCache) throws Exception {
			consumerFuture = client.subscribeAsync(tradeConf.topic, "Subscriber-" + tradeConf.topic, consumerConf);
			producerFuture = client.createProducerAsync(tradeConf.topic, producerConf);
			this.payload = new AtomicReference<>();
			this.producerConf = producerConf;
			this.payloadCache = payloadCache;
			this.client = client;
			topic = tradeConf.topic;

			// Add a byte[] of the appropriate size if it is not already present
			// in the cache.
			this.payload.set(payloadCache.computeIfAbsent(tradeConf.size, byte[]::new));
			rateLimiter = RateLimiter.create(tradeConf.rate);
			stop = new AtomicBoolean(false);
		}

		// Change the message rate/size according to the given configuration.
		public void change(final TradeConfiguration tradeConf) {
			rateLimiter.setRate(tradeConf.rate);
			this.payload.set(payloadCache.computeIfAbsent(tradeConf.size, byte[]::new));
		}

		// Attempt to create a Producer indefinitely. Useful for ensuring
		// messages continue to be sent after broker
		// restarts occur.
		private Producer getNewProducer() throws Exception {
			while (true) {
				try {
					return client.createProducerAsync(topic, producerConf).get();
				} catch (Exception e) {
					Thread.sleep(10000);
				}
			}
		}

		private class MutableBoolean {
			public volatile boolean value = true;
		}

		public void start() throws Exception {
			Producer producer = producerFuture.get();
			final Consumer consumer = consumerFuture.get();
			while (!stop.get()) {
				final MutableBoolean wellnessFlag = new MutableBoolean();
				final Function<Throwable, ? extends MessageId> exceptionHandler = e -> {
					// Unset the well flag in the case of an exception so we can
					// try to get a new Producer.
					wellnessFlag.value = false;
					return null;
				};
				while (!stop.get() && wellnessFlag.value) {
					producer.sendAsync(payload.get()).exceptionally(exceptionHandler);
					rateLimiter.acquire();
				}
				producer.closeAsync();
				if (!stop.get()) {
					// The Producer failed due to an exception: attempt to get
					// another producer.
					producer = getNewProducer();
				} else {
					// We are finished: close the consumer.
					consumer.closeAsync();
				}
			}
		}
	}

	// JCommander arguments for starting a LoadSimulationServer.
	private static class MainArguments {
		@Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
		boolean help;

		@Parameter(names = { "--port" }, description = "Port to listen on for controller", required = true)
		public int port;

		@Parameter(names = { "--service-url" }, description = "Pulsar Service URL", required = true)
		public String serviceURL;
	}

	// Configuration class for initializing or modifying TradeUnits.
	private static class TradeConfiguration {
		public byte command;
		public String topic;
		public double rate;
		public int size;
		public String tenant;
		public String group;

		public TradeConfiguration() {
			command = -1;
			rate = 100;
			size = 1024;
		}
	}

	// Handle input sent from a controller.
	private void handle(final Socket socket) throws Exception {
		final DataInputStream inputStream = new DataInputStream(socket.getInputStream());
		int command;
		while ((command = inputStream.read()) != -1) {
			handle((byte) command, inputStream, new DataOutputStream(socket.getOutputStream()));
		}
	}

	// Decode TradeConfiguration fields common for topic creation and
	// modification.
	private void decodeProducerOptions(final TradeConfiguration tradeConf, final DataInputStream inputStream)
			throws Exception {
		tradeConf.topic = inputStream.readUTF();
		tradeConf.size = inputStream.readInt();
		tradeConf.rate = inputStream.readDouble();
	}

	// Decode TradeConfiguration fields common for group commands.
	private void decodeGroupOptions(final TradeConfiguration tradeConf, final DataInputStream inputStream)
			throws Exception {
		tradeConf.tenant = inputStream.readUTF();
		tradeConf.group = inputStream.readUTF();
	}

	// Handle a command sent from a controller.
	private void handle(final byte command, final DataInputStream inputStream, final DataOutputStream outputStream)
			throws Exception {
		final TradeConfiguration tradeConf = new TradeConfiguration();
		tradeConf.command = command;
		switch (command) {
		case CHANGE_COMMAND:
			// Change the topic's settings if it exists. Report whether the
			// topic was found on this server.
			decodeProducerOptions(tradeConf, inputStream);
			if (topicsToTradeUnits.containsKey(tradeConf.topic)) {
				topicsToTradeUnits.get(tradeConf.topic).change(tradeConf);
				outputStream.write(FOUND_TOPIC);
			} else {
				outputStream.write(NO_SUCH_TOPIC);
			}
			break;
		case STOP_COMMAND:
			// Stop the topic if it exists. Report whether the topic was found,
			// and whether it was already stopped.
			tradeConf.topic = inputStream.readUTF();
			if (topicsToTradeUnits.containsKey(tradeConf.topic)) {
				final boolean wasStopped = topicsToTradeUnits.get(tradeConf.topic).stop.getAndSet(true);
				outputStream.write(wasStopped ? REDUNDANT_COMMAND : FOUND_TOPIC);
			} else {
				outputStream.write(NO_SUCH_TOPIC);
			}
			break;
		case TRADE_COMMAND:
			// Create the topic. It is assumed that the topic does not already
			// exist.
			decodeProducerOptions(tradeConf, inputStream);
			final TradeUnit tradeUnit = new TradeUnit(tradeConf, client, producerConf, consumerConf, payloadCache);
			topicsToTradeUnits.put(tradeConf.topic, tradeUnit);
			executor.submit(() -> {
				try {
					tradeUnit.start();
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			});
			// Tell controller topic creation is finished.
			outputStream.write(NO_SUCH_TOPIC);
			break;
		case CHANGE_GROUP_COMMAND:
			// Change the settings of all topics belonging to a group. Report
			// the number of topics changed.
			decodeGroupOptions(tradeConf, inputStream);
			tradeConf.size = inputStream.readInt();
			tradeConf.rate = inputStream.readDouble();
			// See if a topic belongs to this tenant and group using this regex.
			final String groupRegex = ".*://.*/" + tradeConf.tenant + "/" + tradeConf.group + "-.*/.*";
			int numFound = 0;
			for (Map.Entry<String, TradeUnit> entry : topicsToTradeUnits.entrySet()) {
				final String destination = entry.getKey();
				final TradeUnit unit = entry.getValue();
				if (destination.matches(groupRegex)) {
					++numFound;
					unit.change(tradeConf);
				}
			}
			outputStream.writeInt(numFound);
			break;
		case STOP_GROUP_COMMAND:
			// Stop all topics belonging to a group. Report the number of topics
			// stopped.
			decodeGroupOptions(tradeConf, inputStream);
			// See if a topic belongs to this tenant and group using this regex.
			final String regex = ".*://.*/" + tradeConf.tenant + "/" + tradeConf.group + "-.*/.*";
			int numStopped = 0;
			for (Map.Entry<String, TradeUnit> entry : topicsToTradeUnits.entrySet()) {
				final String destination = entry.getKey();
				final TradeUnit unit = entry.getValue();
				if (destination.matches(regex) && !unit.stop.getAndSet(true)) {
					++numStopped;
				}
			}
			outputStream.writeInt(numStopped);
			break;
		default:
			throw new IllegalArgumentException("Unrecognized command code received: " + command);
		}
		outputStream.flush();
	}

	private static final MessageListener ackListener = Consumer::acknowledgeAsync;

	public LoadSimulationServer(final MainArguments arguments) throws Exception {
		payloadCache = new ConcurrentHashMap<>();
		topicsToTradeUnits = new ConcurrentHashMap<>();
		final EventLoopGroup eventLoopGroup = SystemUtils.IS_OS_LINUX
				? new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors(),
						new DefaultThreadFactory("pulsar-test-client"))
				: new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
						new DefaultThreadFactory("pulsar-test-client"));
		clientConf = new ClientConfiguration();

		// Disable connection pooling.
		clientConf.setConnectionsPerBroker(0);

		// Disable stats on the clients to reduce CPU/memory usage.
		clientConf.setStatsInterval(0, TimeUnit.SECONDS);

		producerConf = new ProducerConfiguration();

		// Disable timeout.
		producerConf.setSendTimeout(0, TimeUnit.SECONDS);

		producerConf.setMessageRoutingMode(ProducerConfiguration.MessageRoutingMode.RoundRobinPartition);

		// Enable batching.
		producerConf.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
		producerConf.setBatchingEnabled(true);
		consumerConf = new ConsumerConfiguration();
		consumerConf.setMessageListener(ackListener);
		client = new PulsarClientImpl(arguments.serviceURL, clientConf, eventLoopGroup);
		port = arguments.port;
		executor = Executors.newCachedThreadPool(new DefaultThreadFactory("test-client"));
	}

	public static void main(String[] args) throws Exception {
		final MainArguments mainArguments = new MainArguments();
		final JCommander jc = new JCommander(mainArguments);
		try {
			jc.parse(args);
		} catch (ParameterException e) {
			jc.usage();
			throw e;
		}
		(new LoadSimulationServer(mainArguments)).run();
	}

	public void run() throws Exception {
		final ServerSocket serverSocket = new ServerSocket(port);

		while (true) {
			// Technically, two controllers can be connected simultaneously, but
			// non-sequential handling of commands
			// has not been tested or considered and is not recommended.
			System.out.println("Listening for controller command...");
			final Socket socket = serverSocket.accept();
			System.out.format("Connected to %s\n", socket.getInetAddress().getHostName());
			executor.submit(() -> {
				try {
					handle(socket);
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			});
		}
	}
}
