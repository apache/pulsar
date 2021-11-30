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

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import java.io.FileInputStream;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceReader {
    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();
    private static final DecimalFormat intFormat = new PaddingDecimalFormat("0", 7);
    private static final DecimalFormat dec = new DecimalFormat("0.000");

    private static final LongAdder totalMessagesReceived = new LongAdder();
    private static final LongAdder totalBytesReceived = new LongAdder();

    private static Recorder recorder = new Recorder(TimeUnit.DAYS.toMillis(10), 5);
    private static Recorder cumulativeRecorder = new Recorder(TimeUnit.DAYS.toMillis(10), 5);

    @Parameters(commandDescription = "Test pulsar reader performance.")
    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "--conf-file" }, description = "Configuration file")
        public String confFile;

        @Parameter(description = "persistent://prop/ns/my-topic", required = true)
        public List<String> topic;

        @Parameter(names = { "-t", "--num-topics" }, description = "Number of topics", validateWith = PositiveNumberParameterValidator.class)
        public int numTopics = 1;

        @Parameter(names = { "-r", "--rate" }, description = "Simulate a slow message reader (rate in msg/s)")
        public double rate = 0;

        @Parameter(names = { "-m",
                "--start-message-id" }, description = "Start message id. This can be either 'earliest', 'latest' or a specific message id by using 'lid:eid'")
        public String startMessageId = "earliest";

        @Parameter(names = { "-q", "--receiver-queue-size" }, description = "Size of the receiver queue")
        public int receiverQueueSize = 1000;

        @Parameter(names = {"-n",
                "--num-messages"}, description = "Number of messages to consume in total. If <= 0, it will keep consuming")
        public long numMessages = 0;

        @Parameter(names = { "-c",
                "--max-connections" }, description = "Max number of TCP connections to a single broker")
        public int maxConnections = 100;

        @Parameter(names = { "-i",
                "--stats-interval-seconds" }, description = "Statistics Interval Seconds. If 0, statistics will be disabled")
        public long statsIntervalSeconds = 0;

        @Parameter(names = { "-u", "--service-url" }, description = "Pulsar Service URL")
        public String serviceURL;

        @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name")
        public String authPluginClassName;

        @Parameter(names = { "--listener-name" }, description = "Listener name for the broker.")
        String listenerName = null;

        @Parameter(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation " +
                "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" " +
                "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
        public String authParams;

        @Parameter(names = {
                "--use-tls" }, description = "Use TLS encryption on the connection")
        public boolean useTls;

        @Parameter(names = {
                "--trust-cert-file" }, description = "Path for the trusted TLS certificate file")
        public String tlsTrustCertsFilePath = "";

        @Parameter(names = {
                "--tls-allow-insecure" }, description = "Allow insecure TLS connection")
        public Boolean tlsAllowInsecureConnection = null;

        @Parameter(names = { "-time",
                "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep consuming")
        public long testTime = 0;

        @Parameter(names = {"-ioThreads", "--num-io-threads"}, description = "Set the number of threads to be " +
                "used for handling connections to brokers, default is 1 thread")
        public int ioThreads = 1;

        @Parameter(names = {"-lt", "--num-listener-threads"}, description = "Set the number of threads"
                + " to be used for message listeners")
        public int listenerThreads = 1;
    }

    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf read");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        if (arguments.topic != null && arguments.topic.size() != arguments.numTopics) {
            // keep compatibility with the previous version
            if (arguments.topic.size() == 1) {
                String prefixTopicName = arguments.topic.get(0);
                List<String> defaultTopics = Lists.newArrayList();
                for (int i = 0; i < arguments.numTopics; i++) {
                    defaultTopics.add(String.format("%s-%d", prefixTopicName, i));
                }
                arguments.topic = defaultTopics;
            } else {
                System.out.println("The size of topics list should be equal to --num-topics");
                jc.usage();
                PerfClientUtils.exit(-1);
            }
        }

        if (arguments.confFile != null) {
            Properties prop = new Properties(System.getProperties());
            prop.load(new FileInputStream(arguments.confFile));

            if (arguments.serviceURL == null) {
                arguments.serviceURL = prop.getProperty("brokerServiceUrl");
            }

            if (arguments.serviceURL == null) {
                arguments.serviceURL = prop.getProperty("webServiceUrl");
            }

            // fallback to previous-version serviceUrl property to maintain backward-compatibility
            if (arguments.serviceURL == null) {
                arguments.serviceURL = prop.getProperty("serviceUrl", "http://localhost:8080/");
            }

            if (arguments.authPluginClassName == null) {
                arguments.authPluginClassName = prop.getProperty("authPlugin", null);
            }

            if (arguments.authParams == null) {
                arguments.authParams = prop.getProperty("authParams", null);
            }

            if (!arguments.useTls) {
                arguments.useTls = Boolean.parseBoolean(prop.getProperty("useTls"));
            }

            if (isBlank(arguments.tlsTrustCertsFilePath)) {
                arguments.tlsTrustCertsFilePath = prop.getProperty("tlsTrustCertsFilePath", "");
            }

            if (arguments.tlsAllowInsecureConnection == null) {
                arguments.tlsAllowInsecureConnection = Boolean.parseBoolean(prop
                        .getProperty("tlsAllowInsecureConnection", ""));
            }
        }

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar performance reader with config: {}", w.writeValueAsString(arguments));

        final RateLimiter limiter = arguments.rate > 0 ? RateLimiter.create(arguments.rate) : null;
        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (arguments.testTime * 1e9);
        ReaderListener<byte[]> listener = (reader, msg) -> {
            if (arguments.testTime > 0) {
                if (System.nanoTime() > testEndTime) {
                    log.info("------------- DONE (reached the maximum duration: [{} seconds] of consumption) --------------", arguments.testTime);
                    PerfClientUtils.exit(0);
                }
            }
            if (arguments.numMessages > 0 && totalMessagesReceived.sum() >= arguments.numMessages) {
                log.info("------------- DONE (reached the maximum number: [{}] of consumption) --------------", arguments.numMessages);
                printAggregatedStats();
                PerfClientUtils.exit(0);
            }
            messagesReceived.increment();
            bytesReceived.add(msg.getData().length);

            totalMessagesReceived.increment();
            totalBytesReceived.add(msg.getData().length);

            if (limiter != null) {
                limiter.acquire();
            }

            long latencyMillis = System.currentTimeMillis() - msg.getPublishTime();
            if (latencyMillis >= 0) {
                recorder.recordValue(latencyMillis);
                cumulativeRecorder.recordValue(latencyMillis);
            }
        };

        ClientBuilder clientBuilder = PulsarClient.builder() //
                .serviceUrl(arguments.serviceURL) //
                .connectionsPerBroker(arguments.maxConnections) //
                .statsInterval(arguments.statsIntervalSeconds, TimeUnit.SECONDS) //
                .ioThreads(arguments.ioThreads) //
                .listenerThreads(arguments.listenerThreads)
                .enableTls(arguments.useTls) //
                .tlsTrustCertsFilePath(arguments.tlsTrustCertsFilePath);

        if (isNotBlank(arguments.authPluginClassName)) {
            clientBuilder.authentication(arguments.authPluginClassName, arguments.authParams);
        }

        if (arguments.tlsAllowInsecureConnection != null) {
            clientBuilder.allowTlsInsecureConnection(arguments.tlsAllowInsecureConnection);
        }

        if (isNotBlank(arguments.listenerName)) {
            clientBuilder.listenerName(arguments.listenerName);
        }

        PulsarClient pulsarClient = clientBuilder.build();

        List<CompletableFuture<Reader<byte[]>>> futures = Lists.newArrayList();

        MessageId startMessageId;
        if ("earliest".equals(arguments.startMessageId)) {
            startMessageId = MessageId.earliest;
        } else if ("latest".equals(arguments.startMessageId)) {
            startMessageId = MessageId.latest;
        } else {
            String[] parts = arguments.startMessageId.split(":");
            startMessageId = new MessageIdImpl(Long.parseLong(parts[0]), Long.parseLong(parts[1]), -1);
        }

        ReaderBuilder<byte[]> readerBuilder = pulsarClient.newReader() //
                .readerListener(listener) //
                .receiverQueueSize(arguments.receiverQueueSize) //
                .startMessageId(startMessageId);

        for (int i = 0; i < arguments.numTopics; i++) {
            final TopicName topicName = TopicName.get(arguments.topic.get(i));

            futures.add(readerBuilder.clone().topic(topicName.toString()).createAsync());
        }

        FutureUtil.waitForAll(futures).get();

        log.info("Start reading from {} topics", arguments.numTopics);

        final long start = System.nanoTime();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));

        long oldTime = System.nanoTime();
        Histogram reportHistogram = null;

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            long total = totalMessagesReceived.sum();
            double rate = messagesReceived.sumThenReset() / elapsed;
            double throughput = bytesReceived.sumThenReset() / elapsed * 8 / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);
            log.info(
                    "Read throughput: {} msg --- {}  msg/s -- {} Mbit/s --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                    intFormat.format(total),
                    dec.format(rate), dec.format(throughput), dec.format(reportHistogram.getMean()),
                    reportHistogram.getValueAtPercentile(50), reportHistogram.getValueAtPercentile(95),
                    reportHistogram.getValueAtPercentile(99), reportHistogram.getValueAtPercentile(99.9),
                    reportHistogram.getValueAtPercentile(99.99), reportHistogram.getMaxValue());

            reportHistogram.reset();
            oldTime = now;
        }

        pulsarClient.close();
    }

    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesReceived.sum() / elapsed;
        double throughput = totalBytesReceived.sum() / elapsed * 8 / 1024 / 1024;
        log.info(
                "Aggregated throughput stats --- {} records received --- {} msg/s --- {} Mbit/s",
                totalMessagesReceived,
                dec.format(rate),
                dec.format(throughput));
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.info(
                "Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - 99.999pct: {} - Max: {}",
                dec.format(reportHistogram.getMean()), reportHistogram.getValueAtPercentile(50),
                reportHistogram.getValueAtPercentile(95), reportHistogram.getValueAtPercentile(99),
                reportHistogram.getValueAtPercentile(99.9), reportHistogram.getValueAtPercentile(99.99),
                reportHistogram.getValueAtPercentile(99.999), reportHistogram.getMaxValue());
    }

    private static final Logger log = LoggerFactory.getLogger(PerformanceReader.class);
}
