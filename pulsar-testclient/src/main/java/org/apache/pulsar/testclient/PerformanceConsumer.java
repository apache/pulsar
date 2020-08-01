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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;

public class PerformanceConsumer {
    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();
    private static final DecimalFormat dec = new DecimalFormat("0.000");

    private static final LongAdder totalMessagesReceived = new LongAdder();
    private static final LongAdder totalBytesReceived = new LongAdder();

    private static Recorder recorder = new Recorder(TimeUnit.DAYS.toMillis(10), 5);
    private static Recorder cumulativeRecorder = new Recorder(TimeUnit.DAYS.toMillis(10), 5);


    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "--conf-file" }, description = "Configuration file")
        public String confFile;

        @Parameter(description = "persistent://prop/ns/my-topic", required = true)
        public List<String> topic;

        @Parameter(names = { "-t", "--num-topics" }, description = "Number of topics")
        public int numTopics = 1;

        @Parameter(names = { "-n", "--num-consumers" }, description = "Number of consumers (per topic)")
        public int numConsumers = 1;

        @Parameter(names = { "-s", "--subscriber-name" }, description = "Subscriber name prefix")
        public String subscriberName = "sub";

        @Parameter(names = { "-st", "--subscription-type" }, description = "Subscription type")
        public SubscriptionType subscriptionType = SubscriptionType.Exclusive;

        @Parameter(names = { "-sp", "--subscription-position" }, description = "Subscription position")
        private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

        @Parameter(names = { "-r", "--rate" }, description = "Simulate a slow message consumer (rate in msg/s)")
        public double rate = 0;

        @Parameter(names = { "-q", "--receiver-queue-size" }, description = "Size of the receiver queue")
        public int receiverQueueSize = 1000;

        @Parameter(names = { "--replicated" }, description = "Whether the subscription status should be replicated")
        public boolean replicatedSubscription = false;

        @Parameter(names = { "--acks-delay-millis" }, description = "Acknowlegments grouping delay in millis")
        public int acknowledgmentsGroupingDelayMillis = 100;

        @Parameter(names = { "-c",
                "--max-connections" }, description = "Max number of TCP connections to a single broker")
        public int maxConnections = 100;

        @Parameter(names = { "-i",
                "--stats-interval-seconds" }, description = "Statistics Interval Seconds. If 0, statistics will be disabled")
        public long statsIntervalSeconds = 0;

        @Parameter(names = { "-u", "--service-url" }, description = "Pulsar Service URL")
        public String serviceURL;

        @Parameter(names = { "--auth_plugin" }, description = "Authentication plugin class name")
        public String authPluginClassName;

        @Parameter(names = { "--listener-name" }, description = "Listener name for the broker.")
        String listenerName = null;

        @Parameter(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
        private int maxPendingChuckedMessage = 0;

        @Parameter(names = { "-ac",
                "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
        private boolean autoAckOldestChunkedMessageOnQueueFull = false;

        @Parameter(names = { "-e",
                "--expire_time_incomplete_chunked_messages" }, description = "Expire time in ms for incomplete chunk messages")
        private long expireTimeOfIncompleteChunkedMessageMs = 0;

        @Parameter(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation " +
                "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" " +
                "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
        public String authParams;

        @Parameter(names = {
                "--trust-cert-file" }, description = "Path for the trusted TLS certificate file")
        public String tlsTrustCertsFilePath = "";

        @Parameter(names = {
                "--tls-allow-insecure" }, description = "Allow insecure TLS connection")
        public Boolean tlsAllowInsecureConnection = null;

        @Parameter(names = { "-k", "--encryption-key-name" }, description = "The private key name to decrypt payload")
        public String encKeyName = null;

        @Parameter(names = { "-v",
                "--encryption-key-value-file" }, description = "The file which contains the private key to decrypt payload")
        public String encKeyFile = null;

        @Parameter(names = { "-time",
                "--test-duration" }, description = "Test duration in secs. If 0, it will keep consuming")
        public long testTime = 0;
    }

    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf consume");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }

        if (arguments.topic.size() != 1) {
            System.out.println("Only one topic name is allowed");
            jc.usage();
            System.exit(-1);
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

            if (isBlank(arguments.tlsTrustCertsFilePath)) {
                arguments.tlsTrustCertsFilePath = prop.getProperty("tlsTrustCertsFilePath", "");
            }

            if (arguments.tlsAllowInsecureConnection == null) {
                arguments.tlsAllowInsecureConnection = Boolean.parseBoolean(prop
                        .getProperty("tlsAllowInsecureConnection", ""));
            }
        }

        // Dump config variables
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar performance consumer with config: {}", w.writeValueAsString(arguments));

        final TopicName prefixTopicName = TopicName.get(arguments.topic.get(0));

        final RateLimiter limiter = arguments.rate > 0 ? RateLimiter.create(arguments.rate) : null;
        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (arguments.testTime * 1e9);
        MessageListener<byte[]> listener = (consumer, msg) -> {
            if (arguments.testTime > 0) {
                if (System.nanoTime() > testEndTime) {
                    log.info("------------------- DONE -----------------------");
                    printAggregatedStats();
                    System.exit(0);
                }
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

            consumer.acknowledgeAsync(msg);
        };

        ClientBuilder clientBuilder = PulsarClient.builder() //
                .serviceUrl(arguments.serviceURL) //
                .connectionsPerBroker(arguments.maxConnections) //
                .statsInterval(arguments.statsIntervalSeconds, TimeUnit.SECONDS) //
                .ioThreads(Runtime.getRuntime().availableProcessors()) //
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

        class EncKeyReader implements CryptoKeyReader {

            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            EncKeyReader(byte[] value) {
                keyInfo.setKey(value);
            }

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
                if (keyName.equals(arguments.encKeyName)) {
                    return keyInfo;
                }
                return null;
            }
        }

        List<Future<Consumer<byte[]>>> futures = Lists.newArrayList();
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer() //
                .messageListener(listener) //
                .receiverQueueSize(arguments.receiverQueueSize) //
                .acknowledgmentGroupTime(arguments.acknowledgmentsGroupingDelayMillis, TimeUnit.MILLISECONDS) //
                .subscriptionType(arguments.subscriptionType)
                .subscriptionInitialPosition(arguments.subscriptionInitialPosition)
                .autoAckOldestChunkedMessageOnQueueFull(arguments.autoAckOldestChunkedMessageOnQueueFull)
                .replicateSubscriptionState(arguments.replicatedSubscription);
        if (arguments.maxPendingChuckedMessage > 0) {
            consumerBuilder.maxPendingChuckedMessage(arguments.maxPendingChuckedMessage);
        }
        if (arguments.expireTimeOfIncompleteChunkedMessageMs > 0) {
            consumerBuilder.expireTimeOfIncompleteChunkedMessage(arguments.expireTimeOfIncompleteChunkedMessageMs,
                    TimeUnit.MILLISECONDS);
        }

        if (arguments.encKeyName != null) {
            byte[] pKey = Files.readAllBytes(Paths.get(arguments.encKeyFile));
            EncKeyReader keyReader = new EncKeyReader(pKey);
            consumerBuilder.cryptoKeyReader(keyReader);
        }

        for (int i = 0; i < arguments.numTopics; i++) {
            final TopicName topicName = (arguments.numTopics == 1) ? prefixTopicName
                    : TopicName.get(String.format("%s-%d", prefixTopicName, i));
            log.info("Adding {} consumers on topic {}", arguments.numConsumers, topicName);

            for (int j = 0; j < arguments.numConsumers; j++) {
                String subscriberName;
                if (arguments.numConsumers > 1) {
                    subscriberName = String.format("%s-%d", arguments.subscriberName, j);
                } else {
                    subscriberName = arguments.subscriberName;
                }

                futures.add(consumerBuilder.clone().topic(topicName.toString()).subscriptionName(subscriberName)
                        .subscribeAsync());
            }
        }

        for (Future<Consumer<byte[]>> future : futures) {
            future.get();
        }

        log.info("Start receiving from {} consumers on {} topics", arguments.numConsumers,
                arguments.numTopics);

        long start = System.nanoTime();

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
            double rate = messagesReceived.sumThenReset() / elapsed;
            double throughput = bytesReceived.sumThenReset() / elapsed * 8 / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info(
                    "Throughput received: {}  msg/s -- {} Mbit/s --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
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

    private static final Logger log = LoggerFactory.getLogger(PerformanceConsumer.class);
}
