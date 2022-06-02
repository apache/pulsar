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
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceConsumer {
    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();
    private static final DecimalFormat intFormat = new PaddingDecimalFormat("0", 7);
    private static final DecimalFormat dec = new DecimalFormat("0.000");

    private static final LongAdder totalMessagesReceived = new LongAdder();
    private static final LongAdder totalBytesReceived = new LongAdder();

    private static final LongAdder totalNumTxnOpenFail = new LongAdder();
    private static final LongAdder totalNumTxnOpenSuccess = new LongAdder();

    private static final LongAdder totalMessageAck = new LongAdder();
    private static final LongAdder totalMessageAckFailed = new LongAdder();
    private static final LongAdder messageAck = new LongAdder();

    private static final LongAdder totalEndTxnOpFailNum = new LongAdder();
    private static final LongAdder totalEndTxnOpSuccessNum = new LongAdder();
    private static final LongAdder numTxnOpSuccess = new LongAdder();

    private static final Recorder recorder = new Recorder(TimeUnit.DAYS.toMillis(10), 5);
    private static final Recorder cumulativeRecorder = new Recorder(TimeUnit.DAYS.toMillis(10), 5);

    @Parameters(commandDescription = "Test pulsar consumer performance.")
    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "-cf", "--conf-file" }, description = "Configuration file")
        public String confFile;

        @Parameter(description = "persistent://prop/ns/my-topic", required = true)
        public List<String> topic;

        @Parameter(names = { "-t", "--num-topics" }, description = "Number of topics",
                validateWith = PositiveNumberParameterValidator.class)
        public int numTopics = 1;

        @Parameter(names = { "-n", "--num-consumers" }, description = "Number of consumers (per subscription), only "
                + "one consumer is allowed when subscriptionType is Exclusive",
                validateWith = PositiveNumberParameterValidator.class)
        public int numConsumers = 1;

        @Parameter(names = { "-ns", "--num-subscriptions" }, description = "Number of subscriptions (per topic)",
                validateWith = PositiveNumberParameterValidator.class)
        public int numSubscriptions = 1;

        @Parameter(names = { "-s", "--subscriber-name" }, description = "Subscriber name prefix", hidden = true)
        public String subscriberName;

        @Parameter(names = { "-ss", "--subscriptions" },
                description = "A list of subscriptions to consume (for example, sub1,sub2)")
        public List<String> subscriptions = Collections.singletonList("sub");

        @Parameter(names = { "-st", "--subscription-type" }, description = "Subscription type")
        public SubscriptionType subscriptionType = SubscriptionType.Exclusive;

        @Parameter(names = { "-sp", "--subscription-position" }, description = "Subscription position")
        private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

        @Parameter(names = { "-r", "--rate" }, description = "Simulate a slow message consumer (rate in msg/s)")
        public double rate = 0;

        @Parameter(names = { "-q", "--receiver-queue-size" }, description = "Size of the receiver queue")
        public int receiverQueueSize = 1000;

        @Parameter(names = { "-p", "--receiver-queue-size-across-partitions" },
                description = "Max total size of the receiver queue across partitions")
        public int maxTotalReceiverQueueSizeAcrossPartitions = 50000;

        @Parameter(names = { "--replicated" }, description = "Whether the subscription status should be replicated")
        public boolean replicatedSubscription = false;

        @Parameter(names = { "--acks-delay-millis" }, description = "Acknowledgements grouping delay in millis")
        public int acknowledgmentsGroupingDelayMillis = 100;

        @Parameter(names = {"-m",
                "--num-messages"},
                description = "Number of messages to consume in total. If <= 0, it will keep consuming")
        public long numMessages = 0;

        @Parameter(names = { "-c",
                "--max-connections" }, description = "Max number of TCP connections to a single broker")
        public int maxConnections = 100;

        @Parameter(names = { "-i",
                "--stats-interval-seconds" },
                description = "Statistics Interval Seconds. If 0, statistics will be disabled")
        public long statsIntervalSeconds = 0;

        @Parameter(names = { "-u", "--service-url" }, description = "Pulsar Service URL")
        public String serviceURL;

        @Parameter(names = { "--auth_plugin" }, description = "Authentication plugin class name", hidden = true)
        public String deprecatedAuthPluginClassName;

        @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name")
        public String authPluginClassName;

        @Parameter(names = { "--listener-name" }, description = "Listener name for the broker.")
        String listenerName = null;

        @Parameter(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
        private int maxPendingChunkedMessage = 0;

        @Parameter(names = { "-ac",
                "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
        private boolean autoAckOldestChunkedMessageOnQueueFull = false;

        @Parameter(names = { "-e",
                "--expire_time_incomplete_chunked_messages" },
                description = "Expire time in ms for incomplete chunk messages")
        private long expireTimeOfIncompleteChunkedMessageMs = 0;

        @Parameter(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation "
                    + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                    + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
        public String authParams;

        @Parameter(names = {
                "--trust-cert-file" }, description = "Path for the trusted TLS certificate file")
        public String tlsTrustCertsFilePath = "";

        @Parameter(names = {
                "--tls-allow-insecure" }, description = "Allow insecure TLS connection")
        public Boolean tlsAllowInsecureConnection = null;

        @Parameter(names = { "-v",
                "--encryption-key-value-file" },
                description = "The file which contains the private key to decrypt payload")
        public String encKeyFile = null;

        @Parameter(names = { "-time",
                "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep consuming")
        public long testTime = 0;

        @Parameter(names = {"-ioThreads", "--num-io-threads"}, description = "Set the number of threads to be "
                + "used for handling connections to brokers. The default value is 1.")
        public int ioThreads = 1;

        @Parameter(names = {"-lt", "--num-listener-threads"}, description = "Set the number of threads"
                + " to be used for message listeners")
        public int listenerThreads = 1;

        @Parameter(names = {"--batch-index-ack" }, description = "Enable or disable the batch index acknowledgment")
        public boolean batchIndexAck = false;

        @Parameter(names = { "-pm", "--pool-messages" }, description = "Use the pooled message", arity = 1)
        private boolean poolMessages = true;

        @Parameter(names = {"-bw", "--busy-wait"}, description = "Enable Busy-Wait on the Pulsar client")
        public boolean enableBusyWait = false;

        @Parameter(names = {"-tto", "--txn-timeout"},  description = "Set the time value of transaction timeout,"
                + " and the time unit is second. (After --txn-enable setting to true, --txn-timeout takes effect)")
        public long transactionTimeout = 10;

        @Parameter(names = {"-nmt", "--numMessage-perTransaction"},
                description = "The number of messages acknowledged by a transaction. "
                + "(After --txn-enable setting to true, -numMessage-perTransaction takes effect")
        public int numMessagesPerTransaction = 50;

        @Parameter(names = {"-txn", "--txn-enable"}, description = "Enable or disable the transaction")
        public boolean isEnableTransaction = false;

        @Parameter(names = {"-ntxn"}, description = "The number of opened transactions, 0 means keeping open."
                + "(After --txn-enable setting to true, -ntxn takes effect.)")
        public long totalNumTxn = 0;

        @Parameter(names = {"-abort"}, description = "Abort the transaction. (After --txn-enable "
                + "setting to true, -abort takes effect)")
        public boolean isAbortTransaction = false;

        @Parameter(names = { "--histogram-file" }, description = "HdrHistogram output file")
        public String histogramFile = null;
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
            PerfClientUtils.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        if (isBlank(arguments.authPluginClassName) && !isBlank(arguments.deprecatedAuthPluginClassName)) {
            arguments.authPluginClassName = arguments.deprecatedAuthPluginClassName;
        }

        if (arguments.topic != null && arguments.topic.size() != arguments.numTopics) {
            // keep compatibility with the previous version
            if (arguments.topic.size() == 1) {
                String prefixTopicName = TopicName.get(arguments.topic.get(0)).toString().trim();
                List<String> defaultTopics = new ArrayList<>();
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

        if (arguments.subscriptionType == SubscriptionType.Exclusive && arguments.numConsumers > 1) {
            System.out.println("Only one consumer is allowed when subscriptionType is Exclusive");
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        if (arguments.subscriptions != null && arguments.subscriptions.size() != arguments.numSubscriptions) {
            // keep compatibility with the previous version
            if (arguments.subscriptions.size() == 1) {
                if (arguments.subscriberName == null) {
                    arguments.subscriberName = arguments.subscriptions.get(0);
                }
                List<String> defaultSubscriptions = new ArrayList<>();
                for (int i = 0; i < arguments.numSubscriptions; i++) {
                    defaultSubscriptions.add(String.format("%s-%d", arguments.subscriberName, i));
                }
                arguments.subscriptions = defaultSubscriptions;
            } else {
                System.out.println("The size of subscriptions list should be equal to --num-subscriptions");
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
        log.info("Starting Pulsar performance consumer with config: {}", w.writeValueAsString(arguments));

        final RateLimiter limiter = arguments.rate > 0 ? RateLimiter.create(arguments.rate) : null;
        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (arguments.testTime * 1e9);

        ClientBuilder clientBuilder = PulsarClient.builder() //
                .memoryLimit(0, SizeUnit.BYTES)
                .enableTransaction(arguments.isEnableTransaction)
                .serviceUrl(arguments.serviceURL) //
                .connectionsPerBroker(arguments.maxConnections) //
                .statsInterval(arguments.statsIntervalSeconds, TimeUnit.SECONDS) //
                .ioThreads(arguments.ioThreads) //
                .listenerThreads(arguments.listenerThreads)
                .enableBusyWait(arguments.enableBusyWait)
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

        AtomicReference<Transaction> atomicReference;
        if (arguments.isEnableTransaction) {
            atomicReference = new AtomicReference<>(pulsarClient.newTransaction()
                    .withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS).build().get());
        } else {
            atomicReference = new AtomicReference<>(null);
        }

        AtomicLong messageAckedCount = new AtomicLong();
        Semaphore messageReceiveLimiter = new Semaphore(arguments.numMessagesPerTransaction);
        Thread thread = Thread.currentThread();
        MessageListener<ByteBuffer> listener = (consumer, msg) -> {
                if (arguments.testTime > 0) {
                    if (System.nanoTime() > testEndTime) {
                        log.info("------------------- DONE -----------------------");
                        PerfClientUtils.exit(0);
                        thread.interrupt();
                    }
                }
                if (arguments.totalNumTxn > 0) {
                    if (totalEndTxnOpFailNum.sum() + totalEndTxnOpSuccessNum.sum() >= arguments.totalNumTxn) {
                        log.info("------------------- DONE -----------------------");
                        PerfClientUtils.exit(0);
                        thread.interrupt();
                    }
                }
                messagesReceived.increment();
                bytesReceived.add(msg.size());

                totalMessagesReceived.increment();
                totalBytesReceived.add(msg.size());

                if (limiter != null) {
                    limiter.acquire();
                }

                long latencyMillis = System.currentTimeMillis() - msg.getPublishTime();
                if (latencyMillis >= 0) {
                    recorder.recordValue(latencyMillis);
                    cumulativeRecorder.recordValue(latencyMillis);
                }
                if (arguments.isEnableTransaction) {
                    try {
                        messageReceiveLimiter.acquire();
                    } catch (InterruptedException e){
                        log.error("Got error: ", e);
                    }
                    consumer.acknowledgeAsync(msg.getMessageId(), atomicReference.get()).thenRun(() -> {
                        totalMessageAck.increment();
                        messageAck.increment();
                    }).exceptionally(throwable ->{
                        log.error("Ack message {} failed with exception", msg, throwable);
                        totalMessageAckFailed.increment();
                        return null;
                    });
                } else {
                    consumer.acknowledgeAsync(msg).thenRun(()->{
                        totalMessageAck.increment();
                        messageAck.increment();
                    }
                    ).exceptionally(throwable ->{
                                log.error("Ack message {} failed with exception", msg, throwable);
                                totalMessageAckFailed.increment();
                                return null;
                            }
                    );
                }
                if (arguments.poolMessages) {
                    msg.release();
                }
                if (arguments.isEnableTransaction
                        && messageAckedCount.incrementAndGet() == arguments.numMessagesPerTransaction) {
                    Transaction transaction = atomicReference.get();
                    if (!arguments.isAbortTransaction) {
                        transaction.commit()
                                .thenRun(() -> {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Commit transaction {}", transaction.getTxnID());
                                    }
                                    totalEndTxnOpSuccessNum.increment();
                                    numTxnOpSuccess.increment();
                                })
                                .exceptionally(exception -> {
                                    log.error("Commit transaction failed with exception : ", exception);
                                    totalEndTxnOpFailNum.increment();
                                    return null;
                                });
                    } else {
                        transaction.abort().thenRun(() -> {
                            if (log.isDebugEnabled()) {
                                log.debug("Abort transaction {}", transaction.getTxnID());
                            }
                            totalEndTxnOpSuccessNum.increment();
                            numTxnOpSuccess.increment();
                        }).exceptionally(exception -> {
                            log.error("Abort transaction {} failed with exception",
                                    transaction.getTxnID().toString(),
                                    exception);
                            totalEndTxnOpFailNum.increment();
                            return null;
                        });
                    }
                    while (true) {
                        try {
                            Transaction newTransaction = pulsarClient.newTransaction()
                                    .withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS)
                                    .build().get();
                            atomicReference.compareAndSet(transaction, newTransaction);
                            totalNumTxnOpenSuccess.increment();
                            messageAckedCount.set(0);
                            messageReceiveLimiter.release(arguments.numMessagesPerTransaction);
                            break;
                        } catch (Exception e) {
                            log.error("Failed to new transaction with exception:", e);
                            totalNumTxnOpenFail.increment();
                        }
                    }
                }

        };

        List<Future<Consumer<ByteBuffer>>> futures = new ArrayList<>();
        ConsumerBuilder<ByteBuffer> consumerBuilder = pulsarClient.newConsumer(Schema.BYTEBUFFER) //
                .messageListener(listener) //
                .receiverQueueSize(arguments.receiverQueueSize) //
                .maxTotalReceiverQueueSizeAcrossPartitions(arguments.maxTotalReceiverQueueSizeAcrossPartitions)
                .acknowledgmentGroupTime(arguments.acknowledgmentsGroupingDelayMillis, TimeUnit.MILLISECONDS) //
                .subscriptionType(arguments.subscriptionType)
                .subscriptionInitialPosition(arguments.subscriptionInitialPosition)
                .autoAckOldestChunkedMessageOnQueueFull(arguments.autoAckOldestChunkedMessageOnQueueFull)
                .enableBatchIndexAcknowledgment(arguments.batchIndexAck)
                .poolMessages(arguments.poolMessages)
                .replicateSubscriptionState(arguments.replicatedSubscription);
        if (arguments.maxPendingChunkedMessage > 0) {
            consumerBuilder.maxPendingChunkedMessage(arguments.maxPendingChunkedMessage);
        }
        if (arguments.expireTimeOfIncompleteChunkedMessageMs > 0) {
            consumerBuilder.expireTimeOfIncompleteChunkedMessage(arguments.expireTimeOfIncompleteChunkedMessageMs,
                    TimeUnit.MILLISECONDS);
        }

        if (isNotBlank(arguments.encKeyFile)) {
            consumerBuilder.defaultCryptoKeyReader(arguments.encKeyFile);
        }

        for (int i = 0; i < arguments.numTopics; i++) {
            final TopicName topicName = TopicName.get(arguments.topic.get(i));

            log.info("Adding {} consumers per subscription on topic {}", arguments.numConsumers, topicName);

            for (int j = 0; j < arguments.numSubscriptions; j++) {
                String subscriberName = arguments.subscriptions.get(j);
                for (int k = 0; k < arguments.numConsumers; k++) {
                    futures.add(consumerBuilder.clone().topic(topicName.toString()).subscriptionName(subscriberName)
                            .subscribeAsync());
                }
            }
        }
        for (Future<Consumer<ByteBuffer>> future : futures) {
            future.get();
        }
        log.info("Start receiving from {} consumers per subscription on {} topics", arguments.numConsumers,
                arguments.numTopics);

        long start = System.nanoTime();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(start, arguments);
            printAggregatedStats();
        }));


        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;
        HistogramLogWriter histogramLogWriter = null;

        if (arguments.histogramFile != null) {
            String statsFileName = arguments.histogramFile;
            log.info("Dumping latency stats to {}", statsFileName);

            PrintStream histogramLog = new PrintStream(new FileOutputStream(statsFileName), false);
            histogramLogWriter = new HistogramLogWriter(histogramLog);

            // Some log header bits
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();
        }

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
            double rateAck = messageAck.sumThenReset() / elapsed;
            long totalTxnOpSuccessNum = 0;
            long totalTxnOpFailNum = 0;
            double rateOpenTxn = 0;
            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            if (arguments.isEnableTransaction) {
                totalTxnOpSuccessNum = totalEndTxnOpSuccessNum.sum();
                totalTxnOpFailNum = totalEndTxnOpFailNum.sum();
                rateOpenTxn = numTxnOpSuccess.sumThenReset() / elapsed;
                log.info("--- Transaction: {} transaction end successfully --- {} transaction end failed "
                                + "--- {}  Txn/s --- AckRate: {} msg/s",
                        totalTxnOpSuccessNum,
                        totalTxnOpFailNum,
                        dec.format(rateOpenTxn),
                        dec.format(rateAck));
            }
            log.info(
                    "Throughput received: {} msg --- {}  msg/s --- {} Mbit/s  "
                            + "--- Latency: mean: {} ms - med: {} "
                            + "- 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                    intFormat.format(total),
                    dec.format(rate), dec.format(throughput), dec.format(reportHistogram.getMean()),
                    reportHistogram.getValueAtPercentile(50), reportHistogram.getValueAtPercentile(95),
                    reportHistogram.getValueAtPercentile(99), reportHistogram.getValueAtPercentile(99.9),
                    reportHistogram.getValueAtPercentile(99.99), reportHistogram.getMaxValue());

            if (histogramLogWriter != null) {
                histogramLogWriter.outputIntervalHistogram(reportHistogram);
            }

            reportHistogram.reset();
            oldTime = now;
        }

        pulsarClient.close();
    }

    private static void printAggregatedThroughput(long start, Arguments arguments) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesReceived.sum() / elapsed;
        double throughput = totalBytesReceived.sum() / elapsed * 8 / 1024 / 1024;
        long totalEndTxnSuccess = 0;
        long totalEndTxnFail = 0;
        long numTransactionOpenFailed = 0;
        long numTransactionOpenSuccess = 0;
        long totalnumMessageAckFailed = 0;
        double rateAck = totalMessageAck.sum() / elapsed;
        double rateOpenTxn = 0;
        if (arguments.isEnableTransaction) {
            totalEndTxnSuccess = totalEndTxnOpSuccessNum.sum();
            totalEndTxnFail = totalEndTxnOpFailNum.sum();
            rateOpenTxn = (totalEndTxnSuccess + totalEndTxnFail) / elapsed;
            totalnumMessageAckFailed = totalMessageAckFailed.sum();
            numTransactionOpenFailed = totalNumTxnOpenFail.sum();
            numTransactionOpenSuccess = totalNumTxnOpenSuccess.sum();
            log.info("-- Transaction: {}  transaction end successfully --- {} transaction end failed "
                            + "--- {} transaction open successfully --- {} transaction open failed "
                            + "--- {} Txn/s ",
                    totalEndTxnSuccess,
                    totalEndTxnFail,
                    numTransactionOpenSuccess,
                    numTransactionOpenFailed,
                    dec.format(rateOpenTxn));
        }
        log.info(
            "Aggregated throughput stats --- {} records received --- {} msg/s --- {} Mbit/s"
                 + " --- AckRate: {}  msg/s --- ack failed {} msg",
            totalMessagesReceived.sum(),
            dec.format(rate),
            dec.format(throughput),
                rateAck,
                totalnumMessageAckFailed);
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.info(
                "Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} "
                        + "- 99.99pct: {} - 99.999pct: {} - Max: {}",
                dec.format(reportHistogram.getMean()), reportHistogram.getValueAtPercentile(50),
                reportHistogram.getValueAtPercentile(95), reportHistogram.getValueAtPercentile(99),
                reportHistogram.getValueAtPercentile(99.9), reportHistogram.getValueAtPercentile(99.99),
                reportHistogram.getValueAtPercentile(99.999), reportHistogram.getMaxValue());
    }

    private static final Logger log = LoggerFactory.getLogger(PerformanceConsumer.class);
}
