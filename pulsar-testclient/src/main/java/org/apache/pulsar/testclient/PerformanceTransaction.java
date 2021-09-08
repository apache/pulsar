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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import java.io.FileInputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTransaction {


    private static final LongAdder totalNumTransaction = new LongAdder();
    private static final LongAdder numTransaction = new LongAdder();
    private static final LongAdder numMessagePerTransaction = new LongAdder();
    private static Recorder recorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static Recorder cumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);


    @Parameters(commandDescription = "Test pulsar transaction performance.")
    static class Arguments {

        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--conf-file"}, description = "Configuration file")
        public String confFile;

        @Parameter(description = "persistent://prop/ns/my-topic", required = true)
        public List<String> topics;

        @Parameter(names = {"-threads", "--num-test-threads"}, description = "Number of test threads")
        public int numTestThreads = 1;


        @Parameter(names = {"-t", "--num-topic"}, description = "Number of topics")
        public int numTopics = 1;

        @Parameter(names = {"-n", "--num-producers"}, description = "Number of producers (per topic)")
        public int numProducers = 1;

        @Parameter(names = {"--separator"}, description = "Separator between the topic and topic number")
        public String separator = "-";

        @Parameter(names = {"-au", "--admin-url"}, description = "Pulsar Admin URL")
        public String adminURL;


        @Parameter(names = {"-u", "--service-url"}, description = "Pulsar Service URL")
        public String serviceURL;


        @Parameter(names = {"-np",
                "--partitions"}, description = "Create partitioned topics with the given number of partitions, set 0 "
                + "to not try to create the topic")
        public Integer partitions = null;

        @Parameter(names = {"-c",
                "--max-connections"}, description = "Max number of TCP connections to a single broker")
        public int maxConnections = 100;

        @Parameter(names = {"-time",
                "--test-duration"}, description = "Test duration in secs. If 0, it will keep publishing")
        public long testTime = 0;


        @Parameter(names = {"-ioThreads", "--num-io-threads"}, description = "Set the number of threads to be " +
                "used for handling connections to brokers, default is 1 thread")
        public int ioThreads = 1;

        @Parameter(names = {"-ss",
                "--subscriptions"}, description = "A list of subscriptions to consume on (e.g. sub1,sub2)")
        public List<String> subscriptions = Collections.singletonList("sub");

        @Parameter(names = {"-n",
                "--num-consumers"}, description = "Number of consumers (per subscription), only one consumer is "
                + "allowed when subscriptionType is Exclusive")
        public int numConsumers = 1;

        @Parameter(names = {"-ns", "--num-subscriptions"}, description = "Number of subscriptions (per topic)")
        public int numSubscriptions = 1;

        @Parameter(names = {"-timeout", "--txn-timeout"}, description = "transaction timeout")
        public long transactionTimeout = 5;

        @Parameter(names = {"-nmt", "--numMessage-perTransaction"},
                description = "the number of a transaction produced")
        public int numMessagesPerTransaction = 1;

        @Parameter(names = {"-ntnx", "--number-txn"}, description = "the number of transaction")
        public long numTransactions = 0;

    }

    public static void main(String[] args)
            throws IOException, PulsarAdminException, ExecutionException, InterruptedException {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf produce");

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

        numMessagePerTransaction.add(arguments.numMessagesPerTransaction);

        if (arguments.topics != null && arguments.topics.size() != arguments.numTopics) {
            // keep compatibility with the previous version
            if (arguments.topics.size() == 1) {
                String prefixTopicName = arguments.topics.get(0);
                List<String> defaultTopics = Lists.newArrayList();
                for (int i = 0; i < arguments.numTopics; i++) {
                    defaultTopics.add(String.format("%s%s%d", prefixTopicName, arguments.separator, i));
                }
                arguments.topics = defaultTopics;
            } else {
                System.out.println("The size of topics list should be equal to --num-topic");
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

            if (arguments.adminURL == null) {
                arguments.adminURL = prop.getProperty("webServiceUrl");
            }
            if (arguments.adminURL == null) {
                arguments.adminURL = prop.getProperty("adminURL", "http://localhost:8080/");
            }
        }


        // Dump config variables
        PerfClientUtils.printJVMInformation(log);

        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar perf transaction with config: {}", w.writeValueAsString(arguments));

        final byte[] payloadBytes = new byte[1024];
        Random random = new Random(0);
        for (int i = 0; i < payloadBytes.length; ++i) {
            payloadBytes[i] = (byte) (random.nextInt(26) + 65);
        }
        if (arguments.partitions != null) {
            PulsarAdminBuilder clientBuilder = PulsarAdmin.builder()
                    .serviceHttpUrl(arguments.adminURL);


            try (PulsarAdmin client = clientBuilder.build()) {
                for (String topic : arguments.topics) {
                    log.info("Creating partitioned topic {} with {} partitions", topic, arguments.partitions);
                    try {
                        client.topics().createPartitionedTopic(topic, arguments.partitions);
                    } catch (PulsarAdminException.ConflictException alreadyExists) {
                        if (log.isDebugEnabled()) {
                            log.debug("Topic {} already exists: {}", topic, alreadyExists);
                        }
                        PartitionedTopicMetadata partitionedTopicMetadata =
                                client.topics().getPartitionedTopicMetadata(topic);
                        if (partitionedTopicMetadata.partitions != arguments.partitions) {
                            log.error(
                                    "Topic {} already exists but it has a wrong number of partitions: {}, expecting {}",
                                    topic, partitionedTopicMetadata.partitions, arguments.partitions);
                            PerfClientUtils.exit(-1);
                        }
                    }
                }
            }
        }


        PulsarClient client = PulsarClient.builder().enableTransaction(true).serviceUrl(arguments.serviceURL)
                .connectionsPerBroker(arguments.maxConnections).ioThreads(arguments.ioThreads)
                .statsInterval(0, TimeUnit.SECONDS)
                .ioThreads(arguments.ioThreads).build();

        ProducerBuilder<byte[]> producerBuilder = client.newProducer()//
                .sendTimeout(0, TimeUnit.SECONDS);

        final List<Future<Producer<byte[]>>> producerFutures = Lists.newArrayList();

        List<Future<Consumer<byte[]>>> conusmerFutures = Lists.newArrayList();
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer(Schema.BYTES) //
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        for (int i = 0; i < arguments.numTopics; i++) {

            String topic = arguments.topics.get(i);
            log.info("Adding {} publishers on topic {}", arguments.numProducers, topic);

            for (int j = 0; j < arguments.numProducers; j++) {
                ProducerBuilder<byte[]> prodBuilder = producerBuilder.clone().topic(topic);

                producerFutures.add(prodBuilder.createAsync());
            }

            final TopicName topicName = TopicName.get(topic);

            log.info("Adding {} consumers per subscription on topic {}", arguments.numConsumers, topicName);

            for (int j = 0; j < arguments.numSubscriptions; j++) {
                String subscriberName = arguments.subscriptions.get(j);
                for (int k = 0; k < arguments.numConsumers; k++) {
                    conusmerFutures
                            .add(consumerBuilder.clone().topic(topicName.toString()).subscriptionName(subscriberName)
                                    .subscribeAsync());
                }
            }
        }
        final List<Producer<byte[]>> producers = Lists.newArrayListWithCapacity(producerFutures.size());
        for (Future<Producer<byte[]>> future : producerFutures) {
            producers.add(future.get());
        }
        final List<Consumer<byte[]>> consumers = Lists.newArrayListWithCapacity(conusmerFutures.size());
        for (Future<Consumer<byte[]>> future : conusmerFutures) {
            consumers.add(future.get());
        }


        ExecutorService executorService = Executors.newFixedThreadPool(arguments.numTestThreads + 1);
        Semaphore semaphore = new Semaphore(arguments.numTestThreads);

        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (arguments.testTime * 1e9);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(startTime);
            printAggregatedStats();
        }));
        AtomicBoolean executing = new AtomicBoolean(true);
        executorService.submit(() -> {
            while (true) {
                if (semaphore.tryAcquire()) {
                    AtomicLong messageSend = new AtomicLong(0);
                    AtomicLong messageReceived = new AtomicLong(0);
                    executorService.submit(() -> {
                        try {
                            AtomicReference<Transaction> atomicReference = new AtomicReference<>(client.newTransaction()
                                    .withTransactionTimeout(arguments.transactionTimeout,
                                            TimeUnit.SECONDS).build().get());
                            Transaction transaction = atomicReference.get();
                            long start = System.nanoTime();
                            while (true) {
                                for (Producer<byte[]> producer : producers) {
                                    producer.newMessage(transaction).value(payloadBytes)
                                            .sendAsync().thenRun(() -> {
                                        messageSend.incrementAndGet();
                                    });
                                }
                                for (Consumer<byte[]> consumer : consumers) {
                                    consumer.receiveAsync().thenAccept(message -> {
                                        consumer.acknowledgeAsync(message.getMessageId(), transaction);
                                        messageReceived.incrementAndGet();
                                    });
                                }

                                if (messageSend.get() >= arguments.numMessagesPerTransaction
                                        && messageReceived.get() >= arguments.numMessagesPerTransaction) {
                                    if (atomicReference.compareAndSet(transaction, client.newTransaction().
                                            withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS)
                                            .build().get())) {
                                        transaction.commit();
                                        totalNumTransaction.increment();
                                        numTransaction.increment();
                                        long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                                        recorder.recordValue(latencyMicros);
                                        cumulativeRecorder.recordValue(latencyMicros);
                                        break;
                                    }
                                }
                            }

                        } catch (Exception e) {
                            log.error("Got error: " + e.getMessage());
                        }
                    });
                    semaphore.release();
                }

                if (arguments.numTransactions > 0) {
                    if (totalNumTransaction.sum() >= arguments.numTransactions) {
                        log.info("------------------- DONE -----------------------");
                        executing.compareAndSet(true, false);
                        break;
                    }
                }
                if (arguments.testTime > 0) {
                    if (System.nanoTime() > testEndTime) {
                        log.info("------------------- DONE -----------------------");
                        executing.compareAndSet(true, false);
                        break;
                    }
                }
            }
        });


        // Print report stats
        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;

        String statsFileName = "perf-transaction-" + System.currentTimeMillis() + ".hgrm";
        log.info("Dumping latency stats to {}", statsFileName);

        PrintStream histogramLog = new PrintStream(new FileOutputStream(statsFileName), false);
        HistogramLogWriter histogramLogWriter = new HistogramLogWriter(histogramLog);

        // Some log header bits
        histogramLogWriter.outputLogFormatVersion();
        histogramLogWriter.outputLegend();

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            if (!executing.get()) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            long total = totalNumTransaction.sum();
            double rate = numTransaction.sumThenReset() / elapsed;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info(
                    "Throughput transaction: {} transaction --- {} transaction/s  --- Latency: mean: {} ms - med: {} "
                            + "- 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                    intFormat.format(total),
                    throughputFormat.format(rate),
                    dec.format(reportHistogram.getMean() / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                    dec.format(reportHistogram.getMaxValue() / 1000.0));

            histogramLogWriter.outputIntervalHistogram(reportHistogram);
            reportHistogram.reset();

            oldTime = now;
        }


    }

    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalNumTransaction.sum() / elapsed;
        double throughput = numMessagePerTransaction.sum() * totalNumTransaction.sum() / elapsed / 1024 / 1024 * 8;
        log.info(
                "Aggregated throughput stats --- {} transaction commit --- {} transaction/s  --- {}msg/transaction"
                        + " --- {} Mbit/s",
                totalNumTransaction.sum(),
                totalFormat.format(rate),
                numMessagePerTransaction.sum(),
                totalFormat.format(throughput));
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.info(
                "Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - "
                        + "99.99pct: {} - 99.999pct: {} - Max: {}",
                dec.format(reportHistogram.getMean() / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(99.999) / 1000.0),
                dec.format(reportHistogram.getMaxValue() / 1000.0));
    }


    static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 8);
    static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 7);
    static final DecimalFormat intFormat = new PaddingDecimalFormat("0", 7);
    static final DecimalFormat totalFormat = new DecimalFormat("0.000");
    private static final Logger log = LoggerFactory.getLogger(PerformanceProducer.class);
}
