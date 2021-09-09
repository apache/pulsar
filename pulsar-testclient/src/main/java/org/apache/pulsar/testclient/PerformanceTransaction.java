/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Iterator;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
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

    private static final LongAdder totalMessageAck = new LongAdder();
    private static final LongAdder messageAck = new LongAdder();


    private static Recorder messageAckRecorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static Recorder messageAckCumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);

    private static Recorder messageSendRecorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static Recorder messageSendRCumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);



    @Parameters(commandDescription = "Test pulsar transaction performance.")
    static class Arguments {

        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--conf-file"}, description = "Configuration file")
        public String confFile;

        @Parameter(names = "--topic-c", description = "consumer will be created to consumer this topic", required =
                true)
        public List<String> consumerTopic = Collections.singletonList("sub");

        @Parameter(names = "--topic-c", description = "producer will be created to produce message to this topic",
                required = true)
        public List<String> producerTopic = Collections.singletonList("test");

        @Parameter(names = {"-threads", "--num-test-threads"}, description = "Number of test threads")
        public int numTestThreads = 1;


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

        @Parameter(names = {"-ns", "--num-subscriptions"}, description = "Number of subscriptions (per topic)")
        public int numSubscriptions = 1;


        @Parameter(names = {"-sp", "--subscription-position"}, description = "Subscription position")
        private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;


        @Parameter(names = {"-st", "--subscription-type"}, description = "Subscription type")
        public SubscriptionType subscriptionType = SubscriptionType.Exclusive;

        @Parameter(names = {"-q", "--receiver-queue-size"}, description = "Size of the receiver queue")
        public int receiverQueueSize = 1000;


        @Parameter(names = {"-timeout", "--txn-timeout"}, description = "transaction timeout")
        public long transactionTimeout = 5;

        @Parameter(names = {"-nmp", "--numMessage-perTransaction-produce"},
                description = "the number of messages produced by  a transaction")
        public int numMessagesProducedPerTransaction = 1;
        @Parameter(names = {"-nmc", "--numMessage-perTransaction-consume"},
                description = "the number of messages consumed by  a transaction")
        public int numMessagesReceivedPerTransaction = 1;

        @Parameter(names = {"-ntnx",
                "--number-txn"}, description = "the number of transaction, if o, it will keep opening")
        public long numTransactions = 0;

        @Parameter(names = {"-txn", "--txn-enable"}, description = " whether transactions need to  be opened ")
        public boolean isEnableTransaction = false;
        @Parameter(names = {"-end"}, description = " how to end a transaction, commit or abort")
        public boolean isCommitedTransaction = true;

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
                for (String topic : arguments.producerTopic) {
                    log.info("Creating  produce partitioned topic {} with {} partitions", topic, arguments.partitions);
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
                for (String topic : arguments.consumerTopic) {
                    log.info("Creating  consume partitioned topic {} with {} partitions", topic, arguments.partitions);
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


        PulsarClient client =
                PulsarClient.builder().enableTransaction(arguments.isEnableTransaction).serviceUrl(arguments.serviceURL)
                        .connectionsPerBroker(arguments.maxConnections)
                        .ioThreads(arguments.ioThreads)
                        .statsInterval(0, TimeUnit.SECONDS)
                        .ioThreads(arguments.ioThreads).build();

        ProducerBuilder<byte[]> producerBuilder = client.newProducer()//
                .sendTimeout(0, TimeUnit.SECONDS);

        final List<Future<Producer<byte[]>>> producerFutures = Lists.newArrayList();


        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer(Schema.BYTES) //
                .subscriptionType(arguments.subscriptionType)
                .receiverQueueSize(arguments.receiverQueueSize)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);


        Iterator<String> consumerTopicIterator = arguments.consumerTopic.iterator();
        Iterator<String> producerTopicIterator = arguments.producerTopic.iterator();

        final List<List<Consumer<byte[]>>> consumers = Lists.newArrayList();
        for (int i = 0; i < arguments.numTestThreads; i++) {
            final List<Consumer<byte[]>> subscriptions = Lists.newArrayListWithCapacity(arguments.numSubscriptions);
            final List<Future<Consumer<byte[]>>> subscriptionFutures =
                    Lists.newArrayListWithCapacity(arguments.numSubscriptions);
            if (consumerTopicIterator.hasNext()) {
                String topic = consumerTopicIterator.next();
                log.info("create subscriptions for topic {}", topic);
                final TopicName topicName = TopicName.get(topic);
                for (int j = 0; j < arguments.numSubscriptions; j++) {
                    String subscriberName = arguments.subscriptions.get(j);
                    subscriptionFutures
                            .add(consumerBuilder.clone().topic(topicName.toString()).subscriptionName(subscriberName)
                                    .subscribeAsync());
                }
            } else {
                consumerTopicIterator = arguments.consumerTopic.iterator();
            }
            if (producerTopicIterator.hasNext()) {
                String topic = producerTopicIterator.next();
                log.info("create producer for topic {}", topic);
                producerFutures.add(producerBuilder.clone().topic(topic).createAsync());
            } else {
                producerTopicIterator = arguments.producerTopic.iterator();
            }

            for (Future<Consumer<byte[]>> future : subscriptionFutures) {
                subscriptions.add(future.get());
            }
            consumers.add(subscriptions);
        }
        final List<Producer<byte[]>> producers = Lists.newArrayListWithCapacity(producerFutures.size());

        for (Future<Producer<byte[]>> future : producerFutures) {
            producers.add(future.get());
        }


        // start perf test

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
                    LongAdder messageSend = new LongAdder();
                    LongAdder messageReceived = new LongAdder();
                    executorService.submit(() -> {
                        try {
                            Producer<byte[]> producer = producers.get(RandomUtils.nextInt() % producers.size());
                            List<Consumer<byte[]>> subscriptions =
                                    consumers.get(RandomUtils.nextInt() % consumers.size());

                            AtomicReference<Transaction> atomicReference = buildTransaction(client, arguments);
                            while (true) {
                                for (Consumer<byte[]> consumer : subscriptions) {

                                    consumer.receiveAsync().thenAccept(message -> {
                                        long receiveTime = System.nanoTime();
                                        if (arguments.isEnableTransaction) {
                                            consumer.acknowledgeAsync(message.getMessageId(), atomicReference.get())
                                                    .thenRun(() -> {
                                                        long latencyMicros = NANOSECONDS.toMicros(
                                                                System.nanoTime() - receiveTime);
                                                        messageAckRecorder.recordValue(latencyMicros);
                                                        messageAckCumulativeRecorder.recordValue(latencyMicros);
                                                    });
                                        } else {
                                            consumer.acknowledgeAsync(message).thenRun(() -> {
                                                long latencyMicros = NANOSECONDS.toMicros(
                                                        System.nanoTime() - receiveTime);
                                                messageAckRecorder.recordValue(latencyMicros);
                                                messageAckCumulativeRecorder.recordValue(latencyMicros);
                                            });
                                        }
                                        messageReceived.increment();
                                    });
                                }
                                Transaction transaction = atomicReference.get();
                                long sendTime = System.nanoTime();
                                if (arguments.isEnableTransaction) {
                                    producer.newMessage(transaction).value(payloadBytes)
                                            .sendAsync().thenRun(() -> {
                                        messageSend.increment();
                                        long latencyMicros = NANOSECONDS.toMicros(
                                                System.nanoTime() - sendTime);
                                        messageAckRecorder.recordValue(latencyMicros);
                                        messageAckCumulativeRecorder.recordValue(latencyMicros);
                                    });
                                } else {
                                    producer.newMessage().value(payloadBytes)
                                            .sendAsync().thenRun(() -> {
                                        messageSend.increment();
                                        long latencyMicros = NANOSECONDS.toMicros(
                                                System.nanoTime() - sendTime);
                                        messageAckRecorder.recordValue(latencyMicros);
                                        messageAckCumulativeRecorder.recordValue(latencyMicros);
                                    });
                                }

                                if (messageSend.sum() >= arguments.numMessagesProducedPerTransaction
                                        || messageReceived.sum() >= arguments.numMessagesReceivedPerTransaction) {
                                    if (arguments.isEnableTransaction) {
                                            if (arguments.isCommitedTransaction) {
                                                transaction.commit();
                                            } else {
                                                transaction.abort();
                                            }
                                        atomicReference.compareAndSet(transaction, client.newTransaction()
                                                .withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS).build().get());
                                    }
                                    totalNumTransaction.increment();
                                    numTransaction.increment();
                                    break;
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

        Histogram reportSendHistogram = null;
        Histogram reportAckHistogram = null;

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

            reportSendHistogram = messageSendRecorder.getIntervalHistogram(reportSendHistogram);
            reportAckHistogram  = messageAckRecorder.getIntervalHistogram(reportAckHistogram);
            log.info(
                    "Throughput transaction: {} transaction --- {} transaction/s  ---send Latency: mean: {} ms - med: {} "
                            + "- 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}" + "---ack Latency: "
                            + "mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}"
                    ,
                    intFormat.format(total),
                    throughputFormat.format(rate),
                    dec.format(reportSendHistogram.getMean() / 1000.0),
                    dec.format(reportSendHistogram.getValueAtPercentile(50) / 1000.0),
                    dec.format(reportSendHistogram.getValueAtPercentile(95) / 1000.0),
                    dec.format(reportSendHistogram.getValueAtPercentile(99) / 1000.0),
                    dec.format(reportSendHistogram.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(reportSendHistogram.getValueAtPercentile(99.99) / 1000.0),
                    dec.format(reportSendHistogram.getMaxValue() / 1000.0),
                    dec.format(reportAckHistogram.getMean() / 1000.0),
                    dec.format(reportAckHistogram.getValueAtPercentile(50) / 1000.0),
                    dec.format(reportAckHistogram.getValueAtPercentile(95) / 1000.0),
                    dec.format(reportAckHistogram.getValueAtPercentile(99) / 1000.0),
                    dec.format(reportAckHistogram.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(reportAckHistogram.getValueAtPercentile(99.99) / 1000.0),
                    dec.format(reportAckHistogram.getMaxValue() / 1000.0));

            histogramLogWriter.outputIntervalHistogram(reportSendHistogram);
            histogramLogWriter.outputIntervalHistogram(reportAckHistogram);
            reportSendHistogram.reset();
            reportAckHistogram.reset();

            oldTime = now;
        }


    }


    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalNumTransaction.sum() / elapsed;
        log.info(
                "Aggregated throughput stats --- {} transaction executed --- {} transaction/s",
                totalNumTransaction.sum(),
                totalFormat.format(rate));
    }



    private static void printAggregatedStats() {
        Histogram reportAckHistogram = messageAckCumulativeRecorder.getIntervalHistogram();
        Histogram reportSendHistogram = messageSendRCumulativeRecorder.getIntervalHistogram();
        log.info(
                "Messages ack aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - "
                        + "99.99pct: {} - 99.999pct: {} - Max: {}",
                dec.format(reportAckHistogram.getMean() / 1000.0),
                dec.format(reportAckHistogram.getValueAtPercentile(50) / 1000.0),
                dec.format(reportAckHistogram.getValueAtPercentile(95) / 1000.0),
                dec.format(reportAckHistogram.getValueAtPercentile(99) / 1000.0),
                dec.format(reportAckHistogram.getValueAtPercentile(99.9) / 1000.0),
                dec.format(reportAckHistogram.getValueAtPercentile(99.99) / 1000.0),
                dec.format(reportAckHistogram.getValueAtPercentile(99.999) / 1000.0),
                dec.format(reportAckHistogram.getMaxValue() / 1000.0));
        log.info(
                "Messages send aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - "
                        + "99.99pct: {} - 99.999pct: {} - Max: {}",
                dec.format(reportSendHistogram.getMean() / 1000.0),
                dec.format(reportSendHistogram.getValueAtPercentile(50) / 1000.0),
                dec.format(reportSendHistogram.getValueAtPercentile(95) / 1000.0),
                dec.format(reportSendHistogram.getValueAtPercentile(99) / 1000.0),
                dec.format(reportSendHistogram.getValueAtPercentile(99.9) / 1000.0),
                dec.format(reportSendHistogram.getValueAtPercentile(99.99) / 1000.0),
                dec.format(reportSendHistogram.getValueAtPercentile(99.999) / 1000.0),
                dec.format(reportSendHistogram.getMaxValue() / 1000.0));
    }
    private  static AtomicReference<Transaction> buildTransaction(PulsarClient pulsarClient, Arguments arguments){
        if(arguments.isEnableTransaction){
            try {
                return new AtomicReference(pulsarClient.newTransaction()
                        .withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS).build());
            } catch (PulsarClientException e) {
                log.error("Got transaction error: " + e.getMessage());
            }
        }
        return null;
    }

    static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 8);
    static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 7);
    static final DecimalFormat intFormat = new PaddingDecimalFormat("0", 7);
    static final DecimalFormat totalFormat = new DecimalFormat("0.000");
    private static final Logger log = LoggerFactory.getLogger(PerformanceProducer.class);
}
