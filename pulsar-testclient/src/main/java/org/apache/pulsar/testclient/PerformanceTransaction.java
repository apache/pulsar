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
import org.apache.curator.shaded.com.google.common.util.concurrent.RateLimiter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTransaction {


    private static final LongAdder totalNumTxnOp = new LongAdder();
    private static final LongAdder totalNumTxnOpFailed = new LongAdder();
    private static final LongAdder totalNumTxnOpSuccess = new LongAdder();
    private static final LongAdder numTxnOp = new LongAdder();

    private static final LongAdder numMessagesAckFailed = new LongAdder();
    private static final LongAdder numMessagesSendFailed = new LongAdder();


    private static final Recorder messageAckRecorder =
            new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static final Recorder messageAckCumulativeRecorder =
            new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);

    private static final Recorder messageSendRecorder =
            new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static final Recorder messageSendRCumulativeRecorder =
            new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);


    @Parameters(commandDescription = "Test pulsar transaction performance.")
    static class Arguments {

        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--conf-file"}, description = "Configuration file")
        public String confFile;

        @Parameter(names = "--topics-c", description = "All topics that need ack for a transaction", required =
                true)
        public List<String> consumerTopic = Collections.singletonList("test-consume");

        @Parameter(names = "--topics-p", description = "All topics that need produce for a transaction",
                required = true)
        public List<String> producerTopic = Collections.singletonList("test-produce");

        @Parameter(names = {"-threads", "--num-test-threads"}, description = "Number of test threads."
                + "This thread is for new transaction and ack the consumerTopic message and produce message to "
                + "producerTopic then commit or abort this transaction. "
                + "Increasing the number of threads will increase the parallelism of the performance test, "
                + "thereby increasing the intensity of the stress test.")
        public int numTestThreads = 1;

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
        private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Earliest;

        @Parameter(names = {"-st", "--subscription-type"}, description = "Subscription type")
        public SubscriptionType subscriptionType = SubscriptionType.Shared;

        @Parameter(names = {"-q", "--receiver-queue-size"}, description = "Size of the receiver queue")
        public int receiverQueueSize = 1000;

        @Parameter(names = {"-tto", "--txn-timeout"}, description = "Set the time value of transaction timeout,"
                + " and the TimeUnit is second. (Only --txn-enable true can it take effect) ")
        public long transactionTimeout = 5;

        @Parameter(names = {"-ntxn",
                "--number-txn"}, description = "Set the number of transaction, if 0, it will keep opening."
                + "If transaction disable, it means the number of task. The task or transaction will produce or "
                + "and consume a specified number of messages.")
        public long numTransactions = 0;

        @Parameter(names = {"-nmp", "--numMessage-perTransaction-produce"},
                description = "Set the number of messages produced in  a transaction."
                        + "If transaction disable, it means the number of messages produced in a task.")
        public int numMessagesProducedPerTransaction = 1;

        @Parameter(names = {"-nmc", "--numMessage-perTransaction-consume"},
                description = "Set the number of messages consumed in  a transaction."
                        + "if transaction disable, it means the number of message consumed in a task.")
        public int numMessagesReceivedPerTransaction = 1;

        @Parameter(names = {"-txn", "--txn-enable"}, description = "Enable or disable transaction")
        public boolean isEnableTransaction = true;

        @Parameter(names = {"-commit"}, description = "Whether to commit or abort the transaction. (Only --txn-enable "
                + "true can it take effect)")
        public boolean isCommitTransaction = true;

        @Parameter(names = "-txnRate", description = "Set the rate of transaction/task open, if 0, it will don`t limit")
        public int openTxnRate = 0;
    }

    public static void main(String[] args)
            throws IOException, PulsarAdminException, ExecutionException, InterruptedException {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf transaction");

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
            }
        }

        PulsarClient client =
                PulsarClient.builder().enableTransaction(arguments.isEnableTransaction)
                        .serviceUrl(arguments.serviceURL)
                        .connectionsPerBroker(arguments.maxConnections)
                        .statsInterval(0, TimeUnit.SECONDS)
                        .ioThreads(arguments.ioThreads)
                        .build();

        ExecutorService executorService = Executors.newFixedThreadPool(arguments.numTestThreads);

        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (arguments.testTime * 1e9);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (arguments.isEnableTransaction) {
                printTxnAggregatedThroughput(startTime, arguments.isCommitTransaction);
            } else {
                printAggregatedThroughput(startTime);
            }
            printAggregatedStats();
        }));

        // start perf test
        AtomicBoolean executing = new AtomicBoolean(true);
        new Thread(() -> {
                 RateLimiter rateLimiter = arguments.openTxnRate > 0
                    ? RateLimiter.create(arguments.openTxnRate)
                    : null;
            for(int i = 0; i < arguments.numTestThreads; i++) {
                    LongAdder messageSend = new LongAdder();
                    LongAdder messageReceived = new LongAdder();
                    executorService.submit(() -> {
                        //The producer and consumer clients are built in advance, and then this thread is
                        //responsible for the production and consumption tasks of the transaction through the loop.
                        //A thread may perform tasks of multiple transactions in a traversing manner.
                        List<Producer<byte[]>> producers = null;
                        List<List<Consumer<byte[]>>> consumers = null;
                        try {
                            producers = buildProducers(client, arguments);
                            consumers = buildConsumer(client, arguments);
                        } catch (Exception e) {
                            log.error("Failed to build Producer/Consumer with exception : " + e);
                        }
                        AtomicReference<Transaction> atomicReference = buildTransaction(client, arguments);
                        //The while loop has no break, and finally ends the execution through the shutdownNow of
                        //0the executorService
                        while (true) {
                            try {
                                Transaction transaction = atomicReference.get();
                                for (List<Consumer<byte[]>> subscriptions : consumers) {
                                    if(messageReceived.sum() == arguments.numMessagesReceivedPerTransaction){
                                        break;
                                    }
                                    for (Consumer<byte[]> consumer : subscriptions) {
                                        if(messageReceived.sum() == arguments.numMessagesReceivedPerTransaction){
                                            break;
                                        }
                                        Message message = null;
                                        try {
                                            message = consumer.receive(2, TimeUnit.SECONDS);
                                            log.info("Receive message {} ", message);
                                        }catch (Exception e){
                                            log.error("{} can`t receive message in 2 sec with exception {}", consumer, e);
                                        }

                                        messageReceived.increment();
                                        long receiveTime = System.nanoTime();
                                        if (arguments.isEnableTransaction) {
                                            consumer.acknowledgeAsync(message.getMessageId(), transaction)
                                                    .thenRun(() -> {
                                                        long latencyMicros = NANOSECONDS.toMicros(
                                                                System.nanoTime() - receiveTime);
                                                        messageAckRecorder.recordValue(latencyMicros);
                                                        messageAckCumulativeRecorder.recordValue(latencyMicros);
                                                    }).exceptionally(exception -> {
                                                if(exception instanceof InterruptedException && ! executing.get()){
                                                    return null;
                                                }
                                                log.error("Ack message failed with transaction {} throw exception {}",
                                                        transaction, exception);
                                                numMessagesAckFailed.increment();
                                                return null;
                                            });
                                        } else {
                                            consumer.acknowledgeAsync(message).thenRun(() -> {
                                                long latencyMicros = NANOSECONDS.toMicros(
                                                        System.nanoTime() - receiveTime);
                                                messageAckRecorder.recordValue(latencyMicros);
                                                messageAckCumulativeRecorder.recordValue(latencyMicros);
                                            }).exceptionally(exception -> {
                                                if(exception instanceof InterruptedException && ! executing.get()){
                                                    return null;
                                                }
                                                log.error("Ack message failed with transaction {} throw exception {}",
                                                        transaction, exception);
                                                numMessagesAckFailed.increment();
                                                return null;
                                            });
                                        }
                                    }
                                }

                                for(Producer<byte[]> producer : producers){
                                    if(messageSend.sum() >= arguments.numMessagesProducedPerTransaction){
                                        break;
                                    }
                                    long sendTime = System.nanoTime();
                                    messageSend.increment();
                                    if (arguments.isEnableTransaction) {
                                        producer.newMessage(transaction).value(payloadBytes)
                                                .sendAsync().thenRun(() -> {
                                            long latencyMicros = NANOSECONDS.toMicros(
                                                    System.nanoTime() - sendTime);
                                            messageSendRecorder.recordValue(latencyMicros);
                                            messageSendRCumulativeRecorder.recordValue(latencyMicros);
                                        }).exceptionally(exception -> {
                                            if(exception instanceof InterruptedException && ! executing.get()){
                                                return null;
                                            }
                                            log.error("Send transaction message failed with exception : " + exception);
                                            numMessagesSendFailed.increment();
                                            return null;
                                        });
                                    } else {
                                        producer.newMessage().value(payloadBytes)
                                                .sendAsync().thenRun(() -> {
                                            long latencyMicros = NANOSECONDS.toMicros(
                                                    System.nanoTime() - sendTime);
                                            messageSendRecorder.recordValue(latencyMicros);
                                            messageSendRCumulativeRecorder.recordValue(latencyMicros);
                                        }).exceptionally(exception -> {
                                            if(exception instanceof InterruptedException && ! executing.get()){
                                                return null;
                                            }
                                            log.error("Send message failed with exception : " + exception);
                                            numMessagesSendFailed.increment();
                                            return null;
                                        });
                                    }
                                }

                                if (messageSend.sum() == arguments.numMessagesProducedPerTransaction
                                        && messageReceived.sum() == arguments.numMessagesReceivedPerTransaction) {
                                    if(rateLimiter != null){
                                    rateLimiter.tryAcquire();
                                    }
                                    if (arguments.isEnableTransaction) {
                                        if (arguments.isCommitTransaction) {
                                            log.info("Committing transaction {}", transaction.getTxnID().toString());
                                            transaction.commit()
                                                    .thenRun(()->{
                                              totalNumTxnOpSuccess.increment();
                                              log.info("Committed transaction {}", transaction.getTxnID().toString());
                                                    })
                                                    .exceptionally(exception -> {
                                                     if(exception instanceof InterruptedException && ! executing.get()){
                                                            return null;
                                                        }
                                                     log.error("Commit transaction {} failed with exception {}",
                                                        transaction.getTxnID().toString(),
                                                        exception);
                                                     totalNumTxnOpFailed.increment();
                                                     return null;
                                            });
                                        } else {
                                            log.info("Aborting transaction {}", transaction.getTxnID().toString());
                                            totalNumTxnOpSuccess.increment();
                                            transaction.abort();
                                        }
                                        atomicReference.compareAndSet(transaction, client.newTransaction()
                                                .withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS)
                                                .build().get());
                                    }
                                    totalNumTxnOp.increment();
                                    numTxnOp.increment();
                                    messageSend.reset();
                                    messageReceived.reset();
                                }
                            } catch (Exception e) {
                                log.error("A exception happened in a transaction work flow :" + e);
                            }
                            if (arguments.numTransactions > 0) {
                                if (totalNumTxnOp.sum() >= arguments.numTransactions) {
                                    log.info("------------------- DONE -----------------------");
                                    executing.compareAndSet(true, false);
                                    executorService.shutdownNow();
                                    break;
                                }
                            }
                            if (arguments.testTime > 0) {
                                if (System.nanoTime() > testEndTime) {
                                    log.info("------------------- DONE -----------------------");
                                    executing.compareAndSet(true, false);
                                    executorService.shutdownNow();
                                    break;
                                }
                            }
                        }
                    });
            }
        }).start();


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

        while (executing.get()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            long total = totalNumTxnOp.sum();
            double rate = numTxnOp.sumThenReset() / elapsed;
            reportSendHistogram = messageSendRecorder.getIntervalHistogram(reportSendHistogram);
            reportAckHistogram = messageAckRecorder.getIntervalHistogram(reportAckHistogram);
            String txnOrTaskLog = arguments.isEnableTransaction
                    ? "Throughput transaction: {} transaction executes --- {} transaction/s"
                    : "Throughput task: {} task executes --- {} task/s";
            log.info(
                    txnOrTaskLog + "  ---send Latency: mean: {} ms - med:"
                            + " {} "
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


    private static void printTxnAggregatedThroughput(long start, boolean isCommit) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalNumTxnOp.sum() / elapsed;
        long numTransactionFailed = totalNumTxnOpFailed.sum();
        long numTransactionSuccess = totalNumTxnOpSuccess.sum();
        long numMessageAckFailed = numMessagesAckFailed.sum();
        long numMessageSendFailed = numMessagesSendFailed.sum();



           String transactionLog = isCommit
                    ? "--- transaction: " + numTransactionSuccess + " transaction commit successfully --- "
                    + numTransactionFailed + " transaction  failed --- "
                    : "--- transaction: " + numTransactionSuccess + " transaction aborted --- ";
        log.info(
                "Aggregated throughput stats --- {} transaction executed --- {} transaction/s "
                        +  transactionLog + " --- {} message ack failed --- {} message send failed",
                totalNumTxnOp.sum(),
                totalFormat.format(rate),
                numMessageAckFailed,
                numMessageSendFailed);
    }

    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalNumTxnOp.sum() / elapsed;
        long numMessageAckFailed = numMessagesAckFailed.sum();
        long numMessageSendFailed = numMessagesSendFailed.sum();
        log.info(
                "Aggregated throughput stats --- {} task executed --- {} task/s --- {} message ack failed "
                        + "--- {} message send failed",
                totalNumTxnOp.sum(),
                totalFormat.format(rate),
                numMessageAckFailed,
                numMessageSendFailed);
    }

    private static void printAggregatedStats() {
        Histogram reportAckHistogram = messageAckCumulativeRecorder.getIntervalHistogram();
        Histogram reportSendHistogram = messageSendRCumulativeRecorder.getIntervalHistogram();
        log.info(
                "Messages ack aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - "
                        + "99.9pct: {} - "
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
                "Messages send aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - "
                        + "99.9pct: {} - "
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

    private static AtomicReference<Transaction> buildTransaction(PulsarClient pulsarClient, Arguments arguments) {
        if (arguments.isEnableTransaction) {
            try {
                return new AtomicReference(pulsarClient.newTransaction()
                        .withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS).build().get());
            } catch (Exception e) {
                log.error("Failed to build a new transaction with exception : " + e);
            }
        }
        return new AtomicReference<>(null);
    }

    static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 8);
    static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 7);
    static final DecimalFormat intFormat = new PaddingDecimalFormat("0", 7);
    static final DecimalFormat totalFormat = new DecimalFormat("0.000");
    private static final Logger log = LoggerFactory.getLogger(PerformanceProducer.class);


    private static  List<List<Consumer<byte[]>>> buildConsumer(PulsarClient client, Arguments arguments)
            throws ExecutionException, InterruptedException {
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer(Schema.BYTES) //
                .subscriptionType(arguments.subscriptionType)
                .receiverQueueSize(arguments.receiverQueueSize)
                .subscriptionInitialPosition(arguments.subscriptionInitialPosition);

        Iterator<String> consumerTopicsIterator = arguments.consumerTopic.iterator();
        List<List<Consumer<byte[]>>> consumers = Lists.newArrayListWithCapacity(arguments.consumerTopic.size());
        while(consumerTopicsIterator.hasNext()){
            String topic = consumerTopicsIterator.next();
            final List<Consumer<byte[]>> subscriptions = Lists.newArrayListWithCapacity(arguments.numSubscriptions);
            final List<Future<Consumer<byte[]>>> subscriptionFutures =
                    Lists.newArrayListWithCapacity(arguments.numSubscriptions);
            log.info("Create subscriptions for topic {}", topic);
            for (int j = 0; j < arguments.numSubscriptions; j++) {
                String subscriberName = arguments.subscriptions.get(j);
                subscriptionFutures
                        .add(consumerBuilder.clone().topic(topic).subscriptionName(subscriberName)
                                .subscribeAsync());
            }
            for (Future<Consumer<byte[]>> future : subscriptionFutures) {
                subscriptions.add(future.get());
            }
            consumers.add(subscriptions);
        }
        return consumers;
    }

    private static List<Producer<byte[]>> buildProducers(PulsarClient client, Arguments arguments)
            throws ExecutionException, InterruptedException {

        ProducerBuilder<byte[]> producerBuilder = client.newProducer(Schema.BYTES) //
                .sendTimeout(0, TimeUnit.SECONDS);

        final List<Future<Producer<byte[]>>> producerFutures = Lists.newArrayList();
        Iterator<String> produceTopicsIterator = arguments.producerTopic.iterator();
        while(produceTopicsIterator.hasNext()){
            String topic = produceTopicsIterator.next();
            log.info("Create producer for topic {}", topic);
            producerFutures.add(producerBuilder.clone().topic(topic).createAsync());
        }
        final List<Producer<byte[]>> producers = Lists.newArrayListWithCapacity(producerFutures.size());

        for (Future<Producer<byte[]>> future : producerFutures) {
            producers.add(future.get());
        }
        return  producers;
    }

}
