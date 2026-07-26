/*
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
import static org.apache.pulsar.testclient.PerfClientUtils.addShutdownHook;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "transaction", description = "Test pulsar transaction performance.")
@CustomLog
public class PerformanceTransaction extends PerformanceBaseArguments{

    /** Same v4-compat SubscriptionType flag as PerformanceConsumer. See its javadoc. */
    public enum SubscriptionType {
        Exclusive,
        Shared,
        Failover,
        Key_Shared
    }


    private static final LongAdder totalNumEndTxnOpFailed = new LongAdder();
    private static final LongAdder totalNumEndTxnOpSuccess = new LongAdder();
    private static final LongAdder numTxnOpSuccess = new LongAdder();
    private static final LongAdder totalNumTxnOpenTxnFail = new LongAdder();
    private static final LongAdder totalNumTxnOpenTxnSuccess = new LongAdder();

    private static final LongAdder numMessagesAckFailed = new LongAdder();
    private static final LongAdder numMessagesAckSuccess = new LongAdder();
    private static final LongAdder numMessagesSendFailed = new LongAdder();
    private static final LongAdder numMessagesSendSuccess = new LongAdder();

    private static final Recorder messageAckRecorder =
            new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static final Recorder messageAckCumulativeRecorder =
            new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);

    private static final Recorder messageSendRecorder =
            new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static final Recorder messageSendRCumulativeRecorder =
            new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);

    @Option(names = "--topics-c", description = "All topics that need ack for a transaction", required =
            true)
    public List<String> consumerTopic = Collections.singletonList("test-consume");

    @Option(names = "--topics-p", description = "All topics that need produce for a transaction",
            required = true)
    public List<String> producerTopic = Collections.singletonList("test-produce");

    @Option(names = {"-threads", "--num-test-threads"}, description = "Number of test threads."
            + "This thread is for a new transaction to ack messages from consumer topics and produce message to "
            + "producer topics, and then commit or abort this transaction. "
            + "Increasing the number of threads increases the parallelism of the performance test, "
            + "thereby increasing the intensity of the stress test.")
    public int numTestThreads = 1;

    @Option(names = {"-au", "--admin-url"}, description = "Pulsar Admin URL", descriptionKey = "webServiceUrl")
    public String adminURL;

    @Option(names = {"-np",
            "--partitions"}, description = "Create partitioned topics with a given number of partitions, 0 means"
            + "not trying to create a topic")
    public Integer partitions = null;

    @Option(names = {"--scalable"}, description = "Create the producer/consumer topics as scalable"
            + " topics (PIP-473) with --scalable-segments initial segments. Required for transactions"
            + " against the scalable-topics (v5) coordinator. Mutually exclusive with --partitions.")
    public boolean scalable = false;

    @Option(names = {"--scalable-segments"}, description = "Number of initial segments for scalable"
            + " topics created via --scalable.")
    public int scalableSegments = 1;

    @Option(names = {"-time",
            "--test-duration"}, description = "Test duration (in second). 0 means keeping publishing")
    public long testTime = 0;

    @Option(names = {"-ss",
            "--subscriptions"}, description = "A list of subscriptions to consume (for example, sub1,sub2)")
    public List<String> subscriptions = Collections.singletonList("sub");

    @Option(names = {"-ns", "--num-subscriptions"}, description = "Number of subscriptions (per topic)")
    public int numSubscriptions = 1;

    @Option(names = {"-sp", "--subscription-position"}, description = "Subscription position")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.EARLIEST;

    @Option(names = {"-st", "--subscription-type"}, description = "Subscription type")
    public SubscriptionType subscriptionType = SubscriptionType.Shared;

    @Option(names = {"-rs", "--replicated" },
            description = "Whether the subscription status should be replicated")
    private boolean replicatedSubscription = false;

    @Option(names = {"-q", "--receiver-queue-size"}, description = "Size of the receiver queue")
    public int receiverQueueSize = 1000;

    @Option(names = {"-tto", "--txn-timeout"}, description = "Set the time value of transaction timeout,"
            + " and the time unit is second. (After --txn-enable setting to true, --txn-timeout takes effect)")
    public long transactionTimeout = 5;

    @Option(names = {"-ntxn",
            "--number-txn"}, description = "Set the number of transaction. 0 means keeping open."
            + "If transaction disabled, it means the number of tasks. The task or transaction produces or "
            + "consumes a specified number of messages.")
    public long numTransactions = 0;

    @Option(names = {"-nmp", "--numMessage-perTransaction-produce"},
            description = "Set the number of messages produced in  a transaction."
                    + "If transaction disabled, it means the number of messages produced in a task.")
    public int numMessagesProducedPerTransaction = 1;

    @Option(names = {"-nmc", "--numMessage-perTransaction-consume"},
            description = "Set the number of messages consumed in a transaction."
                    + "If transaction disabled, it means the number of messages consumed in a task.")
    public int numMessagesReceivedPerTransaction = 1;

    @Option(names = {"--txn-disable"}, description = "Disable transaction")
    public boolean isDisableTransaction = false;

    @Option(names = {"-abort"}, description = "Abort the transaction. (After --txn-disEnable "
            + "setting to false, -abort takes effect)")
    public boolean isAbortTransaction = false;

    @Option(names = "-txnRate", description = "Set the rate of opened transaction or task. 0 means no limit")
    public int openTxnRate = 0;
    public PerformanceTransaction() {
        super("transaction");
    }

    @Override
    public void run() throws Exception {
        super.parseCLI();

        // Reset static counters to avoid stale state from previous runs in the same JVM
        totalNumEndTxnOpFailed.reset();
        totalNumEndTxnOpSuccess.reset();
        numTxnOpSuccess.reset();
        totalNumTxnOpenTxnFail.reset();
        totalNumTxnOpenTxnSuccess.reset();
        numMessagesAckFailed.reset();
        numMessagesAckSuccess.reset();
        numMessagesSendFailed.reset();
        numMessagesSendSuccess.reset();
        messageAckRecorder.reset();
        messageAckCumulativeRecorder.reset();
        messageSendRecorder.reset();
        messageSendRCumulativeRecorder.reset();

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(this)).log("Starting Pulsar perf transaction with config");

        final byte[] payloadBytes = new byte[1024];
        Random random = new Random(0);
        for (int i = 0; i < payloadBytes.length; ++i) {
            payloadBytes[i] = (byte) (random.nextInt(26) + 65);
        }
        if (this.scalable) {
            // Scalable topics (PIP-473) must be pre-created via the admin API — they don't
            // auto-create on produce. Create both the produce and consume topics so a
            // transaction against the scalable-topics coordinator has segment participants.
            final PulsarAdminBuilder adminBuilder = PerfClientUtils
                    .createAdminBuilderFromArguments(this, this.adminURL);
            try (PulsarAdmin adminClient = adminBuilder.build()) {
                List<String> allTopics = new ArrayList<>(this.producerTopic);
                allTopics.addAll(this.consumerTopic);
                for (String topic : allTopics) {
                    try {
                        adminClient.scalableTopics().createScalableTopic(topic, this.scalableSegments);
                        log.info().attr("topic", topic).attr("segments", this.scalableSegments)
                                .log("Created scalable topic");
                    } catch (PulsarAdminException.ConflictException alreadyExists) {
                        log.debug().attr("topic", topic).attr("exists", alreadyExists)
                                .log("Scalable topic already exists");
                    }
                }
            }
        } else if (this.partitions != null) {
            final PulsarAdminBuilder adminBuilder = PerfClientUtils
                    .createAdminBuilderFromArguments(this, this.adminURL);

            try (PulsarAdmin adminClient = adminBuilder.build()) {
                for (String topic : this.producerTopic) {
                    log.info()
                            .attr("topic", topic)
                            .attr("partitions", this.partitions)
                            .log("Creating produce partitioned topic with partitions");
                    try {
                        adminClient.topics().createPartitionedTopic(topic, this.partitions);
                    } catch (PulsarAdminException.ConflictException alreadyExists) {
                        log.debug().attr("topic", topic).attr("exists", alreadyExists).log("Topic already exists");
                        PartitionedTopicMetadata partitionedTopicMetadata =
                                adminClient.topics().getPartitionedTopicMetadata(topic);
                        if (partitionedTopicMetadata.partitions != this.partitions) {
                            log.error()
                                    .attr("topic", topic)
                                    .attr("partitions", partitionedTopicMetadata.partitions)
                                    .attr("expecting", this.partitions)
                                    .log("Topic already exists but it has a wrong number of partitions: , expecting");
                            PerfClientUtils.exit(1);
                        }
                    }
                }
            }
        }

        if (this.subscriptionType == SubscriptionType.Exclusive
                || this.subscriptionType == SubscriptionType.Failover) {
            log.warn().attr("type", this.subscriptionType)
                    .log("V5 has no exclusive/failover subscription type. Falling back to QueueConsumer "
                            + "(Shared-style work distribution).");
        }

        PulsarClientBuilder clientBuilder = PerfClientUtils.createV5ClientBuilderFromArguments(this);
        if (!this.isDisableTransaction) {
            clientBuilder.transactionPolicy(TransactionPolicy.builder()
                    .timeout(Duration.ofSeconds(this.transactionTimeout))
                    .build());
        }
        PulsarClient client = clientBuilder.build();
        try {

            ExecutorService executorService = new ThreadPoolExecutor(this.numTestThreads,
                    this.numTestThreads,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());


            long startTime = System.nanoTime();
            long testEndTime = startTime + (long) (this.testTime * 1e9);
            Thread shutdownHookThread = addShutdownHook(() -> {
                if (!this.isDisableTransaction) {
                    printTxnAggregatedThroughput(startTime);
                } else {
                    printAggregatedThroughput(startTime);
                }
                printAggregatedStats();
            });

            // start perf test
            AtomicBoolean executing = new AtomicBoolean(true);

            RateLimiter rateLimiter = this.openTxnRate > 0
                    ? RateLimiter.create(this.openTxnRate)
                    : null;
            for (int i = 0; i < this.numTestThreads; i++) {
                executorService.submit(() -> {
                    //The producer and consumer clients are built in advance, and then this thread is
                    //responsible for the production and consumption tasks of the transaction through the loop.
                    //A thread may perform tasks of multiple transactions in a traversing manner.
                    List<Producer<byte[]>> producers = null;
                    List<List<QueueConsumer<byte[]>>> consumers = null;
                    AtomicReference<Transaction> atomicReference = null;
                    try {
                        producers = buildProducers(client);
                        consumers = buildConsumer(client);
                        if (!this.isDisableTransaction) {
                            atomicReference = new AtomicReference<>(client.newTransaction());
                        } else {
                            atomicReference = new AtomicReference<>(null);
                        }
                    } catch (Exception e) {
                        if (PerfClientUtils.hasInterruptedException(e)) {
                            Thread.currentThread().interrupt();
                        } else {
                            log.error().exception(e).log("Failed to build Producer/Consumer with exception");
                        }
                        executorService.shutdownNow();
                        PerfClientUtils.exit(1);
                    }
                    //The while loop has no break, and finally ends the execution through the shutdownNow of
                    //the executorService
                    while (!Thread.currentThread().isInterrupted()) {
                        if (this.numTransactions > 0) {
                            if (totalNumTxnOpenTxnFail.sum()
                                    + totalNumTxnOpenTxnSuccess.sum() >= this.numTransactions) {
                                if (totalNumEndTxnOpFailed.sum()
                                        + totalNumEndTxnOpSuccess.sum() < this.numTransactions) {
                                    continue;
                                }
                                log.info("------------------- DONE -----------------------");
                                executing.compareAndSet(true, false);
                                executorService.shutdownNow();
                                PerfClientUtils.exit(0);
                                break;
                            }
                        }
                        if (this.testTime > 0) {
                            if (System.nanoTime() > testEndTime) {
                                log.info("------------------- DONE -----------------------");
                                executing.compareAndSet(true, false);
                                executorService.shutdownNow();
                                PerfClientUtils.exit(0);
                                break;
                            }
                        }
                        Transaction transaction = atomicReference.get();
                        for (List<QueueConsumer<byte[]>> subscriptions : consumers) {
                            for (QueueConsumer<byte[]> consumer : subscriptions) {
                                for (int j = 0; j < this.numMessagesReceivedPerTransaction; j++) {
                                    Message<byte[]> message = null;
                                    try {
                                        message = consumer.receive();
                                    } catch (PulsarClientException e) {
                                        log.error().exception(e).log("Receive message failed");
                                        executorService.shutdownNow();
                                        PerfClientUtils.exit(1);
                                    }
                                    long receiveTime = System.nanoTime();
                                    // V5 acknowledge is synchronous void. Record latency immediately
                                    // and catch any failure into the existing counter.
                                    try {
                                        if (!this.isDisableTransaction) {
                                            consumer.acknowledge(message.id(), transaction);
                                        } else {
                                            consumer.acknowledge(message.id());
                                        }
                                        long latencyMicros = NANOSECONDS.toMicros(
                                                System.nanoTime() - receiveTime);
                                        messageAckRecorder.recordValue(latencyMicros);
                                        messageAckCumulativeRecorder.recordValue(latencyMicros);
                                        numMessagesAckSuccess.increment();
                                    } catch (Exception ackEx) {
                                        if (PerfClientUtils.hasInterruptedException(ackEx)) {
                                            Thread.currentThread().interrupt();
                                        } else {
                                            log.error()
                                                    .exception(ackEx)
                                                    .log("Ack message failed with transaction throw exception");
                                            numMessagesAckFailed.increment();
                                        }
                                    }
                                }
                            }
                        }

                        // V5 transaction-aware sends are queued onto an internal dispatch chain,
                        // so the v4-side txn-coordinator registration of the send can race the
                        // commit() if commit fires before the chain drains. We collect each
                        // per-txn send future here and await them all before committing — this is
                        // also the semantically-correct ordering (commit only after sends land).
                        java.util.List<java.util.concurrent.CompletableFuture<?>> pendingSends =
                                new java.util.ArrayList<>();
                        for (Producer<byte[]> producer : producers) {
                            AsyncProducer<byte[]> asyncProducer = producer.async();
                            for (int j = 0; j < this.numMessagesProducedPerTransaction; j++) {
                                long sendTime = System.nanoTime();
                                var msg = asyncProducer.newMessage().value(payloadBytes);
                                if (!this.isDisableTransaction) {
                                    msg.transaction(transaction);
                                }
                                pendingSends.add(msg.send().whenComplete((id, ex) -> {
                                    if (ex == null) {
                                        long latencyMicros = NANOSECONDS.toMicros(
                                                System.nanoTime() - sendTime);
                                        messageSendRecorder.recordValue(latencyMicros);
                                        messageSendRCumulativeRecorder.recordValue(latencyMicros);
                                        numMessagesSendSuccess.increment();
                                    } else {
                                        if (PerfClientUtils.hasInterruptedException(ex)) {
                                            Thread.currentThread().interrupt();
                                            return;
                                        }
                                        // Ignore the exception when the producer is closed
                                        if (ex.getCause()
                                                instanceof PulsarClientException.AlreadyClosedException) {
                                            return;
                                        }
                                        log.error()
                                                .exception(ex)
                                                .log("Send message failed with exception");
                                        numMessagesSendFailed.increment();
                                    }
                                }));
                            }
                        }

                        // Await all pending sends before committing so the txn-coordinator has
                        // registered every send. allOf().exceptionally() swallows individual send
                        // failures here — they are already counted in the whenComplete above.
                        try {
                            java.util.concurrent.CompletableFuture.allOf(
                                    pendingSends.toArray(new java.util.concurrent.CompletableFuture[0]))
                                    .exceptionally(t -> null)
                                    .join();
                        } catch (Exception awaitEx) {
                            if (PerfClientUtils.hasInterruptedException(awaitEx)) {
                                Thread.currentThread().interrupt();
                            }
                        }

                        if (rateLimiter != null) {
                            rateLimiter.tryAcquire();
                        }
                        if (!this.isDisableTransaction) {
                            if (!this.isAbortTransaction) {
                                transaction.async().commit()
                                        .thenRun(() -> {
                                            numTxnOpSuccess.increment();
                                            totalNumEndTxnOpSuccess.increment();
                                        }).exceptionally(exception -> {
                                            if (PerfClientUtils.hasInterruptedException(exception)) {
                                                Thread.currentThread().interrupt();
                                                return null;
                                            }
                                            log.error()
                                                    .exception(exception)
                                                    .log("Commit transaction failed with exception");
                                            totalNumEndTxnOpFailed.increment();
                                            return null;
                                        });
                            } else {
                                transaction.async().abort()
                                        .thenRun(() -> {
                                            numTxnOpSuccess.increment();
                                            totalNumEndTxnOpSuccess.increment();
                                        }).exceptionally(exception -> {
                                            if (PerfClientUtils.hasInterruptedException(exception)) {
                                                Thread.currentThread().interrupt();
                                                return null;
                                            }
                                            log.error()
                                                    .exception(exception)
                                                    .log("Abort transaction failed with exception");
                                            totalNumEndTxnOpFailed.increment();
                                            return null;
                                        });
                            }
                            while (!Thread.currentThread().isInterrupted()) {
                                try {
                                    Transaction newTransaction = client.newTransaction();
                                    atomicReference.compareAndSet(transaction, newTransaction);
                                    totalNumTxnOpenTxnSuccess.increment();
                                    break;
                                } catch (Exception throwable) {
                                    if (PerfClientUtils.hasInterruptedException(throwable)) {
                                        Thread.currentThread().interrupt();
                                    } else {
                                        log.error()
                                                .exception(throwable)
                                                .log("Failed to new transaction with exception");
                                        totalNumTxnOpenTxnFail.increment();
                                    }
                                }
                            }
                        } else {
                            totalNumTxnOpenTxnSuccess.increment();
                            totalNumEndTxnOpSuccess.increment();
                            numTxnOpSuccess.increment();
                        }
                    }
                });
            }


            // Print report stats
            long oldTime = System.nanoTime();

            Histogram reportSendHistogram = null;
            Histogram reportAckHistogram = null;

            String statsFileName = "perf-transaction-" + System.currentTimeMillis() + ".hgrm";
            log.info().attr("stats", statsFileName).log("Dumping latency stats to");

            PrintStream histogramLog = new PrintStream(new FileOutputStream(statsFileName), false);
            HistogramLogWriter histogramLogWriter = new HistogramLogWriter(histogramLog);

            // Some log header bits
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();

            while (!Thread.currentThread().isInterrupted() && executing.get()) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                long now = System.nanoTime();
                double elapsed = (now - oldTime) / 1e9;
                long total = totalNumEndTxnOpFailed.sum() + totalNumTxnOpenTxnSuccess.sum();
                double rate = numTxnOpSuccess.sumThenReset() / elapsed;
                reportSendHistogram = messageSendRecorder.getIntervalHistogram(reportSendHistogram);
                reportAckHistogram = messageAckRecorder.getIntervalHistogram(reportAckHistogram);
                String label = !this.isDisableTransaction
                        ? "Throughput transaction" : "Throughput task";
                log.infof("%s: %7d --- %7.3f/s"
                                + " --- SendLatency: mean: %7.3f ms - med: %7.3f"
                                + " - 95pct: %7.3f - 99pct: %7.3f"
                                + " - 99.9pct: %7.3f - 99.99pct: %7.3f - Max: %7.3f"
                                + " --- AckLatency: mean: %7.3f ms - med: %7.3f"
                                + " - 95pct: %7.3f - 99pct: %7.3f"
                                + " - 99.9pct: %7.3f - 99.99pct: %7.3f - Max: %7.3f",
                        label, total, rate,
                        reportSendHistogram.getMean() / 1000.0,
                        reportSendHistogram.getValueAtPercentile(50) / 1000.0,
                        reportSendHistogram.getValueAtPercentile(95) / 1000.0,
                        reportSendHistogram.getValueAtPercentile(99) / 1000.0,
                        reportSendHistogram.getValueAtPercentile(99.9) / 1000.0,
                        reportSendHistogram.getValueAtPercentile(99.99) / 1000.0,
                        reportSendHistogram.getMaxValue() / 1000.0,
                        reportAckHistogram.getMean() / 1000.0,
                        reportAckHistogram.getValueAtPercentile(50) / 1000.0,
                        reportAckHistogram.getValueAtPercentile(95) / 1000.0,
                        reportAckHistogram.getValueAtPercentile(99) / 1000.0,
                        reportAckHistogram.getValueAtPercentile(99.9) / 1000.0,
                        reportAckHistogram.getValueAtPercentile(99.99) / 1000.0,
                        reportAckHistogram.getMaxValue() / 1000.0);

                histogramLogWriter.outputIntervalHistogram(reportSendHistogram);
                histogramLogWriter.outputIntervalHistogram(reportAckHistogram);
                reportSendHistogram.reset();
                reportAckHistogram.reset();

                oldTime = now;
            }

            PerfClientUtils.removeAndRunShutdownHook(shutdownHookThread);
        } finally {
            PerfClientUtils.closeClient(client);
        }
    }


    private static void printTxnAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        long numTransactionEndFailed = totalNumEndTxnOpFailed.sum();
        long numTransactionEndSuccess = totalNumEndTxnOpSuccess.sum();
        long total = numTransactionEndFailed + numTransactionEndSuccess;
        double rate = total / elapsed;
        long numMessageAckFailed = numMessagesAckFailed.sum();
        long numMessageAckSuccess = numMessagesAckSuccess.sum();
        long numMessageSendFailed = numMessagesSendFailed.sum();
        long numMessageSendSuccess = numMessagesSendSuccess.sum();
        long numTransactionOpenFailed = totalNumTxnOpenTxnFail.sum();
        long numTransactionOpenSuccess = totalNumTxnOpenTxnSuccess.sum();

        log.infof("Aggregated throughput stats --- %d transaction executed --- %7.3f transaction/s"
                        + " --- %d transaction open successfully --- %d transaction open failed"
                        + " --- %d transaction end successfully --- %d transaction end failed"
                        + " --- %d message ack failed --- %d message send failed"
                        + " --- %d message ack success --- %d message send success",
                total, rate,
                numTransactionOpenSuccess, numTransactionOpenFailed,
                numTransactionEndSuccess, numTransactionEndFailed,
                numMessageAckFailed, numMessageSendFailed,
                numMessageAckSuccess, numMessageSendSuccess);

    }

    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        long total = totalNumEndTxnOpFailed.sum() + totalNumEndTxnOpSuccess.sum();
        double rate = total / elapsed;
        long numMessageAckFailed = numMessagesAckFailed.sum();
        long numMessageAckSuccess = numMessagesAckSuccess.sum();
        long numMessageSendFailed = numMessagesSendFailed.sum();
        long numMessageSendSuccess = numMessagesSendSuccess.sum();
        log.infof("Aggregated throughput stats --- %d task executed --- %.3f task/s"
                        + " --- %d message ack failed --- %d message send failed"
                        + " --- %d message ack success --- %d message send success",
                total, rate,
                numMessageAckFailed, numMessageSendFailed,
                numMessageAckSuccess, numMessageSendSuccess);
    }

    private static void printAggregatedStats() {
        Histogram reportAckHistogram = messageAckCumulativeRecorder.getIntervalHistogram();
        Histogram reportSendHistogram = messageSendRCumulativeRecorder.getIntervalHistogram();
        log.infof("Messages ack aggregated latency stats --- Latency: mean: %7.3f ms"
                        + " - med: %7.3f - 95pct: %7.3f - 99pct: %7.3f"
                        + " - 99.9pct: %7.3f - 99.99pct: %7.3f"
                        + " - 99.999pct: %7.3f - Max: %7.3f",
                reportAckHistogram.getMean() / 1000.0,
                reportAckHistogram.getValueAtPercentile(50) / 1000.0,
                reportAckHistogram.getValueAtPercentile(95) / 1000.0,
                reportAckHistogram.getValueAtPercentile(99) / 1000.0,
                reportAckHistogram.getValueAtPercentile(99.9) / 1000.0,
                reportAckHistogram.getValueAtPercentile(99.99) / 1000.0,
                reportAckHistogram.getValueAtPercentile(99.999) / 1000.0,
                reportAckHistogram.getMaxValue() / 1000.0);
        log.infof("Messages send aggregated latency stats --- Latency: mean: %7.3f ms"
                        + " - med: %7.3f - 95pct: %7.3f - 99pct: %7.3f"
                        + " - 99.9pct: %7.3f - 99.99pct: %7.3f"
                        + " - 99.999pct: %7.3f - Max: %7.3f",
                reportSendHistogram.getMean() / 1000.0,
                reportSendHistogram.getValueAtPercentile(50) / 1000.0,
                reportSendHistogram.getValueAtPercentile(95) / 1000.0,
                reportSendHistogram.getValueAtPercentile(99) / 1000.0,
                reportSendHistogram.getValueAtPercentile(99.9) / 1000.0,
                reportSendHistogram.getValueAtPercentile(99.99) / 1000.0,
                reportSendHistogram.getValueAtPercentile(99.999) / 1000.0,
                reportSendHistogram.getMaxValue() / 1000.0);
    }



    private List<List<QueueConsumer<byte[]>>> buildConsumer(PulsarClient client)
            throws ExecutionException, InterruptedException {

        Iterator<String> consumerTopicsIterator = this.consumerTopic.iterator();
        List<List<QueueConsumer<byte[]>>> consumers = new ArrayList<>(this.consumerTopic.size());
        while (consumerTopicsIterator.hasNext()){
            String topic = consumerTopicsIterator.next();
            final List<QueueConsumer<byte[]>> subscriptions = new ArrayList<>(this.numSubscriptions);
            final List<Future<QueueConsumer<byte[]>>> subscriptionFutures =
                    new ArrayList<>(this.numSubscriptions);
            log.info().attr("topic", topic).log("Create subscriptions for topic");
            for (int j = 0; j < this.numSubscriptions; j++) {
                String subscriberName = this.subscriptions.get(j);
                // V5 QueueConsumerBuilder has no clone(); build fresh per subscription.
                QueueConsumerBuilder<byte[]> b = client.newQueueConsumer(Schema.bytes())
                        .receiverQueueSize(this.receiverQueueSize)
                        .subscriptionInitialPosition(this.subscriptionInitialPosition)
                        .replicateSubscriptionState(this.replicatedSubscription)
                        .topic(topic)
                        .subscriptionName(subscriberName);
                subscriptionFutures.add(b.subscribeAsync());
            }
            for (Future<QueueConsumer<byte[]>> future : subscriptionFutures) {
                subscriptions.add(future.get());
            }
            consumers.add(subscriptions);
        }
        return consumers;
    }

    private List<Producer<byte[]>> buildProducers(PulsarClient client)
            throws ExecutionException, InterruptedException {

        final List<Future<Producer<byte[]>>> producerFutures = new ArrayList<>();
        for (String topic : this.producerTopic) {
            log.info().attr("topic", topic).log("Create producer for topic");
            ProducerBuilder<byte[]> b = client.newProducer(Schema.bytes())
                    .sendTimeout(Duration.ZERO)
                    .topic(topic);
            producerFutures.add(b.createAsync());
        }
        final List<Producer<byte[]>> producers = new ArrayList<>(producerFutures.size());

        for (Future<Producer<byte[]>> future : producerFutures) {
            producers.add(future.get());
        }
        return  producers;
    }

}
