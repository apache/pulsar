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
import static org.apache.pulsar.testclient.PerfClientUtils.LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS;
import static org.apache.pulsar.testclient.PerfClientUtils.addShutdownHook;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import io.github.merlimat.slog.Logger;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import picocli.CommandLine.Option;

/**
 * Client-agnostic implementation of the {@code pulsar-perf} transaction benchmark.
 *
 * <p>Each test thread owns a set of producers and consumers and repeats one unit of work: consume
 * and acknowledge {@code -nmc} messages, produce {@code -nmp} messages, then end the transaction and
 * open the next one. That loop, the CLI options, the send/ack latency accounting and the reports
 * live here. Concrete subclasses bind the client types: {@link PerformanceTransaction} against the
 * V5 client and {@link PerformanceTransactionV4} against the v4 client.
 *
 * @param <ClientT> the client type ({@code PulsarClient} of the respective API generation)
 * @param <ProducerT> the producer handle
 * @param <ConsumerT> the subscribed consumer handle
 * @param <MessageT> the received message type
 * @param <TxnT> the transaction type
 */
public abstract class PerformanceTransactionBase<ClientT, ProducerT, ConsumerT, MessageT, TxnT>
        extends PerformanceBaseArguments {

    /**
     * Logger named after the <em>concrete</em> command class rather than this base, so that the
     * report lines keep identifying the subcommand that produced them.
     */
    protected final Logger log = Logger.get(getClass());

    /** Same v4-compat subscription-type flag as {@link PerformanceConsumerBase.SubscriptionType}. */
    public enum SubscriptionType {
        Exclusive,
        Shared,
        Failover,
        Key_Shared
    }

    private final LongAdder totalNumEndTxnOpFailed = new LongAdder();
    private final LongAdder totalNumEndTxnOpSuccess = new LongAdder();
    private final LongAdder numTxnOpSuccess = new LongAdder();
    private final LongAdder totalNumTxnOpenTxnFail = new LongAdder();
    private final LongAdder totalNumTxnOpenTxnSuccess = new LongAdder();

    private final LongAdder numMessagesAckFailed = new LongAdder();
    private final LongAdder numMessagesAckSuccess = new LongAdder();
    private final LongAdder numMessagesSendFailed = new LongAdder();
    private final LongAdder numMessagesSendSuccess = new LongAdder();

    // Send and ack latencies are recorded in microseconds. Anything slower than this means the
    // benchmark is broken rather than slow, so values are clamped to keep HdrHistogram in range.
    private static final long MAX_LATENCY_MICROS = TimeUnit.HOURS.toMicros(1);

    private final Recorder messageAckRecorder =
            new Recorder(MAX_LATENCY_MICROS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);
    private final Recorder messageAckCumulativeRecorder =
            new Recorder(MAX_LATENCY_MICROS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);

    private final Recorder messageSendRecorder =
            new Recorder(MAX_LATENCY_MICROS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);
    private final Recorder messageSendRCumulativeRecorder =
            new Recorder(MAX_LATENCY_MICROS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);

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

    @Option(names = {"-time",
            "--test-duration"}, description = "Test duration (in second). 0 means keeping publishing")
    public long testTime = 0;

    @Option(names = {"-ss",
            "--subscriptions"}, description = "A list of subscriptions to consume (for example, sub1,sub2)")
    public List<String> subscriptions = Collections.singletonList("sub");

    @Option(names = {"-ns", "--num-subscriptions"}, description = "Number of subscriptions (per topic)")
    public int numSubscriptions = 1;

    @Option(names = {"-st", "--subscription-type"}, description = "Subscription type")
    public SubscriptionType subscriptionType = SubscriptionType.Shared;

    @Option(names = {"-rs", "--replicated" },
            description = "Whether the subscription status should be replicated")
    protected boolean replicatedSubscription = false;

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

    protected PerformanceTransactionBase(String cmdName) {
        super(cmdName);
    }

    // ------------------------------------------------------------------------------------------
    // Client-specific seams
    // ------------------------------------------------------------------------------------------

    /** Build and connect the client, enabling transactions unless {@code --txn-disable} is set. */
    protected abstract ClientT createClient() throws Exception;

    /** Close a client built by {@link #createClient()}; must tolerate a {@code null} argument. */
    protected abstract void closeClient(ClientT client);

    /** Create one producer on {@code topic}. */
    protected abstract CompletableFuture<ProducerT> createProducerAsync(ClientT client, String topic);

    /** Subscribe one consumer to {@code topic} under {@code subscription}. */
    protected abstract CompletableFuture<ConsumerT> subscribeAsync(ClientT client, String topic,
                                                                   String subscription);

    /** Open a new transaction, honouring {@code --txn-timeout}. */
    protected abstract TxnT newTransaction(ClientT client) throws Exception;

    /** Commit a transaction. */
    protected abstract CompletableFuture<Void> commitTransaction(TxnT transaction);

    /** Abort a transaction. */
    protected abstract CompletableFuture<Void> abortTransaction(TxnT transaction);

    /** Receive the next message, blocking until one arrives. */
    protected abstract MessageT receive(ConsumerT consumer) throws Exception;

    /**
     * Acknowledge one message, individually or under {@code transaction} when it is non-null.
     *
     * <p>The returned future is what the reported ack latency measures. On the v4 client that is a
     * broker round trip; V5's {@code acknowledge} is a synchronous void, so its future is already
     * complete and the measurement is local.
     */
    protected abstract CompletableFuture<Void> acknowledgeAsync(ConsumerT consumer, MessageT msg,
                                                                TxnT transaction);

    /** Send one message, individually or under {@code transaction} when it is non-null. */
    protected abstract CompletableFuture<?> sendMessage(ProducerT producer, byte[] payload, TxnT transaction);

    /** Whether a send failure is just the producer having been closed, which is not counted or logged. */
    protected abstract boolean isAlreadyClosedException(Throwable cause);

    /**
     * Whether the worker awaits every send of a transaction before ending it. See
     * {@link PerformanceProducerBase#awaitSendsBeforeEndingTransaction()} for why the two clients
     * differ.
     */
    protected boolean awaitSendsBeforeEndingTransaction() {
        return true;
    }

    /** Hook for per-client run preparation, e.g. warnings about options with no effect. */
    protected void prepareRun() {
    }

    /**
     * Pre-create the topics the run needs. The default handles {@code --partitions} through the
     * admin API; subclasses may add their own topic kinds before delegating.
     */
    protected void createTopicsIfNeeded() throws Exception {
        if (this.partitions == null) {
            return;
        }
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

    // ------------------------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        super.parseCLI();

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

        createTopicsIfNeeded();
        prepareRun();

        ClientT client = createClient();
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
                executorService.submit(() -> runWorker(client, payloadBytes, executorService, executing,
                        rateLimiter, testEndTime));
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
            closeClient(client);
        }
    }

    /**
     * One test thread: build its own producers and consumers, then loop over transactions until the
     * run is stopped. The loop has no break of its own; it ends through the executor's shutdownNow.
     */
    private void runWorker(ClientT client, byte[] payloadBytes, ExecutorService executorService,
                           AtomicBoolean executing, RateLimiter rateLimiter, long testEndTime) {
        // The producer and consumer clients are built in advance, and then this thread is
        // responsible for the production and consumption tasks of the transaction through the loop.
        // A thread may perform tasks of multiple transactions in a traversing manner.
        List<ProducerT> producers = null;
        List<List<ConsumerT>> consumers = null;
        AtomicReference<TxnT> atomicReference = null;
        try {
            producers = buildProducers(client);
            consumers = buildConsumers(client);
            if (!this.isDisableTransaction) {
                atomicReference = new AtomicReference<>(newTransaction(client));
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
            return;
        }

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
            TxnT transaction = atomicReference.get();
            for (List<ConsumerT> subscriptions : consumers) {
                for (ConsumerT consumer : subscriptions) {
                    for (int j = 0; j < this.numMessagesReceivedPerTransaction; j++) {
                        MessageT message;
                        try {
                            message = receive(consumer);
                        } catch (Exception e) {
                            if (PerfClientUtils.hasInterruptedException(e)) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            log.error().exception(e).log("Receive message failed");
                            executorService.shutdownNow();
                            PerfClientUtils.exit(1);
                            return;
                        }
                        acknowledgeAndRecord(consumer, message, transaction);
                    }
                }
            }

            // Send futures of the in-flight transaction, awaited before it is ended when
            // awaitSendsBeforeEndingTransaction() is on, so the commit never races the sends.
            List<CompletableFuture<?>> pendingSends = new ArrayList<>();
            for (ProducerT producer : producers) {
                for (int j = 0; j < this.numMessagesProducedPerTransaction; j++) {
                    pendingSends.add(sendAndRecord(producer, payloadBytes, transaction));
                }
            }

            if (awaitSendsBeforeEndingTransaction()) {
                // allOf().exceptionally() swallows individual send failures here — they are
                // already counted by sendAndRecord.
                try {
                    CompletableFuture.allOf(pendingSends.toArray(new CompletableFuture[0]))
                            .exceptionally(t -> null)
                            .join();
                } catch (Exception awaitEx) {
                    if (PerfClientUtils.hasInterruptedException(awaitEx)) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            if (rateLimiter != null) {
                rateLimiter.tryAcquire();
            }
            if (!this.isDisableTransaction) {
                endTransaction(transaction);
                openNextTransaction(client, atomicReference, transaction);
            } else {
                totalNumTxnOpenTxnSuccess.increment();
                totalNumEndTxnOpSuccess.increment();
                numTxnOpSuccess.increment();
            }
        }
    }

    private void acknowledgeAndRecord(ConsumerT consumer, MessageT message, TxnT transaction) {
        long receiveTime = System.nanoTime();
        acknowledgeAsync(consumer, message, transaction)
                .thenRun(() -> {
                    long latencyMicros = Math.min(NANOSECONDS.toMicros(
                            System.nanoTime() - receiveTime), MAX_LATENCY_MICROS);
                    messageAckRecorder.recordValue(latencyMicros);
                    messageAckCumulativeRecorder.recordValue(latencyMicros);
                    numMessagesAckSuccess.increment();
                })
                .exceptionally(exception -> {
                    if (PerfClientUtils.hasInterruptedException(exception)) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                    log.error()
                            .exception(exception)
                            .log("Ack message failed with transaction throw exception");
                    numMessagesAckFailed.increment();
                    return null;
                });
    }

    private CompletableFuture<?> sendAndRecord(ProducerT producer, byte[] payloadBytes, TxnT transaction) {
        long sendTime = System.nanoTime();
        return sendMessage(producer, payloadBytes, transaction).whenComplete((id, ex) -> {
            if (ex == null) {
                long latencyMicros = Math.min(NANOSECONDS.toMicros(
                        System.nanoTime() - sendTime), MAX_LATENCY_MICROS);
                messageSendRecorder.recordValue(latencyMicros);
                messageSendRCumulativeRecorder.recordValue(latencyMicros);
                numMessagesSendSuccess.increment();
            } else {
                if (PerfClientUtils.hasInterruptedException(ex)) {
                    Thread.currentThread().interrupt();
                    return;
                }
                // Ignore the exception when the producer is closed
                if (isAlreadyClosedException(ex.getCause())) {
                    return;
                }
                log.error()
                        .exception(ex)
                        .log("Send message failed with exception");
                numMessagesSendFailed.increment();
            }
        });
    }

    /** End the transaction according to {@code -abort}, counting the outcome. */
    private void endTransaction(TxnT transaction) {
        final boolean abort = this.isAbortTransaction;
        CompletableFuture<Void> endFuture = abort ? abortTransaction(transaction) : commitTransaction(transaction);
        endFuture.thenRun(() -> {
            numTxnOpSuccess.increment();
            totalNumEndTxnOpSuccess.increment();
        }).exceptionally(exception -> {
            if (PerfClientUtils.hasInterruptedException(exception)) {
                Thread.currentThread().interrupt();
                return null;
            }
            log.error()
                    .exception(exception)
                    .log(abort ? "Abort transaction failed with exception"
                            : "Commit transaction failed with exception");
            totalNumEndTxnOpFailed.increment();
            return null;
        });
    }

    private void openNextTransaction(ClientT client, AtomicReference<TxnT> atomicReference, TxnT previous) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                TxnT newTransaction = newTransaction(client);
                atomicReference.compareAndSet(previous, newTransaction);
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
    }

    private List<List<ConsumerT>> buildConsumers(ClientT client) throws Exception {
        List<List<ConsumerT>> consumers = new ArrayList<>(this.consumerTopic.size());
        for (String topic : this.consumerTopic) {
            final List<CompletableFuture<ConsumerT>> subscriptionFutures =
                    new ArrayList<>(this.numSubscriptions);
            log.info().attr("topic", topic).log("Create subscriptions for topic");
            for (int j = 0; j < this.numSubscriptions; j++) {
                subscriptionFutures.add(subscribeAsync(client, topic, this.subscriptions.get(j)));
            }
            final List<ConsumerT> subscriptions = new ArrayList<>(subscriptionFutures.size());
            for (CompletableFuture<ConsumerT> future : subscriptionFutures) {
                subscriptions.add(future.get());
            }
            consumers.add(subscriptions);
        }
        return consumers;
    }

    private List<ProducerT> buildProducers(ClientT client) throws Exception {
        final List<CompletableFuture<ProducerT>> producerFutures = new ArrayList<>();
        for (String topic : this.producerTopic) {
            log.info().attr("topic", topic).log("Create producer for topic");
            producerFutures.add(createProducerAsync(client, topic));
        }
        final List<ProducerT> producers = new ArrayList<>(producerFutures.size());
        for (CompletableFuture<ProducerT> future : producerFutures) {
            producers.add(future.get());
        }
        return producers;
    }

    private void printTxnAggregatedThroughput(long start) {
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

    private void printAggregatedThroughput(long start) {
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

    private void printAggregatedStats() {
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
}
