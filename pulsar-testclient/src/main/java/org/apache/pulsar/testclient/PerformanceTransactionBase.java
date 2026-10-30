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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import io.github.merlimat.slog.Logger;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

/**
 * Client-agnostic implementation of the {@code pulsar-perf} transaction benchmark.
 *
 * <p>Each test thread owns a set of producers and consumers and repeats one unit of work: consume
 * and acknowledge {@code -nmc} messages, produce {@code -nmp} messages, then end the transaction and
 * open the next one. That loop, the send/ack latency accounting and the reports live here; the options
 * come from the {@link PerformanceTransaction} command. Concrete subclasses bind the client types:
 * {@link PerformanceTransactionV5} against the V5 client and {@link PerformanceTransactionV4} against
 * the v4 client.
 *
 * @param <ClientT> the client type ({@code PulsarClient} of the respective API generation)
 * @param <ProducerT> the producer handle
 * @param <ConsumerT> the subscribed consumer handle
 * @param <MessageT> the received message type
 * @param <TxnT> the transaction type
 */
public abstract class PerformanceTransactionBase<ClientT, ProducerT, ConsumerT, MessageT, TxnT> {

    /**
     * Logger named after the {@code transaction} command rather than the runner, so that the report
     * lines read the same whichever client runs the benchmark.
     */
    protected final Logger log = Logger.get(PerformanceTransaction.class);

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

    /** Counted down once the run reaches {@code -ntxn} or its test duration, which wakes run() to stop. */
    private final CountDownLatch runEnded = new CountDownLatch(1);
    /** Transactions (with {@code --txn-disable}, tasks) the test threads have claimed against {@code -ntxn}. */
    private final AtomicLong transactionsStarted = new AtomicLong();
    /** Ends of the transactions ended so far that may still be in flight, awaited before the run reports done. */
    private final Queue<CompletableFuture<Void>> pendingTxnEnds = new ConcurrentLinkedQueue<>();

    /** The parsed {@code transaction} command line. */
    protected final PerformanceTransaction arguments;

    protected PerformanceTransactionBase(PerformanceTransaction arguments) {
        this.arguments = arguments;
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
        if (arguments.partitions == null) {
            return;
        }
        final PulsarAdminBuilder adminBuilder = PerfClientUtils
                .createAdminBuilderFromArguments(arguments, arguments.adminURL);

        try (PulsarAdmin adminClient = adminBuilder.build()) {
            for (String topic : arguments.producerTopic) {
                log.info()
                        .attr("topic", topic)
                        .attr("partitions", arguments.partitions)
                        .log("Creating produce partitioned topic with partitions");
                try {
                    adminClient.topics().createPartitionedTopic(topic, arguments.partitions);
                } catch (PulsarAdminException.ConflictException alreadyExists) {
                    log.debug().attr("topic", topic).attr("exists", alreadyExists).log("Topic already exists");
                    PartitionedTopicMetadata partitionedTopicMetadata =
                            adminClient.topics().getPartitionedTopicMetadata(topic);
                    if (partitionedTopicMetadata.partitions != arguments.partitions) {
                        log.error()
                                .attr("topic", topic)
                                .attr("partitions", partitionedTopicMetadata.partitions)
                                .attr("expecting", arguments.partitions)
                                .log("Topic already exists but it has a wrong number of partitions: , expecting");
                        PerfClientUtils.exit(1);
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------

    public void run() throws Exception {

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(arguments))
                .log("Starting Pulsar perf transaction with config");

        final byte[] payloadBytes = new byte[1024];
        Random random = new Random(0);
        for (int i = 0; i < payloadBytes.length; ++i) {
            payloadBytes[i] = (byte) (random.nextInt(26) + 65);
        }

        createTopicsIfNeeded();
        prepareRun();

        ClientT client = createClient();
        try {
            ExecutorService executorService = new ThreadPoolExecutor(arguments.numTestThreads,
                    arguments.numTestThreads,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());

            long startTime = System.nanoTime();
            long testEndTime = startTime + (long) (arguments.testTime * 1e9);
            Thread shutdownHookThread = addShutdownHook(() -> {
                if (!arguments.isDisableTransaction) {
                    printTxnAggregatedThroughput(startTime);
                } else {
                    printAggregatedThroughput(startTime);
                }
                printAggregatedStats();
            });

            // start perf test
            RateLimiter rateLimiter = arguments.openTxnRate > 0
                    ? RateLimiter.create(arguments.openTxnRate)
                    : null;
            for (int i = 0; i < arguments.numTestThreads; i++) {
                executorService.submit(() -> runWorker(client, payloadBytes, executorService, rateLimiter,
                        testEndTime));
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

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (runEnded.await(10, TimeUnit.SECONDS)) {
                        break;
                    }
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
                String label = !arguments.isDisableTransaction
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

            if (runEnded.getCount() == 0) {
                // The exit ends the process, and where it does not (in tests), closing the client
                // would cut off the transaction ends still in flight.
                stopWorkers(executorService);
                PerfClientUtils.awaitTransactionEnds(log, pendingTxnEnds, arguments.transactionTimeout);
                log.info("------------------- DONE -----------------------");
                PerfClientUtils.exit(0);
            }
            PerfClientUtils.removeAndRunShutdownHook(shutdownHookThread);
        } finally {
            closeClient(client);
        }
    }

    /**
     * Stop the test threads once the run has reached its end. Each finishes the transaction it is in
     * and stops before opening another. A thread blocked receiving, such as the standby consumer of a
     * Failover subscription, never gets there, so after {@code --txn-timeout}, by when its transaction
     * would time out on the broker anyway, the threads are interrupted, and each ends the transaction
     * it is in on its way out.
     */
    private void stopWorkers(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(arguments.transactionTimeout, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(arguments.transactionTimeout, TimeUnit.SECONDS)) {
                    log.warn("Test threads did not stop within the transaction timeout");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * One test thread: build its own producers and consumers, then loop over transactions until the
     * run reaches its end or the thread is interrupted. The thread claims each transaction against
     * {@code -ntxn} before running it, and ends the one it stops in on its way out.
     */
    private void runWorker(ClientT client, byte[] payloadBytes, ExecutorService executorService,
                           RateLimiter rateLimiter, long testEndTime) {
        // The producer and consumer clients are built in advance, and then this thread is
        // responsible for the production and consumption tasks of the transaction through the loop.
        // A thread may perform tasks of multiple transactions in a traversing manner.
        List<ProducerT> producers = null;
        List<List<ConsumerT>> consumers = null;
        TxnT transaction = null;
        try {
            producers = buildProducers(client);
            consumers = buildConsumers(client);
            if (!arguments.isDisableTransaction) {
                // Opened along with the producers and consumers, so that failing to open it is
                // fatal as failing to build them is; the later opens are retried.
                transaction = newTransaction(client);
                totalNumTxnOpenTxnSuccess.increment();
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

        while (!Thread.currentThread().isInterrupted() && runEnded.getCount() > 0) {
            if (arguments.testTime > 0 && System.nanoTime() > testEndTime) {
                runEnded.countDown();
                break;
            }
            // Claimed before each transaction runs, so that the test threads together run exactly
            // -ntxn transactions.
            if (arguments.numTransactions > 0
                    && transactionsStarted.incrementAndGet() > arguments.numTransactions) {
                runEnded.countDown();
                break;
            }
            if (!arguments.isDisableTransaction && transaction == null) {
                transaction = openTransaction(client);
                if (transaction == null) {
                    // Interrupted while opening it.
                    break;
                }
            }
            for (List<ConsumerT> subscriptions : consumers) {
                for (ConsumerT consumer : subscriptions) {
                    for (int j = 0; j < arguments.numMessagesReceivedPerTransaction; j++) {
                        MessageT message;
                        try {
                            message = receive(consumer);
                        } catch (Exception e) {
                            if (PerfClientUtils.hasInterruptedException(e)) {
                                Thread.currentThread().interrupt();
                                if (transaction != null) {
                                    // Stopped inside this transaction: end it too rather than leave
                                    // it to time out.
                                    pendingTxnEnds.add(endTransaction(transaction));
                                }
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
                for (int j = 0; j < arguments.numMessagesProducedPerTransaction; j++) {
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
            if (!arguments.isDisableTransaction) {
                pendingTxnEnds.removeIf(CompletableFuture::isDone);
                pendingTxnEnds.add(endTransaction(transaction));
                transaction = null;
            } else {
                totalNumTxnOpenTxnSuccess.increment();
                totalNumEndTxnOpSuccess.increment();
                numTxnOpSuccess.increment();
            }
        }
        if (transaction != null) {
            // The run ended before this thread used its first transaction, as when there are more
            // test threads than -ntxn.
            pendingTxnEnds.add(endTransaction(transaction));
        }
    }

    private void acknowledgeAndRecord(ConsumerT consumer, MessageT message, TxnT transaction) {
        long receiveTime = System.nanoTime();
        // Capture the worker so an interrupt targets it rather than whichever thread happens to run
        // the callback. The V5 ack future is already complete when it is returned, so the callbacks
        // below do run on this thread, but the v4 one completes on a client-internal thread — as do
        // the send and end-transaction futures on both clients, which capture it for the same
        // reason.
        final Thread workerThread = Thread.currentThread();
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
                        workerThread.interrupt();
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
        final Thread workerThread = Thread.currentThread();
        return sendMessage(producer, payloadBytes, transaction).whenComplete((id, ex) -> {
            if (ex == null) {
                long latencyMicros = Math.min(NANOSECONDS.toMicros(
                        System.nanoTime() - sendTime), MAX_LATENCY_MICROS);
                messageSendRecorder.recordValue(latencyMicros);
                messageSendRCumulativeRecorder.recordValue(latencyMicros);
                numMessagesSendSuccess.increment();
            } else {
                if (PerfClientUtils.hasInterruptedException(ex)) {
                    workerThread.interrupt();
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

    /**
     * End the transaction according to {@code -abort}, counting the outcome. The returned stage
     * completes, normally, once the outcome is counted.
     */
    private CompletableFuture<Void> endTransaction(TxnT transaction) {
        final boolean abort = arguments.isAbortTransaction;
        final Thread workerThread = Thread.currentThread();
        CompletableFuture<Void> endFuture = abort ? abortTransaction(transaction) : commitTransaction(transaction);
        return endFuture.thenRun(() -> {
            numTxnOpSuccess.increment();
            totalNumEndTxnOpSuccess.increment();
        }).exceptionally(exception -> {
            if (PerfClientUtils.hasInterruptedException(exception)) {
                workerThread.interrupt();
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

    /** Open a transaction, retrying until it opens, or {@code null} once the thread is interrupted. */
    private TxnT openTransaction(ClientT client) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                TxnT transaction = newTransaction(client);
                totalNumTxnOpenTxnSuccess.increment();
                return transaction;
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
        return null;
    }

    /** The count the summary reports as "transaction open successfully". */
    @VisibleForTesting
    long getTransactionsOpenedSuccessfully() {
        return totalNumTxnOpenTxnSuccess.sum();
    }

    /** The count the summary reports as "transaction end successfully". */
    @VisibleForTesting
    long getTransactionsEndedSuccessfully() {
        return totalNumEndTxnOpSuccess.sum();
    }

    private List<List<ConsumerT>> buildConsumers(ClientT client) throws Exception {
        List<List<ConsumerT>> consumers = new ArrayList<>(arguments.consumerTopic.size());
        for (String topic : arguments.consumerTopic) {
            final List<CompletableFuture<ConsumerT>> subscriptionFutures =
                    new ArrayList<>(arguments.numSubscriptions);
            log.info().attr("topic", topic).log("Create subscriptions for topic");
            for (int j = 0; j < arguments.numSubscriptions; j++) {
                subscriptionFutures.add(subscribeAsync(client, topic, arguments.subscriptions.get(j)));
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
        for (String topic : arguments.producerTopic) {
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
