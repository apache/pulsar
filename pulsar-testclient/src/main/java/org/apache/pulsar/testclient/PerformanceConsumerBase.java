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

import static org.apache.pulsar.testclient.PerfClientUtils.LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Client-agnostic implementation of the {@code pulsar-perf} consumer benchmark.
 *
 * <p>The throughput and latency accounting, the subscription fan-out, the transaction lifecycle and
 * the periodic and aggregated reports live here; the options come from the {@link PerformanceConsumer}
 * command. Concrete subclasses bind the client types and implement the seams below:
 * {@link PerformanceConsumerV5} drives the V5
 * Queue/Stream consumers from dedicated poll threads, and {@link PerformanceConsumerV4} drives a
 * v4 {@code Consumer} through a {@code MessageListener}.
 *
 * @param <ClientT> the client type ({@code PulsarClient} of the respective API generation)
 * @param <ConsumerT> the subscribed consumer handle
 * @param <MessageT> the received message type
 * @param <TxnT> the transaction type used when {@code --txn-enable} is set
 */
public abstract class PerformanceConsumerBase<ClientT, ConsumerT, MessageT, TxnT> {

    /**
     * Logger named after the {@code consume} command rather than the runner, so that the report lines
     * read the same whichever client runs the benchmark (the integration tests in {@code PerfToolTest}
     * match on {@code PerformanceConsumer - Aggregated ...}).
     */
    protected final Logger log = Logger.get(PerformanceConsumer.class);

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();

    private final LongAdder totalMessagesReceived = new LongAdder();
    private final LongAdder totalBytesReceived = new LongAdder();

    private final LongAdder totalNumTxnOpenFail = new LongAdder();
    private final LongAdder totalNumTxnOpenSuccess = new LongAdder();

    private final LongAdder totalMessageAck = new LongAdder();
    private final LongAdder totalMessageAckFailed = new LongAdder();
    private final LongAdder messageAck = new LongAdder();

    private final LongAdder totalEndTxnOpFailNum = new LongAdder();
    private final LongAdder totalEndTxnOpSuccessNum = new LongAdder();
    private final LongAdder numTxnOpSuccess = new LongAdder();

    protected static final long MAX_LATENCY_MILLIS = TimeUnit.DAYS.toMillis(10);
    private final Recorder recorder = new Recorder(MAX_LATENCY_MILLIS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);
    private final Recorder cumulativeRecorder =
            new Recorder(MAX_LATENCY_MILLIS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);

    /** Run state shared between {@code run()} and the message handler. */
    private ClientT client;
    private AtomicReference<TxnT> transactionRef;
    private AtomicLong messageAckedCount;
    private Semaphore messageReceiveLimiter;
    private RateLimiter limiter;
    private long testEndTime;
    private Thread mainThread;
    private final AtomicBoolean done = new AtomicBoolean();

    /**
     * Guards the open transaction. Acknowledging under it, and the rollover that follows its last
     * acknowledgement, hold the read lock; ending it when the run stops takes the write lock, so that
     * the end cannot overtake an acknowledgement that is being issued under the transaction.
     */
    private final ReadWriteLock transactionLock = new ReentrantReadWriteLock();
    /** Ends of the transactions ended so far that may still be in flight; see {@link #reportDone()}. */
    private final Queue<CompletableFuture<Void>> pendingTxnEnds = new ConcurrentLinkedQueue<>();

    /** The parsed {@code consume} command line. */
    protected final PerformanceConsumer arguments;

    protected PerformanceConsumerBase(PerformanceConsumer arguments) {
        this.arguments = arguments;
    }

    // ------------------------------------------------------------------------------------------
    // Client-specific seams
    // ------------------------------------------------------------------------------------------

    /** Build and connect the client, enabling transactions when {@code --txn-enable} is set. */
    protected abstract ClientT createClient() throws Exception;

    /** Close a client built by {@link #createClient()}; must tolerate a {@code null} argument. */
    protected abstract void closeClient(ClientT client);

    /** Number of clients used by this command. V4 can use one isolated client per consumer. */
    protected int isolatedClientCount() {
        return 0;
    }

    /** Subscribe one consumer to {@code topic} under {@code subscription}. */
    protected abstract CompletableFuture<ConsumerT> subscribeAsync(ClientT client, String topic,
                                                                   String subscription);

    /**
     * The consumer kind reported on the per-topic "Adding consumers" line. The v4 client subscribes
     * by {@code --subscription-type}; V5 picks a consumer API instead and overrides this.
     */
    protected Object consumerTypeForLog() {
        return arguments.subscriptionType;
    }

    /**
     * Open a new transaction, honouring {@code --txn-timeout}. Used for every rollover, which has
     * its own retry loop and counts each failure, so this must surface a failed open rather than
     * retrying internally.
     */
    protected abstract TxnT newTransaction(ClientT client) throws Exception;

    /**
     * Open the first transaction, before any consumer exists. Separate from
     * {@link #newTransaction} because the V5 client needs to wait out its transaction-coordinator
     * handler's asynchronous connect here, whereas the rollover path must see each failure.
     */
    protected TxnT openFirstTransaction(ClientT client) throws Exception {
        return newTransaction(client);
    }

    /** Commit a transaction. */
    protected abstract CompletableFuture<Void> commitTransaction(TxnT transaction);

    /** Abort a transaction. */
    protected abstract CompletableFuture<Void> abortTransaction(TxnT transaction);

    /** Payload size of a received message, in bytes. */
    protected abstract int messageSize(MessageT msg);

    /** Publish timestamp of a received message, in milliseconds since the epoch. */
    protected abstract long publishTimeMillis(MessageT msg);

    /**
     * Acknowledge one message, individually or under {@code transaction} when it is non-null, and
     * count the outcome with {@link #ackSucceeded()} / {@link #ackFailed(Throwable)}. Doing the
     * counting here rather than in the base lets each client ack in its natural style — the v4
     * client's {@code acknowledgeAsync} completes a future, V5's {@code acknowledge} is a
     * synchronous void.
     */
    protected abstract void acknowledge(ConsumerT consumer, MessageT msg, TxnT transaction);

    /**
     * Start driving the subscribed consumers. A no-op where the client pushes messages itself (the
     * v4 {@code MessageListener}); V5 has no listener, so it starts one poll thread per consumer.
     */
    protected void startConsuming(List<ConsumerT> consumers) throws Exception {
    }

    /** Stop whatever {@link #startConsuming(List)} started, before the client is closed. */
    protected void stopConsuming() {
    }

    /** Release a pooled message once it has been accounted for and acknowledged. */
    protected void releaseMessage(MessageT msg) {
    }

    /** Called for every received message before it is accounted for, with its consumer. */
    protected void onMessageDequeued(ConsumerT consumer) {
    }

    /** Hook for per-client run preparation: option warnings, extra recorders. */
    protected void prepareRun() {
    }

    /** Release resources shared by clients after all consumers have stopped. */
    protected void closeResources() {
    }

    /** Hook for extra lines in the periodic report. */
    protected void reportIntervalExtras(List<ConsumerT> consumers) throws Exception {
    }

    // ------------------------------------------------------------------------------------------

    public void run() throws Exception {

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(arguments))
                .log("Starting Pulsar performance consumer with config");

        prepareRun();

        this.limiter = arguments.rate > 0 ? RateLimiter.create(arguments.rate) : null;
        long startTime = System.nanoTime();
        this.testEndTime = startTime + (long) (arguments.testTime * 1e9);
        this.mainThread = Thread.currentThread();

        int isolatedClientCount = isolatedClientCount();
        List<ClientT> clients = new ArrayList<>(isolatedClientCount > 0 ? isolatedClientCount : 1);
        if (isolatedClientCount > 0) {
            for (int i = 0; i < isolatedClientCount; i++) {
                clients.add(createClient());
            }
        } else {
            clients.add(createClient());
        }
        this.client = clients.get(0);

        if (arguments.isEnableTransaction) {
            this.transactionRef = new AtomicReference<>(openFirstTransaction(client));
            totalNumTxnOpenSuccess.increment();
        } else {
            this.transactionRef = new AtomicReference<>(null);
        }

        this.messageAckedCount = new AtomicLong();
        this.messageReceiveLimiter = new Semaphore(arguments.numMessagesPerTransaction);

        List<CompletableFuture<ConsumerT>> futures = new ArrayList<>();
        int consumerIndex = 0;
        for (int i = 0; i < arguments.numTopics; i++) {
            final TopicName topicName = TopicName.get(arguments.topics.get(i));

            log.info()
                    .attr("adding", arguments.numConsumers)
                    .attr("topic", topicName)
                    .attr("consumerType", consumerTypeForLog())
                    .log("Adding consumers per subscription on topic");

            for (int j = 0; j < arguments.numSubscriptions; j++) {
                String subscription = arguments.subscriptions.get(j);
                for (int k = 0; k < arguments.numConsumers; k++) {
                    ClientT consumerClient = clients.get(consumerIndex++ % clients.size());
                    futures.add(subscribeAsync(consumerClient, topicName.toString(), subscription));
                }
            }
        }
        final List<ConsumerT> consumers = new ArrayList<>(futures.size());
        for (CompletableFuture<ConsumerT> future : futures) {
            consumers.add(future.get());
        }

        startConsuming(consumers);

        log.info()
                .attr("receiving", arguments.numConsumers)
                .attr("subscription", arguments.numTopics)
                .log("Start receiving from consumers per subscription on topics");

        long start = System.nanoTime();

        Thread shutdownHookThread = PerfClientUtils.addShutdownHook(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        });

        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;
        HistogramLogWriter histogramLogWriter = null;

        if (arguments.histogramFile != null) {
            String statsFileName = arguments.histogramFile;
            log.info().attr("stats", statsFileName).log("Dumping latency stats to");

            PrintStream histogramLog = new PrintStream(new FileOutputStream(statsFileName), false);
            histogramLogWriter = new HistogramLogWriter(histogramLog);

            // Some log header bits
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();
        }

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
                log.infof("--- Transaction: %d transaction end successfully"
                                + " --- %d transaction end failed"
                                + " --- %.3f Txn/s --- AckRate: %.3f msg/s",
                        totalTxnOpSuccessNum, totalTxnOpFailNum, rateOpenTxn, rateAck);
            }
            log.infof("Throughput received: %7d msg --- %.3f msg/s --- %.3f Mbit/s"
                            + " --- Latency: mean: %.3f ms - med: %d"
                            + " - 95pct: %d - 99pct: %d"
                            + " - 99.9pct: %d - 99.99pct: %d - Max: %d",
                    total, rate, throughput,
                    reportHistogram.getMean(),
                    reportHistogram.getValueAtPercentile(50),
                    reportHistogram.getValueAtPercentile(95),
                    reportHistogram.getValueAtPercentile(99),
                    reportHistogram.getValueAtPercentile(99.9),
                    reportHistogram.getValueAtPercentile(99.99),
                    reportHistogram.getMaxValue());

            reportIntervalExtras(consumers);

            if (histogramLogWriter != null) {
                histogramLogWriter.outputIntervalHistogram(reportHistogram);
            }

            reportHistogram.reset();
            oldTime = now;

            if (arguments.testTime > 0) {
                if (now > testEndTime) {
                    reportDone();
                }
            }
        }
        // Stop driving the consumers before closing the client so receives do not race with close.
        stopConsuming();
        clients.forEach(this::closeClient);
        closeResources();
        PerfClientUtils.removeAndRunShutdownHook(shutdownHookThread);
    }

    /**
     * Termination conditions that do not depend on having just received a message: the test
     * duration, which must also end a run whose topic has gone quiet, and a run that has already been
     * reported done, possibly by another handler.
     *
     * @return whether the run is done and the caller should stop consuming
     */
    protected final boolean checkDone() {
        if (done.get()) {
            return true;
        }
        if (arguments.testTime > 0 && System.nanoTime() > testEndTime) {
            reportDone();
            return true;
        }
        return false;
    }

    /**
     * Report the run done, once. With transactions, the transaction the run stops in is ended first,
     * and the ends of all of them are awaited: the exit ends the process, and where it does not (in
     * tests), {@link #run()} then closes the clients, either of which would cut them off.
     */
    private void reportDone() {
        if (!done.compareAndSet(false, true)) {
            return;
        }
        if (arguments.isEnableTransaction) {
            endTransactions();
        }
        log.info("------------------- DONE -----------------------");
        PerfClientUtils.exit(0);
        mainThread.interrupt();
    }

    /**
     * End the transaction the run stops in, if any, and wait for the ends of all the transactions the
     * run has ended. Taking the write lock lets the acknowledgements being issued under that
     * transaction go first. Both waits are bounded by {@code --txn-timeout}, and an interrupt ends them.
     */
    private void endTransactions() {
        Lock endLock = transactionLock.writeLock();
        try {
            if (endLock.tryLock(arguments.transactionTimeout, TimeUnit.SECONDS)) {
                try {
                    TxnT transaction = transactionRef.getAndSet(null);
                    if (transaction != null) {
                        pendingTxnEnds.add(endTransaction(transaction));
                    }
                } finally {
                    endLock.unlock();
                }
            } else {
                log.warn()
                        .attr("timeoutSeconds", arguments.transactionTimeout)
                        .log("Gave up ending the open transaction: acknowledgements under it are still in flight");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Wake the handlers waiting for the permits of a transaction that will not be opened. Each
        // hands the permit on when it finds the run over.
        messageReceiveLimiter.release();
        PerfClientUtils.awaitTransactionEnds(log, pendingTxnEnds, arguments.transactionTimeout);
    }

    /**
     * The per-message handler shared by the v4 message listener and the V5 poll loop: accounting,
     * rate limiting, latency recording, acknowledgement and the transaction rollover.
     *
     * @return whether the run is done and the caller should stop consuming
     */
    protected final boolean handleMessage(ConsumerT consumer, MessageT msg) {
        onMessageDequeued(consumer);

        int size = messageSize(msg);
        messagesReceived.increment();
        bytesReceived.add(size);
        totalMessagesReceived.increment();
        totalBytesReceived.add(size);

        if (arguments.numMessages > 0 && totalMessagesReceived.sum() >= arguments.numMessages) {
            reportDone();
            // The run is over, so the message that tripped the limit is not acknowledged, but its
            // buffer is still returned in case messages are pooled.
            releaseMessage(msg);
            return true;
        }

        if (limiter != null) {
            limiter.acquire();
        }

        long latencyMillis = System.currentTimeMillis() - publishTimeMillis(msg);
        if (latencyMillis >= 0) {
            if (latencyMillis >= MAX_LATENCY_MILLIS) {
                latencyMillis = MAX_LATENCY_MILLIS;
            }
            recorder.recordValue(latencyMillis);
            cumulativeRecorder.recordValue(latencyMillis);
        }

        if (arguments.isEnableTransaction) {
            return acknowledgeInTransaction(consumer, msg);
        }
        acknowledge(consumer, msg, null);
        releaseMessage(msg);
        return false;
    }

    /**
     * Acknowledge a message under the open transaction, and roll the transaction over once it holds
     * {@code -nmt} acknowledgements.
     *
     * @return whether the run is done and the caller should stop consuming
     */
    private boolean acknowledgeInTransaction(ConsumerT consumer, MessageT msg) {
        try {
            messageReceiveLimiter.acquire();
        } catch (InterruptedException e) {
            // The consumers are being stopped. Without a permit the acknowledgement has no place in
            // the transaction, so leave the message unacknowledged.
            Thread.currentThread().interrupt();
            releaseMessage(msg);
            return true;
        }
        boolean lastTransactionEnded = false;
        Lock ackLock = transactionLock.readLock();
        ackLock.lock();
        try {
            TxnT transaction = transactionRef.get();
            if (transaction == null) {
                // The run is over and has ended its transactions. Hand the permit on to the next
                // handler waiting for one.
                messageReceiveLimiter.release();
                releaseMessage(msg);
                return true;
            }
            acknowledge(consumer, msg, transaction);
            releaseMessage(msg);
            if (messageAckedCount.incrementAndGet() == arguments.numMessagesPerTransaction) {
                lastTransactionEnded = endAndReopenTransaction();
            }
        } finally {
            ackLock.unlock();
        }
        if (lastTransactionEnded) {
            // Only once the read lock is released: ending the run takes the write lock.
            reportDone();
        }
        return lastTransactionEnded;
    }

    /** Count a successful acknowledgement. Called by {@link #acknowledge}. */
    protected final void ackSucceeded() {
        totalMessageAck.increment();
        messageAck.increment();
    }

    /** Count a failed acknowledgement. Called by {@link #acknowledge}. */
    protected final void ackFailed(Throwable throwable) {
        if (PerfClientUtils.hasInterruptedException(throwable)) {
            Thread.currentThread().interrupt();
            return;
        }
        log.error().exception(throwable).log("Ack message failed with exception");
        totalMessageAckFailed.increment();
    }

    /**
     * End the open transaction according to {@code -abort} and open its replacement, unless it was
     * the last of {@code -ntxn}. Runs with the read lock held, once every acknowledgement under the
     * transaction has been issued; the other handlers wait for the replacement's permits meanwhile.
     *
     * @return whether that was the last transaction of {@code -ntxn}, which ends the run
     */
    private boolean endAndReopenTransaction() {
        // Cleared until the replacement opens, so that a run that stops meanwhile does not end this
        // transaction a second time.
        TxnT transaction = transactionRef.getAndSet(null);
        pendingTxnEnds.removeIf(CompletableFuture::isDone);
        pendingTxnEnds.add(endTransaction(transaction));
        if (arguments.totalNumTxn > 0 && totalNumTxnOpenSuccess.sum() >= arguments.totalNumTxn) {
            // Another transaction would never be used, and would only time out on the broker.
            return true;
        }
        while (!Thread.currentThread().isInterrupted() && !done.get()) {
            try {
                TxnT newTransaction = newTransaction(client);
                transactionRef.set(newTransaction);
                totalNumTxnOpenSuccess.increment();
                messageAckedCount.set(0);
                messageReceiveLimiter.release(arguments.numMessagesPerTransaction);
                break;
            } catch (Exception e) {
                if (PerfClientUtils.hasInterruptedException(e)) {
                    Thread.currentThread().interrupt();
                } else {
                    log.error().exception(e).log("Failed to new transaction with exception");
                    totalNumTxnOpenFail.increment();
                }
            }
        }
        return false;
    }

    /**
     * Commit or abort the transaction according to {@code -abort}, counting the outcome. The returned
     * stage completes, normally, once the outcome is counted.
     */
    private CompletableFuture<Void> endTransaction(TxnT transaction) {
        final boolean abort = arguments.isAbortTransaction;
        CompletableFuture<Void> endFuture = abort ? abortTransaction(transaction) : commitTransaction(transaction);
        return endFuture.thenRun(() -> {
            log.debug().log(abort ? "Abort transaction" : "Commit transaction");
            totalEndTxnOpSuccessNum.increment();
            numTxnOpSuccess.increment();
        }).exceptionally(exception -> {
            if (PerfClientUtils.hasInterruptedException(exception)) {
                Thread.currentThread().interrupt();
                return null;
            }
            log.error().exception(exception)
                    .log(abort ? "Abort transaction failed with exception"
                            : "Commit transaction failed with exception");
            totalEndTxnOpFailNum.increment();
            return null;
        });
    }

    /** The count the summary reports as "transaction open successfully". */
    @VisibleForTesting
    long getTransactionsOpenedSuccessfully() {
        return totalNumTxnOpenSuccess.sum();
    }

    /** The count the summary reports as "transaction end successfully". */
    @VisibleForTesting
    long getTransactionsEndedSuccessfully() {
        return totalEndTxnOpSuccessNum.sum();
    }

    private void printAggregatedThroughput(long start) {
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
            log.infof("-- Transaction: %d transaction end successfully"
                            + " --- %d transaction end failed"
                            + " --- %d transaction open successfully"
                            + " --- %d transaction open failed --- %.3f Txn/s",
                    totalEndTxnSuccess, totalEndTxnFail,
                    numTransactionOpenSuccess, numTransactionOpenFailed, rateOpenTxn);
        }
        log.infof("Aggregated throughput stats --- %d records received"
                        + " --- %.3f msg/s --- %.3f Mbit/s"
                        + " --- AckRate: %.1f msg/s --- ack failed %d msg",
                totalMessagesReceived.sum(), rate, throughput, rateAck, totalnumMessageAckFailed);
    }

    private void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.infof("Aggregated latency stats --- Latency: mean: %.3f ms"
                        + " - med: %d - 95pct: %d - 99pct: %d"
                        + " - 99.9pct: %d - 99.99pct: %d"
                        + " - 99.999pct: %d - Max: %d",
                reportHistogram.getMean(),
                reportHistogram.getValueAtPercentile(50),
                reportHistogram.getValueAtPercentile(95),
                reportHistogram.getValueAtPercentile(99),
                reportHistogram.getValueAtPercentile(99.9),
                reportHistogram.getValueAtPercentile(99.99),
                reportHistogram.getValueAtPercentile(99.999),
                reportHistogram.getMaxValue());
    }
}
