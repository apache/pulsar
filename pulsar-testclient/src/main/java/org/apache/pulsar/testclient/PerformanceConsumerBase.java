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
import com.google.common.util.concurrent.RateLimiter;
import io.github.merlimat.slog.Logger;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.common.naming.TopicName;
import picocli.CommandLine.Option;

/**
 * Client-agnostic implementation of the {@code pulsar-perf} consumer benchmark.
 *
 * <p>The CLI options, the throughput and latency accounting, the subscription fan-out, the
 * transaction lifecycle and the periodic and aggregated reports live here. Concrete subclasses
 * bind the client types and implement the seams below: {@link PerformanceConsumer} drives the V5
 * Queue/Stream consumers from dedicated poll threads, and {@link PerformanceConsumerV4} drives a
 * v4 {@code Consumer} through a {@code MessageListener}.
 *
 * @param <ClientT> the client type ({@code PulsarClient} of the respective API generation)
 * @param <ConsumerT> the subscribed consumer handle
 * @param <MessageT> the received message type
 * @param <TxnT> the transaction type used when {@code --txn-enable} is set
 */
public abstract class PerformanceConsumerBase<ClientT, ConsumerT, MessageT, TxnT>
        extends PerformanceTopicListArguments {

    /**
     * Logger named after the <em>concrete</em> command class rather than this base, so that the
     * report lines keep identifying the subcommand that produced them (the integration tests in
     * {@code PerfToolTest} match on {@code PerformanceConsumer - Aggregated ...}).
     */
    protected final Logger log = Logger.get(getClass());

    /**
     * Subscription type flag values, shared by both commands so the CLI surface does not depend on
     * which client is driving. The names are the v4 ones: {@link PerformanceConsumerV4} maps them
     * straight onto {@code org.apache.pulsar.client.api.SubscriptionType}, while V5 has no single
     * user-facing subscription-type enum (StreamConsumer / QueueConsumer / CheckpointConsumer are
     * separate APIs) and maps them all to a QueueConsumer.
     */
    public enum SubscriptionType {
        Exclusive,
        Shared,
        Failover,
        Key_Shared
    }

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

    @Option(names = { "-n", "--num-consumers" }, description = "Number of consumers (per subscription), only "
            + "one consumer is allowed when subscriptionType is Exclusive",
            converter = PositiveNumberParameterConvert.class
    )
    public int numConsumers = 1;

    @Option(names = { "-ns", "--num-subscriptions" }, description = "Number of subscriptions (per topic)",
            converter = PositiveNumberParameterConvert.class
    )
    public int numSubscriptions = 1;

    @Option(names = { "-s", "--subscriber-name" }, description = "Subscriber name prefix", hidden = true)
    public String subscriberName;

    @Option(names = { "-ss", "--subscriptions" },
            description = "A list of subscriptions to consume (for example, sub1,sub2)")
    public List<String> subscriptions = Collections.singletonList("sub");

    @Option(names = { "-st", "--subscription-type" }, description = "Subscription type")
    public SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Option(names = { "-r", "--rate" }, description = "Simulate a slow message consumer (rate in msg/s)")
    public double rate = 0;

    @Option(names = { "-q", "--receiver-queue-size" }, description = "Size of the receiver queue")
    public int receiverQueueSize = 1000;

    @Option(names = { "-p", "--receiver-queue-size-across-partitions" },
            description = "Max total size of the receiver queue across partitions")
    public int maxTotalReceiverQueueSizeAcrossPartitions = 50000;

    @Option(names = {"-aq", "--auto-scaled-receiver-queue-size"},
            description = "Enable autoScaledReceiverQueueSize")
    public boolean autoScaledReceiverQueueSize = false;

    @Option(names = {"-rs", "--replicated" },
            description = "Whether the subscription status should be replicated")
    public boolean replicatedSubscription = false;

    @Option(names = { "--acks-delay-millis" }, description = "Acknowledgements grouping delay in millis")
    public int acknowledgmentsGroupingDelayMillis = 100;

    @Option(names = {"-m",
            "--num-messages"},
            description = "Number of messages to consume in total. If <= 0, it will keep consuming")
    public long numMessages = 0;

    @Option(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
    protected int maxPendingChunkedMessage = 0;

    @Option(names = { "-ac",
            "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
    protected boolean autoAckOldestChunkedMessageOnQueueFull = false;

    @Option(names = { "-e",
            "--expire_time_incomplete_chunked_messages" },
            description = "Expire time in ms for incomplete chunk messages")
    protected long expireTimeOfIncompleteChunkedMessageMs = 0;

    @Option(names = { "-v",
            "--encryption-key-value-file" },
            description = "The file which contains the private key to decrypt payload")
    public String encKeyFile = null;

    @Option(names = { "-time",
            "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep consuming")
    public long testTime = 0;

    @Option(names = {"--batch-index-ack" }, description = "Enable or disable the batch index acknowledgment")
    public boolean batchIndexAck = false;

    @Option(names = { "-pm", "--pool-messages" }, description = "Use the pooled message", arity = "1")
    protected boolean poolMessages = true;

    @Option(names = {"-tto", "--txn-timeout"},  description = "Set the time value of transaction timeout,"
            + " and the time unit is second. (After --txn-enable setting to true, --txn-timeout takes effect)")
    public long transactionTimeout = 10;

    @Option(names = {"-nmt", "--numMessage-perTransaction"},
            description = "The number of messages acknowledged by a transaction. "
                    + "(After --txn-enable setting to true, -numMessage-perTransaction takes effect")
    public int numMessagesPerTransaction = 50;

    @Option(names = {"-txn", "--txn-enable"}, description = "Enable or disable the transaction")
    public boolean isEnableTransaction = false;

    @Option(names = {"-ntxn"}, description = "The number of opened transactions, 0 means keeping open."
            + "(After --txn-enable setting to true, -ntxn takes effect.)")
    public long totalNumTxn = 0;

    @Option(names = {"-abort"}, description = "Abort the transaction. (After --txn-enable "
            + "setting to true, -abort takes effect)")
    public boolean isAbortTransaction = false;

    @Option(names = { "--histogram-file" }, description = "HdrHistogram output file")
    public String histogramFile = null;

    protected PerformanceConsumerBase(String cmdName) {
        super(cmdName);
    }

    // ------------------------------------------------------------------------------------------
    // Client-specific seams
    // ------------------------------------------------------------------------------------------

    /** Build and connect the client, enabling transactions when {@code --txn-enable} is set. */
    protected abstract ClientT createClient() throws Exception;

    /** Close a client built by {@link #createClient()}; must tolerate a {@code null} argument. */
    protected abstract void closeClient(ClientT client);

    /** Subscribe one consumer to {@code topic} under {@code subscription}. */
    protected abstract CompletableFuture<ConsumerT> subscribeAsync(ClientT client, String topic,
                                                                   String subscription);

    /**
     * The consumer kind reported on the per-topic "Adding consumers" line. The v4 client subscribes
     * by {@code --subscription-type}; V5 picks a consumer API instead and overrides this.
     */
    protected Object consumerTypeForLog() {
        return this.subscriptionType;
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

    /** Hook for extra lines in the periodic report. */
    protected void reportIntervalExtras(List<ConsumerT> consumers) throws Exception {
    }

    // ------------------------------------------------------------------------------------------

    @Override
    public void validate() throws Exception {
        super.validate();
        if (subscriptionType == SubscriptionType.Exclusive && numConsumers > 1) {
            throw new Exception("Only one consumer is allowed when subscriptionType is Exclusive");
        }

        if (subscriptions != null && subscriptions.size() != numSubscriptions) {
            // keep compatibility with the previous version
            if (subscriptions.size() == 1) {
                if (subscriberName == null) {
                    subscriberName = subscriptions.get(0);
                }
                List<String> defaultSubscriptions = new ArrayList<>();
                for (int i = 0; i < numSubscriptions; i++) {
                    defaultSubscriptions.add(String.format("%s-%d", subscriberName, i));
                }
                subscriptions = defaultSubscriptions;
            } else {
                throw new Exception("The size of subscriptions list should be equal to --num-subscriptions");
            }
        }
    }

    @Override
    public void run() throws Exception {

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(this)).log("Starting Pulsar performance consumer with config");

        prepareRun();

        this.limiter = this.rate > 0 ? RateLimiter.create(this.rate) : null;
        long startTime = System.nanoTime();
        this.testEndTime = startTime + (long) (this.testTime * 1e9);
        this.mainThread = Thread.currentThread();

        this.client = createClient();

        if (this.isEnableTransaction) {
            this.transactionRef = new AtomicReference<>(openFirstTransaction(client));
        } else {
            this.transactionRef = new AtomicReference<>(null);
        }

        this.messageAckedCount = new AtomicLong();
        this.messageReceiveLimiter = new Semaphore(this.numMessagesPerTransaction);

        List<CompletableFuture<ConsumerT>> futures = new ArrayList<>();
        for (int i = 0; i < this.numTopics; i++) {
            final TopicName topicName = TopicName.get(this.topics.get(i));

            log.info()
                    .attr("adding", this.numConsumers)
                    .attr("topic", topicName)
                    .attr("consumerType", consumerTypeForLog())
                    .log("Adding consumers per subscription on topic");

            for (int j = 0; j < this.numSubscriptions; j++) {
                String subscription = this.subscriptions.get(j);
                for (int k = 0; k < this.numConsumers; k++) {
                    futures.add(subscribeAsync(client, topicName.toString(), subscription));
                }
            }
        }
        final List<ConsumerT> consumers = new ArrayList<>(futures.size());
        for (CompletableFuture<ConsumerT> future : futures) {
            consumers.add(future.get());
        }

        startConsuming(consumers);

        log.info()
                .attr("receiving", this.numConsumers)
                .attr("subscription", this.numTopics)
                .log("Start receiving from consumers per subscription on topics");

        long start = System.nanoTime();

        Thread shutdownHookThread = PerfClientUtils.addShutdownHook(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        });

        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;
        HistogramLogWriter histogramLogWriter = null;

        if (this.histogramFile != null) {
            String statsFileName = this.histogramFile;
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

            if (this.isEnableTransaction) {
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

            if (this.testTime > 0) {
                if (now > testEndTime) {
                    log.info("------------------- DONE -----------------------");
                    PerfClientUtils.exit(0);
                    mainThread.interrupt();
                }
            }
        }
        // Stop driving the consumers before closing the client so receives do not race with close.
        stopConsuming();
        closeClient(client);
        PerfClientUtils.removeAndRunShutdownHook(shutdownHookThread);
    }

    /**
     * Termination conditions that do not depend on having just received a message. With
     * asynchronous transaction commits the final commit can land after the last available message is
     * consumed, so the transaction count must be re-checked on idle receives too; otherwise the
     * consumer waits forever for a message that will never arrive.
     *
     * @return whether the run is done and the caller should stop consuming
     */
    protected final boolean checkDone() {
        if (this.testTime > 0 && System.nanoTime() > testEndTime) {
            reportDone();
            return true;
        }
        if (this.totalNumTxn > 0
                && totalEndTxnOpFailNum.sum() + totalEndTxnOpSuccessNum.sum() >= this.totalNumTxn) {
            reportDone();
            return true;
        }
        return false;
    }

    private void reportDone() {
        log.info("------------------- DONE -----------------------");
        PerfClientUtils.exit(0);
        mainThread.interrupt();
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

        if (this.numMessages > 0 && totalMessagesReceived.sum() >= this.numMessages) {
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

        if (this.isEnableTransaction) {
            try {
                messageReceiveLimiter.acquire();
            } catch (InterruptedException e) {
                log.error().exception(e).log("Got error");
                Thread.currentThread().interrupt();
            }
            acknowledge(consumer, msg, transactionRef.get());
        } else {
            acknowledge(consumer, msg, null);
        }

        releaseMessage(msg);

        if (this.isEnableTransaction
                && messageAckedCount.incrementAndGet() == this.numMessagesPerTransaction) {
            endAndReopenTransaction();
        }
        return false;
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

    /** End the in-flight transaction according to {@code -abort} and open its replacement. */
    private void endAndReopenTransaction() {
        final TxnT transaction = transactionRef.get();
        final boolean abort = this.isAbortTransaction;
        CompletableFuture<Void> endFuture = abort ? abortTransaction(transaction) : commitTransaction(transaction);
        endFuture.thenRun(() -> {
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

        while (!Thread.currentThread().isInterrupted()) {
            try {
                TxnT newTransaction = newTransaction(client);
                transactionRef.compareAndSet(transaction, newTransaction);
                totalNumTxnOpenSuccess.increment();
                messageAckedCount.set(0);
                messageReceiveLimiter.release(this.numMessagesPerTransaction);
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
        if (this.isEnableTransaction) {
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
