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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.StreamConsumerBuilder;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.auth.PemFileKeyProvider;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "consume", description = "Test pulsar consumer performance.")
@CustomLog
public class PerformanceConsumer extends PerformanceTopicListArguments{

    /**
     * Subscription type flag values. V5 has no single user-facing SubscriptionType enum
     * (StreamConsumer / QueueConsumer / CheckpointConsumer are separate APIs); we accept
     * the v4 names for back-compat and map them all to {@link QueueConsumer}, which gives
     * Shared work-distribution semantics. {@code Exclusive} / {@code Failover} log a
     * warning at run time — they are not exactly emulated.
     */
    public enum SubscriptionType {
        Exclusive,
        Shared,
        Failover,
        Key_Shared
    }

    /**
     * Which V5 scalable-topic consumer API to drive. {@code Queue} gives unordered,
     * individually-acked work distribution; {@code Stream} gives ordered, cumulatively-acked
     * consumption with broker-coordinated 1:1 segment-to-consumer assignment. Switching to
     * {@code Stream} with more consumers than segments is the handle for exercising the
     * auto-split feature (PIP-483).
     */
    public enum ScalableConsumerType {
        Queue,
        Stream
    }

    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();

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

    private static final long MAX_LATENCY = TimeUnit.DAYS.toMillis(10);
    private static final Recorder recorder = new Recorder(MAX_LATENCY, 5);
    private static final Recorder cumulativeRecorder = new Recorder(MAX_LATENCY, 5);

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

    @Option(names = { "-sct", "--scalable-consumer-type" },
            description = "V5 scalable-topic consumer API to use: Queue (unordered, individual ack) "
                    + "or Stream (ordered, cumulative ack, 1:1 segment assignment). Use Stream with "
                    + "more consumers than segments to drive auto-split (PIP-483).")
    public ScalableConsumerType scalableConsumerType = ScalableConsumerType.Queue;

    @Option(names = { "-sp", "--subscription-position" }, description = "Subscription position")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.LATEST;

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
    private int maxPendingChunkedMessage = 0;

    @Option(names = { "-ac",
            "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
    private boolean autoAckOldestChunkedMessageOnQueueFull = false;

    @Option(names = { "-e",
            "--expire_time_incomplete_chunked_messages" },
            description = "Expire time in ms for incomplete chunk messages")
    private long expireTimeOfIncompleteChunkedMessageMs = 0;

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
    private boolean poolMessages = true;

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

    public PerformanceConsumer() {
        super("consume");
    }


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
        // Reset static counters to avoid stale state from previous runs in the same JVM
        messagesReceived.reset();
        bytesReceived.reset();
        totalMessagesReceived.reset();
        totalBytesReceived.reset();
        totalNumTxnOpenFail.reset();
        totalNumTxnOpenSuccess.reset();
        totalMessageAck.reset();
        totalMessageAckFailed.reset();
        messageAck.reset();
        totalEndTxnOpFailNum.reset();
        totalEndTxnOpSuccessNum.reset();
        numTxnOpSuccess.reset();
        recorder.reset();
        cumulativeRecorder.reset();

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(this)).log("Starting Pulsar performance consumer with config");

        if (this.subscriptionType == SubscriptionType.Exclusive
                || this.subscriptionType == SubscriptionType.Failover) {
            log.warn().attr("type", this.subscriptionType)
                    .log("V5 has no exclusive/failover subscription type. Falling back to QueueConsumer "
                            + "(Shared-style work distribution). Latency/throughput numbers may not be "
                            + "directly comparable with the v4 client.");
        }
        if (this.autoScaledReceiverQueueSize) {
            log.warn("--auto-scaled-receiver-queue-size has no V5 equivalent and will be ignored.");
        }
        if (this.batchIndexAck) {
            log.warn("--batch-index-ack has no V5 equivalent and will be ignored.");
        }
        if (!this.poolMessages) {
            log.info("--pool-messages has no effect on V5 (pooled messages are not exposed).");
        }
        if (this.maxPendingChunkedMessage > 0 || this.expireTimeOfIncompleteChunkedMessageMs > 0
                || this.autoAckOldestChunkedMessageOnQueueFull) {
            log.warn("Chunked-message specific knobs (--max_chunked_msg / "
                    + "--expire_time_incomplete_chunked_messages / --auto_ack_chunk_q_full) "
                    + "have no V5 equivalents and will be ignored.");
        }
        if (this.maxTotalReceiverQueueSizeAcrossPartitions != 50000) {
            log.info("--receiver-queue-size-across-partitions has no V5 equivalent and will be ignored.");
        }

        final RateLimiter limiter = this.rate > 0 ? RateLimiter.create(this.rate) : null;
        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (this.testTime * 1e9);

        PulsarClientBuilder clientBuilder = PerfClientUtils.createV5ClientBuilderFromArguments(this);
        if (this.isEnableTransaction) {
            clientBuilder.transactionPolicy(TransactionPolicy.builder()
                    .timeout(Duration.ofSeconds(this.transactionTimeout))
                    .build());
        }
        PulsarClient pulsarClient = clientBuilder.build();

        AtomicReference<Transaction> atomicReference;
        if (this.isEnableTransaction) {
            atomicReference = new AtomicReference<>(PerfClientUtils.newTransactionWithRetry(pulsarClient));
        } else {
            atomicReference = new AtomicReference<>(null);
        }

        AtomicLong messageAckedCount = new AtomicLong();
        Semaphore messageReceiveLimiter = new Semaphore(this.numMessagesPerTransaction);
        Thread thread = Thread.currentThread();

        final ConsumerEncryptionPolicy encryptionPolicy = buildEncryptionPolicyOrNull();

        List<CompletableFuture<PerfConsumer>> futures = new ArrayList<>();
        for (int i = 0; i < this.numTopics; i++) {
            final TopicName topicName = TopicName.get(this.topics.get(i));

            log.info()
                    .attr("adding", this.numConsumers)
                    .attr("topic", topicName)
                    .attr("consumerType", this.scalableConsumerType)
                    .log("Adding consumers per subscription on topic");

            for (int j = 0; j < this.numSubscriptions; j++) {
                String subscriberName = this.subscriptions.get(j);
                for (int k = 0; k < this.numConsumers; k++) {
                    futures.add(subscribeAsync(pulsarClient, topicName.toString(), subscriberName,
                            encryptionPolicy));
                }
            }
        }
        final List<PerfConsumer> consumers = new ArrayList<>(futures.size());
        for (CompletableFuture<PerfConsumer> future : futures) {
            consumers.add(future.get());
        }

        // V5 has no MessageListener — drive each consumer from a dedicated poll thread that calls
        // receive(timeout) and runs the same per-message handler the v4 listener did. One thread
        // per consumer mirrors the v4 dispatch concurrency closely enough for the perf workload.
        ExecutorService consumerExec = Executors.newCachedThreadPool(
                new DefaultThreadFactory("pulsar-perf-consumer-poll"));
        for (PerfConsumer consumer : consumers) {
            consumerExec.submit(() -> pollLoop(consumer, atomicReference, messageAckedCount,
                    messageReceiveLimiter, limiter, testEndTime, thread, pulsarClient));
        }
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

            if (histogramLogWriter != null) {
                histogramLogWriter.outputIntervalHistogram(reportHistogram);
            }

            reportHistogram.reset();
            oldTime = now;

            if (this.testTime > 0) {
                if (now > testEndTime) {
                    log.info("------------------- DONE -----------------------");
                    PerfClientUtils.exit(0);
                    thread.interrupt();
                }
            }
        }
        // Stop the poll threads before closing the client so receive() does not race with close.
        consumerExec.shutdownNow();
        try {
            if (!consumerExec.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Consumer poll executor did not terminate within timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        PerfClientUtils.closeClient(pulsarClient);
        PerfClientUtils.removeAndRunShutdownHook(shutdownHookThread);
    }

    /**
     * Per-consumer poll loop replacing the v4 {@code MessageListener}. Each consumer gets one
     * dedicated thread that drives {@code receive(timeout)} and runs the same per-message
     * handler the v4 listener did (latency record, rate-limit, ack, transaction commit/rollover).
     */
    private void pollLoop(PerfConsumer consumer,
                          AtomicReference<Transaction> atomicReference,
                          AtomicLong messageAckedCount,
                          Semaphore messageReceiveLimiter,
                          RateLimiter limiter,
                          long testEndTime,
                          Thread mainThread,
                          PulsarClient pulsarClient) {
        while (!Thread.currentThread().isInterrupted()) {
            // Termination conditions that don't depend on having just received a message. With
            // asynchronous transaction commits the final commit can land after the last available
            // message is consumed, so the transaction count must be re-checked on idle receives too;
            // otherwise the consumer waits forever for a message that will never arrive.
            if (this.testTime > 0 && System.nanoTime() > testEndTime) {
                log.info("------------------- DONE -----------------------");
                PerfClientUtils.exit(0);
                mainThread.interrupt();
                return;
            }
            if (this.totalNumTxn > 0
                    && totalEndTxnOpFailNum.sum() + totalEndTxnOpSuccessNum.sum() >= this.totalNumTxn) {
                log.info("------------------- DONE -----------------------");
                PerfClientUtils.exit(0);
                mainThread.interrupt();
                return;
            }

            Message<byte[]> msg;
            try {
                msg = consumer.receive(Duration.ofSeconds(1));
            } catch (Exception e) {
                if (PerfClientUtils.hasInterruptedException(e)) {
                    Thread.currentThread().interrupt();
                    return;
                }
                log.warn().exception(e).log("receive failed; retrying");
                continue;
            }
            if (msg == null) {
                continue;
            }

            messagesReceived.increment();
            bytesReceived.add(msg.size());
            totalMessagesReceived.increment();
            totalBytesReceived.add(msg.size());

            if (this.numMessages > 0 && totalMessagesReceived.sum() >= this.numMessages) {
                log.info("------------------- DONE -----------------------");
                PerfClientUtils.exit(0);
                mainThread.interrupt();
                return;
            }

            if (limiter != null) {
                limiter.acquire();
            }

            long latencyMillis = System.currentTimeMillis() - msg.publishTime().toEpochMilli();
            if (latencyMillis >= 0) {
                if (latencyMillis >= MAX_LATENCY) {
                    latencyMillis = MAX_LATENCY;
                }
                recorder.recordValue(latencyMillis);
                cumulativeRecorder.recordValue(latencyMillis);
            }

            // Ack — V5 acknowledge is synchronous void. Catch any failure into the existing counter.
            if (this.isEnableTransaction) {
                try {
                    messageReceiveLimiter.acquire();
                } catch (InterruptedException e) {
                    log.error().exception(e).log("Got error");
                    Thread.currentThread().interrupt();
                }
                Transaction txn = atomicReference.get();
                try {
                    consumer.ackTxn(msg.id(), txn);
                    totalMessageAck.increment();
                    messageAck.increment();
                } catch (Exception e) {
                    if (PerfClientUtils.hasInterruptedException(e)) {
                        Thread.currentThread().interrupt();
                    } else {
                        log.error().exception(e).log("Ack message failed with exception");
                        totalMessageAckFailed.increment();
                    }
                }
            } else {
                try {
                    consumer.ack(msg.id());
                    totalMessageAck.increment();
                    messageAck.increment();
                } catch (Exception e) {
                    if (PerfClientUtils.hasInterruptedException(e)) {
                        Thread.currentThread().interrupt();
                    } else {
                        log.error().exception(e).log("Ack message failed with exception");
                        totalMessageAckFailed.increment();
                    }
                }
            }

            // Transaction commit / rollover after numMessagesPerTransaction acks.
            if (this.isEnableTransaction
                    && messageAckedCount.incrementAndGet() == this.numMessagesPerTransaction) {
                Transaction transaction = atomicReference.get();
                if (!this.isAbortTransaction) {
                    transaction.async().commit()
                            .thenRun(() -> {
                                log.debug().log("Commit transaction");
                                totalEndTxnOpSuccessNum.increment();
                                numTxnOpSuccess.increment();
                            })
                            .exceptionally(exception -> {
                                if (PerfClientUtils.hasInterruptedException(exception)) {
                                    Thread.currentThread().interrupt();
                                    return null;
                                }
                                log.error().exception(exception).log("Commit transaction failed with exception");
                                totalEndTxnOpFailNum.increment();
                                return null;
                            });
                } else {
                    transaction.async().abort()
                            .thenRun(() -> {
                                log.debug().log("Abort transaction");
                                totalEndTxnOpSuccessNum.increment();
                                numTxnOpSuccess.increment();
                            })
                            .exceptionally(exception -> {
                                if (PerfClientUtils.hasInterruptedException(exception)) {
                                    Thread.currentThread().interrupt();
                                    return null;
                                }
                                log.error().exception(exception)
                                        .log("Abort transaction failed with exception");
                                totalEndTxnOpFailNum.increment();
                                return null;
                            });
                }
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Transaction newTransaction = pulsarClient.newTransaction();
                        atomicReference.compareAndSet(transaction, newTransaction);
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
        }
    }

    /**
     * Minimal common view over the V5 {@link QueueConsumer} / {@link StreamConsumer} APIs so the
     * poll loop is independent of which scalable-topic consumer type was selected. The ack methods
     * map to {@code acknowledge} for Queue and {@code acknowledgeCumulative} for Stream.
     */
    private interface PerfConsumer {
        Message<byte[]> receive(Duration timeout) throws Exception;

        void ack(MessageId messageId) throws Exception;

        void ackTxn(MessageId messageId, Transaction txn) throws Exception;
    }

    private CompletableFuture<PerfConsumer> subscribeAsync(PulsarClient client, String topic,
                                                           String subscription,
                                                           ConsumerEncryptionPolicy encryptionPolicy) {
        if (this.scalableConsumerType == ScalableConsumerType.Stream) {
            // StreamConsumer has no receiverQueueSize knob; the rest carries over. Deliberately
            // do NOT set a consumerName: the controller keys group membership by consumer name,
            // so the V5 client's auto-generated unique name keeps every consumer — within one
            // process and across separate `pulsar-perf consume` invocations — a distinct member.
            // (Setting a deterministic name would make two processes collide and the second be
            // treated as a reconnect of the first.)
            StreamConsumerBuilder<byte[]> b = client.newStreamConsumer(Schema.bytes())
                    .acknowledgmentGroupTime(Duration.ofMillis(this.acknowledgmentsGroupingDelayMillis))
                    .subscriptionInitialPosition(this.subscriptionInitialPosition)
                    .replicateSubscriptionState(this.replicatedSubscription)
                    .topic(topic)
                    .subscriptionName(subscription);
            if (encryptionPolicy != null) {
                b.encryptionPolicy(encryptionPolicy);
            }
            return b.subscribeAsync().thenApply(PerformanceConsumer::wrap);
        }
        QueueConsumerBuilder<byte[]> b = client.newQueueConsumer(Schema.bytes())
                .receiverQueueSize(this.receiverQueueSize)
                .acknowledgmentGroupTime(Duration.ofMillis(this.acknowledgmentsGroupingDelayMillis))
                .subscriptionInitialPosition(this.subscriptionInitialPosition)
                .replicateSubscriptionState(this.replicatedSubscription)
                .topic(topic)
                .subscriptionName(subscription);
        if (encryptionPolicy != null) {
            b.encryptionPolicy(encryptionPolicy);
        }
        return b.subscribeAsync().thenApply(PerformanceConsumer::wrap);
    }

    private ConsumerEncryptionPolicy buildEncryptionPolicyOrNull() {
        if (!isNotBlank(this.encKeyFile)) {
            return null;
        }
        // We do not know the key name from --encryption-key-value-file alone; PemFileKeyProvider
        // expects a name → path mapping. Register the file under the same name the producer side
        // used (defaults to the file path's last component if unset upstream).
        String keyName = Path.of(this.encKeyFile).getFileName().toString();
        PemFileKeyProvider keys = PemFileKeyProvider.builder()
                .privateKey(keyName, Path.of(this.encKeyFile))
                .build();
        return ConsumerEncryptionPolicy.builder()
                .privateKeyProvider(keys)
                .build();
    }

    private static PerfConsumer wrap(QueueConsumer<byte[]> consumer) {
        return new PerfConsumer() {
            @Override
            public Message<byte[]> receive(Duration timeout) throws Exception {
                return consumer.receive(timeout);
            }

            @Override
            public void ack(MessageId messageId) throws Exception {
                consumer.acknowledge(messageId);
            }

            @Override
            public void ackTxn(MessageId messageId, Transaction txn) throws Exception {
                consumer.acknowledge(messageId, txn);
            }
        };
    }

    private static PerfConsumer wrap(StreamConsumer<byte[]> consumer) {
        return new PerfConsumer() {
            @Override
            public Message<byte[]> receive(Duration timeout) throws Exception {
                return consumer.receive(timeout);
            }

            @Override
            public void ack(MessageId messageId) throws Exception {
                consumer.acknowledgeCumulative(messageId);
            }

            @Override
            public void ackTxn(MessageId messageId, Transaction txn) throws Exception {
                consumer.acknowledgeCumulative(messageId, txn);
            }
        };
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

    private static void printAggregatedStats() {
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
