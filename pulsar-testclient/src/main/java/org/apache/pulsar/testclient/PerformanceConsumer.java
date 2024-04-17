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
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "consume", description = "Test pulsar consumer performance.")
public class PerformanceConsumer extends PerformanceTopicListArguments{
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

    @Option(names = { "-sp", "--subscription-position" }, description = "Subscription position")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

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
        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar performance consumer with config: {}", w.writeValueAsString(this));

        final Recorder qRecorder = this.autoScaledReceiverQueueSize
                ? new Recorder(this.receiverQueueSize, 5) : null;
        final RateLimiter limiter = this.rate > 0 ? RateLimiter.create(this.rate) : null;
        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (this.testTime * 1e9);

        ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(this)
                .enableTransaction(this.isEnableTransaction);

        PulsarClient pulsarClient = clientBuilder.build();

        AtomicReference<Transaction> atomicReference;
        if (this.isEnableTransaction) {
            atomicReference = new AtomicReference<>(pulsarClient.newTransaction()
                    .withTransactionTimeout(this.transactionTimeout, TimeUnit.SECONDS).build().get());
        } else {
            atomicReference = new AtomicReference<>(null);
        }

        AtomicLong messageAckedCount = new AtomicLong();
        Semaphore messageReceiveLimiter = new Semaphore(this.numMessagesPerTransaction);
        Thread thread = Thread.currentThread();
        MessageListener<ByteBuffer> listener = (consumer, msg) -> {
            if (this.testTime > 0) {
                if (System.nanoTime() > testEndTime) {
                    log.info("------------------- DONE -----------------------");
                    PerfClientUtils.exit(0);
                    thread.interrupt();
                }
            }
            if (this.totalNumTxn > 0) {
                if (totalEndTxnOpFailNum.sum() + totalEndTxnOpSuccessNum.sum() >= this.totalNumTxn) {
                    log.info("------------------- DONE -----------------------");
                    PerfClientUtils.exit(0);
                    thread.interrupt();
                }
            }
            if (qRecorder != null) {
                qRecorder.recordValue(((ConsumerBase<?>) consumer).getTotalIncomingMessages());
            }
            messagesReceived.increment();
            bytesReceived.add(msg.size());

            totalMessagesReceived.increment();
            totalBytesReceived.add(msg.size());

            if (this.numMessages > 0 && totalMessagesReceived.sum() >= this.numMessages) {
                log.info("------------------- DONE -----------------------");
                PerfClientUtils.exit(0);
                thread.interrupt();
            }

            if (limiter != null) {
                limiter.acquire();
            }

            long latencyMillis = System.currentTimeMillis() - msg.getPublishTime();
            if (latencyMillis >= 0) {
                if (latencyMillis >= MAX_LATENCY) {
                    latencyMillis = MAX_LATENCY;
                }
                recorder.recordValue(latencyMillis);
                cumulativeRecorder.recordValue(latencyMillis);
            }
            if (this.isEnableTransaction) {
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
            if (this.poolMessages) {
                msg.release();
            }
            if (this.isEnableTransaction
                    && messageAckedCount.incrementAndGet() == this.numMessagesPerTransaction) {
                Transaction transaction = atomicReference.get();
                if (!this.isAbortTransaction) {
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
                                .withTransactionTimeout(this.transactionTimeout, TimeUnit.SECONDS)
                                .build().get();
                        atomicReference.compareAndSet(transaction, newTransaction);
                        totalNumTxnOpenSuccess.increment();
                        messageAckedCount.set(0);
                        messageReceiveLimiter.release(this.numMessagesPerTransaction);
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
                .receiverQueueSize(this.receiverQueueSize) //
                .maxTotalReceiverQueueSizeAcrossPartitions(this.maxTotalReceiverQueueSizeAcrossPartitions)
                .acknowledgmentGroupTime(this.acknowledgmentsGroupingDelayMillis, TimeUnit.MILLISECONDS) //
                .subscriptionType(this.subscriptionType)
                .subscriptionInitialPosition(this.subscriptionInitialPosition)
                .autoAckOldestChunkedMessageOnQueueFull(this.autoAckOldestChunkedMessageOnQueueFull)
                .enableBatchIndexAcknowledgment(this.batchIndexAck)
                .poolMessages(this.poolMessages)
                .replicateSubscriptionState(this.replicatedSubscription)
                .autoScaledReceiverQueueSizeEnabled(this.autoScaledReceiverQueueSize);
        if (this.maxPendingChunkedMessage > 0) {
            consumerBuilder.maxPendingChunkedMessage(this.maxPendingChunkedMessage);
        }
        if (this.expireTimeOfIncompleteChunkedMessageMs > 0) {
            consumerBuilder.expireTimeOfIncompleteChunkedMessage(this.expireTimeOfIncompleteChunkedMessageMs,
                    TimeUnit.MILLISECONDS);
        }

        if (isNotBlank(this.encKeyFile)) {
            consumerBuilder.defaultCryptoKeyReader(this.encKeyFile);
        }

        for (int i = 0; i < this.numTopics; i++) {
            final TopicName topicName = TopicName.get(this.topics.get(i));

            log.info("Adding {} consumers per subscription on topic {}", this.numConsumers, topicName);

            for (int j = 0; j < this.numSubscriptions; j++) {
                String subscriberName = this.subscriptions.get(j);
                for (int k = 0; k < this.numConsumers; k++) {
                    futures.add(consumerBuilder.clone().topic(topicName.toString()).subscriptionName(subscriberName)
                            .subscribeAsync());
                }
            }
        }
        for (Future<Consumer<ByteBuffer>> future : futures) {
            future.get();
        }
        log.info("Start receiving from {} consumers per subscription on {} topics", this.numConsumers,
                this.numTopics);

        long start = System.nanoTime();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));


        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;
        Histogram qHistogram = null;
        HistogramLogWriter histogramLogWriter = null;

        if (this.histogramFile != null) {
            String statsFileName = this.histogramFile;
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

            if (this.isEnableTransaction) {
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

            if (this.autoScaledReceiverQueueSize && log.isDebugEnabled() && qRecorder != null) {
                qHistogram = qRecorder.getIntervalHistogram(qHistogram);
                log.debug("ReceiverQueueUsage: cnt={},mean={}, min={},max={},25pct={},50pct={},75pct={}",
                        qHistogram.getTotalCount(), dec.format(qHistogram.getMean()),
                        qHistogram.getMinValue(), qHistogram.getMaxValue(),
                        qHistogram.getValueAtPercentile(25),
                        qHistogram.getValueAtPercentile(50),
                        qHistogram.getValueAtPercentile(75)
                );
                qHistogram.reset();
                for (Future<Consumer<ByteBuffer>> future : futures) {
                    ConsumerBase<?> consumerBase = (ConsumerBase<?>) future.get();
                    log.debug("[{}] CurrentReceiverQueueSize={}", consumerBase.getConsumerName(),
                            consumerBase.getCurrentReceiverQueueSize());
                    if (consumerBase instanceof MultiTopicsConsumerImpl) {
                        for (ConsumerImpl<?> consumer : ((MultiTopicsConsumerImpl<?>) consumerBase).getConsumers()) {
                            log.debug("[{}] SubConsumer.CurrentReceiverQueueSize={}", consumer.getConsumerName(),
                                    consumer.getCurrentReceiverQueueSize());
                        }
                    }
                }
            }
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

        pulsarClient.close();
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
